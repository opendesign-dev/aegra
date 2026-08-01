"""A2A endpoint — expose assistants as Agent2Agent agents.

Mirrors the LangSmith Agent Server contract: ``POST /a2a/{assistant_id}`` for
the JSON-RPC methods and ``GET /.well-known/agent-card.json?assistant_id=`` for
discovery.

Wire shapes come from ``a2a.compat.v0_3`` — the A2A 0.3 models the SDK keeps
alongside its 1.0 protobuf types. That is the version LangSmith speaks
(``kind``-tagged parts, ``url`` on the card); the 1.0 shapes are not
interchangeable with it.

Identifier mapping, per the A2A contract:

===========  ==========  ==========================================
A2A          Aegra       Note
===========  ==========  ==========================================
`contextId`  `thread_id` conversation continuity across turns
`taskId`     `run_id`    one request; a new turn starts a new task
===========  ==========  ==========================================
"""

from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import structlog
from a2a.compat.v0_3 import types as a2a
from fastapi import HTTPException

from aegra_api.models import Assistant, RunCreate, User
from aegra_api.services.broker import broker_manager
from aegra_api.services.interop import RunResult, execute_and_wait, prepare_interop_run, read_run_result
from aegra_api.services.run_preparation import _resolve_content_text

logger = structlog.getLogger(__name__)

PROTOCOL_VERSION = "0.3.0"
AGENT_CARD_PATH = "/.well-known/agent-card.json"
DEFAULT_MODES = ["text/plain"]

# Aegra run status -> A2A task state. ``interrupted`` is split by whether the
# run left an interrupt behind: a HITL pause is awaiting input, anything else
# reaching this status was cancelled.
_TERMINAL_STATES = {"success": a2a.TaskState.completed, "error": a2a.TaskState.failed}


def _text_of(message: a2a.Message) -> str:
    """Concatenate the text parts of an A2A message."""
    chunks = [part.root.text for part in message.parts if isinstance(part.root, a2a.TextPart)]
    return "\n".join(chunk for chunk in chunks if chunk)


def message_to_input(message: a2a.Message) -> dict[str, Any]:
    """Turn an A2A message into graph input.

    A2A text parts only carry a conversation turn, so the graph must accept a
    ``messages`` key — the same requirement the LangSmith A2A endpoint places on
    deployed agents. Non-text parts ride along under ``data`` for graphs that
    read them.
    """
    payload: dict[str, Any] = {"messages": [{"role": "user", "content": _text_of(message)}]}

    data_parts = [part.root.data for part in message.parts if isinstance(part.root, a2a.DataPart)]
    if data_parts:
        payload["data"] = data_parts if len(data_parts) > 1 else data_parts[0]
    return payload


def output_text(output: dict[str, Any]) -> str:
    """Extract the agent's reply from a graph's final state.

    Reads the last message regardless of role: a graph whose final message is a
    tool result still owes the caller that text, and returning nothing would
    make a successful task look empty.
    """
    messages = output.get("messages")
    if not isinstance(messages, list) or not messages:
        return ""

    last = messages[-1]
    content = last.get("content") if isinstance(last, dict) else getattr(last, "content", None)
    return _resolve_content_text(content)


def _artifact(text: str) -> a2a.Artifact:
    return a2a.Artifact(artifact_id=str(uuid4()), parts=[a2a.Part(root=a2a.TextPart(text=text))])


def _agent_message(text: str, context_id: str, task_id: str) -> a2a.Message:
    return a2a.Message(
        message_id=str(uuid4()),
        role=a2a.Role.agent,
        parts=[a2a.Part(root=a2a.TextPart(text=text))],
        context_id=context_id,
        task_id=task_id,
    )


def task_state_for(status: str, output: dict[str, Any]) -> a2a.TaskState:
    """Map a run's terminal status onto an A2A task state."""
    if status in _TERMINAL_STATES:
        return _TERMINAL_STATES[status]
    if status != "interrupted":
        return a2a.TaskState.working
    return a2a.TaskState.input_required if "__interrupt__" in output else a2a.TaskState.canceled


def build_task(
    *,
    task_id: str,
    context_id: str,
    state: a2a.TaskState,
    text: str = "",
    error: str | None = None,
) -> a2a.Task:
    """Assemble a Task, attaching the reply as an artifact or the failure as a status message."""
    status = a2a.TaskStatus(state=state)
    if error:
        status.message = _agent_message(error, context_id, task_id)

    artifacts = [_artifact(text)] if text else None
    return a2a.Task(id=task_id, context_id=context_id, status=status, artifacts=artifacts)


def _task_from(result: RunResult, *, context_id: str) -> a2a.Task:
    """Render a run's persisted state as an A2A Task."""
    return build_task(
        task_id=result.run_id,
        context_id=context_id,
        state=task_state_for(result.status, result.output),
        text=output_text(result.output),
        error=None if result.succeeded else result.error,
    )


def build_agent_card(assistant: Assistant, base_url: str) -> a2a.AgentCard:
    """Describe one assistant as an A2A agent.

    A2A requires at least one skill; an assistant is a single capability, so it
    maps to exactly one skill carrying its own id.
    """
    description = assistant.description or f"Aegra assistant '{assistant.name}'."
    return a2a.AgentCard(
        protocol_version=PROTOCOL_VERSION,
        name=assistant.name,
        description=description,
        url=f"{base_url}/a2a/{assistant.assistant_id}",
        preferred_transport=a2a.TransportProtocol.jsonrpc,
        version=str(assistant.version),
        capabilities=a2a.AgentCapabilities(streaming=True, push_notifications=False),
        default_input_modes=DEFAULT_MODES,
        default_output_modes=DEFAULT_MODES,
        skills=[
            a2a.AgentSkill(
                id=assistant.assistant_id,
                name=assistant.name,
                description=description,
                tags=[assistant.graph_id],
                input_modes=DEFAULT_MODES,
                output_modes=DEFAULT_MODES,
            )
        ],
    )


def _run_create(assistant_id: str, message: a2a.Message, *, stream: bool) -> RunCreate:
    return RunCreate(
        assistant_id=assistant_id,
        input=message_to_input(message),
        stream_mode=["values"] if stream else None,
        metadata={"a2a_message_id": message.message_id},
    )


async def send_message(assistant_id: str, params: a2a.MessageSendParams, user: User) -> a2a.Task:
    """Handle ``message/send``: run the assistant to completion, return the Task."""
    context_id = params.message.context_id or str(uuid4())
    result = await execute_and_wait(context_id, _run_create(assistant_id, params.message, stream=False), user)
    return _task_from(result, context_id=context_id)


async def stream_message(
    assistant_id: str,
    params: a2a.MessageSendParams,
    user: User,
) -> AsyncIterator[a2a.Task | a2a.TaskStatusUpdateEvent | a2a.TaskArtifactUpdateEvent]:
    """Handle ``message/stream``: emit the Task, then progress, then a final status.

    The A2A contract requires the last event to carry ``final=True``, so the
    terminal status is emitted from the persisted run rather than from the
    broker's end event — a client that disconnected the broker mid-run would
    otherwise never see a closing state.
    """
    context_id = params.message.context_id or str(uuid4())
    run_id = await prepare_interop_run(context_id, _run_create(assistant_id, params.message, stream=True), user)

    yield build_task(task_id=run_id, context_id=context_id, state=a2a.TaskState.submitted)
    yield a2a.TaskStatusUpdateEvent(
        task_id=run_id,
        context_id=context_id,
        status=a2a.TaskStatus(state=a2a.TaskState.working),
        final=False,
    )

    broker = broker_manager.get_or_create_broker(run_id)
    last_text = ""
    async for _event_id, payload in broker.aiter():
        if not isinstance(payload, tuple) or len(payload) != 2:
            continue
        kind, data = payload
        if kind == "end":
            break
        if kind != "values" or not isinstance(data, dict):
            continue
        text = output_text(data)
        if text and text != last_text:
            last_text = text
            yield a2a.TaskArtifactUpdateEvent(
                task_id=run_id,
                context_id=context_id,
                artifact=_artifact(text),
                append=False,
                last_chunk=False,
            )

    result = await read_run_result(run_id, user.identity)
    state = task_state_for(result.status, result.output) if result else a2a.TaskState.failed
    final_status = a2a.TaskStatus(state=state)
    error = result.error if result else "Run disappeared before its result could be read"
    if state is a2a.TaskState.failed and error:
        final_status.message = _agent_message(error, context_id, run_id)

    yield a2a.TaskStatusUpdateEvent(task_id=run_id, context_id=context_id, status=final_status, final=True)


async def get_task(task_id: str, user: User) -> a2a.Task:
    """Handle ``tasks/get``: rebuild the Task from the persisted run."""
    result = await read_run_result(task_id, user.identity)
    if result is None:
        raise HTTPException(404, f"Task '{task_id}' not found")

    return _task_from(result, context_id=result.thread_id)
