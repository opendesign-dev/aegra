"""E2E tests for the A2A endpoint against a running server.

Speaks raw JSON-RPC over HTTP — the same wire traffic another A2A agent would
send — rather than going through a client library.
"""

import json
import uuid
from typing import Any

import pytest
from httpx import AsyncClient

from aegra_api.settings import settings
from tests.e2e._utils import elog, get_e2e_client


async def _seed_assistant(name: str) -> str:
    """Create (or reuse) an assistant for this test.

    Uniqueness is on (user, graph_id, config), so the name is echoed into config
    to keep each test's assistant distinct; ``do_nothing`` makes reruns
    idempotent. ``stress_test`` is deterministic and makes no LLM calls.
    """
    client = get_e2e_client()
    assistant = await client.assistants.create(
        graph_id="stress_test",
        name=name,
        description="E2E fixture assistant.",
        config={"tags": [name]},
        if_exists="do_nothing",
    )
    return assistant["assistant_id"]


def _message(text: str, context_id: str | None = None, task_id: str | None = None) -> dict[str, Any]:
    message: dict[str, Any] = {
        "role": "user",
        "parts": [{"kind": "text", "text": text}],
        "messageId": str(uuid.uuid4()),
    }
    if context_id:
        message["contextId"] = context_id
    if task_id:
        message["taskId"] = task_id
    return message


def _rpc(method: str, params: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": method, "params": params}


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_agent_card_discovery_e2e() -> None:
    """The well-known card describes the assistant and points back at its endpoint."""
    assistant_id = await _seed_assistant("a2a_card")

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        response = await http.get("/.well-known/agent-card.json", params={"assistant_id": assistant_id})

    assert response.status_code == 200
    card = response.json()
    elog("A2A agent card", card)

    assert card["protocolVersion"] == "0.3.0"
    assert card["url"].endswith(f"/a2a/{assistant_id}")
    assert card["capabilities"]["streaming"] is True
    assert card["skills"][0]["id"] == assistant_id


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_message_send_returns_completed_task_e2e() -> None:
    """message/send runs the agent and returns a completed Task with an artifact."""
    assistant_id = await _seed_assistant("a2a_send")

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=120.0) as http:
        response = await http.post(
            f"/a2a/{assistant_id}",
            json=_rpc("message/send", {"message": _message("Say hello")}),
        )

    assert response.status_code == 200
    body = response.json()
    elog("A2A message/send", body)

    result = body["result"]
    assert result["kind"] == "task"
    assert result["status"]["state"] == "completed"
    assert result["contextId"]
    assert result["artifacts"][0]["parts"][0]["kind"] == "text"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_context_id_continues_the_conversation_e2e() -> None:
    """Reusing contextId keeps both turns on one thread and yields distinct tasks."""
    assistant_id = await _seed_assistant("a2a_multi")

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=120.0) as http:
        first = (
            await http.post(f"/a2a/{assistant_id}", json=_rpc("message/send", {"message": _message("My name is Ada")}))
        ).json()["result"]

        context_id = first["contextId"]
        second = (
            await http.post(
                f"/a2a/{assistant_id}",
                json=_rpc("message/send", {"message": _message("What is my name?", context_id, first["id"])}),
            )
        ).json()["result"]

    elog("A2A second turn", second)

    assert second["contextId"] == context_id
    assert second["id"] != first["id"]

    # contextId is the thread id, so the thread now holds both turns.
    client = get_e2e_client()
    state = await client.threads.get_state(context_id)
    assert len(state["values"]["messages"]) >= 4


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_tasks_get_returns_the_prior_task_e2e() -> None:
    """tasks/get rebuilds a Task from a run created by an earlier message/send."""
    assistant_id = await _seed_assistant("a2a_get")

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=120.0) as http:
        sent = (await http.post(f"/a2a/{assistant_id}", json=_rpc("message/send", {"message": _message("Hi")}))).json()[
            "result"
        ]

        fetched = (await http.post(f"/a2a/{assistant_id}", json=_rpc("tasks/get", {"id": sent["id"]}))).json()["result"]

    assert fetched["id"] == sent["id"]
    assert fetched["contextId"] == sent["contextId"]
    assert fetched["status"]["state"] == "completed"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_unknown_task_returns_a2a_error_code_e2e() -> None:
    """tasks/get for a missing run returns -32001, not an HTTP error."""
    assistant_id = await _seed_assistant("a2a_missing")

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        response = await http.post(f"/a2a/{assistant_id}", json=_rpc("tasks/get", {"id": str(uuid.uuid4())}))

    assert response.status_code == 200
    assert response.json()["error"]["code"] == -32001


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_message_stream_ends_with_a_final_event_e2e() -> None:
    """message/stream emits SSE events and closes with final=True."""
    assistant_id = await _seed_assistant("a2a_stream")

    events: list[dict[str, Any]] = []
    async with (
        AsyncClient(base_url=settings.app.SERVER_URL, timeout=120.0) as http,
        http.stream(
            "POST",
            f"/a2a/{assistant_id}",
            json=_rpc("message/stream", {"message": _message("Stream please")}),
        ) as response,
    ):
        assert response.status_code == 200
        async for line in response.aiter_lines():
            if line.startswith("data: "):
                events.append(json.loads(line[len("data: ") :]))

    elog("A2A stream events", [event.get("result", {}).get("kind") for event in events])

    assert events, "expected at least one SSE event"
    assert events[0]["result"]["kind"] == "task"
    assert events[-1]["result"]["kind"] == "status-update"
    assert events[-1]["result"]["final"] is True
