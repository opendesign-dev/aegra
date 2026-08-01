"""Unit tests for the A2A protocol mapping."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from a2a.compat.v0_3 import types as a2a
from fastapi import HTTPException

from aegra_api.models import Assistant, User
from aegra_api.services.a2a_server import (
    PROTOCOL_VERSION,
    build_agent_card,
    build_task,
    get_task,
    message_to_input,
    output_text,
    send_message,
    task_state_for,
)
from aegra_api.services.interop import RunResult

USER = User(identity="u1", display_name="U1")


def _message(*parts: a2a.TextPart | a2a.DataPart) -> a2a.Message:
    return a2a.Message(
        messageId="m1",
        role=a2a.Role.user,
        parts=[a2a.Part(root=part) for part in parts],
    )


def _assistant(**overrides: Any) -> Assistant:
    now = datetime.now(UTC)
    defaults: dict[str, Any] = {
        "assistant_id": "asst-1",
        "name": "Weather Bot",
        "description": "Answers weather questions.",
        "graph_id": "agent",
        "user_id": "u1",
        "version": 3,
        "created_at": now,
        "updated_at": now,
    }
    return Assistant(**{**defaults, **overrides})


class TestMessageToInput:
    def test_maps_text_part_to_messages_key(self) -> None:
        result = message_to_input(_message(a2a.TextPart(text="hello")))

        assert result == {"messages": [{"role": "user", "content": "hello"}]}

    def test_joins_multiple_text_parts_with_newline(self) -> None:
        result = message_to_input(_message(a2a.TextPart(text="a"), a2a.TextPart(text="b")))

        assert result["messages"][0]["content"] == "a\nb"

    def test_single_data_part_rides_along_unwrapped(self) -> None:
        result = message_to_input(_message(a2a.TextPart(text="hi"), a2a.DataPart(data={"k": 1})))

        assert result["data"] == {"k": 1}

    def test_multiple_data_parts_stay_a_list(self) -> None:
        result = message_to_input(_message(a2a.DataPart(data={"a": 1}), a2a.DataPart(data={"b": 2})))

        assert result["data"] == [{"a": 1}, {"b": 2}]

    def test_message_with_no_text_yields_empty_content(self) -> None:
        result = message_to_input(_message(a2a.DataPart(data={"a": 1})))

        assert result["messages"] == [{"role": "user", "content": ""}]


class TestOutputText:
    def test_reads_last_message_content(self) -> None:
        output = {"messages": [{"role": "user", "content": "q"}, {"role": "assistant", "content": "a"}]}

        assert output_text(output) == "a"

    def test_flattens_content_blocks(self) -> None:
        output = {"messages": [{"role": "assistant", "content": [{"type": "text", "text": "hi"}]}]}

        assert output_text(output) == "hi"

    @pytest.mark.parametrize("output", [{}, {"messages": []}, {"messages": "nope"}, {"other": 1}])
    def test_returns_empty_string_for_unusable_output(self, output: dict[str, Any]) -> None:
        assert output_text(output) == ""


class TestTaskStateFor:
    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            ("success", a2a.TaskState.completed),
            ("error", a2a.TaskState.failed),
            ("pending", a2a.TaskState.working),
            ("running", a2a.TaskState.working),
        ],
    )
    def test_maps_run_status(self, status: str, expected: a2a.TaskState) -> None:
        assert task_state_for(status, {}) is expected

    def test_interrupted_with_pending_interrupt_is_input_required(self) -> None:
        assert task_state_for("interrupted", {"__interrupt__": [{"value": "?"}]}) is a2a.TaskState.input_required

    def test_interrupted_without_interrupt_is_canceled(self) -> None:
        assert task_state_for("interrupted", {"messages": []}) is a2a.TaskState.canceled


class TestBuildTask:
    def test_successful_task_carries_reply_as_artifact(self) -> None:
        task = build_task(task_id="r1", context_id="t1", state=a2a.TaskState.completed, text="done")

        assert task.id == "r1"
        assert task.context_id == "t1"
        assert task.status.state is a2a.TaskState.completed
        assert task.artifacts is not None
        assert task.artifacts[0].parts[0].root.text == "done"

    def test_empty_reply_produces_no_artifacts(self) -> None:
        task = build_task(task_id="r1", context_id="t1", state=a2a.TaskState.completed)

        assert task.artifacts is None

    def test_error_lands_on_status_message(self) -> None:
        task = build_task(task_id="r1", context_id="t1", state=a2a.TaskState.failed, error="boom")

        assert task.status.message is not None
        assert task.status.message.role is a2a.Role.agent
        assert task.status.message.parts[0].root.text == "boom"

    def test_serializes_with_kind_discriminators(self) -> None:
        task = build_task(task_id="r1", context_id="t1", state=a2a.TaskState.completed, text="hi")

        dumped = task.model_dump(mode="json", by_alias=True, exclude_none=True)

        assert dumped["kind"] == "task"
        assert dumped["status"]["state"] == "completed"
        assert dumped["artifacts"][0]["parts"][0] == {"kind": "text", "text": "hi"}


class TestBuildAgentCard:
    def test_card_points_at_the_assistant_endpoint(self) -> None:
        card = build_agent_card(_assistant(), "https://host")

        assert card.url == "https://host/a2a/asst-1"
        assert card.protocol_version == PROTOCOL_VERSION
        assert card.preferred_transport == a2a.TransportProtocol.jsonrpc.value
        assert card.capabilities.streaming is True

    def test_assistant_becomes_a_single_skill_tagged_with_its_graph(self) -> None:
        card = build_agent_card(_assistant(), "https://host")

        assert len(card.skills) == 1
        assert card.skills[0].id == "asst-1"
        assert card.skills[0].tags == ["agent"]

    def test_version_tracks_the_assistant_version(self) -> None:
        assert build_agent_card(_assistant(version=7), "https://host").version == "7"

    def test_missing_description_falls_back_rather_than_failing(self) -> None:
        card = build_agent_card(_assistant(description=None), "https://host")

        assert "Weather Bot" in card.description
        assert card.skills[0].description == card.description


class TestSendMessage:
    """Covers send_message against a stubbed executor.

    These exercise the real attribute reads on the A2A models — the compat
    models expose snake_case attributes behind camelCase aliases, so a
    camelCase read here fails at runtime rather than at import.
    """

    @staticmethod
    def _run(**overrides: Any) -> RunResult:
        defaults: dict[str, Any] = {
            "run_id": "run-1",
            "thread_id": "ctx-1",
            "status": "success",
            "output": {"messages": [{"role": "assistant", "content": "pong"}]},
            "error": None,
        }
        return RunResult(**{**defaults, **overrides})

    @pytest.mark.asyncio
    async def test_reply_becomes_the_task_artifact(self) -> None:
        params = a2a.MessageSendParams(message=_message(a2a.TextPart(text="ping")))

        with patch("aegra_api.services.a2a_server.execute_and_wait", AsyncMock(return_value=self._run())):
            task = await send_message("asst-1", params, USER)

        assert task.id == "run-1"
        assert task.status.state is a2a.TaskState.completed
        assert task.artifacts is not None
        assert task.artifacts[0].parts[0].root.text == "pong"

    @pytest.mark.asyncio
    async def test_supplied_context_id_is_used_as_the_thread(self) -> None:
        message = _message(a2a.TextPart(text="ping"))
        message.context_id = "ctx-42"
        params = a2a.MessageSendParams(message=message)

        execute = AsyncMock(return_value=self._run())
        with patch("aegra_api.services.a2a_server.execute_and_wait", execute):
            task = await send_message("asst-1", params, USER)

        assert execute.await_args.args[0] == "ctx-42"
        assert task.context_id == "ctx-42"

    @pytest.mark.asyncio
    async def test_missing_context_id_generates_a_fresh_thread(self) -> None:
        params = a2a.MessageSendParams(message=_message(a2a.TextPart(text="ping")))

        execute = AsyncMock(return_value=self._run())
        with patch("aegra_api.services.a2a_server.execute_and_wait", execute):
            task = await send_message("asst-1", params, USER)

        generated = execute.await_args.args[0]
        assert generated and task.context_id == generated

    @pytest.mark.asyncio
    async def test_message_id_is_recorded_on_the_run(self) -> None:
        params = a2a.MessageSendParams(message=_message(a2a.TextPart(text="ping")))

        execute = AsyncMock(return_value=self._run())
        with patch("aegra_api.services.a2a_server.execute_and_wait", execute):
            await send_message("asst-1", params, USER)

        assert execute.await_args.args[1].metadata == {"a2a_message_id": "m1"}

    @pytest.mark.asyncio
    async def test_failed_run_surfaces_as_a_failed_task(self) -> None:
        params = a2a.MessageSendParams(message=_message(a2a.TextPart(text="ping")))
        failed = self._run(status="error", output={}, error="boom")

        with patch("aegra_api.services.a2a_server.execute_and_wait", AsyncMock(return_value=failed)):
            task = await send_message("asst-1", params, USER)

        assert task.status.state is a2a.TaskState.failed
        assert task.status.message is not None
        assert task.status.message.parts[0].root.text == "boom"


class TestGetTask:
    """Covers get_task against a stubbed run store."""

    @pytest.mark.asyncio
    async def test_rebuilds_the_task_from_the_persisted_run(self) -> None:
        stored = RunResult(
            run_id="run-7",
            thread_id="ctx-7",
            status="success",
            output={"messages": [{"role": "assistant", "content": "cached"}]},
            error=None,
        )

        with patch("aegra_api.services.a2a_server.read_run_result", AsyncMock(return_value=stored)):
            task = await get_task("run-7", USER)

        assert task.id == "run-7"
        assert task.context_id == "ctx-7"
        assert task.status.state is a2a.TaskState.completed
        assert task.artifacts is not None
        assert task.artifacts[0].parts[0].root.text == "cached"

    @pytest.mark.asyncio
    async def test_thread_id_becomes_the_context_id(self) -> None:
        stored = RunResult(run_id="run-7", thread_id="ctx-9", status="error", output={}, error="nope")

        with patch("aegra_api.services.a2a_server.read_run_result", AsyncMock(return_value=stored)):
            task = await get_task("run-7", USER)

        assert task.context_id == "ctx-9"
        assert task.status.state is a2a.TaskState.failed

    @pytest.mark.asyncio
    async def test_missing_run_raises_404(self) -> None:
        with (
            patch("aegra_api.services.a2a_server.read_run_result", AsyncMock(return_value=None)),
            pytest.raises(HTTPException) as exc,
        ):
            await get_task("gone", USER)

        assert exc.value.status_code == 404
