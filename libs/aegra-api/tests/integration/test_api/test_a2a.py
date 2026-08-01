"""Integration tests for the A2A JSON-RPC endpoint.

Exercises the protocol layer over HTTP — envelope validation, error codes,
method dispatch, and agent card discovery — with the assistant lookup and run
execution mocked out.
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from a2a.compat.v0_3 import types as a2a
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from aegra_api.api.a2a import router as a2a_router
from aegra_api.core.auth_deps import get_current_user, require_auth
from aegra_api.models import Assistant, User
from aegra_api.services.a2a_server import build_task
from aegra_api.services.assistant_service import get_assistant_service

USER = User(identity="test-user", display_name="Test User")


def _assistant() -> Assistant:
    now = datetime.now(UTC)
    return Assistant(
        assistant_id="asst-1",
        name="Weather Bot",
        description="Answers weather questions.",
        graph_id="agent",
        user_id="test-user",
        version=1,
        created_at=now,
        updated_at=now,
    )


class _StubAssistantService:
    """Stands in for AssistantService, honouring only the ownership 404."""

    def __init__(self, assistant: Assistant | None) -> None:
        self._assistant = assistant

    async def get_assistant(self, assistant_id: str) -> Assistant:
        if self._assistant is None:
            raise HTTPException(404, f"Assistant '{assistant_id}' not found")
        return self._assistant


def _client(*, exists: bool = True) -> TestClient:
    """Client whose assistant lookup either resolves or 404s."""
    app = FastAPI()
    app.dependency_overrides[require_auth] = lambda: USER
    app.dependency_overrides[get_current_user] = lambda: USER
    app.dependency_overrides[get_assistant_service] = lambda: _StubAssistantService(_assistant() if exists else None)
    app.include_router(a2a_router)
    return TestClient(app)


def _rpc(method: str, params: dict[str, Any] | None = None, request_id: Any = "1") -> dict[str, Any]:
    body: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        body["params"] = params
    return body


def _text_message(text: str = "hello", **extra: Any) -> dict[str, Any]:
    return {"role": "user", "parts": [{"kind": "text", "text": text}], "messageId": "m1", **extra}


class TestAgentCard:
    def test_returns_card_for_the_requested_assistant(self) -> None:
        response = _client().get("/.well-known/agent-card.json", params={"assistant_id": "asst-1"})

        assert response.status_code == 200
        card = response.json()
        assert card["name"] == "Weather Bot"
        assert card["protocolVersion"] == "0.3.0"
        assert card["url"].endswith("/a2a/asst-1")
        assert card["capabilities"]["streaming"] is True
        assert card["skills"][0]["id"] == "asst-1"

    def test_missing_assistant_id_is_rejected(self) -> None:
        assert _client().get("/.well-known/agent-card.json").status_code == 422

    def test_unknown_assistant_returns_404(self) -> None:
        response = _client(exists=False).get("/.well-known/agent-card.json", params={"assistant_id": "nope"})

        assert response.status_code == 404


class TestJsonRpcEnvelope:
    def test_unparseable_body_returns_parse_error(self) -> None:
        response = _client().post("/a2a/asst-1", content=b"{not json", headers={"Content-Type": "application/json"})

        assert response.status_code == 200
        assert response.json()["error"]["code"] == -32700

    @pytest.mark.parametrize(
        "body",
        [
            {"id": "1", "method": "message/send"},
            {"jsonrpc": "1.0", "id": "1", "method": "message/send"},
            {"jsonrpc": "2.0", "id": "1"},
            {"jsonrpc": "2.0", "id": "1", "method": 42},
        ],
    )
    def test_malformed_envelope_returns_invalid_request(self, body: dict[str, Any]) -> None:
        response = _client().post("/a2a/asst-1", json=body)

        assert response.json()["error"]["code"] == -32600

    def test_non_object_params_returns_invalid_params(self) -> None:
        response = _client().post("/a2a/asst-1", json=_rpc("tasks/get", []))  # type: ignore[arg-type]

        assert response.json()["error"]["code"] == -32602

    def test_unknown_method_is_rejected_before_params_are_examined(self) -> None:
        response = _client().post("/a2a/asst-1", json={"jsonrpc": "2.0", "id": "1", "method": "x", "params": []})

        assert response.json()["error"]["code"] == -32601

    def test_unknown_method_returns_method_not_found(self) -> None:
        response = _client().post("/a2a/asst-1", json=_rpc("tasks/cancel", {"id": "r1"}))

        assert response.json()["error"]["code"] == -32601

    def test_request_id_is_echoed_back(self) -> None:
        response = _client().post("/a2a/asst-1", json=_rpc("nope", {}, request_id=99))

        assert response.json()["id"] == 99

    def test_unknown_assistant_returns_404_before_dispatch(self) -> None:
        response = _client(exists=False).post("/a2a/nope", json=_rpc("message/send", {"message": _text_message()}))

        assert response.status_code == 404


class TestMessageSend:
    def test_returns_task_with_reply_artifact(self) -> None:
        task = build_task(task_id="run-1", context_id="ctx-1", state=a2a.TaskState.completed, text="sunny")

        with patch("aegra_api.api.a2a.send_message", AsyncMock(return_value=task)) as send:
            response = _client().post("/a2a/asst-1", json=_rpc("message/send", {"message": _text_message()}))

        assert response.status_code == 200
        result = response.json()["result"]
        assert result["kind"] == "task"
        assert result["id"] == "run-1"
        assert result["contextId"] == "ctx-1"
        assert result["status"]["state"] == "completed"
        assert result["artifacts"][0]["parts"][0] == {"kind": "text", "text": "sunny"}
        assert send.await_args.args[0] == "asst-1"

    def test_context_id_from_the_message_reaches_the_handler(self) -> None:
        task = build_task(task_id="run-1", context_id="ctx-9", state=a2a.TaskState.completed)

        with patch("aegra_api.api.a2a.send_message", AsyncMock(return_value=task)) as send:
            _client().post(
                "/a2a/asst-1",
                json=_rpc("message/send", {"message": _text_message(contextId="ctx-9")}),
            )

        params = send.await_args.args[1]
        assert params.message.context_id == "ctx-9"

    @pytest.mark.parametrize(
        "params",
        [{}, {"message": {}}, {"message": {"role": "user"}}, {"message": {"parts": [], "messageId": "m1"}}],
    )
    def test_malformed_message_returns_invalid_params(self, params: dict[str, Any]) -> None:
        response = _client().post("/a2a/asst-1", json=_rpc("message/send", params))

        assert response.json()["error"]["code"] == -32602


class TestTasksGet:
    def test_returns_the_persisted_task(self) -> None:
        task = build_task(task_id="run-1", context_id="ctx-1", state=a2a.TaskState.completed, text="done")

        with patch("aegra_api.api.a2a.get_task", AsyncMock(return_value=task)):
            response = _client().post("/a2a/asst-1", json=_rpc("tasks/get", {"id": "run-1"}))

        assert response.json()["result"]["id"] == "run-1"

    def test_unknown_task_maps_to_the_a2a_error_code(self) -> None:
        with patch("aegra_api.api.a2a.get_task", AsyncMock(side_effect=HTTPException(404, "nope"))):
            response = _client().post("/a2a/asst-1", json=_rpc("tasks/get", {"id": "missing"}))

        assert response.status_code == 200
        assert response.json()["error"]["code"] == -32001

    def test_missing_id_returns_invalid_params(self) -> None:
        response = _client().post("/a2a/asst-1", json=_rpc("tasks/get", {}))

        assert response.json()["error"]["code"] == -32602


class TestMessageStream:
    def test_streams_json_rpc_wrapped_events_as_sse(self) -> None:
        async def _events(*_args: Any, **_kwargs: Any) -> Any:
            yield build_task(task_id="run-1", context_id="ctx-1", state=a2a.TaskState.submitted)
            yield a2a.TaskStatusUpdateEvent(
                taskId="run-1",
                contextId="ctx-1",
                status=a2a.TaskStatus(state=a2a.TaskState.completed),
                final=True,
            )

        with patch("aegra_api.api.a2a.stream_message", _events):
            response = _client().post("/a2a/asst-1", json=_rpc("message/stream", {"message": _text_message()}))

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        payloads = [line[len("data: ") :] for line in response.text.splitlines() if line.startswith("data: ")]
        assert len(payloads) == 2

        import json

        first, last = (json.loads(p) for p in payloads)
        assert first["result"]["kind"] == "task"
        assert last["result"]["kind"] == "status-update"
        assert last["result"]["final"] is True

    def test_malformed_message_returns_invalid_params_without_streaming(self) -> None:
        response = _client().post("/a2a/asst-1", json=_rpc("message/stream", {"message": {}}))

        assert response.json()["error"]["code"] == -32602
