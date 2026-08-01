"""Integration tests for the MCP endpoint.

Drives the real Streamable HTTP transport over HTTP — the assistant lookup and
graph schema derivation are mocked, the MCP protocol layer is not.
"""

import json
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.routing import Route

from aegra_api.models import Assistant, User
from aegra_api.services.mcp_server import mcp_asgi_app, mcp_lifespan

USER = User(identity="test-user", display_name="Test User")

MCP_HEADERS = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}


def _assistant(assistant_id: str = "asst-1", name: str = "Weather Bot") -> Assistant:
    now = datetime.now(UTC)
    return Assistant(
        assistant_id=assistant_id,
        name=name,
        description="Answers weather questions.",
        graph_id="agent",
        user_id="test-user",
        version=1,
        created_at=now,
        updated_at=now,
    )


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    async with mcp_lifespan():
        yield


@pytest.fixture
def client() -> Iterator[TestClient]:
    app = FastAPI(lifespan=_lifespan)
    app.router.routes.append(Route("/mcp", endpoint=mcp_asgi_app, methods=["GET", "POST", "DELETE"]))
    with TestClient(app) as test_client:
        yield test_client


def _rpc(method: str, params: dict[str, Any] | None = None, request_id: int = 1) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}


def _result(response: Any) -> dict[str, Any]:
    """Pull the JSON-RPC payload out of either a JSON or an SSE response."""
    if response.headers["content-type"].startswith("application/json"):
        return response.json()
    for line in response.text.splitlines():
        if line.startswith("data: "):
            return json.loads(line[len("data: ") :])
    raise AssertionError(f"no JSON-RPC payload in response: {response.text!r}")


def _authenticated() -> Any:
    return patch("aegra_api.services.mcp_server.require_auth", AsyncMock(return_value=USER))


@contextmanager
def _with_assistants(*assistants: Assistant) -> Iterator[None]:
    """Patch the assistant lookup along with the session it opens."""
    service = AsyncMock()
    service.list_assistants = AsyncMock(return_value=list(assistants))

    maker = MagicMock()
    maker.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
    maker.return_value.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("aegra_api.services.mcp_server.AssistantService", return_value=service),
        patch("aegra_api.services.mcp_server._get_session_maker", return_value=maker),
    ):
        yield


def _with_schema(schema: dict[str, Any] | None = None) -> Any:
    return patch(
        "aegra_api.services.mcp_server._graph_input_schema",
        AsyncMock(return_value=schema or {"type": "object", "properties": {"messages": {"type": "array"}}}),
    )


class TestAuthentication:
    def test_unauthenticated_request_is_rejected_at_the_transport(self, client: TestClient) -> None:
        with patch(
            "aegra_api.services.mcp_server.require_auth",
            AsyncMock(side_effect=HTTPException(401, "Authentication required")),
        ):
            response = client.post("/mcp", json=_rpc("tools/list"), headers=MCP_HEADERS)

        assert response.status_code == 401
        assert response.json()["message"] == "Authentication required"

    def test_auth_failure_uses_the_agent_protocol_envelope(self, client: TestClient) -> None:
        with patch(
            "aegra_api.services.mcp_server.require_auth",
            AsyncMock(side_effect=HTTPException(403, "Forbidden")),
        ):
            response = client.post("/mcp", json=_rpc("tools/list"), headers=MCP_HEADERS)

        assert response.status_code == 403
        assert set(response.json()) >= {"error", "message"}


class TestListTools:
    def test_each_assistant_becomes_a_tool(self, client: TestClient) -> None:
        with _authenticated(), _with_assistants(_assistant()), _with_schema():
            response = client.post("/mcp", json=_rpc("tools/list"), headers=MCP_HEADERS)

        assert response.status_code == 200
        tools = _result(response)["result"]["tools"]
        assert len(tools) == 1
        assert tools[0]["name"] == "Weather_Bot"
        assert tools[0]["description"] == "Answers weather questions."
        assert tools[0]["inputSchema"]["type"] == "object"

    def test_title_preserves_the_unsanitized_assistant_name(self, client: TestClient) -> None:
        with _authenticated(), _with_assistants(_assistant()), _with_schema():
            response = client.post("/mcp", json=_rpc("tools/list"), headers=MCP_HEADERS)

        tool = _result(response)["result"]["tools"][0]
        assert tool["name"] == "Weather_Bot"
        assert tool["title"] == "Weather Bot"

    def test_no_assistants_yields_an_empty_tool_list(self, client: TestClient) -> None:
        with _authenticated(), _with_assistants(), _with_schema():
            response = client.post("/mcp", json=_rpc("tools/list"), headers=MCP_HEADERS)

        assert _result(response)["result"]["tools"] == []

    def test_colliding_names_stay_distinct(self, client: TestClient) -> None:
        pair = (_assistant("asst-1", "Weather Bot"), _assistant("asst-2", "Weather.Bot"))

        with _authenticated(), _with_assistants(*pair), _with_schema():
            response = client.post("/mcp", json=_rpc("tools/list"), headers=MCP_HEADERS)

        names = [tool["name"] for tool in _result(response)["result"]["tools"]]
        assert names == ["Weather_Bot", "asst-2"]
        assert len(set(names)) == 2


class TestCallTool:
    def test_successful_call_returns_the_run_output(self, client: TestClient) -> None:
        run_result = AsyncMock(return_value=type("R", (), {"succeeded": True, "output": {"answer": "sunny"}})())

        with (
            _authenticated(),
            _with_assistants(_assistant()),
            _with_schema(),
            patch("aegra_api.services.mcp_server.execute_and_wait", run_result),
            patch("aegra_api.services.mcp_server.delete_thread_by_id", AsyncMock()),
        ):
            response = client.post(
                "/mcp",
                json=_rpc("tools/call", {"name": "Weather_Bot", "arguments": {"messages": []}}),
                headers=MCP_HEADERS,
            )

        result = _result(response)["result"]
        assert result["isError"] is False
        assert result["structuredContent"] == {"answer": "sunny"}

    def test_failed_run_is_reported_as_a_tool_error(self, client: TestClient) -> None:
        failed = type("R", (), {"succeeded": False, "output": {}, "error": "boom", "status": "error"})()

        with (
            _authenticated(),
            _with_assistants(_assistant()),
            _with_schema(),
            patch("aegra_api.services.mcp_server.execute_and_wait", AsyncMock(return_value=failed)),
            patch("aegra_api.services.mcp_server.delete_thread_by_id", AsyncMock()),
        ):
            response = client.post(
                "/mcp",
                json=_rpc("tools/call", {"name": "Weather_Bot", "arguments": {}}),
                headers=MCP_HEADERS,
            )

        result = _result(response)["result"]
        assert result["isError"] is True
        assert "boom" in result["content"][0]["text"]

    def test_unknown_tool_is_reported_as_an_error(self, client: TestClient) -> None:
        with _authenticated(), _with_assistants(_assistant()), _with_schema():
            response = client.post(
                "/mcp",
                json=_rpc("tools/call", {"name": "does-not-exist", "arguments": {}}),
                headers=MCP_HEADERS,
            )

        result = _result(response)["result"]
        assert result["isError"] is True

    def test_ephemeral_thread_is_deleted_after_the_call(self, client: TestClient) -> None:
        succeeded = type("R", (), {"succeeded": True, "output": {}})()
        delete = AsyncMock()

        with (
            _authenticated(),
            _with_assistants(_assistant()),
            _with_schema(),
            patch("aegra_api.services.mcp_server.execute_and_wait", AsyncMock(return_value=succeeded)),
            patch("aegra_api.services.mcp_server.delete_thread_by_id", delete),
        ):
            client.post(
                "/mcp",
                json=_rpc("tools/call", {"name": "Weather_Bot", "arguments": {}}),
                headers=MCP_HEADERS,
            )

        delete.assert_awaited_once()
        assert delete.await_args.args[1] == USER.identity
