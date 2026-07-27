"""E2E tests for the MCP Streamable HTTP endpoint.

Requires a running server with MCP_ENABLED=true (the default).
"""

import httpx
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from aegra_api.settings import settings

from .._utils import elog

MCP_HEADERS = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "e2e", "version": "1"}},
}


def _mcp_url() -> str:
    return f"{settings.app.SERVER_URL}/mcp"


@pytest.fixture(autouse=True)
def _skip_when_mcp_disabled() -> None:
    if not settings.mcp.MCP_ENABLED:
        pytest.skip("MCP_ENABLED=false; the /mcp endpoint is intentionally absent")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_mcp_endpoint_answers_at_exact_path_without_redirecting() -> None:
    """The endpoint must be reachable at /mcp directly. MCP clients build on httpx,
    which does not follow redirects, so a 3xx here breaks every client."""
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
        response = await client.post(_mcp_url(), json=INITIALIZE, headers=MCP_HEADERS)

    assert response.status_code == 200, response.text
    assert "location" not in {k.lower() for k in response.headers}


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_mcp_is_not_double_nested_under_its_own_mount() -> None:
    """Regression: mounting the MCP sub-app used to nest its route, serving the
    endpoint at /mcp/mcp and 404ing the documented path."""
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
        response = await client.post(f"{_mcp_url()}/mcp", json=INITIALIZE, headers=MCP_HEADERS)

    assert response.status_code == 404


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_lists_one_tool_per_graph_with_its_own_input_schema() -> None:
    async with streamablehttp_client(_mcp_url()) as (read, write, _), ClientSession(read, write) as session:
        init = await session.initialize()
        listing = await session.list_tools()

    elog("MCP tools", [t.name for t in listing.tools])
    assert init.serverInfo.name == "aegra"
    assert listing.tools, "expected at least one graph exposed as a tool"

    for tool in listing.tools:
        assert tool.inputSchema.get("type") == "object", f"{tool.name} must advertise an object schema"
        assert tool.description


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_call_tool_runs_the_graph_and_returns_its_final_state() -> None:
    """subgraph_hitl_agent needs no LLM credentials, so it exercises the full
    transport → auth → graph → serialize path without an external dependency."""
    async with streamablehttp_client(_mcp_url()) as (read, write, _), ClientSession(read, write) as session:
        await session.initialize()
        names = {t.name for t in (await session.list_tools()).tools}
        if "subgraph_hitl_agent" not in names:
            pytest.skip("subgraph_hitl_agent graph not configured in this deployment")
        result = await session.call_tool("subgraph_hitl_agent", {"foo": "bar"})

    text = result.content[0].text if result.content else ""
    elog("MCP call_tool result", text)
    assert result.isError is False
    assert '"foo": "bar"' in text


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_unknown_tool_is_rejected() -> None:
    async with streamablehttp_client(_mcp_url()) as (read, write, _), ClientSession(read, write) as session:
        await session.initialize()
        result = await session.call_tool("no_such_graph", {})

    assert result.isError is True
