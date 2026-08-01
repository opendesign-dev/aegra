"""E2E tests for the MCP endpoint against a running server.

Driven by the official MCP client so the handshake, transport negotiation, and
tool-call round trip are exercised the way a real client would.
"""

import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from aegra_api.settings import settings
from tests.e2e._utils import elog, get_e2e_client


def _mcp_url() -> str:
    return f"{settings.app.SERVER_URL.rstrip('/')}/mcp"


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


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_list_tools_exposes_assistants_e2e() -> None:
    """Every assistant shows up as an MCP tool with an object input schema."""
    name = "mcp_e2e_lister"
    await _seed_assistant(name)

    async with streamablehttp_client(_mcp_url()) as (read, write, _), ClientSession(read, write) as session:
        await session.initialize()
        result = await session.list_tools()

    names = [tool.name for tool in result.tools]
    elog("MCP tools", names)

    assert name in names
    tool = next(tool for tool in result.tools if tool.name == name)
    assert tool.inputSchema["type"] == "object"
    assert tool.description


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_call_tool_runs_the_agent_e2e() -> None:
    """Calling the tool executes a run and returns its output."""
    name = "mcp_e2e_caller"
    await _seed_assistant(name)

    async with streamablehttp_client(_mcp_url()) as (read, write, _), ClientSession(read, write) as session:
        await session.initialize()
        result = await session.call_tool(name, {"messages": [{"role": "user", "content": "Say hi"}]})

    elog("MCP call_tool result", result.model_dump(mode="json"))

    assert result.isError is False
    assert result.content or result.structuredContent


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_each_request_is_independent_e2e() -> None:
    """Two sessions in a row both work — the endpoint keeps no session state."""
    name = "mcp_e2e_stateless"
    await _seed_assistant(name)

    for _ in range(2):
        async with streamablehttp_client(_mcp_url()) as (read, write, _), ClientSession(read, write) as session:
            await session.initialize()
            result = await session.list_tools()

        assert name in [tool.name for tool in result.tools]
