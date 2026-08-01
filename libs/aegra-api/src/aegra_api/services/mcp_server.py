"""MCP endpoint — expose assistants as MCP tools over Streamable HTTP.

Mirrors the LangSmith Agent Server contract: a single stateless ``/mcp``
endpoint where each assistant becomes one tool (name, description, and input
schema taken from the assistant and its graph).

Statelessness is the load-bearing property. There is no MCP session, so every
request re-authenticates and re-resolves the tool list against the caller's own
identity — a tool another user can see is never callable here by proxy.
"""

import contextlib
import re
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import mcp.types as mcp_types
import structlog
from fastapi import HTTPException
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import Receive, Scope, Send

from aegra_api.core.auth_deps import require_auth
from aegra_api.core.orm import _get_session_maker
from aegra_api.models import RunCreate, User
from aegra_api.models.errors import AgentProtocolError, get_error_type
from aegra_api.services.assistant_service import AssistantService
from aegra_api.services.interop import execute_and_wait
from aegra_api.services.langgraph_service import get_langgraph_service
from aegra_api.services.run_cleanup import _CLEANUP_ERRORS, delete_thread_by_id

logger = structlog.getLogger(__name__)

SERVER_NAME = "aegra"

# MCP restricts tool names to this alphabet; assistant names are free-form.
_ILLEGAL_TOOL_CHARS = re.compile(r"[^a-zA-Z0-9_-]")
_TOOL_NAME_MAX_LENGTH = 128

# Scope key holding the authenticated User, set by the ASGI wrapper before the
# request reaches the MCP transport.
_USER_SCOPE_KEY = "aegra_mcp_user"

_EMPTY_INPUT_SCHEMA: dict[str, Any] = {"type": "object", "properties": {}}

mcp_server: Server = Server(SERVER_NAME)


def tool_name_for(name: str, assistant_id: str, taken: set[str]) -> str:
    """Map an assistant onto a unique, MCP-legal tool name.

    Falls back to the assistant id when the name sanitizes to nothing or
    collides — an id is always legal and unique, so no assistant is silently
    dropped from the tool list.
    """
    slug = _ILLEGAL_TOOL_CHARS.sub("_", name).strip("_")[:_TOOL_NAME_MAX_LENGTH]
    if not slug or slug in taken:
        slug = _ILLEGAL_TOOL_CHARS.sub("_", assistant_id)[:_TOOL_NAME_MAX_LENGTH]
    taken.add(slug)
    return slug


def _normalize_input_schema(schema: dict[str, Any] | None) -> dict[str, Any]:
    """Coerce a graph input schema into the object schema MCP requires."""
    if not isinstance(schema, dict) or schema.get("type") != "object":
        return _EMPTY_INPUT_SCHEMA
    return schema


def _current_user() -> User:
    """Read the authenticated user the ASGI wrapper stashed on the scope."""
    request = mcp_server.request_context.request
    user = request.scope.get(_USER_SCOPE_KEY) if isinstance(request, Request) else None
    if not isinstance(user, User):
        raise ValueError("MCP request reached a handler without an authenticated user")
    return user


async def _graph_input_schema(graph_id: str, user: User, cache: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Input JSON Schema for ``graph_id``, memoized per listing.

    Several assistants commonly share one graph; without the cache each would
    re-resolve the same factory.
    """
    if graph_id in cache:
        return cache[graph_id]

    try:
        graph = await get_langgraph_service().get_graph_for_validation(graph_id, user=user)
        schema = _normalize_input_schema(graph.get_input_jsonschema())
    except Exception:
        # A broken graph must not blank the whole tool list — degrade to an
        # open schema and keep the assistant callable.
        logger.warning("Failed to derive MCP input schema", graph_id=graph_id, exc_info=True)
        schema = _EMPTY_INPUT_SCHEMA

    cache[graph_id] = schema
    return schema


@mcp_server.list_tools()
async def list_tools() -> list[mcp_types.Tool]:
    """One tool per assistant visible to the caller."""
    user = _current_user()
    maker = _get_session_maker()
    async with maker() as session:
        assistants = await AssistantService(session, user, get_langgraph_service()).list_assistants()

    tools: list[mcp_types.Tool] = []
    taken: set[str] = set()
    schema_cache: dict[str, dict[str, Any]] = {}
    for assistant in assistants:
        tools.append(
            mcp_types.Tool(
                name=tool_name_for(assistant.name, assistant.assistant_id, taken),
                title=assistant.name,
                description=assistant.description or f"Run the '{assistant.name}' agent.",
                inputSchema=await _graph_input_schema(assistant.graph_id, user, schema_cache),
            )
        )
    return tools


async def _resolve_assistant_id(tool_name: str, user: User) -> str:
    """Reverse the tool name back to an assistant the caller owns.

    Resolved per call rather than from the transport's tool cache: that cache is
    process-global and would let one user's listing decide another's call.
    """
    maker = _get_session_maker()
    async with maker() as session:
        assistants = await AssistantService(session, user, get_langgraph_service()).list_assistants()

    taken: set[str] = set()
    for assistant in assistants:
        if tool_name_for(assistant.name, assistant.assistant_id, taken) == tool_name:
            return assistant.assistant_id
    raise ValueError(f"Unknown tool '{tool_name}'")


@mcp_server.call_tool(validate_input=False)
async def call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any] | mcp_types.CallToolResult:
    """Run the assistant behind ``name`` once and return its output.

    Input validation is left off so the graph's own schema is the single
    authority; the transport's validator reads a shared cache that a concurrent
    caller may have populated from a different tool list.
    """
    user = _current_user()
    assistant_id = await _resolve_assistant_id(name, user)

    thread_id = str(uuid4())
    request = RunCreate(assistant_id=assistant_id, input=arguments or {})
    try:
        result = await execute_and_wait(thread_id, request, user)
    finally:
        try:
            await delete_thread_by_id(thread_id, user.identity)
        except _CLEANUP_ERRORS:
            logger.exception("Failed to delete ephemeral MCP thread", thread_id=thread_id)

    if not result.succeeded:
        return mcp_types.CallToolResult(
            content=[mcp_types.TextContent(type="text", text=result.error or f"Run {result.status}")],
            isError=True,
        )
    return result.output


# Set for the duration of the app's lifespan; None means /mcp is not serving.
_session_manager: StreamableHTTPSessionManager | None = None


@contextlib.asynccontextmanager
async def mcp_lifespan() -> AsyncIterator[None]:
    """Run the transport's task group for the life of the app.

    A fresh manager per lifespan on purpose: ``run()`` may only be called once
    per instance, so a module-level one would fail every startup after the
    first in the same process — reload, tests, or an embedding host.
    """
    global _session_manager

    manager = StreamableHTTPSessionManager(app=mcp_server, stateless=True)
    async with manager.run():
        _session_manager = manager
        try:
            yield
        finally:
            _session_manager = None


def _error_response(status_code: int, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=AgentProtocolError(error=get_error_type(status_code), message=message).model_dump(),
    )


class _MCPEndpoint:
    """Raw ASGI endpoint for ``/mcp``.

    A class instance rather than a function on purpose: Starlette's ``Route``
    wraps plain functions into request/response handlers, which would hide the
    send channel the Streamable HTTP transport writes SSE through. ``Mount`` is
    not an option either — it only matches ``/mcp/...``, never a bare ``/mcp``,
    and MCP clients do not follow the resulting redirect.
    """

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Authenticate, then hand the connection to the MCP transport.

        Auth runs here rather than inside a tool handler so a rejected caller
        gets an HTTP 401 — what MCP clients look for to begin an auth flow —
        instead of a JSON-RPC error buried in a 200.
        """
        manager = _session_manager
        if manager is None:
            await _error_response(503, "MCP transport is not running")(scope, receive, send)
            return

        request = Request(scope, receive)
        try:
            user = await require_auth(request)
        except HTTPException as exc:
            await _error_response(exc.status_code, str(exc.detail))(scope, receive, send)
            return

        scope[_USER_SCOPE_KEY] = user
        await manager.handle_request(scope, receive, send)


mcp_asgi_app = _MCPEndpoint()
