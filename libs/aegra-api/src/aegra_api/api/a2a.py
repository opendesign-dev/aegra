"""A2A JSON-RPC endpoint and agent card discovery.

Two routes, matching the LangSmith Agent Server surface::

    POST /a2a/{assistant_id}                          JSON-RPC 2.0
    GET  /.well-known/agent-card.json?assistant_id=   discovery

JSON-RPC errors travel in a 200 response per the spec — only transport-level
failures (auth, unparseable body) use HTTP status codes.
"""

import json
from collections.abc import AsyncIterator
from typing import Any

import structlog
from a2a.compat.v0_3 import types as a2a
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError
from sse_starlette import EventSourceResponse

from aegra_api.core.auth_deps import auth_dependency, get_current_user
from aegra_api.core.sse import get_sse_headers, make_sse_response
from aegra_api.models import User
from aegra_api.models.errors import NOT_FOUND, SSE_RESPONSE
from aegra_api.services.a2a_server import (
    AGENT_CARD_PATH,
    build_agent_card,
    get_task,
    send_message,
    stream_message,
)
from aegra_api.services.assistant_service import AssistantService, get_assistant_service

logger = structlog.getLogger(__name__)

router = APIRouter(tags=["A2A"], dependencies=auth_dependency)

STREAM_METHOD = "message/stream"
METHODS = frozenset({"message/send", STREAM_METHOD, "tasks/get"})

# The errors this endpoint can raise, out of the wider set the response model accepts.
RpcError = (
    a2a.JSONParseError
    | a2a.InvalidRequestError
    | a2a.MethodNotFoundError
    | a2a.InvalidParamsError
    | a2a.InternalError
    | a2a.TaskNotFoundError
)


def _rpc_error(request_id: Any, error: RpcError) -> dict[str, Any]:
    return a2a.JSONRPCErrorResponse(id=request_id, error=error).model_dump(
        mode="json", by_alias=True, exclude_none=True
    )


def _rpc_result(request_id: Any, result: BaseModel) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": result.model_dump(mode="json", by_alias=True, exclude_none=True),
    }


def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


@router.get(AGENT_CARD_PATH, responses={**NOT_FOUND})
async def get_agent_card(
    request: Request,
    assistant_id: str = Query(..., description="Assistant to describe."),
    service: AssistantService = Depends(get_assistant_service),
) -> dict[str, Any]:
    """Return the A2A agent card for one assistant.

    Discovery is per-assistant rather than per-deployment because an Aegra
    server hosts many agents; the query parameter is what selects among them.
    """
    assistant = await service.get_assistant(assistant_id)
    card = build_agent_card(assistant, _base_url(request))
    return card.model_dump(mode="json", by_alias=True, exclude_none=True)


async def _sse_body(
    assistant_id: str,
    request_id: Any,
    params: a2a.MessageSendParams,
    user: User,
) -> AsyncIterator[bytes]:
    """Wrap each streamed A2A event in a JSON-RPC success envelope."""
    try:
        async for event in stream_message(assistant_id, params, user):
            payload = json.dumps(_rpc_result(request_id, event))
            yield f"data: {payload}\n\n".encode()
    except HTTPException as exc:
        payload = json.dumps(_rpc_error(request_id, a2a.InvalidParamsError(message=str(exc.detail))))
        yield f"data: {payload}\n\n".encode()
    except Exception:
        logger.exception("A2A stream failed", assistant_id=assistant_id)
        payload = json.dumps(_rpc_error(request_id, a2a.InternalError()))
        yield f"data: {payload}\n\n".encode()


async def _dispatch(
    method: str,
    raw_params: dict[str, Any],
    assistant_id: str,
    user: User,
) -> BaseModel:
    """Route a JSON-RPC call to its handler, validating params for that method.

    The caller checks ``method`` against :data:`METHODS` first; dispatching an
    unknown one here would be indistinguishable from a params failure, since
    Pydantic's ``ValidationError`` is itself a ``ValueError``.
    """
    if method == "message/send":
        return await send_message(assistant_id, a2a.MessageSendParams.model_validate(raw_params), user)
    return await get_task(a2a.TaskQueryParams.model_validate(raw_params).id, user)


@router.post("/a2a/{assistant_id}", response_model=None, responses={**SSE_RESPONSE, **NOT_FOUND})
async def a2a_rpc(
    assistant_id: str,
    request: Request,
    user: User = Depends(get_current_user),
    service: AssistantService = Depends(get_assistant_service),
) -> JSONResponse | EventSourceResponse:
    """Handle one A2A JSON-RPC call.

    ``message/stream`` answers with SSE; the other methods answer with JSON.
    """
    try:
        body = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse(_rpc_error(None, a2a.JSONParseError()))

    if not isinstance(body, dict) or body.get("jsonrpc") != "2.0" or not isinstance(body.get("method"), str):
        return JSONResponse(_rpc_error(body.get("id") if isinstance(body, dict) else None, a2a.InvalidRequestError()))

    request_id = body.get("id")
    method = body["method"]
    if method not in METHODS:
        return JSONResponse(_rpc_error(request_id, a2a.MethodNotFoundError()))

    raw_params = body.get("params")
    if raw_params is None:
        raw_params = {}
    if not isinstance(raw_params, dict):
        return JSONResponse(_rpc_error(request_id, a2a.InvalidParamsError()))

    # 404s before any run is created, so an unknown or unowned assistant never
    # reaches the executor.
    await service.get_assistant(assistant_id)

    if method == STREAM_METHOD:
        try:
            params = a2a.MessageSendParams.model_validate(raw_params)
        except ValidationError as exc:
            return JSONResponse(_rpc_error(request_id, a2a.InvalidParamsError(data=exc.errors(include_url=False))))
        return make_sse_response(_sse_body(assistant_id, request_id, params, user), headers=get_sse_headers())

    try:
        result = await _dispatch(method, raw_params, assistant_id, user)
    except ValidationError as exc:
        return JSONResponse(_rpc_error(request_id, a2a.InvalidParamsError(data=exc.errors(include_url=False))))
    except HTTPException as exc:
        if exc.status_code == 404 and method == "tasks/get":
            return JSONResponse(_rpc_error(request_id, a2a.TaskNotFoundError()))
        raise

    return JSONResponse(_rpc_result(request_id, result))
