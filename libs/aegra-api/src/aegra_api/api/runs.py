"""Run endpoints for Agent Protocol"""

from collections.abc import AsyncGenerator, Awaitable, Callable, MutableMapping
from datetime import UTC, datetime
from typing import Any, get_args

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from redis import RedisError
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette import EventSourceResponse

from aegra_api.core.active_runs import active_runs, drain_task
from aegra_api.core.auth_deps import auth_dependency, get_current_user
from aegra_api.core.auth_handlers import build_auth_context, handle_event
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import get_session, get_session_maker
from aegra_api.core.sse import create_end_event, get_sse_headers, make_sse_response, sse_to_bytes
from aegra_api.models import Run, RunCreate, RunSearchRequest, RunSelectField, RunStatus, User
from aegra_api.models.enums import CancelAction, StreamMode
from aegra_api.models.errors import CONFLICT, NOT_FOUND, SSE_RESPONSE
from aegra_api.models.filters import assume_utc, validate_time_range
from aegra_api.models.runs import RunsCancelRequest, project_runs
from aegra_api.services.broker import broker_manager
from aegra_api.services.run_cancellation import rollback_runs, signal_cancel, wait_for_settle
from aegra_api.services.run_preparation import prepare_run
from aegra_api.services.run_search import build_run_filters
from aegra_api.services.run_waiters import TERMINAL_STATES, encode_output, heartbeat_wait_body, wrap_run_result
from aegra_api.services.streaming_service import normalize_stream_modes, streaming_service
from aegra_api.settings import settings
from aegra_api.utils.status_compat import validate_run_status

router = APIRouter(tags=["Thread Runs"], dependencies=auth_dependency)

logger = structlog.getLogger(__name__)


# Default stream modes for background run execution
DEFAULT_STREAM_MODES = ["values"]


async def _authorize_run_creation(user: User, request: RunCreate, thread_id: str) -> None:
    """Dispatch the ``threads.create_run`` @auth.on event and merge handler filters.

    Deny raises 403 before the run is created; allow may inject config/context. Used
    by every run-creation entrypoint — create, stream, wait (and the stateless
    wrappers that delegate to them) — so an operator's authorization policy applies
    uniformly, not just on the plain create path.
    """
    ctx = build_auth_context(user, "threads", "create_run")
    value = {**request.model_dump(), "thread_id": thread_id}
    filters = await handle_event(ctx, value)
    # Handler returned a filter dict, else the value it mutated in place.
    source = filters if filters else value
    handler_config = source.get("config")
    if isinstance(handler_config, dict):
        request.config = {**(request.config or {}), **handler_config}
    handler_context = source.get("context")
    if isinstance(handler_context, dict):
        request.context = {**(request.context or {}), **handler_context}


def _cancel_handler(run_id: str) -> Callable[[MutableMapping[str, Any]], Awaitable[None]]:
    """Build the SSE close handler that cancels ``run_id`` when the client drops."""

    async def _cancel_on_client_close(_msg: MutableMapping[str, Any]) -> None:
        try:
            await broker_manager.request_cancel(run_id, "cancel")
        except (RedisError, OSError):
            # Swallow infra/transport failures so sse-starlette's task group tears
            # down cleanly; the lease reaper picks up runs we couldn't reach.
            logger.exception("Failed to cancel run on client disconnect", run_id=run_id)

    return _cancel_on_client_close


@router.post("/threads/{thread_id}/runs", response_model=Run, responses={**NOT_FOUND, **CONFLICT})
async def create_run(
    thread_id: str,
    request: RunCreate,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Run:
    """Create and execute a new run.

    Starts graph execution asynchronously and returns the run record
    immediately with status `pending`. Poll the run or use the stream
    endpoint to follow progress. Provide either `input` or `command` (for
    human-in-the-loop resumption) but not both.
    """
    existing_thread = await session.scalar(select(ThreadORM).where(ThreadORM.thread_id == thread_id))
    if existing_thread and existing_thread.user_id != user.identity:
        raise HTTPException(404, f"Thread '{thread_id}' not found")

    await _authorize_run_creation(user, request, thread_id)

    _run_id, run, _job = await prepare_run(session, thread_id, request, user, initial_status="pending")

    return run


@router.post("/threads/{thread_id}/runs/stream", responses={**SSE_RESPONSE, **NOT_FOUND, **CONFLICT})
async def create_and_stream_run(
    thread_id: str,
    request: RunCreate,
    user: User = Depends(get_current_user),
) -> EventSourceResponse:
    """Create a new run and stream its execution via SSE.

    Returns a `text/event-stream` response with Server-Sent Events. Each
    event has a `type` field (e.g. `values`, `updates`, `messages`,
    `metadata`, `end`) and a JSON `data` payload.

    Set `on_disconnect` to `"continue"` if the run should keep executing
    after the client disconnects (default is `"cancel"`). Use `stream_mode`
    to control which event types are emitted.

    A periodic SSE keepalive comment is sent every
    ``KEEPALIVE_INTERVAL_SECS`` so idle proxies don't drop long-running
    silent nodes (e.g. agents holding an upstream WebSocket).
    """
    maker = get_session_maker()
    async with maker() as session:
        existing_thread = await session.scalar(select(ThreadORM).where(ThreadORM.thread_id == thread_id))
        if existing_thread and existing_thread.user_id != user.identity:
            raise HTTPException(404, f"Thread '{thread_id}' not found")

        await _authorize_run_creation(user, request, thread_id)

        run_id, run, _job = await prepare_run(session, thread_id, request, user, initial_status="pending")

    # Default to cancel on disconnect - this matches user expectation that clicking
    # "Cancel" in the frontend will stop the backend task. Users can explicitly
    # set on_disconnect="continue" if they want the task to continue.
    cancel_on_disconnect = (request.on_disconnect or "cancel").lower() == "cancel"

    return make_sse_response(
        sse_to_bytes(streaming_service.stream_run_execution(run, None)),
        close_handler=_cancel_handler(run_id) if cancel_on_disconnect else None,
        headers={
            **get_sse_headers(),
            "Location": f"/threads/{thread_id}/runs/{run_id}/stream",
            "Content-Location": f"/threads/{thread_id}/runs/{run_id}",
        },
    )


@router.get("/threads/{thread_id}/runs/{run_id}", response_model=Run, responses={**NOT_FOUND})
async def get_run(
    thread_id: str,
    run_id: str,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Run:
    """Get a run by its ID.

    Returns the current state of the run including its status, input, output,
    and error information.
    """
    # Authorization check (read action on runs resource)
    ctx = build_auth_context(user, "runs", "read")
    value = {"run_id": run_id, "thread_id": thread_id}
    await handle_event(ctx, value)

    stmt = select(RunORM).where(
        RunORM.run_id == str(run_id),
        RunORM.thread_id == thread_id,
        RunORM.user_id == user.identity,
    )
    run_orm = await session.scalar(stmt)
    if not run_orm:
        raise HTTPException(404, f"Run '{run_id}' not found")

    # No refresh needed: fresh per-request session + expire_on_commit=False means
    # the scalar() row is already current; a refresh() would just re-SELECT it.
    return Run.model_validate(run_orm)


_RUN_SELECT_FIELDS = frozenset(get_args(RunSelectField))


@router.get("/threads/{thread_id}/runs")
async def list_runs(
    thread_id: str,
    limit: int = Query(10, ge=1, le=1000, description="Maximum number of runs to return"),
    offset: int = Query(0, ge=0, description="Number of runs to skip for pagination"),
    status: str | None = Query(
        None, description="Filter by run status (e.g. pending, running, success, error, interrupted)"
    ),
    created_after: datetime | None = Query(
        None, description="Only runs created at or after this timestamp (ISO 8601; naive means UTC)."
    ),
    created_before: datetime | None = Query(
        None, description="Only runs created at or before this timestamp (ISO 8601; naive means UTC)."
    ),
    select_fields: list[str] | None = Query(
        None, alias="select", description="Return only these run fields (SDK RunSelectField values)."
    ),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[Run] | list[dict[str, Any]]:
    """List runs for a thread.

    Returns runs ordered by creation time (newest first). Use `status` and the
    `created_after`/`created_before` window to filter, `limit`/`offset` to
    paginate, and `select` to project fields.
    """
    if select_fields:
        invalid = [f for f in select_fields if f not in _RUN_SELECT_FIELDS]
        if invalid:
            raise HTTPException(422, f"Invalid select columns: {invalid}. Expected: {sorted(_RUN_SELECT_FIELDS)}")
    after = assume_utc(created_after) if created_after else None
    before = assume_utc(created_before) if created_before else None
    try:
        validate_time_range(after, before, "created")
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    stmt = (
        select(RunORM)
        .where(
            RunORM.thread_id == thread_id,
            RunORM.user_id == user.identity,
            *([RunORM.status == status] if status else []),
            *([RunORM.created_at >= after] if after else []),
            *([RunORM.created_at <= before] if before else []),
        )
        .limit(limit)
        .offset(offset)
        .order_by(RunORM.created_at.desc())
    )
    result = await session.scalars(stmt)
    rows = result.all()
    runs = [Run.model_validate(r) for r in rows]
    if select_fields:
        return project_runs(runs, select_fields)
    return runs


@router.patch("/threads/{thread_id}/runs/{run_id}", response_model=Run, responses={**NOT_FOUND})
async def update_run(
    thread_id: str,
    run_id: str,
    request: RunStatus,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Run:
    """Update a run's status.

    Primarily used to interrupt a running execution. Set `status` to
    `"interrupted"` to cooperatively stop the run.
    """
    run_orm = await session.scalar(
        select(RunORM).where(
            RunORM.run_id == str(run_id),
            RunORM.thread_id == thread_id,
            RunORM.user_id == user.identity,
        )
    )
    if not run_orm:
        raise HTTPException(404, f"Run '{run_id}' not found")

    # Handle interruption/cancellation
    # Validate status conforms to API specification
    validated_status = validate_run_status(request.status)

    if validated_status == "interrupted":
        logger.info("interrupting run", run_id=run_id, thread_id=thread_id, user_id=user.identity)
        # Handle interruption - use interrupt_run for cooperative interruption
        await streaming_service.interrupt_run(run_id)
        # cancel_requested makes the interrupt durable: the owning worker's
        # heartbeat honors it even if the pub/sub signal was lost.
        await session.execute(
            update(RunORM)
            .where(RunORM.run_id == str(run_id))
            .values(status="interrupted", cancel_requested=True, updated_at=datetime.now(UTC))
        )
        await session.commit()

    # Return final run state
    run_orm = await session.scalar(select(RunORM).where(RunORM.run_id == run_id))
    if not run_orm:
        raise HTTPException(404, f"Run '{run_id}' not found")
    # Refresh to ensure we have the latest data after our own update
    await session.refresh(run_orm)
    return Run.model_validate(run_orm)


@router.get("/threads/{thread_id}/runs/{run_id}/join", responses={**NOT_FOUND})
async def join_run(
    thread_id: str,
    run_id: str,
    user: User = Depends(get_current_user),
) -> StreamingResponse:
    """Wait for a run to complete and return its output.

    Returns a chunked ``application/json`` response. While the run is still
    executing, the server sends periodic ``\\n`` heartbeat bytes to keep the
    connection alive through proxies and load balancers (AWS ALB, Cloudflare,
    etc.). The final chunk is the JSON result. Leading whitespace is ignored
    by JSON parsers, so clients can parse the concatenated body normally.

    If the run is already in a terminal state, the output is returned
    immediately with no heartbeat overhead.

    Sessions are managed manually (not via ``Depends``) to avoid holding a
    pool connection during the long wait.
    """
    maker = get_session_maker()

    # Short-lived session: validate run exists and check terminal state
    async with maker() as session:
        run_orm = await session.scalar(
            select(RunORM).where(
                RunORM.run_id == str(run_id),
                RunORM.thread_id == thread_id,
                RunORM.user_id == user.identity,
            )
        )
        if not run_orm:
            raise HTTPException(404, f"Run '{run_id}' not found")

        if run_orm.status in TERMINAL_STATES:
            result = wrap_run_result(run_orm.status, run_orm.output, run_orm.error_message)
            return StreamingResponse(
                iter([encode_output(result)]),
                media_type="application/json",
            )

    return StreamingResponse(
        heartbeat_wait_body(
            run_id,
            thread_id,
            user.identity,
            timeout=settings.worker.BG_JOB_TIMEOUT_SECS,
        ),
        media_type="application/json",
        headers={
            "Location": f"/threads/{thread_id}/runs/{run_id}/join",
            "Content-Location": f"/threads/{thread_id}/runs/{run_id}",
        },
    )


@router.post("/threads/{thread_id}/runs/wait", responses={**NOT_FOUND, **CONFLICT})
async def wait_for_run(
    thread_id: str,
    request: RunCreate,
    user: User = Depends(get_current_user),
) -> StreamingResponse:
    """Create a run, execute it, and wait for completion.

    Returns a chunked ``application/json`` response with periodic ``\\n``
    heartbeat bytes to keep the connection alive. The final chunk is the
    JSON result. Uses ``BG_JOB_TIMEOUT_SECS`` (default 1 hour) as the
    safety-net timeout.

    Sessions are managed manually (not via ``Depends``) to avoid holding a
    pool connection during the long wait.
    """
    maker = get_session_maker()

    # Session block: all pre-execution DB work (validate, create run, submit)
    async with maker() as session:
        existing_thread = await session.scalar(select(ThreadORM).where(ThreadORM.thread_id == thread_id))
        if existing_thread and existing_thread.user_id != user.identity:
            raise HTTPException(404, f"Thread '{thread_id}' not found")

        await _authorize_run_creation(user, request, thread_id)

        run_id, _run, _job = await prepare_run(session, thread_id, request, user, initial_status="pending")

    # No pool connection held from here — safe for long waits
    return StreamingResponse(
        heartbeat_wait_body(
            run_id,
            thread_id,
            user.identity,
            timeout=settings.worker.BG_JOB_TIMEOUT_SECS,
        ),
        media_type="application/json",
        headers={
            "Location": f"/threads/{thread_id}/runs/{run_id}/join",
            "Content-Location": f"/threads/{thread_id}/runs/{run_id}",
        },
    )


_STREAM_MODES = frozenset(get_args(StreamMode))


def _parse_stream_modes(values: list[str] | None) -> frozenset[str] | None:
    """Validate the SDK's repeated ``stream_mode`` query param into an allowlist."""
    if not values:
        return None
    requested = [stripped for value in values for mode in value.split(",") if (stripped := mode.strip())]
    invalid = [mode for mode in requested if mode not in _STREAM_MODES]
    if invalid:
        raise HTTPException(422, f"Invalid stream mode: {invalid[0]}")
    return normalize_stream_modes(requested)


@router.get("/threads/{thread_id}/runs/{run_id}/stream", responses={**SSE_RESPONSE, **NOT_FOUND})
async def stream_run(
    thread_id: str,
    run_id: str,
    last_event_id: str | None = Header(None, alias="Last-Event-ID"),
    stream_mode: list[str] | None = Query(
        None, description="Narrow this connection to a subset of the run's stream modes."
    ),
    cancel_on_disconnect: bool = Query(False, description="Cancel the run when this client disconnects."),
    user: User = Depends(get_current_user),
) -> EventSourceResponse:
    """Stream an existing run's execution via SSE.

    Attach to a run that was created without streaming (e.g. via the create
    endpoint) to receive its events in real time. If the run has already
    finished, a single `end` event is emitted. Use the `Last-Event-ID`
    header to resume from a specific event after a disconnect.

    `stream_mode` must be a subset of the modes the run was created with;
    control events (`metadata`, `error`, `end`) are always delivered. By
    default a disconnect leaves the run alone — other consumers may still be
    attached via `/join` or another `/stream` — so pass
    `cancel_on_disconnect=true` to cancel it instead.

    A periodic SSE keepalive comment is sent every
    ``KEEPALIVE_INTERVAL_SECS`` so idle proxies don't drop attached streams.
    """
    modes = _parse_stream_modes(stream_mode)
    maker = get_session_maker()
    async with maker() as session:
        run_orm = await session.scalar(
            select(RunORM).where(
                RunORM.run_id == str(run_id),
                RunORM.thread_id == thread_id,
                RunORM.user_id == user.identity,
            )
        )
        if not run_orm:
            raise HTTPException(404, f"Run '{run_id}' not found")

        run_status = run_orm.status
        run_model = Run.model_validate(run_orm)

    # Terminal with no Last-Event-ID: nothing to replay, just close the stream.
    # With a Last-Event-ID, fall through so the buffer replays what was missed.
    if run_status in TERMINAL_STATES and not last_event_id:
        final_status = "error" if run_status == "error" else run_status

        async def generate_final() -> AsyncGenerator[str, None]:
            yield create_end_event(status=final_status)

        return make_sse_response(
            sse_to_bytes(generate_final()),
            headers={
                **get_sse_headers(),
                "Location": f"/threads/{thread_id}/runs/{run_id}/stream",
                "Content-Location": f"/threads/{thread_id}/runs/{run_id}",
            },
        )

    return make_sse_response(
        sse_to_bytes(streaming_service.stream_run_execution(run_model, last_event_id, stream_modes=modes)),
        close_handler=_cancel_handler(run_id) if cancel_on_disconnect else None,
        headers={
            **get_sse_headers(),
            "Location": f"/threads/{thread_id}/runs/{run_id}/stream",
            "Content-Location": f"/threads/{thread_id}/runs/{run_id}",
        },
    )


_THREAD_STREAM_MODES = frozenset({"run_modes", "lifecycle", "state_update"})


@router.get("/threads/{thread_id}/stream", responses={**SSE_RESPONSE, **NOT_FOUND})
async def join_thread_stream(
    thread_id: str,
    last_event_id: str | None = Header(None, alias="Last-Event-ID"),
    stream_mode: str | None = Query(
        None, description="Comma-separated ThreadStreamMode values (SDK sends 'stream_mode')."
    ),
    user: User = Depends(get_current_user),
) -> EventSourceResponse:
    """Join the stream of the thread's active (or most recent) run.

    Resolves the thread's in-flight run — falling back to the newest run —
    and attaches to its event stream with `Last-Event-ID` replay, exactly
    like the run-scoped stream endpoint. Only `run_modes` is served.
    """
    requested = [m.strip() for m in stream_mode.split(",")] if stream_mode else ["run_modes"]
    invalid = [m for m in requested if m not in _THREAD_STREAM_MODES]
    if invalid:
        raise HTTPException(422, f"Invalid stream mode: {invalid[0]}")
    if "run_modes" not in requested:
        raise HTTPException(422, "Only the 'run_modes' thread stream mode is supported")

    maker = get_session_maker()
    async with maker() as session:
        run_orm = await session.scalar(
            select(RunORM)
            .where(
                RunORM.thread_id == thread_id,
                RunORM.user_id == user.identity,
                RunORM.status.in_(("pending", "running")),
            )
            .order_by(RunORM.created_at.desc())
            .limit(1)
        )
        if run_orm is None:
            run_orm = await session.scalar(
                select(RunORM)
                .where(RunORM.thread_id == thread_id, RunORM.user_id == user.identity)
                .order_by(RunORM.created_at.desc())
                .limit(1)
            )
        if run_orm is None:
            raise HTTPException(404, f"Thread '{thread_id}' has no runs to stream")
        run_status = run_orm.status
        run_model = Run.model_validate(run_orm)

    if run_status in TERMINAL_STATES and not last_event_id:

        async def generate_final() -> AsyncGenerator[str, None]:
            yield create_end_event(status="error" if run_status == "error" else run_status)

        return make_sse_response(
            sse_to_bytes(generate_final()),
            headers={
                **get_sse_headers(),
                "Location": f"/threads/{thread_id}/stream",
                "Content-Location": f"/threads/{thread_id}/runs/{run_model.run_id}",
            },
        )

    return make_sse_response(
        sse_to_bytes(streaming_service.stream_run_execution(run_model, last_event_id)),
        headers={
            **get_sse_headers(),
            "Location": f"/threads/{thread_id}/stream",
            "Content-Location": f"/threads/{thread_id}/runs/{run_model.run_id}",
        },
    )


@router.post(
    "/threads/{thread_id}/runs/{run_id}/cancel",
    response_model=Run,
    responses={**NOT_FOUND},
)
async def cancel_run_endpoint(
    thread_id: str,
    run_id: str,
    wait: int = Query(0, ge=0, le=1, description="Set to 1 to wait for the run task to settle before returning."),
    action: CancelAction = Query(
        "interrupt",
        description=(
            "Cancellation strategy: 'interrupt' (default) for a cooperative "
            "interrupt that lets the graph save partial state, or 'rollback' to "
            "cancel then delete the run and the checkpoints it produced."
        ),
    ),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Run:
    """Cancel or interrupt a running execution.

    Use `action=interrupt` (default) to cooperatively interrupt so the graph can
    handle the interrupt and save partial state, or `action=rollback` to cancel
    and then discard the run record plus its checkpoints. Set `wait=1` to block
    until the background task has fully settled before returning the updated run.
    """
    run_orm = await session.scalar(
        select(RunORM).where(
            RunORM.run_id == run_id,
            RunORM.thread_id == thread_id,
            RunORM.user_id == user.identity,
        )
    )
    if not run_orm:
        raise HTTPException(404, f"Run '{run_id}' not found")

    logger.info("cancelling run", action=action, run_id=run_id, thread_id=thread_id, user_id=user.identity)
    await signal_cancel(session, [run_id], action)

    # Rollback must always settle first: deleting checkpoints while the executor
    # is still finalizing would let it write rows back afterwards.
    if wait or action == "rollback":
        await wait_for_settle(session, [run_id])

    # Reload the settled snapshot (also what a rollback returns post-delete).
    session.expire_all()
    run_orm = await session.scalar(
        select(RunORM).where(
            RunORM.run_id == run_id,
            RunORM.thread_id == thread_id,
            RunORM.user_id == user.identity,
        )
    )
    if not run_orm:
        raise HTTPException(404, f"Run '{run_id}' not found after cancellation")
    run = Run.model_validate(run_orm)

    if action == "rollback":
        await rollback_runs(session, [run_id])
    return run


# response_model=None: with `select` the items are partial dicts.
@router.post("/runs/search", response_model=None)
async def search_runs(
    request: RunSearchRequest,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[Run] | list[dict[str, Any]]:
    """Search runs across every thread the caller owns.

    Filter by assistant, thread, run id, status, metadata, or a `created_after` /
    `created_before` window; each filter takes a scalar or a list. Results are
    paginated via `limit`/`offset`, ordered by `sort_by`/`sort_order` (default
    `created_at` descending), and `select` projects fields.

    Callers holding `runs:search:all` search every user's runs instead — for
    platforms aggregating runs they don't own. Scope comes from the permission
    alone; no request field can widen it.
    """
    ctx = build_auth_context(user, "runs", "search")
    auth_filters = await handle_event(ctx, request.model_dump(exclude_none=True))

    sort_column = getattr(RunORM, request.sort_by) if request.sort_by else RunORM.created_at
    direction = sort_column.asc() if request.sort_order == "asc" else sort_column.desc()
    stmt = (
        select(RunORM)
        .where(*build_run_filters(request, user, auth_filters))
        # Secondary sort keeps offset pagination stable when the primary key ties.
        .order_by(direction, RunORM.run_id.asc())
        .offset(request.offset)
        .limit(request.limit)
    )
    runs = [Run.model_validate(row) for row in (await session.scalars(stmt)).all()]
    if not request.select:
        return runs
    return project_runs(runs, request.select)


@router.post("/runs/count", response_model=int)
async def count_runs(
    request: RunSearchRequest,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> int:
    """Count runs matching the same filters as `/runs/search`."""
    ctx = build_auth_context(user, "runs", "search")
    auth_filters = await handle_event(ctx, request.model_dump(exclude_none=True))

    where = build_run_filters(request, user, auth_filters)
    stmt = select(func.count()).select_from(RunORM).where(*where)
    return await session.scalar(stmt) or 0


@router.post("/runs/cancel", status_code=204)
async def cancel_runs_bulk(
    request: RunsCancelRequest,
    action: CancelAction = Query(
        "interrupt",
        description="'interrupt' marks runs interrupted; 'rollback' also deletes them and their checkpoints.",
    ),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Response:
    """Bulk-cancel runs by status, or by thread_id + run_ids."""
    where = [RunORM.user_id == user.identity]
    by_status = request.status is not None
    if by_status:
        statuses = ("pending", "running") if request.status == "all" else (request.status,)
        where.append(RunORM.status.in_(statuses))
    else:
        where.append(RunORM.thread_id == request.thread_id)
        where.append(RunORM.run_id.in_(request.run_ids or []))
    rows = list((await session.scalars(select(RunORM).where(*where))).all())
    if not rows:
        # A status sweep is a set operation: "nothing matched" means the caller's
        # intent already holds, so it succeeds. Named run_ids that don't exist are
        # a genuine 404.
        if by_status:
            return Response(status_code=204)
        raise HTTPException(404, "No runs found to cancel")

    run_ids = [row.run_id for row in rows]
    await signal_cancel(session, run_ids, action)

    if action == "rollback":
        # Settle before deleting so a finalizing executor cannot write rows back.
        await wait_for_settle(session, run_ids)
        await rollback_runs(session, run_ids)
    return Response(status_code=204)


@router.delete(
    "/threads/{thread_id}/runs/{run_id}",
    status_code=204,
    responses={**NOT_FOUND, **CONFLICT},
)
async def delete_run(
    thread_id: str,
    run_id: str,
    force: int = Query(0, ge=0, le=1, description="Set to 1 to cancel an active run before deleting it."),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> None:
    """Delete a run record.

    If the run is active (pending or running) and `force=0`, returns 409
    Conflict. Set `force=1` to cancel the run first (best-effort) and then
    delete it. Returns 204 No Content on success.
    """
    # Authorization check (delete action on runs resource)
    ctx = build_auth_context(user, "runs", "delete")
    value = {"run_id": run_id, "thread_id": thread_id}
    await handle_event(ctx, value)
    run_orm = await session.scalar(
        select(RunORM).where(
            RunORM.run_id == str(run_id),
            RunORM.thread_id == thread_id,
            RunORM.user_id == user.identity,
        )
    )
    if not run_orm:
        raise HTTPException(404, f"Run '{run_id}' not found")

    # If active and not forcing, reject deletion
    if run_orm.status in ["pending", "running"] and not force:
        raise HTTPException(
            status_code=409,
            detail="Run is active. Retry with force=1 to cancel and delete.",
        )

    # If forcing and active, cancel first
    if force and run_orm.status in ["pending", "running"]:
        logger.info("force-cancelling active run before delete", run_id=run_id, thread_id=thread_id)
        await streaming_service.cancel_run(run_id)
        # Best-effort: wait for bg task to settle
        task = active_runs.get(run_id)
        if task:
            await drain_task(task, run_id)

    # Delete the record
    await session.execute(
        delete(RunORM).where(
            RunORM.run_id == str(run_id),
            RunORM.thread_id == thread_id,
            RunORM.user_id == user.identity,
        )
    )
    await session.commit()

    # Clean up active task if exists
    task = active_runs.pop(run_id, None)
    if task and not task.done():
        task.cancel()

    # 204 No Content
    return
