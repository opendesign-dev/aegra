"""Thread record endpoints — CRUD, search, prune, copy.

The checkpointed graph state behind a thread is served by ``api/thread_state``.
"""

import contextlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from aegra_api.core.active_runs import active_runs, drain_task
from aegra_api.core.auth_deps import auth_dependency, get_current_user
from aegra_api.core.auth_filters import THREADS_SEARCH_ALL, build_metadata_filter, build_visibility_filters
from aegra_api.core.auth_handlers import build_auth_context, handle_event
from aegra_api.core.database import db_manager
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import ThreadState as ThreadStateORM
from aegra_api.core.orm import get_session
from aegra_api.models import (
    Thread,
    ThreadCreate,
    ThreadList,
    ThreadSearchRequest,
    ThreadUpdate,
    User,
)
from aegra_api.models.errors import CONFLICT, NOT_FOUND
from aegra_api.models.threads import ThreadPruneRequest
from aegra_api.services.run_cleanup import delete_thread_by_id, delete_thread_checkpoints
from aegra_api.services.streaming_service import streaming_service
from aegra_api.services.thread_state_service import refresh_materialized_state
from aegra_api.utils.extract import extract_path_value, validate_extract

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig

router = APIRouter(tags=["Threads"], dependencies=auth_dependency)
logger = structlog.getLogger(__name__)


# --- Sort resolution for /threads/search ---

_DEFAULT_SORT_FIELD = "created_at"
_DEFAULT_SORT_ASC = False


def _resolve_sort(request: ThreadSearchRequest) -> tuple[Any, bool]:
    """Resolve (ORM column, is_ascending) for /threads/search.

    ``sort_by`` is Pydantic-validated against the column Literal;
    ``state_updated_at`` maps to updated_at (values materialize on finalize,
    which touches updated_at in the same transaction).
    """
    if request.sort_by:
        field = "updated_at" if request.sort_by == "state_updated_at" else request.sort_by
        asc = (request.sort_order or "desc").lower() == "asc"
        return getattr(ThreadORM, field), asc
    return getattr(ThreadORM, _DEFAULT_SORT_FIELD), _DEFAULT_SORT_ASC


# --- Helper for safe ORM -> Pydantic conversion (Test/Mock compatible) ---


def _serialize_thread(
    thread_orm: ThreadORM,
    default_metadata: dict[str, Any] | None = None,
    *,
    include_ttl: bool = False,
    state: ThreadStateORM | None = None,
) -> Thread:
    """
    Safely converts ThreadORM to Thread model using dictionary construction.
    This handles None values and MagicMocks that appear in tests, preventing
    Pydantic V2 ValidationErrors.
    """

    def _coerce_str(val: Any, default: str) -> str:
        try:
            s = str(val)
            # Handle MagicMock objects in tests converting to strings like "<MagicMock...>"
            return default if "MagicMock" in s else s
        except Exception:
            return default

    def _coerce_dict(val: Any, default: dict[str, Any]) -> dict[str, Any]:
        if val is None:
            return default
        if isinstance(val, dict):
            return val
        # Try to convert dict-like objects (mocks)
        with contextlib.suppress(Exception):
            if hasattr(val, "items"):
                return dict(val.items())
        return default

    # 1. ID
    t_id = _coerce_str(getattr(thread_orm, "thread_id", None), "unknown")

    # 2. Status
    status = _coerce_str(getattr(thread_orm, "status", "idle"), "idle")

    # 3. User ID
    u_id = _coerce_str(getattr(thread_orm, "user_id", ""), "")

    # 4. Metadata (map metadata_json -> metadata)
    # Use provided default if ORM is None (e.g. during creation before refresh)
    meta_source = getattr(thread_orm, "metadata_json", None)
    if meta_source is None and default_metadata is not None:
        meta_source = default_metadata
    metadata = _coerce_dict(meta_source, {})

    # 5. Timestamps (Default to NOW if None/Mock fails)
    c_at = getattr(thread_orm, "created_at", None)
    if not isinstance(c_at, datetime):
        c_at = datetime.now(UTC)

    u_at = getattr(thread_orm, "updated_at", None)
    if not isinstance(u_at, datetime):
        u_at = datetime.now(UTC)

    # Latest state lives in thread_state (1:1); present only when the caller joined it.
    values = getattr(state, "values", None) if state is not None else None
    interrupts = getattr(state, "interrupts", None) if state is not None else None
    ttl = getattr(thread_orm, "ttl", None) if include_ttl else None

    # Validate from dict (more robust than validate(orm_obj) for partial mocks)
    return Thread.model_validate(
        {
            "thread_id": t_id,
            "status": status,
            "metadata": metadata,
            "values": values if isinstance(values, dict) else None,
            "interrupts": interrupts if isinstance(interrupts, dict) else None,
            "ttl": ttl if isinstance(ttl, dict) else None,
            "user_id": u_id,
            "created_at": c_at,
            "updated_at": u_at,
        }
    )


async def _apply_supersteps(
    session: AsyncSession,
    thread: ThreadORM,
    supersteps: list[dict[str, Any]],
    user: User,
) -> None:
    """Seed a new thread by applying superstep state updates in order.

    Each superstep item is ``{"updates": [{"values", "as_node"?}]}`` (SDK
    shape). Requires the thread to carry a ``graph_id`` so state updates can
    resolve the graph's channels.
    """
    graph_id = (thread.metadata_json or {}).get("graph_id")
    if not graph_id:
        raise HTTPException(422, "supersteps require a graph_id on the thread")

    from aegra_api.services.langgraph_service import create_thread_config, get_langgraph_service

    service = get_langgraph_service()
    raw_config = create_thread_config(thread.thread_id, user)
    config = cast("RunnableConfig", raw_config)
    async with service.get_graph(graph_id, config=raw_config, access_context="threads.update", user=user) as agent:
        agent = agent.with_config(config)
        for step in supersteps:
            updates = step.get("updates") or []
            for item in updates:
                if item.get("command") is not None:
                    raise HTTPException(422, "supersteps with 'command' are not supported")
                await agent.aupdate_state(config, item.get("values"), as_node=item.get("as_node"))
    await refresh_materialized_state(session, thread, user)


# --- Endpoints ---


@router.post("/threads", response_model=Thread, responses={**CONFLICT})
async def create_thread(
    request: ThreadCreate,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Thread:
    """Create a new conversation thread.

    Threads hold conversation state and checkpoint history. Provide a
    `thread_id` for idempotent creation, or let the server generate one.
    Set `if_exists` to `"do_nothing"` to return the existing thread when the
    ID already exists instead of raising a 409 conflict.
    """
    # Authorization check
    ctx = build_auth_context(user, "threads", "create")
    value = request.model_dump()
    filters = await handle_event(ctx, value)

    # If handler modified metadata, update request
    if filters and "metadata" in filters:
        handler_meta = filters["metadata"]
        if isinstance(handler_meta, dict):
            request.metadata = {**(request.metadata or {}), **handler_meta}
    elif value.get("metadata"):
        # Handler may have modified value dict directly
        handler_meta = value["metadata"]
        if isinstance(handler_meta, dict):
            request.metadata = {**(request.metadata or {}), **handler_meta}

    thread_id = request.thread_id or str(uuid4())

    if request.thread_id:
        existing_stmt = select(ThreadORM).where(
            ThreadORM.thread_id == thread_id,
            ThreadORM.user_id == user.identity,
        )
        existing = await session.scalar(existing_stmt)

        if existing:
            if request.if_exists == "do_nothing":
                return _serialize_thread(existing)
            else:
                raise HTTPException(409, f"Thread '{thread_id}' already exists")

    metadata = request.metadata or {}
    # Always enforce owner from authenticated user
    metadata["owner"] = user.identity
    # Preserve client-provided values; only set defaults if missing.
    metadata.setdefault("assistant_id", None)
    metadata.setdefault("graph_id", request.graph_id)
    metadata.setdefault("thread_name", "")

    thread_orm = ThreadORM(
        thread_id=thread_id,
        status="idle",
        metadata_json=metadata,
        ttl=request.ttl,
        user_id=user.identity,
    )

    session.add(thread_orm)
    await session.commit()

    if request.supersteps:
        await _apply_supersteps(session, thread_orm, request.supersteps, user)

    with contextlib.suppress(Exception):
        await session.refresh(thread_orm)

    # Pass metadata explicitly in case refresh failed (tests/mocks)
    return _serialize_thread(thread_orm, default_metadata=metadata)


@router.get("/threads", response_model=ThreadList)
async def list_threads(
    user: User = Depends(get_current_user), session: AsyncSession = Depends(get_session)
) -> ThreadList:
    """List all threads owned by the authenticated user.

    Returns every thread without filtering. Use the search endpoint for
    filtered queries.
    """
    # Authorization check (search action for listing)
    ctx = build_auth_context(user, "threads", "search")
    value = {}
    filters = await handle_event(ctx, value)

    # Build query with filters if provided
    stmt = select(ThreadORM).where(ThreadORM.user_id == user.identity)
    if filters:
        # Apply filters from authorization handler
        # For now, we'll apply user_id filter which is already there
        # Additional filters can be added here based on handler response
        pass
    result = await session.scalars(stmt)
    rows = result.all()

    # Use safe serialization
    user_threads = [_serialize_thread(t) for t in rows]
    return ThreadList(threads=user_threads, total=len(user_threads))


@router.get("/threads/{thread_id}", response_model=Thread, responses={**NOT_FOUND})
async def get_thread(
    thread_id: str,
    include: list[str] | None = Query(None, description="Extra fields to include; supports 'ttl'."),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Thread:
    """Get a thread by its ID.

    Returns 404 if the thread does not exist or does not belong to the
    authenticated user.
    """
    # Authorization check
    ctx = build_auth_context(user, "threads", "read")
    value = {"thread_id": thread_id}
    await handle_event(ctx, value)

    stmt = select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user.identity)
    thread = await session.scalar(stmt)
    if not thread:
        raise HTTPException(404, f"Thread '{thread_id}' not found")

    # The SDK sends include as a comma-joined single param.
    include_parts = {p for raw in include or [] for p in raw.split(",") if p}
    # Latest values/interrupts live in thread_state (1:1); fetch for the detail view.
    state = await session.scalar(select(ThreadStateORM).where(ThreadStateORM.thread_id == thread_id))
    return _serialize_thread(thread, include_ttl="ttl" in include_parts, state=state)


@router.patch("/threads/{thread_id}", response_model=None, responses={**NOT_FOUND})
async def update_thread(
    thread_id: str,
    request: ThreadUpdate,
    prefer: str | None = Header(None, description="Set to 'return=minimal' for a 204 with no body."),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Thread | Response:
    """Update a thread's metadata.

    Merges the provided metadata with the existing metadata (shallow merge).
    Send `Prefer: return=minimal` to get a 204 with no body instead of the
    updated thread (the SDK's `return_minimal=True`).
    """
    # Authorization check
    ctx = build_auth_context(user, "threads", "update")
    value = {**request.model_dump(), "thread_id": thread_id}
    filters = await handle_event(ctx, value)

    # If handler modified metadata, update request
    if filters and "metadata" in filters:
        handler_meta = filters["metadata"]
        if isinstance(handler_meta, dict):
            request.metadata = {**(request.metadata or {}), **handler_meta}
    elif value.get("metadata"):
        handler_meta = value["metadata"]
        if isinstance(handler_meta, dict):
            request.metadata = {**(request.metadata or {}), **handler_meta}

    stmt = select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user.identity)
    thread = await session.scalar(stmt)

    if not thread:
        raise HTTPException(404, f"Thread '{thread_id}' not found")

    thread.updated_at = datetime.now(UTC)

    if request.metadata:
        current_metadata = dict(thread.metadata_json or {})
        current_metadata.update(request.metadata)
        thread.metadata_json = current_metadata
    if isinstance(request.ttl, dict):
        # The Pydantic validator normalizes int minutes to the config dict.
        thread.ttl = request.ttl

    await session.commit()

    if _wants_minimal(prefer):
        return Response(status_code=204)

    await session.refresh(thread)
    return _serialize_thread(thread, include_ttl=request.ttl is not None)


def _wants_minimal(prefer: str | None) -> bool:
    """Whether a Prefer header asks for the no-body form (RFC 7240)."""
    if not prefer:
        return False
    return any(token.strip().replace(" ", "").lower() == "return=minimal" for token in prefer.split(","))


@router.delete("/threads/{thread_id}", responses={**NOT_FOUND})
async def delete_thread(
    thread_id: str,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    """Delete a thread by its ID.

    Permanently removes everything the thread owns: its runs and materialized
    state (via cascade) plus its checkpoints, blobs, and pending writes. Any
    active runs are cancelled first.

    No opt-out, matching the SDK's ``threads.delete(thread_id)`` — it takes no
    cascade flag, so the delete is all-or-nothing.
    """
    # Authorization check
    ctx = build_auth_context(user, "threads", "delete")
    value = {"thread_id": thread_id}
    await handle_event(ctx, value)

    stmt = select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user.identity)
    thread = await session.scalar(stmt)
    if not thread:
        raise HTTPException(404, f"Thread '{thread_id}' not found")

    active_runs_stmt = select(RunORM).where(
        RunORM.thread_id == thread_id,
        RunORM.user_id == user.identity,
        RunORM.status.in_(["pending", "running"]),
    )
    active_runs_list = (await session.scalars(active_runs_stmt)).all()

    if active_runs_list:
        logger.info("cancelling active runs before thread delete", count=len(active_runs_list), thread_id=thread_id)
        for run in active_runs_list:
            run_id = run.run_id
            await streaming_service.cancel_run(run_id)
            task = active_runs.pop(run_id, None)
            if task and not task.done():
                task.cancel()
                await drain_task(task, run_id)

    await session.delete(thread)
    await session.commit()

    # After the commit: the checkpointer tables have no FK to thread, so cascade
    # never reaches them and the blobs would keep the conversation state alive.
    await delete_thread_checkpoints(thread_id)

    return {"status": "deleted"}


def _wants_state(request: ThreadSearchRequest) -> bool:
    """Whether the search needs thread_state joined (values filter / projection)."""
    if request.values or request.extract:
        return True
    return bool(request.select) and bool({"values", "interrupts"} & set(request.select))


def _build_thread_filters(
    request: ThreadSearchRequest, user: User, auth_filters: dict[str, Any] | None = None
) -> list[Any]:
    """Shared WHERE predicates for /threads/search and /threads/count.

    Scope comes from ``threads:search:all``; the ``values`` filter targets
    ``thread_state`` (the caller joins it when ``_wants_state`` is true).
    """
    where: list[ColumnElement[bool]] = build_visibility_filters(ThreadORM.user_id, user, THREADS_SEARCH_ALL)
    if request.status:
        where.append(ThreadORM.status == request.status)
    if request.metadata:
        # JSONB containment: type-correct, deep-nested, GIN-indexable. Mirrors
        # AssistantService.search_assistants for cross-endpoint consistency.
        where.append(ThreadORM.metadata_json.op("@>")(request.metadata))
    if request.values:
        where.append(ThreadStateORM.values.op("@>")(request.values))
    if request.ids:
        where.append(ThreadORM.thread_id.in_(request.ids))
    # Served by idx_thread_user_created (user_id, created_at DESC).
    if request.created_after is not None:
        where.append(ThreadORM.created_at >= request.created_after)
    if request.created_before is not None:
        where.append(ThreadORM.created_at <= request.created_before)
    # Compiled as its own predicate, not folded into request.metadata: the handler
    # may return flat constraints or $or/$contains operators, which a plain dict
    # merge would silently drop.
    auth_filter = build_metadata_filter(ThreadORM.metadata_json, auth_filters)
    if auth_filter is not None:
        where.append(auth_filter)
    return where


# response_model=None: with `select`/`extract` items become partial dicts.
@router.post("/threads/search", response_model=None)
async def search_threads(
    request: ThreadSearchRequest,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[Thread] | list[dict[str, Any]]:
    """Search threads with auth_filters.

    Filter by status, metadata, latest state values, or explicit ids. Results
    are paginated via `limit`/`offset`; `select` projects fields and `extract`
    adds keys pulled from values/metadata via dot/bracket paths.
    """
    # Authorization check
    ctx = build_auth_context(user, "threads", "search")
    value = request.model_dump()
    auth_filters = await handle_event(ctx, value)

    extract = validate_extract(request.extract) if request.extract else None

    # Only join thread_state when the query actually needs values (filter/projection);
    # plain list/search scans the narrow thread table without the large state blob.
    need_state = _wants_state(request)
    if need_state:
        stmt = (
            select(ThreadORM, ThreadStateORM)
            .outerjoin(ThreadStateORM, ThreadStateORM.thread_id == ThreadORM.thread_id)
            .where(*_build_thread_filters(request, user, auth_filters))
        )
    else:
        stmt = select(ThreadORM).where(*_build_thread_filters(request, user, auth_filters))
    offset = request.offset or 0
    limit = request.limit or 20
    column, asc = _resolve_sort(request)
    direction = column.asc() if asc else column.desc()
    # Secondary sort on thread_id keeps offset pagination stable when the
    # primary sort key has duplicates (status buckets, microsecond ties).
    stmt = stmt.order_by(direction, ThreadORM.thread_id.asc()).offset(offset).limit(limit)

    if need_state:
        threads_models = [_serialize_thread(t, state=s) for t, s in (await session.execute(stmt)).all()]
    else:
        threads_models = [_serialize_thread(t) for t in (await session.scalars(stmt)).all()]

    if not request.select and not extract:
        return threads_models

    wanted = set(request.select) if request.select else None
    projected: list[dict[str, Any]] = []
    for model in threads_models:
        data = model.model_dump(mode="json")
        row = {k: v for k, v in data.items() if k in wanted} if wanted else dict(data)
        if extract:
            for alias, path in extract.items():
                row[alias] = extract_path_value(data, path)
        projected.append(row)
    return projected


@router.post("/threads/count", response_model=int)
async def count_threads(
    request: ThreadSearchRequest,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> int:
    """Count threads matching the given auth_filters.

    Accepts the same auth_filters as `/threads/search` (status, metadata, values,
    ids) but returns only the total count.
    """
    ctx = build_auth_context(user, "threads", "search")
    value = request.model_dump()
    auth_filters = await handle_event(ctx, value)

    stmt = select(func.count()).select_from(ThreadORM)
    if request.values:  # values filter targets thread_state
        stmt = stmt.join(ThreadStateORM, ThreadStateORM.thread_id == ThreadORM.thread_id)
    stmt = stmt.where(*_build_thread_filters(request, user, auth_filters))
    return await session.scalar(stmt) or 0


@router.post("/threads/prune")
async def prune_threads(
    request: ThreadPruneRequest,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> dict[str, int]:
    """Prune threads by id.

    ``delete`` removes each thread entirely (runs, checkpoints, row);
    ``keep_latest`` keeps the thread but drops all but the newest checkpoint
    per namespace. Missing/unowned threads are skipped, not errors.
    """
    ctx = build_auth_context(user, "threads", "delete")
    await handle_event(ctx, {"thread_ids": request.thread_ids, "strategy": request.strategy})

    if not request.thread_ids:
        return {"pruned_count": 0}

    owned = list(
        (
            await session.scalars(
                select(ThreadORM.thread_id).where(
                    ThreadORM.thread_id.in_(request.thread_ids),
                    ThreadORM.user_id == user.identity,
                )
            )
        ).all()
    )
    if request.strategy == "keep_latest":
        await db_manager.get_checkpointer().aprune_keep_latest(owned)
        return {"pruned_count": len(owned)}

    pruned = 0
    for thread_id in owned:
        try:
            await delete_thread_by_id(thread_id, user.identity)
            pruned += 1
        except HTTPException as exc:
            # Reference behavior: skip silently, count only successes.
            logger.debug("Prune skipped thread", thread_id=thread_id, detail=exc.detail)
    return {"pruned_count": pruned}


@router.post("/threads/{thread_id}/copy", response_model=Thread, responses={**NOT_FOUND})
async def copy_thread(
    thread_id: str,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> Thread:
    """Copy a thread and its full checkpoint history into a new thread.

    Creates a new idle thread owned by the caller, duplicating the source
    thread's metadata and every checkpoint. The original is unchanged. Cost is
    O(checkpoint history) — avoid on very large threads.
    """
    ctx = build_auth_context(user, "threads", "create")
    await handle_event(ctx, {"thread_id": thread_id})

    src = await session.scalar(
        select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user.identity)
    )
    if not src:
        raise HTTPException(404, f"Thread '{thread_id}' not found")

    new_thread_id = str(uuid4())
    new_thread = ThreadORM(
        thread_id=new_thread_id,
        status="idle",
        metadata_json=dict(src.metadata_json or {}),
        user_id=user.identity,
    )
    session.add(new_thread)
    # Carry the materialized state over too (checkpointer rows copied below).
    src_state = await session.scalar(select(ThreadStateORM).where(ThreadStateORM.thread_id == thread_id))
    if src_state is not None:
        session.add(
            ThreadStateORM(
                thread_id=new_thread_id,
                values=src_state.values,
                interrupts=src_state.interrupts,
                values_hash=src_state.values_hash,
            )
        )
    await session.commit()

    await _copy_thread_checkpoints(thread_id, new_thread_id)

    return _serialize_thread(new_thread)


# Explicit column lists — the saver has no copy API, so we duplicate its rows
# directly (same coupling as adelete_thread) with the thread_id remapped.
_COPY_STATEMENTS = (
    "INSERT INTO checkpoints (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata) "
    "SELECT %s, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata "
    "FROM checkpoints WHERE thread_id = %s",
    "INSERT INTO checkpoint_blobs (thread_id, checkpoint_ns, channel, version, type, blob) "
    "SELECT %s, checkpoint_ns, channel, version, type, blob "
    "FROM checkpoint_blobs WHERE thread_id = %s",
    "INSERT INTO checkpoint_writes "
    "(thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob) "
    "SELECT %s, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob "
    "FROM checkpoint_writes WHERE thread_id = %s",
)


async def _copy_thread_checkpoints(src_thread_id: str, dst_thread_id: str) -> None:
    """Duplicate all checkpoint rows from src to dst thread."""
    pool = db_manager.lg_pool
    if pool is None:
        raise HTTPException(503, "Checkpoint store not initialized")
    # One transaction so a partial failure never leaves half-copied checkpoints,
    # regardless of the pool's autocommit setting.
    async with pool.connection() as conn, conn.transaction():
        for stmt in _COPY_STATEMENTS:
            await conn.execute(stmt, (dst_thread_id, src_thread_id))
