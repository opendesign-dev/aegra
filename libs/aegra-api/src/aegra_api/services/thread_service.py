"""Thread business logic.

Mirrors assistant_service / cron_service: routes keep HTTP concerns, auth
dispatch goes through Authenticated._dispatch, and the SQL-level
``user_id == user.identity`` predicate is the tenant boundary
(GHSA-m98r-6667-4wq7).

Scope: the thread entity itself — CRUD, search, copy, prune, and the latest-state
cache. The checkpoint-snapshot endpoints (state, history) stay in the route layer;
they are views over the checkpointer rather than over a database row.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import structlog
from fastapi import Depends, HTTPException
from langchain_core.runnables import RunnableConfig
from sqlalchemy import Select, delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from aegra_api.core.auth_deps import get_current_user
from aegra_api.core.database import db_manager
from aegra_api.core.orm import Assistant as AssistantORM
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import ThreadStateCache as ThreadStateCacheORM
from aegra_api.core.orm import get_session
from aegra_api.core.query import build_order_by, paginate
from aegra_api.core.scoping import SYSTEM_IDENTITY, read_scope
from aegra_api.models.auth import User
from aegra_api.models.threads import (
    Superstep,
    ThreadCreate,
    ThreadPruneRequest,
    ThreadSearchRequest,
    ThreadUpdate,
)
from aegra_api.services.authenticated import Authenticated
from aegra_api.services.graph_factory import AccessContext
from aegra_api.services.langgraph_service import (
    create_thread_config,
    get_langgraph_service,
    inject_user_context,
)
from aegra_api.services.thread_state_cache import as_pair, discard, extract, materialize, read, store
from aegra_api.utils.run_utils import _merge_jsonb

logger = structlog.getLogger(__name__)

# ``state_updated_at`` lives on the cache table, every other sort key on thread.
_STATE_SORT_KEY = "state_updated_at"


def graph_id_of(thread: ThreadORM) -> str | None:
    """The graph a thread is bound to, or None while it is still unbound."""
    return (thread.metadata_json or {}).get("graph_id")


async def _assistant_config(session: AsyncSession, assistant_id: str, user: User) -> dict[str, Any]:
    """Fallback for threads bound before ``thread.config`` existed: today's assistant config.

    Not equivalent — an assistant edited since the run yields a config the checkpoint was
    never written with — but closer than dropping the assistant's config entirely.
    """
    assistant = await session.scalar(
        select(AssistantORM).where(
            AssistantORM.assistant_id == assistant_id,
            or_(AssistantORM.user_id == user.identity, AssistantORM.user_id == SYSTEM_IDENTITY),
        )
    )
    return (assistant.config or {}) if assistant is not None else {}


async def thread_graph_config(session: AsyncSession, thread: ThreadORM, user: User) -> RunnableConfig:
    """The config a thread's graph must be loaded with, on reads as much as runs.

    A factory graph that branches on ``configurable`` compiles a different node set
    without it, and LangGraph re-derives ``tasks`` / ``interrupts`` / ``next`` from the
    loaded nodes — so a read that skips it silently reports no pending interrupt for a
    thread that is paused on one.
    """
    assistant_id = (thread.metadata_json or {}).get("assistant_id")
    if not assistant_id:
        return create_thread_config(thread.thread_id, user)

    bound = thread.config or await _assistant_config(session, str(assistant_id), user)
    config = _merge_jsonb(bound)
    configurable = dict(config.get("configurable") or {})
    # Forced like create_run_config does: a stored config must not redirect the read to
    # another thread or assistant.
    configurable["thread_id"] = thread.thread_id
    configurable["assistant_id"] = str(assistant_id)
    config["configurable"] = configurable
    return inject_user_context(user, config)


class ThreadService(Authenticated):
    """Thread CRUD, search, and latest-state access."""

    resource = "threads"

    async def _owned(self, thread_id: str) -> ThreadORM:
        thread = await self.session.scalar(self._scoped().where(ThreadORM.thread_id == thread_id))
        if not thread:
            raise HTTPException(404, f"Thread '{thread_id}' not found")
        return thread

    def _scoped(self) -> Select[tuple[ThreadORM]]:
        """Rows the caller owns — the gate for fetching or mutating one thread."""
        return select(ThreadORM).where(ThreadORM.user_id == self.user.identity)

    def _readable(self) -> ColumnElement[bool]:
        """Rows the caller may list; reaches past ownership only with ``threads:read_all``."""
        return read_scope(ThreadORM.user_id, self.user, resource="threads")

    def _merge_handler_metadata(self, request: ThreadCreate | ThreadUpdate, value: dict[str, Any]) -> None:
        """Fold metadata an ``@auth.on`` handler injected back into the request."""
        injected = value.get("metadata")
        if isinstance(injected, dict):
            request.metadata = {**(request.metadata or {}), **injected}

    async def create(self, request: ThreadCreate) -> ThreadORM:
        """Create a thread; ``if_exists='do_nothing'`` returns the existing one."""
        value = request.model_dump()
        filters = await self._dispatch("create", value)
        self._merge_handler_metadata(request, {**value, **(filters or {})})

        thread_id = request.thread_id or str(uuid4())
        if request.thread_id:
            existing = await self.session.scalar(self._scoped().where(ThreadORM.thread_id == thread_id))
            if existing:
                if request.if_exists == "do_nothing":
                    return existing
                raise HTTPException(409, f"Thread '{thread_id}' already exists")

        metadata = dict(request.metadata or {})
        # Ownership always comes from the authenticated identity, never the body.
        metadata["owner"] = self.user.identity
        metadata.setdefault("assistant_id", None)
        metadata.setdefault("graph_id", None)
        metadata.setdefault("thread_name", "")

        thread = ThreadORM(
            thread_id=thread_id,
            status="idle",
            metadata_json=metadata,
            ttl=request.ttl.model_dump() if request.ttl else None,
            user_id=self.user.identity,
        )
        self.session.add(thread)
        await self.session.commit()
        return thread

    async def get(self, thread_id: str) -> ThreadORM:
        await self._dispatch("read", {"thread_id": thread_id})
        return await self._owned(thread_id)

    async def update(self, thread_id: str, request: ThreadUpdate) -> ThreadORM:
        """Shallow-merge metadata; replace ``ttl`` outright when supplied."""
        value = {**request.model_dump(), "thread_id": thread_id}
        filters = await self._dispatch("update", value)
        self._merge_handler_metadata(request, {**value, **(filters or {})})

        thread = await self._owned(thread_id)
        thread.updated_at = datetime.now(UTC)
        if request.metadata:
            thread.metadata_json = {**(thread.metadata_json or {}), **request.metadata}
        if request.ttl is not None:
            thread.ttl = request.ttl.model_dump()

        await self.session.commit()
        await self.session.refresh(thread)
        return thread

    async def list_all(self) -> list[ThreadORM]:
        """Every thread the caller may read, unpaginated — matches assistants.list."""
        await self._dispatch("search", {})
        return list((await self.session.scalars(select(ThreadORM).where(self._readable()))).all())

    # --- search ---

    def _filtered(self, stmt: Select[Any], request: ThreadSearchRequest, filters: dict[str, Any] | None) -> Select[Any]:
        if request.status:
            stmt = stmt.where(ThreadORM.status == request.status)
        if request.thread_id is not None:
            stmt = stmt.where(ThreadORM.thread_id.in_(request.thread_id))
        for predicate in request.time_predicates(ThreadORM.created_at, ThreadORM.updated_at):
            stmt = stmt.where(predicate)

        metadata = dict(request.metadata or {})
        handler_meta = (filters or {}).get("metadata")
        if isinstance(handler_meta, dict):
            metadata.update(handler_meta)
        if metadata:
            # JSONB containment, served by idx_thread_metadata_gin.
            stmt = stmt.where(ThreadORM.metadata_json.op("@>")(metadata))

        on = ThreadStateCacheORM.thread_id == ThreadORM.thread_id
        if request.values:
            # Inner join: an unmaterialized thread cannot match a state filter.
            stmt = stmt.join(ThreadStateCacheORM, on).where(ThreadStateCacheORM.values.op("@>")(request.values))
        elif request.sort_by == _STATE_SORT_KEY:
            stmt = stmt.outerjoin(ThreadStateCacheORM, on)
        return stmt

    def _order_by(self, request: ThreadSearchRequest) -> list[Any]:
        column = (
            ThreadStateCacheORM.updated_at
            if request.sort_by == _STATE_SORT_KEY
            else getattr(ThreadORM, request.sort_by or "created_at")
        )
        return build_order_by(column, sort_order=request.sort_order, tiebreak=ThreadORM.thread_id)

    async def search(self, request: ThreadSearchRequest) -> list[ThreadORM]:
        value = request.model_dump()
        filters = await self._dispatch("search", value)

        base = select(ThreadORM).where(self._readable())
        stmt = self._filtered(base, request, filters).order_by(*self._order_by(request))
        stmt = paginate(stmt, limit=request.limit, offset=request.offset)
        return list((await self.session.scalars(stmt)).all())

    async def count(self, request: ThreadSearchRequest) -> int:
        """Same filters as search, without pagination or ordering."""
        value = request.model_dump()
        filters = await self._dispatch("search", value)

        base = select(func.count(ThreadORM.thread_id)).where(self._readable())
        return await self.session.scalar(self._filtered(base, request, filters)) or 0

    # --- latest state ---

    @asynccontextmanager
    async def _bound_graph(
        self, thread: ThreadORM, graph_id: str, access_context: AccessContext
    ) -> AsyncGenerator[tuple[Any, RunnableConfig]]:
        """Yield the thread's graph already bound to its config, plus that config.

        Callers still resolve ``graph_id`` themselves because each has a different
        answer for an unbound thread: 404-ish silence, a 400, or a skip.
        """
        config = await thread_graph_config(self.session, thread, self.user)
        async with get_langgraph_service().get_graph(
            graph_id, config=config, access_context=access_context, user=self.user
        ) as graph:
            yield graph.with_config(config), config

    async def _snapshot(self, thread: ThreadORM) -> Any:
        """Read the live checkpoint snapshot, or None when there is none to read.

        Reading a thread entity must not fail just because its state is
        unavailable, and graph code can raise anything, so every failure here
        degrades to "no state" with a log line.
        """
        graph_id = graph_id_of(thread)
        if not graph_id:
            return None
        try:
            async with self._bound_graph(thread, graph_id, "threads.read") as (graph, config):
                return await graph.aget_state(config)
        except Exception as exc:
            logger.warning("Thread state read failed", thread_id=thread.thread_id, error=str(exc))
            return None

    async def cached_states(self, thread_ids: Sequence[str]) -> dict[str, ThreadStateCacheORM]:
        """Materialized state for a page of threads — one query, no graph loads."""
        return await read(self.session, thread_ids)

    async def state(self, thread: ThreadORM) -> tuple[dict[str, Any] | None, dict[str, list[Any]]]:
        """Latest ``(values, interrupts)``, reading through to the checkpointer.

        A miss warms the cache so threads that predate materialization become
        searchable by state the first time anyone reads them.
        """
        cached = (await self.cached_states([thread.thread_id])).get(thread.thread_id)
        if cached is not None:
            return as_pair(cached)

        snapshot = await self._snapshot(thread)
        if snapshot is None:
            return None, {}

        values, interrupts = extract(snapshot)
        await store(thread.thread_id, values=values, interrupts=interrupts)
        return values, interrupts

    async def apply_supersteps(self, thread: ThreadORM, supersteps: Sequence[Superstep]) -> None:
        """Pre-fill state by replaying updates, one checkpoint per update.

        Requires a bound graph — without one there is nowhere to write.
        """
        graph_id = graph_id_of(thread)
        if not graph_id:
            raise HTTPException(400, "supersteps require metadata.graph_id to name the target graph")

        async with self._bound_graph(thread, graph_id, "threads.create_run") as (graph, config):
            for step in supersteps:
                for step_update in step.updates:
                    await graph.aupdate_state(config, step_update.values, as_node=step_update.as_node)
            snapshot = await graph.aget_state(config)

        values, interrupts = extract(snapshot)
        await materialize(self.session, thread.thread_id, values=values, interrupts=interrupts)
        await self.session.commit()

    # --- lifecycle ---

    async def copy(self, thread_id: str) -> ThreadORM:
        """Duplicate a thread under a fresh id.

        Checkpoint history comes along only when the checkpointer implements
        ``acopy_thread``; otherwise the copy starts from the source's latest
        materialized state, which is still enough to keep reading the thread.
        """
        await self._dispatch("create", {"thread_id": thread_id})
        source = await self._owned(thread_id)
        target_id = str(uuid4())

        # Checkpoints first: a failure here leaves nothing committed.
        if db_manager.supports("acopy_thread"):
            await db_manager.get_checkpointer().acopy_thread(thread_id, target_id)
        else:
            logger.info("Checkpointer cannot copy history; copying latest state only", thread_id=thread_id)

        target = ThreadORM(
            thread_id=target_id,
            status="idle",
            metadata_json={**(source.metadata_json or {}), "copied_from": thread_id},
            ttl=source.ttl,
            user_id=self.user.identity,
        )
        self.session.add(target)
        cached = (await self.cached_states([thread_id])).get(thread_id)
        if cached is not None:
            await materialize(self.session, target_id, values=cached.values, interrupts=cached.interrupts)
        await self.session.commit()
        return target

    async def prune(self, request: ThreadPruneRequest) -> int:
        """Reclaim thread storage; returns how many threads were pruned."""
        await self._dispatch("delete", {"thread_ids": request.thread_ids})

        threads = list(
            (await self.session.scalars(self._scoped().where(ThreadORM.thread_id.in_(request.thread_ids)))).all()
        )
        if not threads:
            return 0

        if request.strategy == "delete":
            return await self._delete_threads([thread.thread_id for thread in threads])

        collapsed = [await self._collapse_history(thread) for thread in threads]
        return collapsed.count(True)

    async def _delete_threads(self, ids: Sequence[str]) -> int:
        await discard(self.session, ids)
        await self.session.execute(delete(ThreadORM).where(ThreadORM.thread_id.in_(ids)))
        await self.session.commit()

        checkpointer = db_manager.get_checkpointer()
        for thread_id in ids:
            await checkpointer.adelete_thread(thread_id)
        return len(ids)

    async def _collapse_history(self, thread: ThreadORM) -> bool:
        """Drop a thread's checkpoints, re-seeding the latest values as one.

        Skipped when the thread has pending interrupts: they only resume against
        the checkpoint that raised them, so collapsing would strand the run.
        """
        graph_id = graph_id_of(thread)
        if not graph_id:
            return False

        async with self._bound_graph(thread, graph_id, "threads.read") as (graph, config):
            values, interrupts = extract(await graph.aget_state(config))
            if interrupts or not values:
                return False

            await db_manager.get_checkpointer().adelete_thread(thread.thread_id)
            await graph.aupdate_state(config, values)
            values, interrupts = extract(await graph.aget_state(config))

        await materialize(self.session, thread.thread_id, values=values, interrupts=interrupts)
        await self.session.commit()
        return True


def get_thread_service(
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
) -> ThreadService:
    """FastAPI dependency: a ThreadService bound to the request identity."""
    return ThreadService(session, user)
