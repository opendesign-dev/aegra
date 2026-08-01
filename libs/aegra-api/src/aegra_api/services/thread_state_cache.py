"""Materialized view of each thread's latest state.

The checkpointer owns the truth. This cache exists so list/search/count can read
``values`` and ``interrupts`` without one graph load per row, and so containment
filtering on ``values`` has an indexable target.

Refreshed at every state transition this server performs: run finalization,
explicit state updates, and superstep pre-fill. Nothing else writes graph state,
so a cache row is as current as the checkpoint that produced it.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi.encoders import jsonable_encoder
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.orm import ThreadStateCache as ThreadStateCacheORM
from aegra_api.core.orm import _get_session_maker

logger = structlog.getLogger(__name__)


def extract(snapshot: Any) -> tuple[dict[str, Any] | None, dict[str, list[Any]]]:
    """Pull ``(values, interrupts)`` out of a LangGraph state snapshot."""
    if snapshot is None:
        return None, {}

    interrupts: dict[str, list[Any]] = {}
    for task in getattr(snapshot, "tasks", ()) or ():
        raised = getattr(task, "interrupts", None)
        if raised:
            # LangGraph's Interrupt is a dataclass; encoding it yields the SDK's
            # {value, id} shape, which the dict-typed response field accepts.
            interrupts[str(getattr(task, "id", ""))] = jsonable_encoder(list(raised))
    return getattr(snapshot, "values", None), interrupts


def as_pair(row: ThreadStateCacheORM | None) -> tuple[dict[str, Any] | None, dict[str, list[Any]]]:
    """A cache row as the ``(values, interrupts)`` pair the response models take."""
    return (row.values, row.interrupts or {}) if row is not None else (None, {})


def _digest(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


async def materialize(
    session: AsyncSession,
    thread_id: str,
    *,
    values: dict[str, Any] | None,
    interrupts: dict[str, list[Any]] | None,
) -> None:
    """Upsert a thread's latest state, skipping the write when nothing changed.

    Does not commit — the caller owns the transaction boundary.
    """
    encoded = jsonable_encoder({"values": values, "interrupts": interrupts or {}})
    fingerprint = _digest(encoded)
    row = {
        "thread_id": thread_id,
        "values": encoded["values"],
        "interrupts": encoded["interrupts"],
        "values_hash": fingerprint,
        "updated_at": datetime.now(UTC),
    }
    stmt = insert(ThreadStateCacheORM).values(row)
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=[ThreadStateCacheORM.thread_id],
            set_={k: stmt.excluded[k] for k in ("values", "interrupts", "values_hash", "updated_at")},
            where=ThreadStateCacheORM.values_hash.is_distinct_from(fingerprint),
        )
    )


async def store(
    thread_id: str,
    *,
    values: dict[str, Any] | None,
    interrupts: dict[str, list[Any]] | None,
) -> None:
    """Cache a state on its own session, for callers outside a transaction.

    A stale cache costs search freshness, never correctness, so the whole
    sequence is guarded: no cache problem may fail the operation that triggered it.
    """
    try:
        maker = _get_session_maker()
        async with maker() as session:
            await materialize(session, thread_id, values=values, interrupts=interrupts)
            await session.commit()
    except Exception as exc:
        logger.warning("Thread state materialization failed", thread_id=thread_id, error=str(exc))


async def refresh(thread_id: str, graph: Any, config: dict[str, Any]) -> None:
    """Read a loaded graph's latest state and cache it. Best-effort, see ``store``."""
    try:
        snapshot = await graph.aget_state(config)
    except Exception as exc:
        logger.warning("Thread state read failed", thread_id=thread_id, error=str(exc))
        return
    values, interrupts = extract(snapshot)
    await store(thread_id, values=values, interrupts=interrupts)


async def read(session: AsyncSession, thread_ids: Sequence[str]) -> dict[str, ThreadStateCacheORM]:
    """Fetch cached state for many threads in one query, keyed by thread id."""
    if not thread_ids:
        return {}
    result = await session.scalars(select(ThreadStateCacheORM).where(ThreadStateCacheORM.thread_id.in_(thread_ids)))
    return {row.thread_id: row for row in result.all()}


async def discard(session: AsyncSession, thread_ids: Sequence[str]) -> None:
    """Drop cached state so the next read re-materializes it. Does not commit."""
    if not thread_ids:
        return
    await session.execute(delete(ThreadStateCacheORM).where(ThreadStateCacheORM.thread_id.in_(thread_ids)))
