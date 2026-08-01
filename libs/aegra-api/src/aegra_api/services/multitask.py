"""Concurrency control for runs on the same thread (double-texting).

``resolve`` runs at creation time, before the new run is persisted, and reports
whether it may be dispatched immediately. ``drain`` runs after a run finalizes
and hands the thread over to whatever ``enqueue`` held back.

Ordering matters: resolve must settle the incumbent runs *before* the new row
exists, or the new run would see itself as competition.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import structlog
from fastapi import HTTPException
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.database import db_manager
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.models.enums import ACTIVE_RUN_STATUSES
from aegra_api.models.run_job import RunJob
from aegra_api.services.streaming_service import streaming_service

logger = structlog.getLogger(__name__)

# How long to let an interrupted incumbent settle before starting the new run.
# Bounded because a stuck incumbent must not hold the new run forever; if it has
# not settled the new run proceeds and both write to the thread.
_SETTLE_ATTEMPTS = 20
_SETTLE_INTERVAL_SECONDS = 0.25


async def _active_runs(session: AsyncSession, thread_id: str) -> list[RunORM]:
    stmt = select(RunORM).where(RunORM.thread_id == thread_id, RunORM.status.in_(ACTIVE_RUN_STATUSES))
    return list((await session.scalars(stmt)).all())


async def _await_settled(session: AsyncSession, thread_id: str) -> None:
    """Poll until the thread has no active run, or the attempt budget runs out."""
    for _ in range(_SETTLE_ATTEMPTS):
        await asyncio.sleep(_SETTLE_INTERVAL_SECONDS)
        session.expire_all()
        if not await _active_runs(session, thread_id):
            return
    logger.warning("Incumbent runs did not settle; starting anyway", thread_id=thread_id)


async def resolve(session: AsyncSession, thread_id: str, strategy: str | None) -> bool:
    """Settle in-flight runs per ``strategy``; return whether to dispatch now.

    ``None`` keeps the historical behaviour of letting concurrent runs proceed.
    """
    if strategy is None:
        return True

    incumbents = await _active_runs(session, thread_id)
    if not incumbents:
        return True

    run_ids = [run.run_id for run in incumbents]
    if strategy == "reject":
        raise HTTPException(409, f"Thread '{thread_id}' already has an active run")

    if strategy == "enqueue":
        return False

    # Checked before anything is interrupted: refusing after the fact would leave
    # the thread half-rolled-back.
    if strategy == "rollback":
        require_run_state_discard()

    for run_id in run_ids:
        await streaming_service.interrupt_run(run_id)
    await session.execute(update(RunORM).where(RunORM.run_id.in_(run_ids)).values(status="interrupted"))
    await session.commit()
    await _await_settled(session, thread_id)

    if strategy == "rollback":
        await discard_run_state(run_ids)
    return True


def require_run_state_discard() -> None:
    """Refuse ``rollback`` when the checkpointer cannot delete per run.

    A 501 rather than a quiet downgrade to ``interrupt``: keeping the state the
    caller asked to discard would be indistinguishable from success.
    """
    if not db_manager.supports("adelete_for_runs"):
        raise HTTPException(
            501,
            "rollback needs per-run checkpoint deletion, which the installed "
            "langgraph-checkpoint-postgres does not implement; use action/strategy "
            "'interrupt' to stop the run without discarding its state",
        )


async def discard_run_state(run_ids: Sequence[str]) -> None:
    """Drop the checkpoints those runs wrote, restoring the prior thread state."""
    require_run_state_discard()
    await db_manager.get_checkpointer().adelete_for_runs(run_ids)


async def drain(thread_id: str) -> None:
    """Dispatch the oldest run ``enqueue`` is holding on this thread.

    The claim is a conditional UPDATE, so two runs finalizing at once cannot both
    hand off the same queued run. Failures are logged rather than raised: the
    caller is a finalizing run whose own outcome is already committed.
    """
    try:
        maker = _get_session_maker()
        async with maker() as session:
            job = await _claim_next(session, thread_id)
        if job is not None:
            await _submit(job)
    except Exception as exc:
        logger.warning("Queued run hand-off failed", thread_id=thread_id, error=str(exc))


async def _claim_next(session: AsyncSession, thread_id: str) -> RunJob | None:
    next_queued = (
        select(RunORM.run_id)
        .where(
            RunORM.thread_id == thread_id,
            RunORM.status == "pending",
            RunORM.dispatched.is_(False),
        )
        .order_by(RunORM.created_at.asc())
        .limit(1)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    claimed = await session.scalar(
        update(RunORM)
        .where(RunORM.run_id == next_queued, RunORM.dispatched.is_(False))
        .values(dispatched=True)
        .returning(RunORM)
    )
    await session.commit()
    if claimed is None:
        return None
    logger.info("Dispatching queued run", run_id=claimed.run_id, thread_id=thread_id)
    return RunJob.from_run_orm(claimed)


async def _submit(job: RunJob) -> None:
    """Hand a job to the executor.

    Deferred import: the executor factory pulls in the concrete executors, which
    import run_executor, which imports this module.
    """
    from aegra_api.services.executor import executor

    await executor.submit(job)
