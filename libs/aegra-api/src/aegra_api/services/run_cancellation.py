"""Cancellation orchestration for runs.

Cancelling is a multi-step protocol, not a single UPDATE: persist the intent so
it survives a lost pub/sub message, signal the executor, settle unclaimed runs
the API owns, and — for rollback — wait for the worker to release its lease
before deleting the checkpoints it may still be writing. The single-run and
bulk endpoints run the same protocol, so it lives here rather than in either.
"""

import asyncio
from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.database import db_manager
from aegra_api.core.orm import Run as RunORM
from aegra_api.models.enums import TERMINAL_RUN_STATUSES, CancelAction
from aegra_api.services.streaming_service import streaming_service

logger = structlog.getLogger(__name__)

# Bound the settle poll so a crashed worker (orphaned lease on a terminal run)
# cannot block a rollback forever.
_SETTLE_ATTEMPTS = 20
_SETTLE_DELAY_SECONDS = 0.5


async def mark_cancel_requested(session: AsyncSession, run_ids: list[str]) -> None:
    """Persist the cancel intent so the owning worker's heartbeat honors it.

    Durable and cross-instance: survives a dropped pub/sub signal and even a
    worker crash (the reaper re-enqueues the run with the marker intact). Pub/sub
    remains only an accelerator.
    """
    await session.execute(update(RunORM).where(RunORM.run_id.in_(run_ids)).values(cancel_requested=True))
    await session.commit()


async def interrupt_pending(session: AsyncSession, run_ids: list[str]) -> None:
    """Finalize unclaimed pending runs as interrupted — no executor will settle them.

    A running run is left to its executor, which finalizes ``interrupted`` via the
    marker/pub/sub. Guarded on ``status='pending'`` so a run a worker just claimed
    is left for that worker, not stolen into a terminal state mid-flight.
    """
    await session.execute(
        update(RunORM)
        .where(RunORM.run_id.in_(run_ids), RunORM.status == "pending")
        .values(status="interrupted", updated_at=datetime.now(UTC))
    )
    await session.commit()


async def wait_for_settle(
    session: AsyncSession, run_ids: list[str], *, attempts: int = _SETTLE_ATTEMPTS, delay: float = _SETTLE_DELAY_SECONDS
) -> None:
    """Poll until every run's executor has fully settled it (terminal + lease released).

    The termination signal is worker-controlled, never the API's own write. A prod
    worker writes the terminal status in finalize_run but drops the lease only
    afterwards, so lease-release is the definitive "worker is done" point — the
    safe moment to delete checkpoints for a rollback. In dev ``claimed_by`` is
    always NULL, so this reduces to waiting for the local task's terminal status.
    """
    for attempt in range(attempts):
        # Check before sleeping: interrupt_pending just settled any unclaimed run,
        # so the exit condition is often already true on entry.
        if attempt:
            await asyncio.sleep(delay)
        rows = (await session.execute(select(RunORM.status, RunORM.claimed_by).where(RunORM.run_id.in_(run_ids)))).all()
        if all(status in TERMINAL_RUN_STATUSES and claimed_by is None for status, claimed_by in rows):
            return


async def signal_cancel(session: AsyncSession, run_ids: list[str], action: CancelAction) -> None:
    """Mark the intent, fire the pub/sub accelerator, and settle unclaimed runs.

    Intent is persisted before signalling so a lost message cannot let the run
    finish and overwrite ``interrupted`` back to ``success``.
    """
    await mark_cancel_requested(session, run_ids)
    for run_id in run_ids:
        if action == "interrupt":
            await streaming_service.interrupt_run(run_id)
        else:
            await streaming_service.cancel_run(run_id)
    await interrupt_pending(session, run_ids)


async def rollback_runs(session: AsyncSession, run_ids: list[str]) -> None:
    """Discard rolled-back runs and the checkpoints they produced.

    Callers must have settled the runs first: deleting while an executor is
    finalizing would let it write the rows back afterwards.
    """
    await db_manager.get_checkpointer().adelete_for_runs(run_ids)
    await session.execute(delete(RunORM).where(RunORM.run_id.in_(run_ids)))
    await session.commit()
