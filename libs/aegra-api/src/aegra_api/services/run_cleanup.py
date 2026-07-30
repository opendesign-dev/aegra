"""Shared helpers for deleting ephemeral threads after stateless runs complete.

Consumed by stateless_runs.py and crons.py — both create short-lived threads
that need cleanup after the underlying run finishes.
"""

import asyncio

import structlog
from sqlalchemy import select

from aegra_api.core.active_runs import TRANSPORT_ERRORS, active_runs, drain_task
from aegra_api.core.database import db_manager
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import get_session_maker
from aegra_api.services.executor import executor
from aegra_api.services.streaming_service import streaming_service

logger = structlog.getLogger(__name__)

# Strong refs so fire-and-forget cleanup tasks survive GC until done.
background_cleanup_tasks: set[asyncio.Task[None]] = set()

# Alias kept for the name callers read at their catch sites; one definition.
CLEANUP_ERRORS = TRANSPORT_ERRORS


async def delete_thread_checkpoints(thread_id: str) -> None:
    """Delete the thread's checkpoints, blobs, and pending writes.

    The checkpointer tables carry no foreign key to ``thread``, so dropping the
    row cascades to runs and thread_state but leaves this behind — and the blobs
    hold the conversation state, not just bookkeeping. Best-effort: a checkpointer
    failure must not abort a delete whose metadata rows are already gone.
    """
    try:
        await db_manager.get_checkpointer().adelete_thread(thread_id)
    except CLEANUP_ERRORS:
        logger.exception("Failed to delete thread checkpoints", thread_id=thread_id)


async def delete_thread_by_id(thread_id: str, user_id: str) -> None:
    """Delete a thread, its runs, and its checkpoints.

    Opens its own DB session so it can be called after the request session has
    been closed (e.g. in a finally block or background task).
    """
    maker = get_session_maker()
    async with maker() as session:
        active_runs_stmt = select(RunORM).where(
            RunORM.thread_id == thread_id,
            RunORM.user_id == user_id,
            RunORM.status.in_(["pending", "running"]),
        )
        active_runs_list = (await session.scalars(active_runs_stmt)).all()

        for run in active_runs_list:
            run_id = run.run_id
            await streaming_service.cancel_run(run_id)
            task = active_runs.pop(run_id, None)
            if task and not task.done():
                task.cancel()
                await drain_task(task, run_id)

        thread = await session.scalar(
            select(ThreadORM).where(
                ThreadORM.thread_id == thread_id,
                ThreadORM.user_id == user_id,
            )
        )
        if not thread:
            return
        await session.delete(thread)
        await session.commit()

    await delete_thread_checkpoints(thread_id)


async def cleanup_after_background_run(run_id: str, thread_id: str, user_id: str) -> None:
    """Wait for a background run to finish, then delete its ephemeral thread.

    executor.wait_for_completion works both in-process (dev) and cross-instance
    (prod with Redis workers).
    """
    try:
        await executor.wait_for_completion(run_id, timeout=3600.0)
    except (asyncio.CancelledError, TimeoutError):
        # Cancellation = shutdown; timeout = run exceeded 1h cap. Either way we
        # still proceed to delete the thread below — no need to log.
        pass
    except CLEANUP_ERRORS:
        logger.exception("Error waiting for background run", run_id=run_id)

    try:
        await delete_thread_by_id(thread_id, user_id)
    except CLEANUP_ERRORS:
        logger.exception("Failed to delete ephemeral thread", thread_id=thread_id, run_id=run_id)


def schedule_background_cleanup(run_id: str, thread_id: str, user_id: str) -> asyncio.Task[None]:
    """Fire-and-forget background cleanup, strong ref held until done."""
    task = asyncio.create_task(cleanup_after_background_run(run_id, thread_id, user_id))
    background_cleanup_tasks.add(task)
    task.add_done_callback(background_cleanup_tasks.discard)
    return task
