"""Thread-level SSE: one subscription spanning every run on a thread.

Run-level streaming attaches to a single run's broker. Clients following a
conversation want to stay attached across runs instead, so this walks the thread's
runs in creation order, forwarding whichever of the three ``ThreadStreamMode``
views the caller asked for.

Built on the per-run brokers rather than a thread-level one: brokers are keyed by
run, and a second keying scheme would need its own lifecycle, reaper, and
cross-instance fan-out.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Collection

import structlog
from sqlalchemy import Select, select

from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.core.sse import format_sse_message
from aegra_api.models.auth import User
from aegra_api.models.enums import TERMINAL_RUN_STATUSES
from aegra_api.models.runs import Run
from aegra_api.services.streaming_service import streaming_service
from aegra_api.services.thread_service import ThreadService
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)

# The SDK's own default when the caller names no mode.
DEFAULT_MODES = frozenset({"run_modes"})


async def stream_thread(
    thread_id: str,
    user: User,
    *,
    modes: Collection[str] | None = None,
    last_event_id: str | None = None,
) -> AsyncGenerator[str, None]:
    """Forward the thread's activity until it goes idle or the client leaves.

    Ends after ``THREAD_STREAM_IDLE_TIMEOUT_SECONDS`` with no new run, so an
    abandoned subscription cannot hold a connection open indefinitely.
    """
    wanted = set(modes or DEFAULT_MODES)
    seen: set[str] = set()
    resume_from = last_event_id

    while True:
        run = await _next_run(thread_id, user.identity, seen) or await _wait_for_run(thread_id, user.identity, seen)
        if run is None:
            return

        seen.add(run.run_id)
        if "lifecycle" in wanted:
            yield _lifecycle("run.start", run)

        if "run_modes" in wanted:
            async for message in streaming_service.stream_run_execution(run, resume_from):
                yield message
        elif run.status not in TERMINAL_RUN_STATUSES:
            # Nothing to forward, but the run has to settle before its lifecycle
            # and state events can describe a settled thread.
            await _await_terminal(run.run_id)
        # A Last-Event-ID only addresses the run it came from.
        resume_from = None

        if "lifecycle" in wanted:
            yield _lifecycle("run.end", await _fetch(select(RunORM).where(RunORM.run_id == run.run_id)) or run)
        if "state_update" in wanted:
            yield await _state_update(thread_id, user)


async def _fetch(stmt: Select[tuple[RunORM]]) -> Run | None:
    """Run the statement on its own short-lived session, as a response model."""
    maker = _get_session_maker()
    async with maker() as session:
        row = await session.scalar(stmt)
    return Run.model_validate(row) if row is not None else None


async def _next_run(thread_id: str, user_id: str, seen: Collection[str]) -> Run | None:
    """Oldest run on the thread that this stream has not forwarded yet."""
    stmt = (
        select(RunORM)
        .where(RunORM.thread_id == thread_id, RunORM.user_id == user_id)
        .order_by(RunORM.created_at.asc())
        .limit(1)
    )
    if seen:
        stmt = stmt.where(RunORM.run_id.notin_(list(seen)))
    return await _fetch(stmt)


async def _wait_for_run(thread_id: str, user_id: str, seen: Collection[str]) -> Run | None:
    """Poll for a new run; None once the idle timeout is reached."""
    interval = settings.event_streaming.THREAD_STREAM_POLL_INTERVAL_SECONDS
    timeout = settings.event_streaming.THREAD_STREAM_IDLE_TIMEOUT_SECONDS

    waited = 0.0
    while waited < timeout:
        await asyncio.sleep(interval)
        waited += interval
        run = await _next_run(thread_id, user_id, seen)
        if run is not None:
            return run

    logger.info("Thread stream idle timeout", thread_id=thread_id, seconds=timeout)
    return None


async def _await_terminal(run_id: str) -> None:
    """Block until the run settles, bounded by the background job timeout.

    Deferred import: the executor factory loads the concrete executors, which
    import the run executor, which reaches back into this package.
    """
    from aegra_api.services.executor import executor

    await executor.wait_for_completion(run_id, timeout=settings.worker.BG_JOB_TIMEOUT_SECS)


def _lifecycle(kind: str, run: Run) -> str:
    return format_sse_message("lifecycle", {"type": kind, "run_id": run.run_id, "status": run.status})


async def _state_update(thread_id: str, user: User) -> str:
    """The thread's current state as a single event."""
    maker = _get_session_maker()
    async with maker() as session:
        service = ThreadService(session, user)
        values, interrupts = await service.state(await service.get(thread_id))
    return format_sse_message("state_update", {"values": values, "interrupts": interrupts})
