"""Shared run execution for the MCP and A2A interop endpoints.

Both protocols are request/response — submit, wait, read the result — so they
need a non-streaming counterpart to the SDK's ``/runs/wait``, which can only
hand back a ``StreamingResponse``.
"""

from dataclasses import dataclass, replace
from typing import Any

import structlog
from fastapi import HTTPException
from sqlalchemy import select

from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.models import RunCreate, User
from aegra_api.services.executor import executor
from aegra_api.services.run_preparation import _prepare_run
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)


_TERMINAL_STATUSES = frozenset({"success", "error", "interrupted"})


@dataclass(frozen=True)
class RunResult:
    """State of an interop run, as persisted."""

    run_id: str
    thread_id: str
    status: str
    output: dict[str, Any]
    error: str | None

    @property
    def succeeded(self) -> bool:
        return self.status == "success"


async def read_run_result(run_id: str, user_id: str) -> RunResult | None:
    """Read a run the caller owns, or None if there is no such run.

    Scoped by ``user_id`` in the WHERE clause, so an id guessed off the wire
    (A2A's ``taskId``) cannot read another user's run.
    """
    maker = _get_session_maker()
    async with maker() as session:
        run = await session.scalar(select(RunORM).where(RunORM.run_id == run_id, RunORM.user_id == user_id))
    if run is None:
        return None
    return RunResult(
        run_id=run.run_id,
        thread_id=run.thread_id,
        status=run.status,
        output=run.output or {},
        error=run.error_message,
    )


async def prepare_interop_run(thread_id: str, request: RunCreate, user: User) -> str:
    """Submit a run on ``thread_id`` and return its id, without waiting.

    ``thread_id`` may come straight off the wire (A2A's ``contextId``), so
    ownership is checked before ``_prepare_run`` — which would otherwise happily
    attach the run to another user's thread. A 404 rather than a 403 keeps the
    existence of someone else's thread unobservable.
    """
    maker = _get_session_maker()
    async with maker() as session:
        existing = await session.scalar(select(ThreadORM).where(ThreadORM.thread_id == thread_id))
        if existing and existing.user_id != user.identity:
            raise HTTPException(404, f"Thread '{thread_id}' not found")

        run_id, _run, _job = await _prepare_run(session, thread_id, request, user, initial_status="pending")
    return run_id


async def execute_and_wait(thread_id: str, request: RunCreate, user: User) -> RunResult:
    """Create a run on ``thread_id``, wait for it to finish, and read the result.

    The session closes before the wait so a long run never holds a pool
    connection — the same reason ``wait_for_run`` manages its session by hand.
    ``wait_for_completion`` returns rather than raising on timeout, so the
    persisted status is what decides success, not the fact that the wait ended.
    """
    run_id = await prepare_interop_run(thread_id, request, user)

    await executor.wait_for_completion(run_id, timeout=settings.worker.BG_JOB_TIMEOUT_SECS)

    result = await read_run_result(run_id, user.identity)
    if result is None:
        return RunResult(
            run_id=run_id,
            thread_id=thread_id,
            status="error",
            output={},
            error="Run disappeared before its result could be read",
        )
    if result.status in _TERMINAL_STATUSES:
        return result

    logger.warning("Interop run did not reach a terminal state", run_id=run_id, status=result.status)
    return replace(result, error=result.error or f"Run did not complete (status={result.status})")
