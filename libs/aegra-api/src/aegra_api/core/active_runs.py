"""Global registry of in-flight asyncio tasks for graph executions.

Defined in a dependency-free module so that any layer (API routes, broker
managers, streaming service) can import it without circular dependencies.
"""

import asyncio

import structlog
from redis import RedisError
from sqlalchemy.exc import SQLAlchemyError

logger = structlog.getLogger(__name__)

active_runs: dict[str, asyncio.Task[None]] = {}


# Transport-class failures cleanup paths tolerate. Deliberately narrow: anything
# else is a bug and must reach the caller.
TRANSPORT_ERRORS: tuple[type[BaseException], ...] = (RedisError, SQLAlchemyError, OSError)


async def drain_task(task: asyncio.Task[None], run_id: str) -> None:
    """Wait for a cancelled run task to stop before its rows are deleted.

    Infra failures are logged — the run's own executor owns the outcome, so they
    are not fatal here. Programmer errors propagate instead: swallowing them would
    hide real bugs on the cancel/cleanup path.
    """
    try:
        await task
    except asyncio.CancelledError:
        # Only the task's own cancellation is expected; ours must keep propagating.
        if not task.cancelled():
            raise
    except TRANSPORT_ERRORS:
        logger.warning("run task raised while draining", run_id=run_id, exc_info=True)
