"""Checkpointer retention sweeper.

Key names, defaults and semantics follow LangGraph Platform's ``checkpointer.ttl``, but the
implementation is Aegra's: the open-source checkpointer exposes no TTL hook, so expiry is
driven from the ``thread`` table instead of by the saver.

Expiry is measured from ``updated_at``, so a thread's clock restarts on every run. A
per-thread ``ttl`` (from ``threads.create(ttl=...)``) overrides both the lifetime and the
strategy for that thread.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Literal, cast

import structlog
from sqlalchemy import Numeric, func, literal, literal_column, select
from sqlalchemy import cast as sql_cast

from aegra_api.config import load_checkpointer_ttl_config
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.models.auth import User
from aegra_api.services.thread_service import ThreadService

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.sql.elements import ColumnElement

    from aegra_api.config import CheckpointerTTLConfig

logger = structlog.getLogger(__name__)

Strategy = Literal["delete", "keep_latest"]
STRATEGIES: tuple[Strategy, ...] = ("delete", "keep_latest")

DEFAULT_STRATEGY: Strategy = "delete"
DEFAULT_SWEEP_INTERVAL_MINUTES = 5.0
DEFAULT_SWEEP_LIMIT = 10_000

_ONE_MINUTE = literal_column("INTERVAL '1 minute'")


def expired(default_ttl: float | None) -> ColumnElement[bool]:
    """Threads whose lifetime has elapsed since their last activity.

    A thread's own ``ttl`` wins over ``default_ttl``. With neither, the coalesce yields NULL
    and the comparison drops the row — unconfigured means immortal.
    """
    fallback = literal(default_ttl, Numeric) if default_ttl is not None else literal(None, Numeric)
    minutes = func.coalesce(sql_cast(ThreadORM.ttl["ttl"].astext, Numeric), fallback)
    return ThreadORM.updated_at + minutes * _ONE_MINUTE < func.now()


def strategy_for(thread: ThreadORM, fallback: Strategy) -> Strategy:
    declared = (thread.ttl or {}).get("strategy")
    return cast("Strategy", declared) if declared in STRATEGIES else fallback


class ThreadTTLSweeper:
    """Reclaims expired threads on a timer; inert unless ``checkpointer.ttl`` is configured."""

    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._config: CheckpointerTTLConfig | None = None

    @property
    def config(self) -> CheckpointerTTLConfig | None:
        return self._config

    def configure(self, config: CheckpointerTTLConfig | None = None) -> None:
        self._config = config if config is not None else load_checkpointer_ttl_config()

    async def start(self) -> None:
        if self._config is None:
            self.configure()
        if self._config is None:
            logger.info("Thread TTL sweeper disabled (no checkpointer.ttl configured)")
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "Thread TTL sweeper started",
            strategy=self._config.get("strategy", DEFAULT_STRATEGY),
            default_ttl_minutes=self._config.get("default_ttl"),
            sweep_interval_minutes=self._config.get("sweep_interval_minutes", DEFAULT_SWEEP_INTERVAL_MINUTES),
        )

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
            logger.info("Thread TTL sweeper stopped")

    async def _loop(self) -> None:
        config = self._config
        if config is None:
            return
        interval = float(config.get("sweep_interval_minutes", DEFAULT_SWEEP_INTERVAL_MINUTES)) * 60
        while self._running:
            await asyncio.sleep(interval)
            try:
                await self.sweep()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in thread TTL sweeper")

    async def sweep(self) -> int:
        """Run one pass; returns how many threads were reclaimed. Unconfigured sweeps nothing."""
        config = self._config
        if config is None:
            return 0
        maker = _get_session_maker()
        async with maker() as session:
            threads = await self._claim(session, config)
            if not threads:
                return 0
            reclaimed = await self._reclaim(session, threads, config)
        if reclaimed:
            logger.info("Reclaimed expired threads", count=reclaimed)
        return reclaimed

    async def _claim(self, session: AsyncSession, config: CheckpointerTTLConfig) -> list[ThreadORM]:
        """Take a bounded batch, skipping rows another pod is already reclaiming."""
        stmt = (
            select(ThreadORM)
            .where(expired(config.get("default_ttl")))
            .order_by(ThreadORM.updated_at.asc())
            .limit(int(config.get("sweep_limit", DEFAULT_SWEEP_LIMIT)))
            .with_for_update(skip_locked=True)
        )
        return list((await session.scalars(stmt)).all())

    async def _reclaim(
        self, session: AsyncSession, threads: list[ThreadORM], config: CheckpointerTTLConfig
    ) -> int:
        """Apply each thread's strategy.

        Reuses ThreadService's own reclamation rather than its ``prune`` entrypoint: prune is
        owner-scoped and dispatches the ``delete`` auth handler, neither of which applies to a
        system-driven sweep.
        """
        fallback = config.get("strategy", DEFAULT_STRATEGY)
        by_strategy: dict[Strategy, list[ThreadORM]] = {"delete": [], "keep_latest": []}
        for thread in threads:
            by_strategy[strategy_for(thread, fallback)].append(thread)

        reclaimed = 0
        for thread in by_strategy["keep_latest"]:
            # Per-thread service: collapsing loads the graph under the thread owner's identity.
            service = ThreadService(session, User(identity=thread.user_id))
            try:
                if await service._collapse_history(thread):
                    reclaimed += 1
            except Exception:
                logger.exception("Failed to collapse expired thread", thread_id=thread.thread_id)

        drop = [thread.thread_id for thread in by_strategy["delete"]]
        if drop:
            owner = by_strategy["delete"][0].user_id
            reclaimed += await ThreadService(session, User(identity=owner))._delete_threads(drop)
        return reclaimed


thread_ttl_sweeper = ThreadTTLSweeper()
