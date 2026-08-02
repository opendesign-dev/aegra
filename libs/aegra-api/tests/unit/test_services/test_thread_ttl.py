"""Checkpointer retention: expiry predicate, strategy resolution, and sweeper wiring.

Retention deletes user data, so the parts that decide *what* expires are asserted against
compiled SQL rather than mocks — a coalesce written the wrong way round would silently
reclaim every thread instead of none.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import Select, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.elements import ColumnElement

from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.services import thread_ttl
from aegra_api.services.thread_ttl import ThreadTTLSweeper


def _compile(stmt: Select[Any]) -> str:
    """Postgres dialect: SKIP LOCKED and JSONB operators do not render on the default one."""
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def _sql(predicate: ColumnElement[bool]) -> str:
    return _compile(select(ThreadORM.thread_id).where(predicate))


def _thread(ttl: dict[str, Any] | None = None, user_id: str = "alice") -> ThreadORM:
    thread = ThreadORM()
    thread.thread_id = "t-1"
    thread.user_id = user_id
    thread.ttl = ttl
    return thread


class TestExpiry:
    def test_measured_from_last_activity(self) -> None:
        """Clock restarts on every run, so a busy thread never expires under it."""
        assert "updated_at" in _sql(thread_ttl.expired(60))

    def test_default_ttl_applies_when_the_thread_declares_none(self) -> None:
        assert "60" in _sql(thread_ttl.expired(60))

    def test_per_thread_ttl_is_preferred(self) -> None:
        """coalesce puts the thread's own ttl first; the default is only the fallback."""
        sql = _sql(thread_ttl.expired(60))
        assert sql.index("ttl") < sql.index("60"), "the thread's own ttl must be the first coalesce arm"

    def test_without_any_ttl_nothing_expires(self) -> None:
        """A NULL lifetime makes the comparison NULL, so unconfigured means immortal."""
        assert "NULL" in _sql(thread_ttl.expired(None)).upper()


class TestStrategy:
    def test_falls_back_to_the_global_strategy(self) -> None:
        assert thread_ttl.strategy_for(_thread(), "delete") == "delete"
        assert thread_ttl.strategy_for(_thread({"ttl": 5}), "keep_latest") == "keep_latest"

    def test_per_thread_strategy_overrides_it(self) -> None:
        assert thread_ttl.strategy_for(_thread({"strategy": "keep_latest"}), "delete") == "keep_latest"

    def test_unrecognised_strategy_is_ignored(self) -> None:
        """A typo must not silently disable reclamation for that thread."""
        assert thread_ttl.strategy_for(_thread({"strategy": "archive"}), "delete") == "delete"


class TestDefaults:
    def test_match_langgraph_platform(self) -> None:
        assert thread_ttl.DEFAULT_STRATEGY == "delete"
        assert thread_ttl.DEFAULT_SWEEP_INTERVAL_MINUTES == 5.0
        assert thread_ttl.DEFAULT_SWEEP_LIMIT == 10_000


class TestSweeper:
    async def test_unconfigured_sweeps_nothing(self) -> None:
        sweeper = ThreadTTLSweeper()
        sweeper.configure({})
        sweeper._config = None
        assert await sweeper.sweep() == 0

    async def test_start_is_inert_without_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No checkpointer.ttl must leave existing deployments exactly as they were."""
        monkeypatch.setattr(thread_ttl, "load_checkpointer_ttl_config", lambda: None)
        sweeper = ThreadTTLSweeper()

        await sweeper.start()

        assert sweeper._task is None
        await sweeper.stop()

    async def test_claim_is_bounded_and_skips_rows_another_pod_holds(self) -> None:
        """Without SKIP LOCKED two pods would fight over the same batch; without a limit one
        pass could hold locks over the whole table."""
        sweeper = ThreadTTLSweeper()
        session = AsyncMock()
        result = Mock()
        result.all.return_value = []
        session.scalars.return_value = result

        await sweeper._claim(session, {"default_ttl": 60, "sweep_limit": 25})

        sql = _compile(session.scalars.await_args.args[0]).upper()
        assert "FOR UPDATE" in sql
        assert "SKIP LOCKED" in sql
        assert "LIMIT 25" in sql

    async def test_claim_takes_the_stalest_first(self) -> None:
        sweeper = ThreadTTLSweeper()
        session = AsyncMock()
        result = Mock()
        result.all.return_value = []
        session.scalars.return_value = result

        await sweeper._claim(session, {"default_ttl": 60})

        sql = _compile(session.scalars.await_args.args[0])
        assert "ORDER BY" in sql.upper() and "updated_at" in sql
