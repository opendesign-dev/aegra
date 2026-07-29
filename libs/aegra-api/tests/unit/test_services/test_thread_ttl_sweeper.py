"""Unit tests for ThreadTTLSweeper."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.dialects import postgresql

from aegra_api.services.thread_ttl_sweeper import ThreadTTLSweeper, _expired

MODULE = "aegra_api.services.thread_ttl_sweeper"


def _compiled_predicate() -> str:
    return str(_expired().compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def _session_with_stale(stale: list[str]) -> AsyncMock:
    """Mock session whose locked SELECT returns *stale* thread ids."""
    session = AsyncMock()
    scalars_result = MagicMock()
    scalars_result.all.return_value = stale
    session.scalars = AsyncMock(return_value=scalars_result)
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    return session


def _maker_for(session: AsyncMock) -> MagicMock:
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=ctx)


class TestExpiredPredicate:
    """A thread's own ttl must drive the sweep, else the stored policy is silently inert."""

    def test_reads_the_threads_own_ttl_minutes(self) -> None:
        assert "ttl ->> 'ttl'" in _compiled_predicate()

    def test_falls_back_to_configured_global_minutes(self) -> None:
        with patch(f"{MODULE}.settings") as mock_settings:
            mock_settings.checkpointer.CHECKPOINTER_TTL_MINUTES = 1234
            sql = _compiled_predicate()
        assert "coalesce" in sql.lower()
        assert "1234" in sql

    def test_compares_against_updated_at(self) -> None:
        sql = _compiled_predicate()
        assert "thread.updated_at <" in sql
        assert "make_interval" in sql

    def test_passes_the_ttl_through_make_intervals_secs_argument(self) -> None:
        """Postgres types make_interval's ``mins`` as integer and only ``secs`` as
        double precision, so a float ttl in the mins slot raises UndefinedFunction."""
        sql = _compiled_predicate()
        args = sql.split("make_interval(", 1)[1].rsplit(")", 1)[0]
        leading, last = args.rsplit(",", 1)
        assert [p.strip() for p in leading.split(",")][:6] == ["0"] * 6
        assert "* 60" in last


class TestThreadTTLSweeperTick:
    @pytest.mark.asyncio
    async def test_deletes_checkpoints_and_thread_rows_for_stale(self) -> None:
        session = _session_with_stale(["t1", "t2"])
        checkpointer = AsyncMock()
        with (
            patch(f"{MODULE}.get_session_maker", return_value=_maker_for(session)),
            patch(f"{MODULE}.db_manager.get_checkpointer", return_value=checkpointer),
        ):
            await ThreadTTLSweeper()._tick()

        # Checkpoints deleted per stale thread, then a single cascading row delete.
        assert checkpointer.adelete_thread.await_count == 2
        session.execute.assert_awaited_once()
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_noop_when_nothing_stale(self) -> None:
        session = _session_with_stale([])
        checkpointer = AsyncMock()
        with (
            patch(f"{MODULE}.get_session_maker", return_value=_maker_for(session)),
            patch(f"{MODULE}.db_manager.get_checkpointer", return_value=checkpointer),
        ):
            await ThreadTTLSweeper()._tick()

        checkpointer.adelete_thread.assert_not_awaited()
        session.execute.assert_not_awaited()
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_checkpoint_delete_failure_does_not_block_row_delete(self) -> None:
        session = _session_with_stale(["t1"])
        checkpointer = AsyncMock()
        checkpointer.adelete_thread = AsyncMock(side_effect=RuntimeError("saver down"))
        with (
            patch(f"{MODULE}.get_session_maker", return_value=_maker_for(session)),
            patch(f"{MODULE}.db_manager.get_checkpointer", return_value=checkpointer),
        ):
            await ThreadTTLSweeper()._tick()

        # A checkpointer error is logged, not raised — the row delete still runs.
        session.execute.assert_awaited_once()
        session.commit.assert_awaited_once()
