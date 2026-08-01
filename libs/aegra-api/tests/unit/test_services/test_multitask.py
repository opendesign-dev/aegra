"""Unit tests for double-texting strategies.

Every strategy is exercised against the same "thread already has an active run"
setup, because the whole point of the module is choosing between them.
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from aegra_api.core.orm import Run as RunORM
from aegra_api.services import multitask
from tests.fixtures.database import make_session_maker


def _run(run_id: str, *, status: str = "running") -> RunORM:
    return RunORM(
        run_id=run_id,
        thread_id="t-1",
        assistant_id="agent",
        user_id="u-1",
        status=status,
        input={},
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


def _session(active: list[RunORM]) -> AsyncMock:
    session = AsyncMock()
    result = MagicMock()
    result.all.return_value = active
    session.scalars.return_value = result
    session.expire_all = MagicMock()
    return session


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", [None, "reject", "interrupt", "rollback", "enqueue"])
async def test_idle_thread_always_dispatches(strategy: str | None) -> None:
    """With nothing in flight there is no conflict to resolve."""
    assert await multitask.resolve(_session([]), "t-1", strategy) is True


@pytest.mark.asyncio
async def test_none_strategy_leaves_concurrent_runs_alone() -> None:
    """Omitting the field keeps the historical concurrent behaviour."""
    session = _session([_run("r-1")])

    assert await multitask.resolve(session, "t-1", None) is True
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_reject_returns_409() -> None:
    with pytest.raises(HTTPException) as exc:
        await multitask.resolve(_session([_run("r-1")]), "t-1", "reject")

    assert exc.value.status_code == 409


@pytest.mark.asyncio
async def test_enqueue_holds_the_run_back() -> None:
    """A queued run must not be dispatched, and must not disturb the incumbent."""
    session = _session([_run("r-1")])

    assert await multitask.resolve(session, "t-1", "enqueue") is False
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_interrupt_stops_incumbents_then_dispatches() -> None:
    session = _session([_run("r-1"), _run("r-2")])

    with patch.object(multitask.streaming_service, "interrupt_run", new=AsyncMock()) as interrupt:
        # Second poll reports the thread settled.
        session.scalars.return_value.all.side_effect = [[_run("r-1"), _run("r-2")], []]
        assert await multitask.resolve(session, "t-1", "interrupt") is True

    assert interrupt.await_count == 2
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_rollback_discards_the_interrupted_runs_checkpoints() -> None:
    """Rollback differs from interrupt only in dropping the state they wrote."""
    session = _session([_run("r-1")])
    checkpointer = AsyncMock()

    with (
        patch.object(multitask.streaming_service, "interrupt_run", new=AsyncMock()),
        patch.object(multitask.db_manager, "supports", return_value=True),
        patch.object(multitask.db_manager, "get_checkpointer", return_value=checkpointer),
    ):
        session.scalars.return_value.all.side_effect = [[_run("r-1")], []]
        assert await multitask.resolve(session, "t-1", "rollback") is True

    checkpointer.adelete_for_runs.assert_awaited_once_with(["r-1"])


@pytest.mark.asyncio
async def test_drain_dispatches_the_claimed_run() -> None:
    queued = _run("r-9", status="pending")
    queued.execution_params = {
        "graph_id": "agent",
        "user": {"identity": "u-1", "scopes": []},
        "execution": {},
        "behavior": {},
        "run_metadata": {},
    }
    session = AsyncMock()
    session.scalar.return_value = queued

    submitted: list[Any] = []

    async def _submit(job: Any) -> None:
        submitted.append(job)

    with (
        patch.object(multitask, "_get_session_maker", return_value=make_session_maker(session)),
        patch.object(multitask, "_submit", new=_submit),
    ):
        await multitask.drain("t-1")

    assert [job.identity.run_id for job in submitted] == ["r-9"]


@pytest.mark.asyncio
async def test_drain_is_a_noop_when_nothing_is_queued() -> None:
    session = AsyncMock()
    session.scalar.return_value = None

    with (
        patch.object(multitask, "_get_session_maker", return_value=make_session_maker(session)),
        patch.object(multitask, "_submit", new=AsyncMock()) as submit,
    ):
        await multitask.drain("t-1")

    submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_drain_never_raises_into_the_finalizing_run() -> None:
    """The caller's own outcome is already committed; a hand-off failure is logged."""
    with patch.object(multitask, "_get_session_maker", side_effect=RuntimeError("db gone")):
        await multitask.drain("t-1")


class TestRollbackCapabilityGate:
    """`rollback` must refuse rather than quietly behave like `interrupt`.

    `AsyncPostgresSaver` declares `adelete_for_runs` but the base class raises,
    so keeping the state the caller asked to discard would look like success.
    """

    def test_missing_capability_raises_501(self) -> None:
        with (
            patch.object(multitask.db_manager, "supports", return_value=False),
            pytest.raises(HTTPException) as exc,
        ):
            multitask.require_run_state_discard()

        assert exc.value.status_code == 501
        assert "interrupt" in str(exc.value.detail)

    def test_present_capability_passes(self) -> None:
        with patch.object(multitask.db_manager, "supports", return_value=True):
            multitask.require_run_state_discard()

    @pytest.mark.asyncio
    async def test_refusal_happens_before_anything_is_interrupted(self) -> None:
        """Refusing mid-way would leave the thread interrupted but not rolled back."""
        session = _session([_run("r-1")])

        with (
            patch.object(multitask.db_manager, "supports", return_value=False),
            patch.object(multitask.streaming_service, "interrupt_run", new=AsyncMock()) as interrupt,
            pytest.raises(HTTPException),
        ):
            await multitask.resolve(session, "t-1", "rollback")

        interrupt.assert_not_awaited()
        session.execute.assert_not_called()
