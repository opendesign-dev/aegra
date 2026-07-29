"""Unit tests for checkpoint cleanup on thread deletion.

Regression: the checkpointer tables carry no foreign key to ``thread``, so
deleting a thread cascaded to runs and thread_state but left checkpoints, blobs,
and pending writes behind — 44 rows per thread measured against a real database,
with the conversation state sitting in the blobs. Callers believed the data was
gone when it was not.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from aegra_api.services import run_cleanup


@pytest.fixture
def saver(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Patch db_manager so the checkpointer is observable."""
    checkpointer = AsyncMock()
    manager = MagicMock()
    manager.get_checkpointer.return_value = checkpointer
    monkeypatch.setattr(run_cleanup, "db_manager", manager)
    return checkpointer


class TestDeleteThreadCheckpoints:
    """The helper both API and cascade paths share."""

    @pytest.mark.asyncio
    async def test_delegates_to_the_saver(self, saver: AsyncMock) -> None:
        """adelete_thread is what drops all three checkpointer tables."""
        await run_cleanup.delete_thread_checkpoints("t-1")
        saver.adelete_thread.assert_awaited_once_with("t-1")

    @pytest.mark.asyncio
    async def test_checkpointer_failure_does_not_propagate(self, saver: AsyncMock) -> None:
        """Best-effort: the metadata rows are already gone by this point.

        Raising here would surface a 500 on a delete that mostly succeeded, and
        the caller has no way to retry just the checkpoint half.
        """
        saver.adelete_thread.side_effect = SQLAlchemyError("checkpointer down")
        await run_cleanup.delete_thread_checkpoints("t-1")

    @pytest.mark.asyncio
    async def test_programmer_errors_still_propagate(self, saver: AsyncMock) -> None:
        """Only infra failures are swallowed; a bug must not be hidden."""
        saver.adelete_thread.side_effect = TypeError("wrong arity")
        with pytest.raises(TypeError):
            await run_cleanup.delete_thread_checkpoints("t-1")


class TestDeleteThreadByIdCleansCheckpoints:
    """delete_thread_by_id is the cascade path (stateless cleanup, cron, assistant)."""

    @pytest.mark.asyncio
    async def test_checkpoints_deleted_after_the_row(self, saver: AsyncMock, monkeypatch: pytest.MonkeyPatch) -> None:
        """The row goes first, then its checkpoints — no flag, no opt-out."""
        thread = MagicMock()
        session = AsyncMock()
        session.scalar = AsyncMock(return_value=thread)
        scalars = MagicMock()
        scalars.all.return_value = []
        session.scalars = AsyncMock(return_value=scalars)

        maker = MagicMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=session)
        ctx.__aexit__ = AsyncMock(return_value=False)
        maker.return_value = ctx
        monkeypatch.setattr(run_cleanup, "_get_session_maker", lambda: maker)

        await run_cleanup.delete_thread_by_id("t-1", "user-1")

        session.delete.assert_awaited_once_with(thread)
        saver.adelete_thread.assert_awaited_once_with("t-1")

    @pytest.mark.asyncio
    async def test_missing_thread_skips_checkpoint_deletion(
        self, saver: AsyncMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A thread the caller does not own must not have its state touched.

        The ownership-scoped lookup returning None means either "gone" or "not
        yours"; wiping checkpoints on that path would let one user destroy
        another's state by guessing an id.
        """
        session = AsyncMock()
        session.scalar = AsyncMock(return_value=None)
        scalars = MagicMock()
        scalars.all.return_value = []
        session.scalars = AsyncMock(return_value=scalars)

        maker = MagicMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=session)
        ctx.__aexit__ = AsyncMock(return_value=False)
        maker.return_value = ctx
        monkeypatch.setattr(run_cleanup, "_get_session_maker", lambda: maker)

        await run_cleanup.delete_thread_by_id("t-1", "user-1")

        saver.adelete_thread.assert_not_awaited()
