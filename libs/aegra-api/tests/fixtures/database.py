"""Database fixtures for tests"""

from collections.abc import AsyncIterator, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock


class DummySessionBase:
    """Minimal emulation of SQLAlchemy AsyncSession for testing

    Override scalar/scalars/commit/refresh in subclasses/fixtures to return
    appropriate rows for a test. By default, returns empty data.
    """

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def add(self, _):
        """AsyncSession.add is sync in SQLAlchemy"""
        return None

    async def commit(self):
        return None

    async def refresh(self, _obj):
        return None

    async def scalar(self, _stmt):
        return None

    async def scalars(self, _stmt):
        class Result:
            def all(self_inner):
                return []

        return Result()


def override_get_session_dep(
    session_factory: Callable[[], DummySessionBase],
) -> Callable[[], AsyncIterator[DummySessionBase]]:
    """Create a dependency override for get_session"""

    async def _dep():
        yield session_factory()

    return _dep


def make_session_maker(session: Any) -> MagicMock:
    """Stand in for ``_get_session_maker()``, handing out one prepared session.

    Shared home for a helper that had been copy-pasted into every test module
    that patches the session maker.
    """
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=ctx)
