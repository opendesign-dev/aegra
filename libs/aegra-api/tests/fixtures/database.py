"""Database fixtures for tests"""

from collections.abc import AsyncIterator, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock


def make_mock_session(**overrides: Any) -> AsyncMock:
    """AsyncMock AsyncSession whose result objects are sync, like the real ones.

    A bare AsyncMock makes ``.all()`` awaitable, so callers doing
    ``list((await session.scalars(stmt)).all())`` — e.g. multitask resolution on
    run creation — get a coroutine and raise TypeError.
    """
    session = AsyncMock()
    session.add = MagicMock()
    result = MagicMock()
    result.all.return_value = []
    result.first.return_value = None
    result.one_or_none.return_value = None
    session.scalars = AsyncMock(return_value=result)
    session.execute = AsyncMock(return_value=result)
    for name, value in overrides.items():
        setattr(session, name, value)
    return session


class DummySessionBase:
    """Minimal emulation of SQLAlchemy AsyncSession for testing

    Override scalar/scalars/commit/refresh in subclasses/fixtures to return
    appropriate rows for a test. By default, returns empty data.
    """

    # Statement params are positional-only: subclasses name them freely (stmt,
    # _stmt, query) without tripping an override mismatch.
    async def __aenter__(self) -> "DummySessionBase":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object, /) -> bool:
        return False

    def add(self, obj: Any, /) -> None:
        """AsyncSession.add is sync in SQLAlchemy"""
        return None

    async def commit(self) -> None:
        return None

    async def refresh(self, obj: Any, /) -> None:
        return None

    async def scalar(self, stmt: Any, /) -> Any:
        return None

    async def scalars(self, stmt: Any, /) -> Any:
        class Result:
            def all(self) -> list[Any]:
                return []

        return Result()


def override_get_session_dep(
    session_factory: Callable[[], DummySessionBase],
) -> Callable[[], AsyncIterator[DummySessionBase]]:
    """Create a dependency override for get_session"""

    async def _dep():
        yield session_factory()

    return _dep
