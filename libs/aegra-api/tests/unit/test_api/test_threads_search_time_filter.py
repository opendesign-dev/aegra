"""Unit tests for the created_after/created_before predicates in /threads/search."""

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from aegra_api.api.threads import _search_filters
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.models import ThreadSearchRequest, User


class Compiled:
    """A compiled WHERE clause, queryable by SQL text and by bound value."""

    def __init__(self, predicates: list[Any]) -> None:
        stmt = select(ThreadORM.thread_id).where(*predicates)
        compiled = stmt.compile(dialect=postgresql.dialect())
        self.sql = str(compiled)
        self.values = list(compiled.params.values())

    def __contains__(self, fragment: str) -> bool:
        return fragment in self.sql

    def bound(self, value: Any) -> bool:
        return value in self.values


def _filters(request: ThreadSearchRequest) -> Compiled:
    return Compiled(_search_filters(request, User(identity="user-1", display_name="User One")))


class TestThreadSearchTimeWindow:
    """The window narrows on thread.created_at and stays optional."""

    def test_no_window_adds_no_predicate(self) -> None:
        """Omitting both bounds leaves created_at out of the WHERE entirely."""
        assert "created_at" not in _filters(ThreadSearchRequest())

    def test_created_after_is_inclusive_lower_bound(self) -> None:
        after = datetime(2026, 1, 1, tzinfo=UTC)
        result = _filters(ThreadSearchRequest(created_after=after))
        assert "thread.created_at >= " in result
        assert result.bound(after)

    def test_created_before_is_inclusive_upper_bound(self) -> None:
        before = datetime(2026, 2, 1, tzinfo=UTC)
        result = _filters(ThreadSearchRequest(created_before=before))
        assert "thread.created_at <= " in result
        assert result.bound(before)

    def test_both_bounds_produce_a_closed_window(self) -> None:
        result = _filters(
            ThreadSearchRequest(
                created_after=datetime(2026, 1, 1, tzinfo=UTC),
                created_before=datetime(2026, 2, 1, tzinfo=UTC),
            )
        )
        assert "thread.created_at >= " in result
        assert "thread.created_at <= " in result

    def test_window_composes_with_other_filters(self) -> None:
        """The window is additive — ownership and metadata predicates survive."""
        result = _filters(
            ThreadSearchRequest(
                metadata={"assistant_id": "a-1"},
                status="idle",
                created_after=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        assert "thread.user_id = " in result
        assert "thread.status = " in result
        assert "@>" in result
        assert "thread.created_at >= " in result
        assert result.bound("user-1")
