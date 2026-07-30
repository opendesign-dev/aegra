"""Unit tests for the created_after/created_before predicates in /threads/search."""

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from aegra_api.api.threads import _build_thread_filters
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
    return Compiled(_build_thread_filters(request, User(identity="user-1", display_name="User One")))


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


class TestThreadHandlerFilters:
    """`@auth.on` filters are compiled, not merged into request.metadata.

    Regression: search/count previously only looked for a ``{"metadata": {...}}``
    envelope, so a handler returning flat constraints — or any ``$or`` /
    ``$contains`` operator — had its authorization scope silently dropped.
    """

    def test_flat_handler_filter_is_applied(self) -> None:
        result = Compiled(
            _build_thread_filters(
                ThreadSearchRequest(),
                User(identity="user-1"),
                {"team_id": "t1"},
            )
        )
        assert "@>" in result
        assert result.bound({"team_id": "t1"})

    def test_metadata_envelope_still_applied(self) -> None:
        """The historical envelope shape keeps working."""
        result = Compiled(
            _build_thread_filters(
                ThreadSearchRequest(),
                User(identity="user-1"),
                {"metadata": {"team_id": "t1"}},
            )
        )
        assert result.bound({"team_id": "t1"})

    def test_handler_operator_is_compiled_not_dropped(self) -> None:
        """A ``$contains`` operator would be meaningless as a dict merge."""
        result = Compiled(
            _build_thread_filters(
                ThreadSearchRequest(),
                User(identity="user-1"),
                {"tags": {"$contains": "admin"}},
            )
        )
        assert "@>" in result
        assert result.bound(["admin"])

    def test_handler_filter_does_not_replace_request_metadata(self) -> None:
        """Both constraints survive: the caller's filter AND the handler's."""
        result = Compiled(
            _build_thread_filters(
                ThreadSearchRequest(metadata={"env": "prod"}),
                User(identity="user-1"),
                {"team_id": "t1"},
            )
        )
        assert result.bound({"env": "prod"})
        assert result.bound({"team_id": "t1"})

    def test_no_handler_filter_adds_nothing(self) -> None:
        bare = Compiled(_build_thread_filters(ThreadSearchRequest(), User(identity="user-1"), None))
        assert "@>" not in bare


class TestThreadSearchScope:
    """Owner scoping mirrors runs: permission-only, no request field."""

    def test_scoped_to_caller_without_permission(self) -> None:
        result = _filters(ThreadSearchRequest())
        assert "thread.user_id = " in result
        assert result.bound("user-1")

    def test_permission_drops_ownership_predicate(self) -> None:
        result = Compiled(
            _build_thread_filters(
                ThreadSearchRequest(),
                User(identity="user-1", permissions=["threads:search:all"]),
            )
        )
        assert "thread.user_id" not in result
