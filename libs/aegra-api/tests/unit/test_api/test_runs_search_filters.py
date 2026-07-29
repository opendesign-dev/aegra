"""Unit tests for the WHERE predicates behind /runs/search and /runs/count."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock, patch

from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from aegra_api.api.runs import _RUN_SELECT_FIELDS, _run_search_filters
from aegra_api.core.orm import Run as RunORM
from aegra_api.models import RunSearchRequest, User


class Compiled:
    """A compiled WHERE clause, queryable by SQL text and by bound value."""

    def __init__(self, predicates: list[Any]) -> None:
        stmt = select(RunORM.run_id).where(*predicates)
        # No literal_binds: JSONB values have no literal renderer, so values are
        # asserted through the bound params instead of the SQL text.
        compiled = stmt.compile(dialect=postgresql.dialect())
        self.sql = str(compiled)
        self.values = list(compiled.params.values())

    def __contains__(self, fragment: str) -> bool:
        return fragment in self.sql

    def bound(self, value: Any) -> bool:
        return value in self.values


def _user() -> User:
    return User(identity="user-1", display_name="User One")


def _filters(request: RunSearchRequest, auth_filters: dict[str, Any] | None = None) -> Compiled:
    """Build predicates with graph resolution stubbed to a known registry."""
    service = Mock()
    service.list_graphs.return_value = {"known-graph": "graph.py"}
    with patch("aegra_api.api.runs.get_langgraph_service", return_value=service):
        return Compiled(_run_search_filters(request, _user(), auth_filters))


class TestOwnershipScoping:
    """Every query is scoped to the caller at the SQL layer."""

    def test_user_id_always_present(self) -> None:
        """An unfiltered search still constrains user_id."""
        result = _filters(RunSearchRequest())
        assert "runs.user_id = " in result
        assert result.bound("user-1")

    def test_user_id_present_alongside_other_filters(self) -> None:
        """Adding filters never replaces the ownership predicate."""
        result = _filters(RunSearchRequest(thread_id="t-1", status="success"))
        assert "runs.user_id = " in result
        assert result.bound("user-1")

    def test_bare_request_has_no_extra_predicates(self) -> None:
        """No filters means no time, status, or assistant predicates leak in."""
        result = _filters(RunSearchRequest())
        assert "created_at" not in result
        assert "runs.status" not in result
        assert "assistant_id" not in result


class TestScalarFilters:
    """Scalar filters map to equality predicates on their own columns."""

    def test_thread_id_filter(self) -> None:
        result = _filters(RunSearchRequest(thread_id="thread-9"))
        assert "runs.thread_id = " in result
        assert result.bound("thread-9")

    def test_status_filter(self) -> None:
        result = _filters(RunSearchRequest(status="error"))
        assert "runs.status = " in result
        assert result.bound("error")

    def test_assistant_id_passed_through_when_not_a_graph(self) -> None:
        """A UUID assistant id is used verbatim."""
        result = _filters(RunSearchRequest(assistant_id="assistant-uuid-1"))
        assert "runs.assistant_id = " in result
        assert result.bound("assistant-uuid-1")

    def test_graph_id_resolved_to_canonical_assistant_id(self) -> None:
        """A graph id resolves the same way run creation resolves it."""
        result = _filters(RunSearchRequest(assistant_id="known-graph"))
        assert "runs.assistant_id = " in result
        assert not result.bound("known-graph")

    def test_metadata_filter_uses_jsonb_containment(self) -> None:
        """Metadata filtering is JSONB containment, not string equality."""
        result = _filters(RunSearchRequest(metadata={"team": "core"}))
        assert "@>" in result
        assert result.bound({"team": "core"})


class TestTimeWindow:
    """created_after/created_before become inclusive range predicates."""

    def test_created_after_is_inclusive_lower_bound(self) -> None:
        after = datetime(2026, 1, 1, tzinfo=UTC)
        result = _filters(RunSearchRequest(created_after=after))
        assert "runs.created_at >= " in result
        assert result.bound(after)

    def test_created_before_is_inclusive_upper_bound(self) -> None:
        before = datetime(2026, 2, 1, tzinfo=UTC)
        result = _filters(RunSearchRequest(created_before=before))
        assert "runs.created_at <= " in result
        assert result.bound(before)

    def test_both_bounds_produce_a_closed_window(self) -> None:
        result = _filters(
            RunSearchRequest(
                created_after=datetime(2026, 1, 1, tzinfo=UTC),
                created_before=datetime(2026, 2, 1, tzinfo=UTC),
            )
        )
        assert "runs.created_at >= " in result
        assert "runs.created_at <= " in result


class TestAuthHandlerFilters:
    """Filters returned by an @auth.on handler are appended, not substituted."""

    def test_handler_filter_compiled_into_where(self) -> None:
        """A handler's metadata constraint reaches the query without dropping ownership."""
        result = _filters(RunSearchRequest(), {"team_id": "t-1"})
        assert "@>" in result
        assert "runs.user_id = " in result
        assert result.bound("user-1")

    def test_no_handler_filter_is_a_noop(self) -> None:
        """None from the handler adds nothing."""
        result = _filters(RunSearchRequest(), None)
        assert "@>" not in result


class TestSelectFieldRegistry:
    """The GET and POST paths share one select-field vocabulary."""

    def test_select_fields_match_model_literal(self) -> None:
        """_RUN_SELECT_FIELDS is derived from RunSelectField, so they cannot drift."""
        assert "run_id" in _RUN_SELECT_FIELDS
        assert "assistant_id" in _RUN_SELECT_FIELDS
        assert "not_a_column" not in _RUN_SELECT_FIELDS
