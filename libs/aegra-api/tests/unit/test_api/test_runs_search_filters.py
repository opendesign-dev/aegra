"""Unit tests for the WHERE predicates behind /runs/search and /runs/count."""

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
        # An IN predicate binds its whole list as one expanding param, so look inside lists too.
        return value in self.values or any(isinstance(v, list) and value in v for v in self.values)


def _user(*, permissions: list[str] | None = None) -> User:
    return User(identity="user-1", display_name="User One", permissions=permissions or [])


def _filters(
    request: RunSearchRequest,
    auth_filters: dict[str, Any] | None = None,
    *,
    cross_user: bool = False,
) -> Compiled:
    """Build predicates with graph resolution stubbed to a known registry."""
    service = Mock()
    service.list_graphs.return_value = {"known-graph": "graph.py"}
    with patch("aegra_api.api.runs.get_langgraph_service", return_value=service):
        return Compiled(_run_search_filters(request, _user(), auth_filters, cross_user=cross_user))


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


class TestListForms:
    """One field per concept, accepting a scalar or a list — matching the SDK's search shape."""

    def test_thread_id_accepts_a_list(self) -> None:
        result = _filters(RunSearchRequest(thread_id=["t-1", "t-2"]))
        assert "runs.thread_id IN " in result
        assert result.bound("t-1")
        assert result.bound("t-2")

    def test_ids_filter(self) -> None:
        """Named `ids`, like threads.search — not `run_ids`."""
        result = _filters(RunSearchRequest(ids=["r-1", "r-2"]))
        assert "runs.run_id IN " in result
        assert result.bound("r-1")

    def test_status_accepts_a_list(self) -> None:
        result = _filters(RunSearchRequest(status=["success", "error"]))
        assert "runs.status IN " in result
        assert result.bound("success")
        assert result.bound("error")

    def test_assistant_id_list_passed_through_when_not_graphs(self) -> None:
        result = _filters(RunSearchRequest(assistant_id=["assistant-uuid-1", "assistant-uuid-2"]))
        assert "runs.assistant_id IN " in result
        assert result.bound("assistant-uuid-1")

    def test_assistant_id_list_resolves_graph_ids(self) -> None:
        """Every entry resolves the same way a scalar assistant_id does."""
        result = _filters(RunSearchRequest(assistant_id=["known-graph"]))
        assert "runs.assistant_id IN " in result
        assert not result.bound("known-graph")

    def test_list_form_keeps_ownership_predicate(self) -> None:
        """List forms are additive, not a way around caller scoping."""
        result = _filters(RunSearchRequest(ids=["r-1"]))
        assert "runs.user_id = " in result
        assert result.bound("user-1")


class TestCrossUserScoping:
    """Scope comes from permissions only — no request field can widen it."""

    def test_cross_user_drops_ownership_predicate(self) -> None:
        """A permitted caller sees the whole deployment."""
        result = _filters(RunSearchRequest(), cross_user=True)
        assert "runs.user_id" not in result

    def test_default_is_caller_scoped(self) -> None:
        result = _filters(RunSearchRequest())
        assert "runs.user_id = " in result
        assert result.bound("user-1")

    def test_cross_user_keeps_other_filters(self) -> None:
        result = _filters(RunSearchRequest(status=["error"]), cross_user=True)
        assert "runs.status IN " in result
        assert "runs.user_id" not in result

    def test_no_user_field_exists_to_send(self) -> None:
        """Regression: a `user_ids` request field would let a client widen its own
        scope, so the surface must not carry one — the permission is the only gate."""
        assert "user_ids" not in RunSearchRequest.model_fields


class TestSelectFieldRegistry:
    """The GET and POST paths share one select-field vocabulary."""

    def test_select_fields_match_model_literal(self) -> None:
        """_RUN_SELECT_FIELDS is derived from RunSelectField, so they cannot drift."""
        assert "run_id" in _RUN_SELECT_FIELDS
        assert "assistant_id" in _RUN_SELECT_FIELDS
        assert "not_a_column" not in _RUN_SELECT_FIELDS
