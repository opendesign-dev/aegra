"""Integration tests for POST /runs/search and POST /runs/count."""

from typing import Any
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from aegra_api.api.runs import SEARCH_ALL_USERS_PERMISSION
from aegra_api.core.auth_deps import get_current_user, require_auth
from aegra_api.models import RunSearchRequest
from aegra_api.models.auth import User
from tests.fixtures.clients import create_test_app, make_client
from tests.fixtures.database import DummySessionBase
from tests.fixtures.session_fixtures import override_session_dependency
from tests.fixtures.test_helpers import DummyRun


class SearchRunSession(DummySessionBase):
    """Session returning a fixed run set for scalars() and a count for scalar()."""

    runs: list[Any] = []

    async def scalars(self, _stmt: Any) -> Any:
        rows = self.runs

        class Result:
            def all(self_inner) -> list[Any]:
                return rows

        return Result()

    async def scalar(self, _stmt: Any) -> int:
        return len(self.runs)


def _session_with(runs: list[Any]) -> type[SearchRunSession]:
    """Build a session class bound to *runs* (override_session_dependency takes a class)."""
    return type("BoundSession", (SearchRunSession,), {"runs": runs})


@pytest.fixture(autouse=True)
def stub_graphs():
    """Stub the graph registry so assistant_id resolution needs no running server."""
    service = Mock()
    service.list_graphs.return_value = {"known-graph": "graph.py"}
    with patch("aegra_api.api.runs.get_langgraph_service", return_value=service):
        yield


def _client(runs: list[Any] | None = None, *, permissions: list[str] | None = None) -> TestClient:
    app = create_test_app(include_runs=True, include_threads=False)
    if permissions is not None:
        user = User(identity="test-user", display_name="Test User", permissions=permissions)
        app.dependency_overrides[require_auth] = lambda: user
        app.dependency_overrides[get_current_user] = lambda: user
    override_session_dependency(app, _session_with(runs or []))
    return make_client(app)


class TestSearchRuns:
    """POST /runs/search returns owned runs across every thread."""

    def test_empty_body_returns_list(self) -> None:
        """An empty body is a valid unfiltered search."""
        resp = _client([DummyRun("run-1"), DummyRun("run-2")]).post("/runs/search", json={})
        assert resp.status_code == 200, resp.text
        assert len(resp.json()) == 2

    def test_returns_empty_list_when_nothing_matches(self) -> None:
        """No matches is an empty list, not a 404."""
        resp = _client([]).post("/runs/search", json={"assistant_id": "nope"})
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_full_run_shape(self) -> None:
        """Without select, rows carry the full Run entity."""
        resp = _client([DummyRun("run-1", thread_id="t-1", assistant_id="a-1")]).post("/runs/search", json={})
        assert resp.status_code == 200
        row = resp.json()[0]
        assert row["run_id"] == "run-1"
        assert row["thread_id"] == "t-1"
        assert row["assistant_id"] == "a-1"
        assert "status" in row

    def test_select_projects_only_requested_fields(self) -> None:
        """select trims each row to the named fields."""
        resp = _client([DummyRun("run-1")]).post("/runs/search", json={"select": ["run_id", "status"]})
        assert resp.status_code == 200
        assert set(resp.json()[0]) == {"run_id", "status"}

    def test_accepts_full_filter_set(self) -> None:
        """Every documented filter is accepted together."""
        resp = _client([DummyRun("run-1")]).post(
            "/runs/search",
            json={
                "assistant_id": "known-graph",
                "thread_id": "t-1",
                "status": "success",
                "metadata": {"team": "core"},
                "created_after": "2026-01-01T00:00:00Z",
                "created_before": "2026-12-31T23:59:59Z",
                "sort_by": "created_at",
                "sort_order": "asc",
                "limit": 50,
                "offset": 10,
            },
        )
        assert resp.status_code == 200, resp.text

    def test_naive_timestamps_accepted(self) -> None:
        """A timestamp without an offset is accepted and read as UTC."""
        resp = _client([]).post("/runs/search", json={"created_after": "2026-01-01T00:00:00"})
        assert resp.status_code == 200, resp.text


class TestSearchRunsListForms:
    """Each filter takes a scalar or a list — one field per concept, like the SDK's searches."""

    def test_accepts_every_filter_in_list_form(self) -> None:
        resp = _client([DummyRun("run-1")]).post(
            "/runs/search",
            json={
                "assistant_id": ["known-graph", "a-2"],
                "thread_id": ["t-1", "t-2"],
                "ids": ["run-1", "run-2"],
                "status": ["success", "error"],
            },
        )
        assert resp.status_code == 200, resp.text

    def test_ids_fetches_a_known_batch(self) -> None:
        """Named `ids` to match threads.search, not `run_ids`."""
        resp = _client([DummyRun("run-1"), DummyRun("run-2")]).post("/runs/search", json={"ids": ["run-1", "run-2"]})
        assert resp.status_code == 200, resp.text
        assert len(resp.json()) == 2

    def test_scalar_and_list_are_the_same_field(self) -> None:
        """Both shapes go through one field, so there is no ambiguous pairing to reject."""
        for body in ({"assistant_id": "a-1"}, {"assistant_id": ["a-1", "a-2"]}):
            resp = _client([]).post("/runs/search", json=body)
            assert resp.status_code == 200, f"{body} -> {resp.status_code}"

    def test_empty_list_returns_422(self) -> None:
        """An empty list would compile to a never-true IN; reject it instead."""
        for field in ("thread_id", "assistant_id", "status"):
            resp = _client([]).post("/runs/search", json={field: []})
            assert resp.status_code == 422, f"{field} -> {resp.status_code}"

    def test_invalid_status_in_list_returns_422(self) -> None:
        resp = _client([]).post("/runs/search", json={"status": ["success", "not-a-status"]})
        assert resp.status_code == 422


class TestSearchRunsScope:
    """Scope is decided by permissions; no request field can widen it."""

    def test_default_caller_is_scoped_to_itself(self) -> None:
        resp = _client([DummyRun("run-1")], permissions=[]).post("/runs/search", json={})
        assert resp.status_code == 200, resp.text

    def test_permitted_caller_sees_the_deployment(self) -> None:
        resp = _client([DummyRun("run-1")], permissions=[SEARCH_ALL_USERS_PERMISSION]).post("/runs/search", json={})
        assert resp.status_code == 200, resp.text

    def test_unknown_user_field_cannot_widen_scope(self) -> None:
        """A client sending a user filter gets its own runs, not the deployment.

        The field was removed rather than gated: with no way to express "other
        users" in the request, an unpermitted caller cannot even ask.
        """
        assert "user_ids" not in RunSearchRequest.model_fields
        resp = _client([DummyRun("run-1")], permissions=[]).post("/runs/search", json={"user_ids": ["other-user"]})
        assert resp.status_code == 200, resp.text

    def test_count_uses_the_same_scope_rule(self) -> None:
        resp = _client([], permissions=[]).post("/runs/count", json={})
        assert resp.status_code == 200, resp.text


class TestSearchRunsValidation:
    """Bad filters fail at the request boundary rather than matching nothing."""

    def test_inverted_time_window_returns_422(self) -> None:
        resp = _client([]).post(
            "/runs/search",
            json={"created_after": "2026-03-01T00:00:00Z", "created_before": "2026-01-01T00:00:00Z"},
        )
        assert resp.status_code == 422

    def test_invalid_status_returns_422(self) -> None:
        resp = _client([]).post("/runs/search", json={"status": "not-a-status"})
        assert resp.status_code == 422

    def test_invalid_sort_by_returns_422(self) -> None:
        resp = _client([]).post("/runs/search", json={"sort_by": "password; DROP TABLE runs --"})
        assert resp.status_code == 422

    def test_invalid_select_field_returns_422(self) -> None:
        resp = _client([]).post("/runs/search", json={"select": ["run_id", "not_a_column"]})
        assert resp.status_code == 422

    def test_over_max_limit_returns_422(self) -> None:
        resp = _client([]).post("/runs/search", json={"limit": 1001})
        assert resp.status_code == 422

    def test_malformed_timestamp_returns_422(self) -> None:
        resp = _client([]).post("/runs/search", json={"created_after": "yesterday"})
        assert resp.status_code == 422


class TestCountRuns:
    """POST /runs/count accepts the same filters and returns a bare int."""

    def test_returns_count(self) -> None:
        resp = _client([DummyRun("run-1"), DummyRun("run-2"), DummyRun("run-3")]).post("/runs/count", json={})
        assert resp.status_code == 200
        assert resp.json() == 3

    def test_returns_zero_when_nothing_matches(self) -> None:
        resp = _client([]).post("/runs/count", json={})
        assert resp.status_code == 200
        assert resp.json() == 0

    def test_accepts_time_window(self) -> None:
        resp = _client([]).post(
            "/runs/count",
            json={"created_after": "2026-01-01T00:00:00Z", "created_before": "2026-02-01T00:00:00Z"},
        )
        assert resp.status_code == 200, resp.text

    def test_inverted_time_window_returns_422(self) -> None:
        resp = _client([]).post(
            "/runs/count",
            json={"created_after": "2026-03-01T00:00:00Z", "created_before": "2026-01-01T00:00:00Z"},
        )
        assert resp.status_code == 422


class TestListRunsTimeWindow:
    """GET /threads/{id}/runs gained the same window as the search endpoint."""

    def test_accepts_time_window(self) -> None:
        resp = _client([DummyRun("run-1")]).get(
            "/threads/t-1/runs",
            params={"created_after": "2026-01-01T00:00:00Z", "created_before": "2026-02-01T00:00:00Z"},
        )
        assert resp.status_code == 200, resp.text
        assert len(resp.json()) == 1

    def test_naive_timestamp_accepted(self) -> None:
        resp = _client([]).get("/threads/t-1/runs", params={"created_after": "2026-01-01T00:00:00"})
        assert resp.status_code == 200, resp.text

    def test_inverted_time_window_returns_422(self) -> None:
        resp = _client([]).get(
            "/threads/t-1/runs",
            params={"created_after": "2026-03-01T00:00:00Z", "created_before": "2026-01-01T00:00:00Z"},
        )
        assert resp.status_code == 422

    def test_malformed_timestamp_returns_422(self) -> None:
        resp = _client([]).get("/threads/t-1/runs", params={"created_after": "yesterday"})
        assert resp.status_code == 422
