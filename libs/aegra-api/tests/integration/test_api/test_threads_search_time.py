"""Integration tests for the created_after/created_before window on thread search."""

import pytest
from fastapi.testclient import TestClient

from tests.fixtures.clients import create_test_app, make_client
from tests.fixtures.session_fixtures import ThreadSession, override_session_dependency
from tests.fixtures.test_helpers import DummyThread


@pytest.fixture
def client() -> TestClient:
    app = create_test_app(include_runs=False, include_threads=True)
    thread = DummyThread("thread-1")
    thread.metadata_json = {"assistant_id": "a-1"}
    override_session_dependency(app, ThreadSession, threads=[thread])
    return make_client(app)


class TestSearchWindowAccepted:
    """Valid windows reach the query on both /search and /count."""

    def test_search_accepts_both_bounds(self, client: TestClient) -> None:
        resp = client.post(
            "/threads/search",
            json={"created_after": "2026-01-01T00:00:00Z", "created_before": "2026-02-01T00:00:00Z"},
        )
        assert resp.status_code == 200, resp.text
        assert len(resp.json()) == 1

    def test_search_accepts_lower_bound_only(self, client: TestClient) -> None:
        resp = client.post("/threads/search", json={"created_after": "2026-01-01T00:00:00Z"})
        assert resp.status_code == 200, resp.text

    def test_search_accepts_naive_timestamp(self, client: TestClient) -> None:
        """A timestamp without an offset is read as UTC rather than rejected."""
        resp = client.post("/threads/search", json={"created_before": "2026-01-01T00:00:00"})
        assert resp.status_code == 200, resp.text

    def test_window_composes_with_assistant_metadata_filter(self, client: TestClient) -> None:
        """The window stacks with the metadata filter used for per-assistant history."""
        resp = client.post(
            "/threads/search",
            json={
                "metadata": {"assistant_id": "a-1"},
                "created_after": "2026-01-01T00:00:00Z",
                "sort_by": "created_at",
                "sort_order": "desc",
            },
        )
        assert resp.status_code == 200, resp.text


class TestSearchWindowValidation:
    """Bad windows are rejected at the boundary."""

    def test_inverted_window_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/threads/search",
            json={"created_after": "2026-03-01T00:00:00Z", "created_before": "2026-01-01T00:00:00Z"},
        )
        assert resp.status_code == 422

    def test_malformed_timestamp_returns_422(self, client: TestClient) -> None:
        resp = client.post("/threads/search", json={"created_after": "yesterday"})
        assert resp.status_code == 422

    def test_count_inverted_window_returns_422(self, client: TestClient) -> None:
        """/threads/count shares the request model, so it shares the validation."""
        resp = client.post(
            "/threads/count",
            json={"created_after": "2026-03-01T00:00:00Z", "created_before": "2026-01-01T00:00:00Z"},
        )
        assert resp.status_code == 422
