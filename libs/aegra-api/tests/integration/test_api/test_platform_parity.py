"""Integration tests for three LangGraph Platform behaviours Aegra was missing.

Sourced from the Agent Server changelog:
* ``Prefer: return=minimal`` on PATCH /threads → 204, no body (v0.7). The SDK's
  ``update(return_minimal=True)`` documents a ``None`` return; Aegra used to send
  a full body, so the client got a value where it expected none.
* ``GET /runs/crons/{cron_id}`` (v0.10). Absent from langgraph-sdk 0.4.2's
  CronClient, which is why an SDK-driven endpoint sweep did not surface it.
* ``/ok`` liveness path (v0.7), aliased onto the existing ``/live`` handler.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from tests.fixtures.clients import create_test_app, make_client
from tests.fixtures.database import DummySessionBase
from tests.fixtures.session_fixtures import override_session_dependency
from tests.fixtures.test_helpers import DummyThread, make_cron_row


class ThreadPatchSession(DummySessionBase):
    """Session yielding one owned thread for the PATCH path."""

    async def scalar(self, _stmt: Any) -> Any:
        thread = DummyThread("t-1", user_id="test-user")
        thread.metadata_json = {"owner": "test-user"}
        return thread

    async def commit(self) -> None:
        return None

    async def refresh(self, _obj: Any) -> None:
        return None


@pytest.fixture
def thread_client() -> TestClient:
    app = create_test_app(include_runs=False, include_threads=True)
    override_session_dependency(app, ThreadPatchSession)
    return make_client(app)


class TestPreferReturnMinimal:
    """PATCH /threads/{id} honours RFC 7240's Prefer: return=minimal."""

    def test_header_yields_204_with_no_body(self, thread_client: TestClient) -> None:
        resp = thread_client.patch(
            "/threads/t-1",
            json={"metadata": {"a": 1}},
            headers={"Prefer": "return=minimal"},
        )
        assert resp.status_code == 204
        assert resp.content == b""

    def test_without_header_returns_the_thread(self, thread_client: TestClient) -> None:
        resp = thread_client.patch("/threads/t-1", json={"metadata": {"a": 1}})
        assert resp.status_code == 200
        assert resp.json()["thread_id"] == "t-1"

    def test_other_prefer_values_are_ignored(self, thread_client: TestClient) -> None:
        """An unrelated Prefer token must not suppress the body."""
        resp = thread_client.patch(
            "/threads/t-1",
            json={"metadata": {"a": 1}},
            headers={"Prefer": "wait=10"},
        )
        assert resp.status_code == 200

    def test_token_among_several_is_honoured(self, thread_client: TestClient) -> None:
        """Prefer is a comma-separated list; the token can appear anywhere."""
        resp = thread_client.patch(
            "/threads/t-1",
            json={"metadata": {"a": 1}},
            headers={"Prefer": "wait=10, return=minimal"},
        )
        assert resp.status_code == 204

    def test_matching_is_case_insensitive(self, thread_client: TestClient) -> None:
        resp = thread_client.patch(
            "/threads/t-1",
            json={"metadata": {"a": 1}},
            headers={"Prefer": "Return=Minimal"},
        )
        assert resp.status_code == 204


class TestGetCronById:
    """GET /runs/crons/{cron_id} returns the caller's cron or 404."""

    @pytest.fixture
    def client(self) -> tuple[TestClient, AsyncMock]:
        from aegra_api.api import crons as crons_module
        from aegra_api.services.cron_service import get_cron_service

        service = AsyncMock()
        app = create_test_app(include_runs=False, include_threads=False)
        app.include_router(crons_module.router)
        app.dependency_overrides[get_cron_service] = lambda: service
        return make_client(app), service

    def test_returns_the_cron(self, client: tuple[TestClient, AsyncMock]) -> None:
        http, service = client
        from aegra_api.services.cron_service import cron_to_response

        service.get_cron.return_value = cron_to_response(make_cron_row(cron_id="c-1"))

        resp = http.get("/runs/crons/c-1")

        assert resp.status_code == 200
        assert resp.json()["cron_id"] == "c-1"
        service.get_cron.assert_awaited_once_with("c-1", "test-user")

    def test_propagates_404(self, client: tuple[TestClient, AsyncMock]) -> None:
        http, service = client
        from fastapi import HTTPException

        service.get_cron.side_effect = HTTPException(status_code=404, detail="Cron 'nope' not found")

        resp = http.get("/runs/crons/nope")

        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"]


class TestLivenessPaths:
    """/ok is the Platform's liveness path; /live is Aegra's original."""

    @pytest.fixture
    def client(self) -> TestClient:
        return make_client(create_test_app())

    @pytest.mark.parametrize("path", ["/ok", "/live"])
    def test_both_paths_served(self, client: TestClient, path: str) -> None:
        resp = client.get(path)
        assert resp.status_code == 200
        assert resp.json() == {"status": "alive"}

    def test_no_backend_touched(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        """Liveness must answer even with the database gone — that is the point.

        /health and /ready probe backends; a liveness probe that did the same
        would restart a pod whose only problem is an unreachable dependency.
        """
        from aegra_api.core import health

        broken = MagicMock()
        broken.engine = None
        broken.get_checkpointer.side_effect = RuntimeError("db down")
        monkeypatch.setattr(health, "db_manager", broken)

        assert client.get("/ok").status_code == 200
