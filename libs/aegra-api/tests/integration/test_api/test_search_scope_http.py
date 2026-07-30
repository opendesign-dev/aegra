"""The `<resource>:search:all` gates, exercised over HTTP.

The predicate logic itself is pinned in tests/unit/test_core/test_search_scope.py.
What this file proves is the wiring: a permission on the authenticated user
actually reaches the SQL the search endpoint emits.
"""

from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import Executable

from aegra_api.core.orm import get_session as core_get_session
from tests.fixtures.clients import create_test_app, make_client
from tests.fixtures.database import override_get_session_dep
from tests.fixtures.session_fixtures import BasicSession


class CapturingSession(BasicSession):
    """Records the compiled SQL of every statement the route executes."""

    def __init__(self) -> None:
        super().__init__()
        self.statements: list[str] = []

    def _record(self, stmt: Executable) -> None:
        self.statements.append(str(stmt.compile(dialect=postgresql.dialect())))

    async def scalars(self, _stmt: Any) -> Any:
        self._record(_stmt)

        class Result:
            def all(self) -> list[Any]:
                return []

        return Result()

    async def scalar(self, _stmt: Any) -> Any:
        self._record(_stmt)
        return 0

    async def execute(self, _stmt: Any) -> Any:
        self._record(_stmt)

        class Result:
            def all(self) -> list[Any]:
                return []

        return Result()

    @property
    def sql(self) -> str:
        return "\n".join(self.statements)


def _client(session: CapturingSession, *, permissions: list[str] | None = None) -> TestClient:
    app = create_test_app(include_runs=True, include_threads=True, permissions=permissions)
    # One shared instance, not the usual per-request factory: the assertions read
    # back the SQL it collected.
    app.dependency_overrides[core_get_session] = override_get_session_dep(lambda: session)
    return make_client(app)


THREAD_ENDPOINTS = ["/threads/search", "/threads/count"]
RUN_ENDPOINTS = ["/runs/search", "/runs/count"]


class TestThreadSearchScope:
    @pytest.mark.parametrize("path", THREAD_ENDPOINTS)
    def test_scoped_to_caller_without_permission(self, path: str) -> None:
        session = CapturingSession()
        resp = _client(session).post(path, json={})
        assert resp.status_code == 200, resp.text
        assert "thread.user_id = " in session.sql

    @pytest.mark.parametrize("path", THREAD_ENDPOINTS)
    def test_permission_drops_ownership_predicate(self, path: str) -> None:
        session = CapturingSession()
        resp = _client(session, permissions=["threads:search:all"]).post(path, json={})
        assert resp.status_code == 200, resp.text
        assert "thread.user_id = " not in session.sql

    @pytest.mark.parametrize("path", THREAD_ENDPOINTS)
    def test_other_resource_permission_does_not_widen_scope(self, path: str) -> None:
        session = CapturingSession()
        resp = _client(session, permissions=["runs:search:all"]).post(path, json={})
        assert resp.status_code == 200, resp.text
        assert "thread.user_id = " in session.sql

    def test_owner_field_in_body_is_rejected_not_honoured(self) -> None:
        """There is no request field for other owners; sending one must not scope."""
        session = CapturingSession()
        resp = _client(session).post("/threads/search", json={"user_ids": ["someone-else"]})
        assert resp.status_code == 200, resp.text
        # Unknown key ignored by the model, and the caller stays pinned to itself.
        assert "thread.user_id = " in session.sql
        assert "someone-else" not in session.sql


class TestRunSearchScope:
    @pytest.mark.parametrize("path", RUN_ENDPOINTS)
    def test_scoped_to_caller_without_permission(self, path: str) -> None:
        session = CapturingSession()
        resp = _client(session).post(path, json={})
        assert resp.status_code == 200, resp.text
        assert "runs.user_id = " in session.sql

    @pytest.mark.parametrize("path", RUN_ENDPOINTS)
    def test_permission_drops_ownership_predicate(self, path: str) -> None:
        session = CapturingSession()
        resp = _client(session, permissions=["runs:search:all"]).post(path, json={})
        assert resp.status_code == 200, resp.text
        assert "runs.user_id = " not in session.sql

    @pytest.mark.parametrize("path", RUN_ENDPOINTS)
    def test_other_resource_permission_does_not_widen_scope(self, path: str) -> None:
        session = CapturingSession()
        resp = _client(session, permissions=["threads:search:all"]).post(path, json={})
        assert resp.status_code == 200, resp.text
        assert "runs.user_id = " in session.sql
