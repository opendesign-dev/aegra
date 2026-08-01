"""Integration tests for the SDK's bulk run endpoints.

``POST /runs/cancel`` and ``POST /runs/batch`` were missing entirely, so the SDK
methods that target them returned 404.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.testclient import TestClient

from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import get_session
from tests.fixtures.clients import create_test_app, make_client
from tests.fixtures.database import DummySessionBase
from tests.fixtures.session_fixtures import override_get_session_dep


def _run(run_id: str, *, thread_id: str = "t-1", status: str = "running") -> RunORM:
    return RunORM(
        run_id=run_id,
        thread_id=thread_id,
        assistant_id="agent",
        user_id="test-user",
        status=status,
        input={},
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


def _client(active: list[RunORM]) -> TestClient:
    app = create_test_app(include_runs=True, include_threads=False)

    class Session(DummySessionBase):
        async def scalars(self, _stmt: object) -> object:
            result = MagicMock()
            result.all.return_value = active
            return result

        async def execute(self, _stmt: object) -> object:
            return MagicMock()

        async def commit(self) -> None:
            return None

    app.dependency_overrides[get_session] = override_get_session_dep(Session)
    return make_client(app)


class TestBulkCancel:
    def test_cancels_matched_runs_and_reports_ids(self) -> None:
        client = _client([_run("r-1"), _run("r-2")])

        with patch("aegra_api.api.stateless_runs.streaming_service.interrupt_run", new=AsyncMock()) as interrupt:
            resp = client.post("/runs/cancel", json={"status": "running"})

        assert resp.status_code == 200
        assert resp.json() == {"cancelled_count": 2, "run_ids": ["r-1", "r-2"], "action": "interrupt"}
        assert interrupt.await_count == 2

    def test_no_matches_reports_zero_rather_than_404(self) -> None:
        resp = _client([]).post("/runs/cancel", json={"status": "pending"})

        assert resp.status_code == 200
        assert resp.json()["cancelled_count"] == 0

    def test_rollback_discards_the_runs_checkpoints(self) -> None:
        """``action=rollback`` is the only variant that touches the checkpointer."""
        client = _client([_run("r-1")])
        checkpointer = AsyncMock()

        with (
            patch("aegra_api.api.stateless_runs.streaming_service.interrupt_run", new=AsyncMock()),
            patch("aegra_api.services.multitask.db_manager.supports", return_value=True),
            patch("aegra_api.services.multitask.db_manager.get_checkpointer", return_value=checkpointer),
        ):
            resp = client.post("/runs/cancel?action=rollback", json={"thread_id": "t-1", "run_ids": ["r-1"]})

        assert resp.json()["action"] == "rollback"
        checkpointer.adelete_for_runs.assert_awaited_once_with(["r-1"])

    def test_rollback_is_refused_when_the_checkpointer_cannot_scope_deletion(self) -> None:
        """501 rather than silently behaving like `interrupt`."""
        client = _client([_run("r-1")])

        with (
            patch("aegra_api.api.stateless_runs.streaming_service.interrupt_run", new=AsyncMock()) as interrupt,
            patch("aegra_api.services.multitask.db_manager.supports", return_value=False),
        ):
            resp = client.post("/runs/cancel?action=rollback", json={"thread_id": "t-1", "run_ids": ["r-1"]})

        assert resp.status_code == 501
        interrupt.assert_not_awaited()

    def test_run_ids_without_a_thread_is_rejected(self) -> None:
        """The SDK always pairs them; accepting a bare id list would scan globally."""
        resp = _client([]).post("/runs/cancel", json={"run_ids": ["r-1"]})
        assert resp.status_code == 422

    def test_no_target_is_rejected(self) -> None:
        resp = _client([]).post("/runs/cancel", json={})
        assert resp.status_code == 422

    def test_unknown_action_is_rejected(self) -> None:
        resp = _client([]).post("/runs/cancel?action=obliterate", json={"status": "all"})
        assert resp.status_code == 422


class TestBatchCreate:
    @pytest.mark.asyncio
    async def test_each_payload_becomes_its_own_run(self) -> None:
        from aegra_api.api.stateless_runs import stateless_create_run_batch
        from aegra_api.models import RunCreate, User

        created = []

        async def _fake_create(request: RunCreate, user: User, session: object) -> object:
            created.append(request.assistant_id)
            return MagicMock(run_id=f"r-{len(created)}")

        with patch("aegra_api.api.stateless_runs.stateless_create_run", new=_fake_create):
            runs = await stateless_create_run_batch(
                [RunCreate(assistant_id="a", input={}), RunCreate(assistant_id="b", input={})],
                User(identity="test-user", scopes=[]),
                AsyncMock(),
            )

        assert created == ["a", "b"]
        assert [r.run_id for r in runs] == ["r-1", "r-2"]

    def test_empty_batch_is_rejected(self) -> None:
        resp = _client([]).post("/runs/batch", json=[])
        assert resp.status_code == 422
