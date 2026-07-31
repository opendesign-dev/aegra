"""Unit tests for CronScheduler background task.

Database, run preparation, and the settle writes are mocked. Every ``_fire_cron`` exit
path ends in exactly one ``CronService`` settle call, so these tests assert on that seam;
the settle rules themselves (advance vs disable, failure counting) live in
test_cron_service.py.
"""

import asyncio
import contextlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException

from aegra_api.services.cron_scheduler import (
    CronScheduler,
    _build_run_request,
    _resolve_approval,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cron_orm(
    *,
    cron_id: str = "cron-001",
    assistant_id: str = "asst-001",
    thread_id: str | None = None,
    user_id: str = "test-user",
    schedule: str = "*/5 * * * *",
    payload: dict[str, Any] | None = None,
    metadata: Any = None,
    enabled: bool = True,
    on_run_completed: str | None = None,
    end_time: datetime | None = None,
    next_run_date: datetime | None = None,
) -> Mock:
    """Build a mock CronORM row for scheduler tests."""
    now = datetime.now(UTC)
    cron = Mock()
    cron.cron_id = cron_id
    cron.assistant_id = assistant_id
    cron.thread_id = thread_id
    cron.user_id = user_id
    cron.schedule = schedule
    cron.payload = payload if payload is not None else {"input": {"msg": "tick"}}
    cron.metadata_dict = {} if metadata is None else metadata
    cron.enabled = enabled
    cron.on_run_completed = on_run_completed
    cron.end_time = end_time
    cron.next_run_date = next_run_date or now
    return cron


@contextlib.contextmanager
def _patch_service() -> Iterator[Mock]:
    """Patch the CronService the scheduler claims and settles through."""
    with patch("aegra_api.services.cron_scheduler.CronService") as cls:
        service = cls.return_value
        service.claim_due_crons = AsyncMock(return_value=[])
        service.advance_next_run = AsyncMock()
        service.release_claim = AsyncMock()
        service.disable_cron = AsyncMock()
        yield service


@contextlib.contextmanager
def _patch_session_maker() -> Iterator[AsyncMock]:
    """One AsyncMock session, handed out for the claim read and every firing."""
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    with patch("aegra_api.services.cron_scheduler.get_session_maker", return_value=Mock(return_value=session)):
        yield session


def _patch_prepare_run(**kwargs: Any) -> Any:
    """Patch prepare_run, returning the ``(run_id, run, job)`` triple by default."""
    kwargs.setdefault("return_value", ("run-1", Mock(), None))
    return patch("aegra_api.services.cron_scheduler.prepare_run", new_callable=AsyncMock, **kwargs)


# ---------------------------------------------------------------------------
# Lifecycle: start / stop
# ---------------------------------------------------------------------------


class TestSchedulerLifecycle:
    """Test CronScheduler.start() and stop()."""

    @pytest.mark.asyncio
    async def test_start_creates_task(self) -> None:
        scheduler = CronScheduler()
        with patch.object(scheduler, "_loop", new_callable=AsyncMock):
            await scheduler.start()
            assert scheduler._task is not None
            assert scheduler._running is True
            await scheduler.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_task(self) -> None:
        scheduler = CronScheduler()
        with patch.object(scheduler, "_loop", new_callable=AsyncMock):
            await scheduler.start()
            await scheduler.stop()
            assert scheduler._running is False
            assert scheduler._task is None

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self) -> None:
        scheduler = CronScheduler()
        await scheduler.stop()  # should not raise
        assert scheduler._running is False
        assert scheduler._task is None


# ---------------------------------------------------------------------------
# _tick
# ---------------------------------------------------------------------------


class TestSchedulerTick:
    """Test CronScheduler._tick()."""

    @pytest.mark.asyncio
    async def test_tick_claims_through_cron_service(self) -> None:
        """Claiming is a service concern; the tick only fans the result out."""
        scheduler = CronScheduler()

        with (
            _patch_session_maker(),
            _patch_service() as service,
            patch.object(scheduler, "_fire_cron", new_callable=AsyncMock) as mock_fire,
        ):
            await scheduler._tick()

        service.claim_due_crons.assert_awaited_once()
        assert isinstance(service.claim_due_crons.await_args.args[0], datetime)
        mock_fire.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_tick_fires_due_crons(self) -> None:
        scheduler = CronScheduler()
        cron = _make_cron_orm()

        with (
            _patch_session_maker() as session,
            _patch_service() as service,
            patch.object(scheduler, "_fire_cron", new_callable=AsyncMock) as mock_fire,
        ):
            service.claim_due_crons.return_value = [cron]
            await scheduler._tick()

        assert mock_fire.await_args.args[:2] == (session, cron)
        # The claim instant travels with the firing: the advance anchors to it.
        assert isinstance(mock_fire.await_args.args[2], datetime)

    @pytest.mark.asyncio
    async def test_tick_continues_on_fire_error(self) -> None:
        """A failing cron should not prevent other crons from firing."""
        scheduler = CronScheduler()
        fired: list[str] = []

        async def _side_effect(_session: Any, cron: Any, _claimed_at: Any) -> None:
            fired.append(cron.cron_id)
            if cron.cron_id == "fail":
                raise RuntimeError("boom")

        with (
            _patch_session_maker(),
            _patch_service() as service,
            patch.object(scheduler, "_fire_cron", side_effect=_side_effect),
        ):
            service.claim_due_crons.return_value = [_make_cron_orm(cron_id="fail"), _make_cron_orm(cron_id="ok")]
            await scheduler._tick()

        assert fired == ["fail", "ok"]

    @pytest.mark.asyncio
    async def test_tick_caps_concurrent_firings(self) -> None:
        """Firing serially lets a slow batch outlive its own claims, at which point
        another poller re-claims the tail and fires it a second time."""
        scheduler = CronScheduler()
        crons = [_make_cron_orm(cron_id=f"c{i}") for i in range(5)]
        inflight = 0
        peak = 0

        async def _slow_fire(_session: Any, _cron: Any, _claimed_at: Any) -> None:
            nonlocal inflight, peak
            inflight += 1
            peak = max(peak, inflight)
            await asyncio.sleep(0.01)
            inflight -= 1

        with (
            _patch_session_maker(),
            _patch_service() as service,
            patch("aegra_api.services.cron_scheduler.settings") as cfg,
            patch.object(scheduler, "_fire_cron", side_effect=_slow_fire),
        ):
            cfg.cron.CRON_FIRE_CONCURRENCY = 2
            service.claim_due_crons.return_value = crons
            await scheduler._tick()

        assert peak == 2


# ---------------------------------------------------------------------------
# _fire_cron
# ---------------------------------------------------------------------------


class TestFireCron:
    """Test CronScheduler._fire_cron()."""

    @pytest.mark.asyncio
    async def test_fired_cron_advances_the_schedule(self) -> None:
        cron = _make_cron_orm(end_time=None)

        with _patch_service() as service, _patch_prepare_run() as mock_prepare:
            await CronScheduler._fire_cron(AsyncMock(), cron)

        mock_prepare.assert_awaited_once()
        service.advance_next_run.assert_awaited_once_with(cron.cron_id, base=ANY)
        service.release_claim.assert_not_awaited()
        service.disable_cron.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_schedules_cleanup_for_stateless_cron_by_default(self) -> None:
        cron = _make_cron_orm(thread_id=None, end_time=None)

        with (
            _patch_service(),
            _patch_prepare_run(),
            patch("aegra_api.services.cron_scheduler.uuid4", return_value="eph-thread-1"),
            patch("aegra_api.services.cron_scheduler.schedule_background_cleanup") as mock_schedule,
        ):
            await CronScheduler._fire_cron(AsyncMock(), cron)

        mock_schedule.assert_called_once_with("run-1", "eph-thread-1", cron.user_id)

    @pytest.mark.asyncio
    async def test_skips_cleanup_for_thread_bound_cron(self) -> None:
        cron = _make_cron_orm(thread_id="thread-bound-1", end_time=None)

        with (
            _patch_service(),
            _patch_prepare_run(),
            patch("aegra_api.services.cron_scheduler.schedule_background_cleanup") as mock_schedule,
        ):
            await CronScheduler._fire_cron(AsyncMock(), cron)

        mock_schedule.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_cleanup_when_on_run_completed_is_keep(self) -> None:
        cron = _make_cron_orm(thread_id=None, on_run_completed="keep", end_time=None)

        with (
            _patch_service(),
            _patch_prepare_run(),
            patch("aegra_api.services.cron_scheduler.schedule_background_cleanup") as mock_schedule,
        ):
            await CronScheduler._fire_cron(AsyncMock(), cron)

        mock_schedule.assert_not_called()

    @pytest.mark.asyncio
    async def test_deletes_stateless_thread_when_run_setup_fails(self) -> None:
        cron = _make_cron_orm(thread_id=None, end_time=None)

        with (
            _patch_service(),
            _patch_prepare_run(side_effect=RuntimeError("boom")),
            patch("aegra_api.services.cron_scheduler.uuid4", return_value="eph-thread-fail"),
            patch("aegra_api.services.cron_scheduler.delete_thread_by_id", new_callable=AsyncMock) as mock_delete,
        ):
            await CronScheduler._fire_cron(AsyncMock(), cron)

        mock_delete.assert_awaited_once_with("eph-thread-fail", cron.user_id)

    @pytest.mark.asyncio
    async def test_uses_cron_thread_id_when_set(self) -> None:
        cron = _make_cron_orm(thread_id="t-bound", end_time=None)

        with _patch_service(), _patch_prepare_run() as mock_prepare:
            await CronScheduler._fire_cron(AsyncMock(), cron)

        assert mock_prepare.call_args.args[1] == "t-bound"

    @pytest.mark.asyncio
    async def test_generates_uuid_thread_when_no_thread_id(self) -> None:
        cron = _make_cron_orm(thread_id=None, end_time=None)

        with _patch_service(), _patch_prepare_run() as mock_prepare:
            await CronScheduler._fire_cron(AsyncMock(), cron)

        assert len(mock_prepare.call_args.args[1]) == 36  # UUID format

    @pytest.mark.asyncio
    async def test_server_error_keeps_the_occurrence_and_counts_it(self) -> None:
        """5xx is infrastructure: retry the same occurrence, but let the failure cap
        catch a cron that keeps blowing up."""
        cron = _make_cron_orm(end_time=None)

        with (
            _patch_service() as service,
            _patch_prepare_run(side_effect=HTTPException(503, "executor unavailable")),
        ):
            await CronScheduler._fire_cron(AsyncMock(), cron)

        service.release_claim.assert_awaited_once_with(cron.cron_id, failed=True)
        service.advance_next_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_busy_thread_spends_the_occurrence_without_blame(self) -> None:
        """409 means the previous occurrence is still running — the multitask contract,
        not a cron fault, so it must not count toward auto-disable."""
        cron = _make_cron_orm(thread_id="t-busy", end_time=None)

        with (
            _patch_service() as service,
            _patch_prepare_run(side_effect=HTTPException(409, "thread is already running a task")),
        ):
            await CronScheduler._fire_cron(AsyncMock(), cron)

        service.advance_next_run.assert_awaited_once_with(cron.cron_id, failed=False, base=ANY)
        service.release_claim.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bad_config_spends_the_occurrence_and_counts_it(self) -> None:
        """A missing graph fails identically next tick, so retrying every poll interval
        only spams the log; spend the occurrence and count it."""
        cron = _make_cron_orm(end_time=None)

        with (
            _patch_service() as service,
            _patch_prepare_run(side_effect=HTTPException(404, "graph not found")),
        ):
            await CronScheduler._fire_cron(AsyncMock(), cron)

        service.advance_next_run.assert_awaited_once_with(cron.cron_id, failed=True, base=ANY)

    @pytest.mark.asyncio
    async def test_unexpected_error_keeps_the_occurrence_and_counts_it(self) -> None:
        cron = _make_cron_orm(end_time=None)

        with (
            _patch_service() as service,
            _patch_prepare_run(side_effect=RuntimeError("database connection lost")),
        ):
            await CronScheduler._fire_cron(AsyncMock(), cron)

        service.release_claim.assert_awaited_once_with(cron.cron_id, failed=True)
        service.advance_next_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_revoked_user_disables_the_cron(self) -> None:
        """Refuse to forge a User for an identity the operator's store rejects."""
        cron = _make_cron_orm(end_time=None)

        with (
            _patch_service() as service,
            _patch_prepare_run() as mock_prepare,
            patch("aegra_api.services.cron_scheduler.validate_cron_user", new_callable=AsyncMock, return_value=False),
        ):
            await CronScheduler._fire_cron(AsyncMock(), cron)

        service.disable_cron.assert_awaited_once_with(cron.cron_id)
        mock_prepare.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_misfired_occurrence_is_skipped_without_firing(self) -> None:
        """A cron due during a long outage must not fire hours off-schedule at restart."""
        cron = _make_cron_orm(end_time=None, next_run_date=datetime.now(UTC) - timedelta(hours=6))

        with (
            _patch_service() as service,
            _patch_prepare_run() as mock_prepare,
            patch("aegra_api.services.cron_scheduler.settings") as cfg,
        ):
            cfg.cron.CRON_MISFIRE_GRACE_SECONDS = 300
            await CronScheduler._fire_cron(AsyncMock(), cron)

        mock_prepare.assert_not_awaited()
        service.advance_next_run.assert_awaited_once_with(cron.cron_id, base=ANY)

    @pytest.mark.asyncio
    async def test_occurrence_inside_the_grace_window_still_fires(self) -> None:
        cron = _make_cron_orm(end_time=None, next_run_date=datetime.now(UTC) - timedelta(seconds=30))

        with (
            _patch_service(),
            _patch_prepare_run() as mock_prepare,
            patch("aegra_api.services.cron_scheduler.settings") as cfg,
        ):
            cfg.cron.CRON_MISFIRE_GRACE_SECONDS = 300
            cfg.cron.CRON_APPROVAL_TIMEOUT_SECONDS = 86_400
            await CronScheduler._fire_cron(AsyncMock(), cron)

        mock_prepare.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_zero_grace_fires_however_overdue(self) -> None:
        """The default keeps catching up: a missed occurrence still runs, once."""
        cron = _make_cron_orm(end_time=None, next_run_date=datetime.now(UTC) - timedelta(days=7))

        with _patch_service(), _patch_prepare_run() as mock_prepare:
            await CronScheduler._fire_cron(AsyncMock(), cron)

        mock_prepare.assert_awaited_once()


# ---------------------------------------------------------------------------
# _build_run_request
# ---------------------------------------------------------------------------


class TestBuildRunRequest:
    """The scheduled firing and the rejection share one builder."""

    PAYLOAD = {
        "input": {"data": True},
        "config": {"k": "v"},
        "context": {"c": 1},
        "interrupt_before": ["step1"],
        "stream_mode": "values",
        "webhook": "https://hooks.example.com/cron",
        "durability": "sync",
        "after_seconds": 30,
    }

    def test_run_inherits_the_cron_metadata(self) -> None:
        """The SDK documents cron metadata as "metadata to assign to the cron job runs",
        and it is the only channel a firing has: no request context, and a stateless
        cron's thread is deleted once the run finishes."""
        cron = _make_cron_orm(metadata={"tenant_id": "t1", "task_id": "job-7"})

        assert _build_run_request(cron).metadata == {"tenant_id": "t1", "task_id": "job-7"}

    def test_rejection_run_inherits_it_too(self) -> None:
        """A timed-out rejection is still a run of this cron; dropping provenance there
        would make it invisible to any metadata-filtered view."""
        request = _build_run_request(_make_cron_orm(metadata={"tenant_id": "t1"}), command={"resume": []})

        assert request.metadata == {"tenant_id": "t1"}

    def test_unusable_metadata_does_not_block_the_firing(self) -> None:
        """A non-dict in the column is corruption, not a reason to stop the schedule."""
        assert _build_run_request(_make_cron_orm(metadata="not-a-dict")).metadata == {}

    def test_scheduled_request_carries_the_stored_payload(self) -> None:
        request = _build_run_request(_make_cron_orm(payload=self.PAYLOAD))

        assert request.input == {"data": True}
        assert request.config == {"k": "v"}
        assert request.interrupt_before == ["step1"]
        assert request.stream_mode == "values"
        assert request.after_seconds == 30
        assert request.command is None

    def test_rejection_answers_the_interrupt_without_starting_a_turn(self) -> None:
        """The rejection resumes into the same graph under the same config — only the
        scheduled input and its delay drop out, because this run is not that turn."""
        reject = {"resume": {"decisions": [{"type": "reject"}]}}
        request = _build_run_request(_make_cron_orm(payload=self.PAYLOAD), command=reject)

        assert request.command == reject
        assert request.input is None
        assert request.after_seconds is None
        assert request.config == {"k": "v"}
        assert request.context == {"c": 1}
        assert request.interrupt_before == ["step1"]
        assert request.webhook == "https://hooks.example.com/cron"
        assert request.durability == "sync"


# ---------------------------------------------------------------------------
# _loop
# ---------------------------------------------------------------------------


class TestSchedulerLoop:
    """Test CronScheduler._loop() behavior."""

    @pytest.mark.asyncio
    async def test_loop_stops_when_running_is_false(self) -> None:
        scheduler = CronScheduler()
        scheduler._running = True

        tick_count = 0

        async def counting_tick() -> None:
            nonlocal tick_count
            tick_count += 1
            scheduler._running = False  # stop after first tick

        with (
            patch.object(scheduler, "_tick", side_effect=counting_tick),
            patch("aegra_api.services.cron_scheduler.settings") as mock_settings,
        ):
            mock_settings.cron.CRON_POLL_INTERVAL_SECONDS = 0.01
            await scheduler._loop()

        assert tick_count == 1

    @pytest.mark.asyncio
    async def test_loop_handles_cancelled_error(self) -> None:
        scheduler = CronScheduler()
        scheduler._running = True

        async def raise_cancelled() -> None:
            raise asyncio.CancelledError

        with (
            patch.object(scheduler, "_tick", side_effect=raise_cancelled),
            patch("aegra_api.services.cron_scheduler.settings") as mock_settings,
        ):
            mock_settings.cron.CRON_POLL_INTERVAL_SECONDS = 0.01
            # Should exit cleanly
            await scheduler._loop()

    @pytest.mark.asyncio
    async def test_loop_handles_cancelled_error_during_sleep(self) -> None:
        """CancelledError raised during asyncio.sleep must not kill the loop silently."""
        scheduler = CronScheduler()
        scheduler._running = True

        call_count = 0
        original_sleep = asyncio.sleep

        async def cancelling_sleep(delay: float) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                await original_sleep(delay)
                return
            # Second sleep: simulate external cancellation
            raise asyncio.CancelledError

        with (
            patch.object(scheduler, "_tick", new_callable=AsyncMock),
            patch("aegra_api.services.cron_scheduler.settings") as mock_settings,
            patch("aegra_api.services.cron_scheduler.asyncio.sleep", side_effect=cancelling_sleep),
        ):
            mock_settings.cron.CRON_POLL_INTERVAL_SECONDS = 0.01
            # Should exit cleanly without propagating CancelledError
            await scheduler._loop()

    @pytest.mark.asyncio
    async def test_loop_survives_generic_exception(self) -> None:
        scheduler = CronScheduler()
        scheduler._running = True

        call_count = 0

        async def failing_tick() -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                scheduler._running = False
                return
            raise ValueError("unexpected error")

        with (
            patch.object(scheduler, "_tick", side_effect=failing_tick),
            patch("aegra_api.services.cron_scheduler.settings") as mock_settings,
        ):
            mock_settings.cron.CRON_POLL_INTERVAL_SECONDS = 0.01
            await scheduler._loop()

        assert call_count == 2


# ---------------------------------------------------------------------------
# _resolve_approval
# ---------------------------------------------------------------------------


class TestResolveApproval:
    """A thread awaiting a human decision holds its schedule; a timeout decides."""

    @staticmethod
    def _session(
        thread_status: str | None,
        waited_seconds: float | None = None,
        interrupts: dict[str, list[Any]] | None = None,
    ) -> AsyncMock:
        """Three scalar() reads: thread status, newest pause, then pending interrupts.

        Each statement is recorded in ``session.queries`` so the predicates themselves
        can be asserted — with the reads mocked, the SQL is the only place the
        cancel/pause distinction is visible.
        """
        paused_at = None if waited_seconds is None else datetime.now(UTC) - timedelta(seconds=waited_seconds)
        reads = [thread_status, paused_at, interrupts]
        session = AsyncMock()
        session.queries = []

        def scalar(stmt: Any) -> Any:
            session.queries.append(str(stmt))
            return reads.pop(0) if reads else None

        session.scalar = AsyncMock(side_effect=scalar)
        return session

    @staticmethod
    @contextlib.contextmanager
    def _settings(timeout: int) -> Iterator[Mock]:
        """Patch cron settings; the poll interval gates the hold's log level."""
        with patch("aegra_api.services.cron_scheduler.settings") as cfg:
            cfg.cron.CRON_APPROVAL_TIMEOUT_SECONDS = timeout
            cfg.cron.CRON_POLL_INTERVAL_SECONDS = 60
            cfg.cron.CRON_MISFIRE_GRACE_SECONDS = 0
            yield cfg

    # A pause that declares it accepts a rejection (HumanInterrupt convention).
    REJECTABLE = {
        "task-1": [{"id": "i1", "value": {"action_request": {"action": "refund"}, "config": {"allow_ignore": True}}}]
    }
    # A pause with no such declaration — resuming it is not a rejection.
    OPAQUE = {"task-1": [{"id": "i1", "value": {"action_requests": [{"name": "execute"}]}}]}
    # LangChain HumanInTheLoopMiddleware convention, two actions awaiting one answer each.
    MIDDLEWARE = {
        "task-1": [
            {
                "id": "i1",
                "value": {
                    "action_requests": [{"name": "refund"}, {"name": "send_email"}],
                    "review_configs": [
                        {"action_name": "refund", "allowed_decisions": ["approve", "reject"]},
                        {"action_name": "send_email", "allowed_decisions": ["approve", "reject"]},
                    ],
                },
            }
        ]
    }

    @pytest.mark.asyncio
    async def test_stateless_cron_fires(self) -> None:
        """No bound thread means no approval to wait on."""
        cron = _make_cron_orm(thread_id=None)
        assert (await _resolve_approval(self._session(None), cron))[0] == "fire"

    @pytest.mark.asyncio
    async def test_idle_thread_fires(self) -> None:
        """A cancel leaves the thread idle; only a HITL pause marks it interrupted."""
        cron = _make_cron_orm(thread_id="t1")
        assert (await _resolve_approval(self._session("idle"), cron))[0] == "fire"

    @pytest.mark.asyncio
    async def test_interrupted_thread_holds_the_firing(self) -> None:
        """Firing anyway would advance the checkpoint out from under the approver."""
        cron = _make_cron_orm(thread_id="t1")
        session = self._session("interrupted", 60)
        with self._settings(86_400):
            assert (await _resolve_approval(session, cron))[0] == "hold"
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancelled_runs_are_excluded_from_the_wait(self) -> None:
        """A cancel also settles as ``interrupted``. If cancelled runs counted, one old
        cancel would make the wait look days long and time out the approval that just
        paused, on the very first tick."""
        cron = _make_cron_orm(thread_id="t1")
        session = self._session("interrupted", 60)
        with self._settings(86_400):
            await _resolve_approval(session, cron)
        paused_query = session.queries[1]
        assert "cancel_requested" in paused_query
        # Newest pause, not oldest: earlier rows are prior cycles already abandoned.
        assert "max(" in paused_query

    @pytest.mark.asyncio
    async def test_timeout_rejects_a_rejectable_pause(self) -> None:
        """Timing out decides on the reviewer's behalf: reject, through the graph, so the
        checkpoint is cleared instead of holding an interrupt nobody will answer."""
        cron = _make_cron_orm(thread_id="t1")
        session = self._session("interrupted", 100_000, self.REJECTABLE)
        with self._settings(86_400):
            assert (await _resolve_approval(session, cron))[0] == "reject"
        # The rejection goes out as a run, not as a status rewrite behind the graph's back.
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_timeout_rejects_a_middleware_pause_once_per_action(self) -> None:
        """``HumanInTheLoopMiddleware`` validates the decision count against the number of
        action requests and raises on a mismatch, so one rejection per action is required."""
        cron = _make_cron_orm(thread_id="t1")
        session = self._session("interrupted", 100_000, self.MIDDLEWARE)
        with self._settings(86_400):
            decision, command = await _resolve_approval(session, cron)
        assert decision == "reject"
        assert command is not None
        decisions = command["resume"]["decisions"]
        assert [d["type"] for d in decisions] == ["reject", "reject"]
        assert all(d["message"] for d in decisions)

    @pytest.mark.asyncio
    async def test_pause_that_declares_no_rejection_is_only_released(self) -> None:
        """Without ``allow_ignore`` the resume value reaches the graph raw, where a
        response list crashes at best and reads as approval to anything that only checks
        truthiness. Release the thread instead of guessing."""
        cron = _make_cron_orm(thread_id="t1")
        session = self._session("interrupted", 100_000, self.OPAQUE)
        with self._settings(86_400):
            assert (await _resolve_approval(session, cron))[0] == "fire"
        # Only the thread is released; the paused runs keep the status they actually have,
        # because a terminal run rewritten outside finalize_run gets no webhook either.
        session.execute.assert_awaited_once()
        released = str(session.execute.await_args.args[0])
        assert "thread" in released.lower()
        assert "error_message" not in released
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_plain_interrupt_payload_is_never_auto_rejected(self) -> None:
        """``interrupt("Provide value:")`` hands the resume value straight back to the
        node, so a response list would be concatenated or compared as-is."""
        cron = _make_cron_orm(thread_id="t1")
        session = self._session("interrupted", 100_000, {"task-1": [{"id": "i1", "value": "Provide value:"}]})
        with self._settings(86_400):
            assert (await _resolve_approval(session, cron))[0] == "fire"

    @pytest.mark.asyncio
    async def test_static_breakpoint_is_never_auto_resumed(self) -> None:
        """A static breakpoint carries no interrupt at all, and resuming one *continues*
        execution — the opposite of the rejection this setting promises."""
        cron = _make_cron_orm(thread_id="t1")
        session = self._session("interrupted", 100_000, interrupts=None)
        with self._settings(86_400):
            assert (await _resolve_approval(session, cron))[0] == "fire"

    @pytest.mark.asyncio
    async def test_empty_interrupt_map_counts_as_nothing_to_reject(self) -> None:
        """A materialized-but-empty map is not a decision point either."""
        cron = _make_cron_orm(thread_id="t1")
        session = self._session("interrupted", 100_000, {"task-1": []})
        with self._settings(86_400):
            assert (await _resolve_approval(session, cron))[0] == "fire"

    @pytest.mark.asyncio
    async def test_no_paused_run_fires(self) -> None:
        """Thread says interrupted but no run is: a race, not something to wait on."""
        cron = _make_cron_orm(thread_id="t1")
        assert (await _resolve_approval(self._session("interrupted", None), cron))[0] == "fire"

    @pytest.mark.asyncio
    async def test_zero_timeout_waits_forever(self) -> None:
        """0 opts out of the timeout: the hold never expires into a rejection."""
        cron = _make_cron_orm(thread_id="t1")
        session = self._session("interrupted", 10_000_000, self.REJECTABLE)
        with self._settings(0):
            assert (await _resolve_approval(session, cron))[0] == "hold"

    @pytest.mark.asyncio
    async def test_ongoing_hold_stops_logging_at_info(self) -> None:
        """A hold is re-checked every tick; INFO only on the first tick that sees it,
        else a day-long wait buys ~1.4k identical lines per cron."""
        cron = _make_cron_orm(thread_id="t1")
        with patch("aegra_api.services.cron_scheduler.logger") as log, self._settings(86_400):
            await _resolve_approval(self._session("interrupted", 5), cron)
            assert log.info.called and not log.debug.called
            log.reset_mock()
            await _resolve_approval(self._session("interrupted", 3_600), cron)
            assert log.debug.called and not log.info.called
