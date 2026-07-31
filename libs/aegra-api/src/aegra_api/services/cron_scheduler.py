"""Background scheduler that fires due cron jobs.

Wakes up every ``CRON_POLL_INTERVAL_SECONDS`` (default 60 s), claims the enabled crons
whose ``next_run_date`` has passed, and fires each one through the normal
run-preparation pipeline. A fired cron advances to its next occurrence; a failed one
either keeps the occurrence for the next tick or spends it, and counts toward
``CRON_MAX_CONSECUTIVE_FAILURES`` so a permanently broken cron stops retrying. Claims
last ``CRON_CLAIM_DURATION_SECONDS`` and firings run ``CRON_FIRE_CONCURRENCY``-wide, so
a slow batch cannot outlive its own claims and get re-claimed by another poller.

Follows the same ``start()/stop()`` lifecycle pattern used by
:class:`aegra_api.services.lease_reaper.LeaseReaper`.
"""

import asyncio
import contextlib
import random
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

import structlog
from fastapi import HTTPException
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.orm import Cron as CronORM
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import ThreadState as ThreadStateORM
from aegra_api.core.orm import get_session_maker
from aegra_api.models import RunCreate, User
from aegra_api.services.cron_service import CronService, should_delete_stateless_thread
from aegra_api.services.run_cleanup import delete_thread_by_id, schedule_background_cleanup
from aegra_api.services.run_preparation import prepare_run
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)


def _inherited_metadata(cron: CronORM) -> dict[str, Any]:
    """Metadata a fired run inherits from its cron — its only provenance channel.

    A firing has no request context, and a stateless cron's thread is deleted afterwards.
    """
    inherited = cron.metadata_dict
    return dict(inherited) if isinstance(inherited, dict) else {}


def _build_run_request(cron: CronORM, *, command: dict[str, Any] | None = None) -> RunCreate:
    """Build the ``RunCreate`` for one firing of *cron*.

    With *command* the run answers a pending decision instead of starting the scheduled
    turn, so it carries no ``input`` and no delay. Every other stored parameter still
    applies: it resumes into the same graph, under the same config, that the schedule
    would have run.
    """
    payload = cron.payload or {}
    answering = command is not None
    return RunCreate(
        assistant_id=cron.assistant_id,
        input=None if answering else payload.get("input"),
        config=payload.get("config"),
        context=payload.get("context"),
        interrupt_before=payload.get("interrupt_before"),
        interrupt_after=payload.get("interrupt_after"),
        stream_subgraphs=payload.get("stream_subgraphs"),
        stream_mode=payload.get("stream_mode"),
        multitask_strategy=payload.get("multitask_strategy"),
        webhook=payload.get("webhook"),
        command=command if answering else payload.get("command"),
        durability=payload.get("durability"),
        after_seconds=None if answering else payload.get("after_seconds"),
        if_not_exists="create",
        metadata=_inherited_metadata(cron),
    )


APPROVAL_REJECT_MESSAGE = "Approval timed out; rejected on the reviewer's behalf."

# What the approval gate decided for this firing:
#   fire   — nothing is pending (or the stale pause was written off); run the payload
#   hold   — a decision is still pending; skip this occurrence, keep next_run_date
#   reject — the wait expired and the pause accepts a rejection; answer it instead
ApprovalDecision = Literal["fire", "hold", "reject"]


def _rejection_for(payload: Any) -> dict[str, Any] | None:
    """The rejection answer one pause understands, or ``None`` if it accepts none.

    Recognises ``HumanInTheLoopMiddleware`` (``action_requests`` + ``review_configs``,
    resumed with one decision per request) and HumanInterrupt (``config.allow_ignore``).
    Anything else gets its resume value back raw, where a decision list is a crash at
    best and reads as approval to whatever only checks truthiness.
    """
    if not isinstance(payload, dict):
        return None

    requests = payload.get("action_requests")
    if isinstance(requests, list) and requests:
        configs = payload.get("review_configs")
        allowed = {
            decision
            for config in (configs if isinstance(configs, list) else [])
            if isinstance(config, dict)
            for decision in (config.get("allowed_decisions") or [])
        }
        if "reject" not in allowed:
            return None
        decisions = [{"type": "reject", "message": APPROVAL_REJECT_MESSAGE} for _ in requests]
        return {"resume": {"decisions": decisions}}

    config = payload.get("config")
    if isinstance(config, dict) and config.get("allow_ignore"):
        return {"resume": [{"type": "ignore", "args": None}]}
    return None


async def _rejection_command(session: AsyncSession, thread_id: str) -> dict[str, Any] | None:
    """The rejection answer for the pending pause, or ``None`` if it accepts none.

    Read from the materialized ``thread_state``: with ``THREAD_STATE_MATERIALIZE`` off
    nothing looks rejectable, which errs toward never resuming work nobody approved.
    Static breakpoints (``interrupt_before``/``interrupt_after``) carry no interrupt at
    all, and resuming one simply *continues* execution — the opposite of a rejection.
    """
    raw = await session.scalar(select(ThreadStateORM.interrupts).where(ThreadStateORM.thread_id == thread_id))
    if not isinstance(raw, dict):
        return None
    for items in raw.values():
        for item in items or []:
            value = item.get("value") if isinstance(item, dict) else None
            if (command := _rejection_for(value)) is not None:
                return command
    return None


async def _resolve_approval(session: AsyncSession, cron: CronORM) -> tuple[ApprovalDecision, dict[str, Any] | None]:
    """Decide what a thread awaiting a human decision means for this firing.

    A HITL pause sets ``thread.status = 'interrupted'`` (a cancel leaves it ``idle``), so
    that is the signal. Firing the payload anyway would append input and advance the
    checkpoint, discarding the very context the approver is looking at — so the
    occurrence is skipped while the decision is outstanding.

    Past ``CRON_APPROVAL_TIMEOUT_SECONDS`` it is rejected where the pause accepts one,
    and otherwise written off by releasing the thread. Either way the paused runs keep
    their ``interrupted`` status: terminal runs are not rewritten behind the graph's back.
    """
    if cron.thread_id is None:
        return "fire", None
    owned = (ThreadORM.thread_id == cron.thread_id, ThreadORM.user_id == cron.user_id)
    if await session.scalar(select(ThreadORM.status).where(*owned)) != "interrupted":
        return "fire", None

    # A cancel also settles as ``interrupted``, so cancel_requested is what separates
    # "paused for a human" from "someone stopped it" — without it one old cancelled run
    # makes the wait look days long and times out the approval that just paused.
    paused = (
        RunORM.thread_id == cron.thread_id,
        RunORM.user_id == cron.user_id,
        RunORM.status == "interrupted",
        RunORM.cancel_requested.is_(False),
    )
    # Clock from the newest pause, not thread.updated_at: any later touch of the thread
    # would push the timeout out indefinitely. Newest, because earlier rows are prior
    # cycles already abandoned by a re-fire.
    paused_at = await session.scalar(select(func.max(RunORM.updated_at)).where(*paused))
    if paused_at is None:
        return "fire", None

    now = datetime.now(UTC)
    timeout = settings.cron.CRON_APPROVAL_TIMEOUT_SECONDS
    waited = (now - paused_at).total_seconds()
    if timeout <= 0 or waited < timeout:
        # A hold is re-evaluated every tick, so INFO only on the first tick that sees it;
        # a day-long wait would otherwise buy ~1.4k identical lines per cron.
        log = logger.info if waited < settings.cron.CRON_POLL_INTERVAL_SECONDS else logger.debug
        log(
            "Holding cron firing while thread awaits approval",
            cron_id=cron.cron_id,
            thread_id=cron.thread_id,
            waited_seconds=int(waited),
        )
        return "hold", None

    if (rejection := await _rejection_command(session, cron.thread_id)) is not None:
        logger.warning(
            "Approval timed out; rejecting on the reviewer's behalf",
            cron_id=cron.cron_id,
            thread_id=cron.thread_id,
            waited_seconds=int(waited),
        )
        return "reject", rejection

    await session.execute(update(ThreadORM).where(*owned).values(status="idle", updated_at=now))
    await session.commit()
    logger.warning(
        "Approval timed out with no rejectable interrupt; released the thread",
        cron_id=cron.cron_id,
        thread_id=cron.thread_id,
        waited_seconds=int(waited),
    )
    return "fire", None


def _is_misfired(cron: CronORM, now: datetime) -> bool:
    """Whether this occurrence is too late to be worth firing.

    A daily digest whose server was down for a week must not fire at 03:00 on the way
    back up. A grace of 0 keeps catching up regardless of age.
    """
    grace = settings.cron.CRON_MISFIRE_GRACE_SECONDS
    if grace <= 0 or cron.next_run_date is None:
        return False
    return (now - cron.next_run_date).total_seconds() > grace


async def validate_cron_user(user_id: str) -> bool:
    """Liveness check run before forging a ``User`` for a firing.

    Accepts any non-empty id. Replace this module attribute to check a real identity
    store; returning False disables the cron instead of firing it.
    """
    return bool(user_id)


class CronScheduler:
    """Periodically fires due cron jobs by creating runs."""

    def __init__(self) -> None:
        """Initialize the scheduler state for the background polling loop."""
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        """Start the background polling task."""
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "Cron scheduler started",
            interval_seconds=settings.cron.CRON_POLL_INTERVAL_SECONDS,
            claim_seconds=settings.cron.CRON_CLAIM_DURATION_SECONDS,
            concurrency=settings.cron.CRON_FIRE_CONCURRENCY,
        )

    async def stop(self) -> None:
        """Stop the background polling task and wait for cancellation to finish."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        logger.info("Cron scheduler stopped")

    async def _loop(self) -> None:
        """Tick then sleep so overdue crons are claimed on the very first iteration.

        Sleep-before-tick would delay post-restart recovery by the full poll interval
        (default 60s) for any cron that was due during downtime. The sleep sits in its own
        try so a persistent _tick failure (DB down, session factory not initialised)
        cannot spin the loop at CPU speed.

        The sleep carries up to a second of jitter: instances started together would
        otherwise poll in lockstep forever, piling every claim query onto the same instant.
        """
        interval = settings.cron.CRON_POLL_INTERVAL_SECONDS
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in cron scheduler tick")
            try:
                await asyncio.sleep(interval + random.random())  # noqa: S311 — spread pollers, not security
            except asyncio.CancelledError:
                break

    async def _tick(self) -> None:
        """Claim the due crons and fire them, ``CRON_FIRE_CONCURRENCY`` at a time.

        Each cron gets its own session so a SQL failure on one cannot put a shared
        transaction into ``InFailedSqlTransaction`` and silently kill the rest of the
        batch. The concurrency cap is what keeps a slow ``prepare_run`` from pushing the
        tail of a batch past its claim, where another poller would re-claim and re-fire it.
        """
        maker = get_session_maker()
        claimed_at = datetime.now(UTC)
        async with maker() as claim_session:
            due_crons = await CronService(claim_session).claim_due_crons(claimed_at)

        if not due_crons:
            logger.debug("Cron tick: no jobs due")
            return

        logger.info("Cron tick: found due jobs", count=len(due_crons))
        slots = asyncio.Semaphore(settings.cron.CRON_FIRE_CONCURRENCY)

        async def fire(cron: CronORM) -> None:
            async with slots, maker() as session:
                try:
                    await self._fire_cron(session, cron, claimed_at)
                except Exception:
                    logger.exception("Failed to fire cron job", cron_id=cron.cron_id)
                    with contextlib.suppress(Exception):
                        await session.rollback()

        await asyncio.gather(*(fire(cron) for cron in due_crons))

    @staticmethod
    async def _fire_cron(session: AsyncSession, cron: CronORM, claimed_at: datetime | None = None) -> None:
        """Fire one occurrence, then settle the cron: advance, retry, or disable.

        Every exit path settles exactly once through ``CronService``; they differ only in
        whether the occurrence is spent and whether it counted as a failure. Both the
        misfire window and the next occurrence anchor to *claimed_at*, so firing latency
        cannot walk the schedule forward and skip slots on a sub-minute schedule.
        """
        service = CronService(session)
        claimed_at = claimed_at or datetime.now(UTC)

        if _is_misfired(cron, claimed_at):
            logger.warning(
                "Skipping cron occurrence past the misfire grace window",
                cron_id=cron.cron_id,
                due_at=cron.next_run_date.isoformat() if cron.next_run_date else None,
            )
            await service.advance_next_run(cron.cron_id, base=claimed_at)
            return

        approval, rejection = await _resolve_approval(session, cron)
        if approval == "hold":
            # Keep next_run_date: the next tick re-checks, so the cron resumes on its own
            # once the decision lands.
            await service.release_claim(cron.cron_id)
            return

        if not await validate_cron_user(cron.user_id):
            logger.warning(
                "Disabling cron because the owning user failed liveness check",
                cron_id=cron.cron_id,
                user_id=cron.user_id,
            )
            await service.disable_cron(cron.cron_id)
            return

        request = _build_run_request(cron, command=rejection)
        thread_id = cron.thread_id or str(uuid4())
        user = User(identity=cron.user_id, display_name="cron-scheduler", is_authenticated=True)
        drop_thread = should_delete_stateless_thread(cron)

        try:
            run_id, _run, _job = await prepare_run(session, thread_id, request, user, initial_status="pending")
        except HTTPException as exc:
            # 5xx is infrastructure: keep the occurrence for the next tick. A 409 means the
            # thread is still busy with the previous occurrence — the multitask contract,
            # not a cron fault, so spend the occurrence without counting it. Any other 4xx
            # (no such graph, invalid payload) fails identically next tick, so spend it
            # *and* count it toward the auto-disable cap.
            retryable = exc.status_code >= 500
            logger.error(
                "Cron run creation failed",
                cron_id=cron.cron_id,
                status_code=exc.status_code,
                detail=exc.detail,
                retrying=retryable,
            )
            if drop_thread:
                await CronScheduler._delete_stateless_thread(thread_id, cron)
            if retryable:
                await service.release_claim(cron.cron_id, failed=True)
            else:
                await service.advance_next_run(cron.cron_id, failed=exc.status_code != 409, base=claimed_at)
            return
        except Exception:
            logger.exception("Cron run creation failed unexpectedly", cron_id=cron.cron_id)
            if drop_thread:
                await CronScheduler._delete_stateless_thread(thread_id, cron)
            # Unproven cause, so treat it as transient: keep the occurrence, count the
            # failure, and let the cap stop a cron that keeps blowing up.
            await service.release_claim(cron.cron_id, failed=True)
            return

        logger.info(
            "Cron rejected a timed-out approval" if rejection else "Cron fired run",
            cron_id=cron.cron_id,
            run_id=run_id,
            thread_id=thread_id,
        )
        if drop_thread:
            schedule_background_cleanup(run_id, thread_id, cron.user_id)
        await service.advance_next_run(cron.cron_id, base=claimed_at)

    @staticmethod
    async def _delete_stateless_thread(thread_id: str, cron: CronORM) -> None:
        """Drop the ephemeral thread of a stateless cron whose run setup failed."""
        try:
            await delete_thread_by_id(thread_id, cron.user_id)
        except Exception:
            logger.exception(
                "Failed to delete stateless cron thread after run setup error",
                thread_id=thread_id,
                cron_id=cron.cron_id,
            )


# Module-level singleton (matches executor / lease_reaper pattern)
cron_scheduler = CronScheduler()
