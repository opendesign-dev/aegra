"""Run-completion webhook delivery.

Two halves. ``enqueue`` writes an outbox row, so the intent to notify is durable
the moment the run finalizes. ``webhook_sweeper`` drains the outbox, POSTing the
final Run payload and backing off on failure.

Split this way because delivery is slow and fallible while finalization must not
be: a webhook that is down cannot be allowed to stall or fail a run.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import WebhookDelivery as WebhookDeliveryORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.models.runs import Run
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)


async def enqueue(session: AsyncSession, run_id: str, url: str | None) -> None:
    """Record the intent to notify. Does not commit — share the run's transaction.

    Being in the same transaction as finalization is the point: a crash in the
    retry window can then only replay the delivery, never lose it.
    """
    if not url:
        return
    session.add(WebhookDeliveryORM(run_id=run_id, url=url))


class WebhookSweeper:
    """Drains the webhook outbox with capped exponential backoff."""

    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("Webhook sweeper started", interval_seconds=settings.webhook.WEBHOOK_POLL_INTERVAL_SECONDS)

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        logger.info("Webhook sweeper stopped")

    async def _loop(self) -> None:
        interval = settings.webhook.WEBHOOK_POLL_INTERVAL_SECONDS
        while self._running:
            await asyncio.sleep(interval)
            try:
                await self._sweep()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in webhook sweeper")

    async def _sweep(self) -> None:
        maker = _get_session_maker()
        async with maker() as session:
            due = await self._claim_due(session)
            if not due:
                return
            async with httpx.AsyncClient(timeout=settings.webhook.WEBHOOK_TIMEOUT_SECONDS) as client:
                for delivery, payload in due:
                    await self._deliver(session, client, delivery, payload)
            await session.commit()

    async def _claim_due(self, session: AsyncSession) -> list[tuple[WebhookDeliveryORM, dict[str, Any]]]:
        """Take a batch of due deliveries, skipping rows another pod holds.

        The payload is the run itself, so the whole batch's runs are loaded in one
        query rather than one per delivery.
        """
        stmt = (
            select(WebhookDeliveryORM)
            .where(
                WebhookDeliveryORM.status == "pending",
                WebhookDeliveryORM.next_attempt_at <= datetime.now(UTC),
            )
            .order_by(WebhookDeliveryORM.next_attempt_at.asc())
            .limit(settings.webhook.WEBHOOK_BATCH_SIZE)
            .with_for_update(skip_locked=True)
        )
        deliveries = list((await session.scalars(stmt)).all())
        if not deliveries:
            return []

        runs = await session.scalars(
            select(RunORM).where(RunORM.run_id.in_([delivery.run_id for delivery in deliveries]))
        )
        payloads = {run.run_id: Run.model_validate(run).model_dump(mode="json") for run in runs.all()}

        claimed: list[tuple[WebhookDeliveryORM, dict[str, Any]]] = []
        for delivery in deliveries:
            payload = payloads.get(delivery.run_id)
            if payload is None:
                delivery.status = "failed"
                delivery.last_error = "run no longer exists"
                continue
            claimed.append((delivery, payload))
        return claimed

    async def _deliver(
        self,
        session: AsyncSession,
        client: httpx.AsyncClient,
        delivery: WebhookDeliveryORM,
        payload: dict[str, Any],
    ) -> None:
        attempts = delivery.attempts + 1
        try:
            response = await client.post(delivery.url, json=payload)
            response.raise_for_status()
        except (httpx.HTTPError, OSError) as exc:
            await self._record_failure(session, delivery, attempts, str(exc))
            return

        await session.execute(
            update(WebhookDeliveryORM)
            .where(WebhookDeliveryORM.id == delivery.id)
            .values(status="delivered", attempts=attempts, last_error=None, updated_at=datetime.now(UTC))
        )
        logger.info("Webhook delivered", run_id=delivery.run_id, attempts=attempts)

    async def _record_failure(
        self, session: AsyncSession, delivery: WebhookDeliveryORM, attempts: int, error: str
    ) -> None:
        exhausted = attempts >= settings.webhook.WEBHOOK_MAX_ATTEMPTS
        backoff = min(
            settings.webhook.WEBHOOK_BACKOFF_BASE_SECONDS * 2 ** (attempts - 1),
            settings.webhook.WEBHOOK_BACKOFF_MAX_SECONDS,
        )
        await session.execute(
            update(WebhookDeliveryORM)
            .where(WebhookDeliveryORM.id == delivery.id)
            .values(
                status="failed" if exhausted else "pending",
                attempts=attempts,
                last_error=error[:500],
                next_attempt_at=datetime.now(UTC) + timedelta(seconds=backoff),
                updated_at=datetime.now(UTC),
            )
        )
        logger.warning(
            "Webhook delivery failed",
            run_id=delivery.run_id,
            attempts=attempts,
            exhausted=exhausted,
            error=error,
        )


webhook_sweeper = WebhookSweeper()
