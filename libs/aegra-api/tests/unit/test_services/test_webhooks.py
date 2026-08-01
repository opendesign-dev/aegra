"""Unit tests for run-completion webhook delivery."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from aegra_api.core.orm import WebhookDelivery as WebhookDeliveryORM
from aegra_api.services import webhooks
from aegra_api.settings import settings


class TestEnqueue:
    @pytest.mark.asyncio
    async def test_no_url_writes_nothing(self) -> None:
        session = AsyncMock()
        session.add = MagicMock()

        await webhooks.enqueue(session, "r-1", None)

        session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_outbox_row_joins_the_callers_transaction(self) -> None:
        """No commit here: losing the row and the terminal status must be atomic."""
        session = AsyncMock()
        session.add = MagicMock()

        await webhooks.enqueue(session, "r-1", "https://example.test/hook")

        row = session.add.call_args.args[0]
        assert (row.run_id, row.url) == ("r-1", "https://example.test/hook")
        session.commit.assert_not_awaited()


def _delivery(*, attempts: int = 0) -> WebhookDeliveryORM:
    return WebhookDeliveryORM(
        id="d-1",
        run_id="r-1",
        url="https://example.test/hook",
        status="pending",
        attempts=attempts,
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
    )


class TestDeliver:
    @pytest.mark.asyncio
    async def test_success_marks_delivered(self) -> None:
        session = AsyncMock()
        client = AsyncMock()
        client.post.return_value = httpx.Response(200, request=httpx.Request("POST", "https://example.test/hook"))

        await webhooks.webhook_sweeper._deliver(session, client, _delivery(), {"run_id": "r-1"})

        params = session.execute.await_args.args[0].compile().params
        assert params["status"] == "delivered"
        assert params["attempts"] == 1

    @pytest.mark.asyncio
    async def test_transport_failure_schedules_a_retry(self) -> None:
        session = AsyncMock()
        client = AsyncMock()
        client.post.side_effect = httpx.ConnectError("refused")

        await webhooks.webhook_sweeper._deliver(session, client, _delivery(), {"run_id": "r-1"})

        params = session.execute.await_args.args[0].compile().params
        assert params["status"] == "pending"
        assert params["next_attempt_at"] > datetime.now(UTC)

    @pytest.mark.asyncio
    async def test_last_attempt_marks_failed(self) -> None:
        """Retrying forever would keep a dead endpoint in the sweep batch."""
        session = AsyncMock()
        client = AsyncMock()
        client.post.side_effect = httpx.ConnectError("refused")
        last = settings.webhook.WEBHOOK_MAX_ATTEMPTS - 1

        await webhooks.webhook_sweeper._deliver(session, client, _delivery(attempts=last), {"run_id": "r-1"})

        params = session.execute.await_args.args[0].compile().params
        assert params["status"] == "failed"
        assert params["attempts"] == settings.webhook.WEBHOOK_MAX_ATTEMPTS

    @pytest.mark.asyncio
    async def test_backoff_is_capped(self) -> None:
        session = AsyncMock()
        client = AsyncMock()
        client.post.side_effect = httpx.ConnectError("refused")

        with patch.object(settings.webhook, "WEBHOOK_MAX_ATTEMPTS", 99):
            await webhooks.webhook_sweeper._deliver(session, client, _delivery(attempts=40), {"run_id": "r-1"})

        params = session.execute.await_args.args[0].compile().params
        ceiling = datetime.now(UTC) + timedelta(seconds=settings.webhook.WEBHOOK_BACKOFF_MAX_SECONDS + 5)
        assert params["next_attempt_at"] <= ceiling


class TestClaimDue:
    @staticmethod
    def _session(deliveries: list[WebhookDeliveryORM], runs: list[object]) -> AsyncMock:
        """Answer the deliveries query then the batched runs query, in that order."""
        session = AsyncMock()
        session.scalars.side_effect = [MagicMock(all=lambda: deliveries), MagicMock(all=lambda: runs)]
        return session

    @pytest.mark.asyncio
    async def test_delivery_for_a_deleted_run_is_abandoned(self) -> None:
        """The payload is the Run; without it there is nothing to POST."""
        delivery = _delivery()

        assert await webhooks.webhook_sweeper._claim_due(self._session([delivery], [])) == []
        assert delivery.status == "failed"

    @pytest.mark.asyncio
    async def test_empty_batch_skips_the_run_lookup(self) -> None:
        """No due deliveries must not cost a second round trip."""
        session = self._session([], [])

        assert await webhooks.webhook_sweeper._claim_due(session) == []
        assert session.scalars.await_count == 1
