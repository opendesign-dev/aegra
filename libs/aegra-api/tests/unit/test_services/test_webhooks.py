"""Unit tests for single-attempt webhook delivery, signing, and SSRF validation."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from aegra_api.services.webhooks import (
    WebhookValidationError,
    _sign,
    deliver_webhook,
    validate_webhook_url,
)

MODULE = "aegra_api.services.webhooks"


def _client_returning(*responses: Any) -> MagicMock:
    """Patchable AsyncClient whose post() yields *responses* (values raise if exceptions)."""
    client = AsyncMock()
    client.post = AsyncMock(side_effect=list(responses))
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=client)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=ctx), client


class TestDeliverWebhookAttemptCount:
    """Retry belongs to the outbox. Retrying here too would make WEBHOOK_MAX_ATTEMPTS
    count rounds rather than POSTs, multiplying real requests by itself."""

    @pytest.mark.asyncio
    async def test_posts_exactly_once_on_transport_error(self) -> None:
        maker, client = _client_returning(httpx.ConnectError("refused"))
        with patch(f"{MODULE}.httpx.AsyncClient", maker):
            ok = await deliver_webhook("https://example.com/hook", {"run_id": "r1"})
        assert ok is False
        assert client.post.await_count == 1

    @pytest.mark.asyncio
    async def test_posts_exactly_once_on_non_2xx(self) -> None:
        maker, client = _client_returning(MagicMock(status_code=500))
        with patch(f"{MODULE}.httpx.AsyncClient", maker):
            ok = await deliver_webhook("https://example.com/hook", {"run_id": "r1"})
        assert ok is False
        assert client.post.await_count == 1

    @pytest.mark.asyncio
    async def test_returns_true_on_2xx(self) -> None:
        maker, client = _client_returning(MagicMock(status_code=204))
        with patch(f"{MODULE}.httpx.AsyncClient", maker):
            ok = await deliver_webhook("https://example.com/hook", {"run_id": "r1"})
        assert ok is True
        assert client.post.await_count == 1

    @pytest.mark.asyncio
    async def test_never_raises_on_unexpected_status(self) -> None:
        maker, _ = _client_returning(MagicMock(status_code=418))
        with patch(f"{MODULE}.httpx.AsyncClient", maker):
            assert await deliver_webhook("https://example.com/hook", {}) is False


class TestSigning:
    @pytest.mark.asyncio
    async def test_omits_signature_header_when_no_secret(self) -> None:
        maker, client = _client_returning(MagicMock(status_code=200))
        with patch(f"{MODULE}.httpx.AsyncClient", maker), patch(f"{MODULE}.settings") as s:
            s.webhook.WEBHOOK_SIGNING_SECRET = ""
            s.webhook.WEBHOOK_TIMEOUT_SECONDS = 30.0
            await deliver_webhook("https://example.com/hook", {})
        assert "Webhook-Signature" not in client.post.await_args.kwargs["headers"]

    @pytest.mark.asyncio
    async def test_signs_over_timestamp_and_body(self) -> None:
        maker, client = _client_returning(MagicMock(status_code=200))
        with patch(f"{MODULE}.httpx.AsyncClient", maker), patch(f"{MODULE}.settings") as s:
            s.webhook.WEBHOOK_SIGNING_SECRET = "shh"
            s.webhook.WEBHOOK_TIMEOUT_SECONDS = 30.0
            await deliver_webhook("https://example.com/hook", {"run_id": "r1"})

        header = client.post.await_args.kwargs["headers"]["Webhook-Signature"]
        body = client.post.await_args.kwargs["content"]
        timestamp = header.split(",")[0].removeprefix("t=")
        assert header == _sign("shh", timestamp, body)

    def test_sign_is_stable_for_same_inputs(self) -> None:
        assert _sign("k", "100", b"{}") == _sign("k", "100", b"{}")
        assert _sign("k", "100", b"{}") != _sign("k", "101", b"{}")


class TestValidateWebhookUrl:
    def test_allows_public_https(self) -> None:
        with patch(f"{MODULE}._resolves_to_private", return_value=False):
            assert validate_webhook_url("https://example.com/h") == "https://example.com/h"

    def test_none_passes_through(self) -> None:
        assert validate_webhook_url(None) is None

    @pytest.mark.parametrize("url", ["ftp://example.com", "file:///etc/passwd", "example.com/h"])
    def test_rejects_non_http_scheme(self, url: str) -> None:
        with pytest.raises(WebhookValidationError, match="scheme"):
            validate_webhook_url(url)

    def test_rejects_missing_host(self) -> None:
        with pytest.raises(WebhookValidationError, match="host"):
            validate_webhook_url("https:///path")

    def test_rejects_private_host(self) -> None:
        with patch(f"{MODULE}._resolves_to_private", return_value=True), patch(f"{MODULE}.settings") as s:
            s.webhook.WEBHOOK_ALLOW_PRIVATE_IPS = False
            with pytest.raises(WebhookValidationError, match="private or reserved"):
                validate_webhook_url("https://internal.local/h")

    def test_allows_private_host_when_opted_in(self) -> None:
        with patch(f"{MODULE}._resolves_to_private", return_value=True), patch(f"{MODULE}.settings") as s:
            s.webhook.WEBHOOK_ALLOW_PRIVATE_IPS = True
            assert validate_webhook_url("https://internal.local/h") == "https://internal.local/h"
