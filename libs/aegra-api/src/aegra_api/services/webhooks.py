"""Outbound webhook delivery for run completion.

Fires a POST at each run's terminal state, mirroring LangGraph Platform. This
module owns one attempt: a per-attempt timeout, optional Standard-Webhooks-style
HMAC-SHA256 signing, and SSRF hardening (private/loopback/link-local/reserved IPs
blocked unless explicitly allowed). Retry, backoff, and dead-lettering belong to
the outbox deliverer.
"""

import hashlib
import hmac
import ipaddress
import json
import socket
import time
from typing import Any
from urllib.parse import urlparse

import httpx
import structlog

from aegra_api.settings import settings
from aegra_api.utils.url import redact_url

logger = structlog.getLogger(__name__)


class WebhookValidationError(ValueError):
    """Raised when a webhook URL fails scheme/host or SSRF validation."""


def validate_webhook_url(value: str | None) -> str | None:
    """Validate a webhook URL, returning it unchanged (or None).

    Requires an http(s) scheme and a host, and — unless
    ``WEBHOOK_ALLOW_PRIVATE_IPS`` — a host that does not resolve to a private,
    loopback, link-local, or reserved address (SSRF guard).
    """
    if value is None:
        return None
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https"):
        raise WebhookValidationError("webhook must use http or https scheme")
    if not parsed.hostname:
        raise WebhookValidationError("webhook must include a host")
    if not settings.webhook.WEBHOOK_ALLOW_PRIVATE_IPS and _resolves_to_private(parsed.hostname):
        raise WebhookValidationError("webhook host resolves to a private or reserved address")
    return value


def _resolves_to_private(host: str) -> bool:
    """True when *host* is or resolves to a private/reserved IP.

    Guards SSRF to internal services and the cloud metadata endpoint. Fails
    closed: a resolution error is treated as private (blocked).
    """
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return True
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            return True
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            return True
    return False


def _sign(secret: str, timestamp: str, body: bytes) -> str:
    """Standard-Webhooks-style HMAC-SHA256 over ``{timestamp}.{body}``."""
    signed = f"{timestamp}.".encode() + body
    digest = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


async def deliver_webhook(url: str, payload: dict[str, Any]) -> bool:
    """POST *payload* to *url* exactly once; True on a 2xx. Never raises.

    Retry and backoff belong to the outbox deliverer, which survives restarts.
    Retrying here too would make ``WEBHOOK_MAX_ATTEMPTS`` count rounds, not POSTs.
    """
    body = json.dumps(payload, default=str).encode()
    safe_url = redact_url(url)
    headers = {"Content-Type": "application/json"}
    if settings.webhook.WEBHOOK_SIGNING_SECRET:
        # Fresh timestamp per call so each outbox retry carries a valid signature.
        ts = str(int(time.time()))
        headers["Webhook-Signature"] = _sign(settings.webhook.WEBHOOK_SIGNING_SECRET, ts, body)

    async with httpx.AsyncClient(timeout=settings.webhook.WEBHOOK_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.post(url, content=body, headers=headers)
        except httpx.HTTPError as exc:
            logger.warning("Webhook attempt failed", url=safe_url, error=str(exc))
            return False
        if 200 <= resp.status_code < 300:
            return True
        logger.warning("Webhook non-2xx", url=safe_url, status=resp.status_code)
        return False
