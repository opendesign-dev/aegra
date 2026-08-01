"""Webhook URL validation shared by run and cron request models."""

from urllib.parse import urlparse

WEBHOOK_MAX_LEN = 2048


def validate_webhook_url(value: str | None) -> str | None:
    """Reject malformed or non-http(s) webhook URLs at the API boundary.

    Delivery POSTs to whatever is stored here, so this is the SSRF entry point.
    """
    if value is None:
        return None
    if len(value) > WEBHOOK_MAX_LEN:
        raise ValueError(f"webhook must be at most {WEBHOOK_MAX_LEN} characters")
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("webhook must use http or https scheme")
    if not parsed.netloc:
        raise ValueError("webhook must include a host")
    return value
