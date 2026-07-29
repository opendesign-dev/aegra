"""Integration tests for health and info endpoints."""

from aegra_api.settings import settings
from tests.fixtures.clients import create_test_app, make_client


def test_info_reports_cron_flag_from_runtime_settings(monkeypatch) -> None:
    """GET /info should expose the current CRON_ENABLED runtime flag."""
    monkeypatch.setattr(settings.cron, "CRON_ENABLED", False)

    client = make_client(create_test_app())
    response = client.get("/info")

    assert response.status_code == 200
    flags = response.json()["flags"]
    # Assert the runtime-driven flag exactly; the always-on capability flags are
    # checked as a set so adding a capability doesn't break this test.
    assert flags["crons"] is False
    assert flags["webhooks"] == settings.webhook.WEBHOOK_ENABLED
    assert {"assistants", "multitask", "batch", "store", "checkpointer"} <= flags.keys()
    assert all(flags[name] is True for name in ("assistants", "multitask", "batch", "store", "checkpointer"))
