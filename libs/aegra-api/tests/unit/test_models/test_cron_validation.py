"""Validation tests for cron Pydantic models.

Covers reviewer-requested guards: webhook scheme, payload size, end_time
must be in the future, max_length on string fields, on_run_completed
literal restriction.
"""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from aegra_api.models.crons import CronCreate, CronUpdate


class TestWebhookValidation:
    @pytest.mark.parametrize(
        "url",
        [
            "ftp://example.com/hook",
            "javascript:alert(1)",
            "file:///etc/passwd",
            "//example.com/hook",
            "://no-scheme.example",
            "http:///no-host",
        ],
    )
    def test_rejects_non_http_or_malformed_scheme(self, url: str) -> None:
        with pytest.raises(ValidationError):
            CronCreate(assistant_id="a", schedule="* * * * *", webhook=url)

    @pytest.mark.parametrize("url", ["http://example.com/hook", "https://example.com/hook"])
    def test_accepts_http_https(self, url: str) -> None:
        req = CronCreate(assistant_id="a", schedule="* * * * *", webhook=url)
        assert req.webhook == url


class TestEndTimeMustBeFuture:
    def test_rejects_past_end_time_on_create(self) -> None:
        with pytest.raises(ValidationError, match="future"):
            CronCreate(
                assistant_id="a",
                schedule="* * * * *",
                end_time=datetime.now(UTC) - timedelta(seconds=1),
            )

    def test_accepts_future_end_time_on_create(self) -> None:
        end = datetime.now(UTC) + timedelta(days=1)
        req = CronCreate(assistant_id="a", schedule="* * * * *", end_time=end)
        assert req.end_time == end

    def test_rejects_past_end_time_on_update(self) -> None:
        with pytest.raises(ValidationError, match="future"):
            CronUpdate(end_time=datetime.now(UTC) - timedelta(days=365))


class TestPayloadSizeCap:
    def test_rejects_oversized_input(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from aegra_api.models import crons as crons_mod

        monkeypatch.setattr(crons_mod.settings.cron, "CRON_MAX_PAYLOAD_BYTES", 256)
        with pytest.raises(ValidationError, match="payload"):
            CronCreate(
                assistant_id="a",
                schedule="* * * * *",
                input={"messages": [{"role": "user", "content": "x" * 1024}]},
            )


class TestOnRunCompletedLiteral:
    def test_rejects_unknown_value(self) -> None:
        with pytest.raises(ValidationError):
            CronCreate(
                assistant_id="a",
                schedule="* * * * *",
                on_run_completed="create_new",  # type: ignore[arg-type]
            )

    @pytest.mark.parametrize("value", ["delete", "keep"])
    def test_accepts_allowed_values(self, value: str) -> None:
        req = CronCreate(
            assistant_id="a",
            schedule="* * * * *",
            on_run_completed=value,  # type: ignore[arg-type]
        )
        assert req.on_run_completed == value


class TestMaxLengthGuards:
    def test_rejects_oversized_schedule(self) -> None:
        with pytest.raises(ValidationError):
            CronCreate(assistant_id="a", schedule="*" * 1024)

    def test_rejects_oversized_timezone(self) -> None:
        with pytest.raises(ValidationError):
            CronCreate(assistant_id="a", schedule="* * * * *", timezone="X" * 256)

    def test_rejects_oversized_webhook(self) -> None:
        with pytest.raises(ValidationError):
            CronCreate(
                assistant_id="a",
                schedule="* * * * *",
                webhook="https://example.com/" + ("x" * 4096),
            )


class TestCronId:
    def test_defaults_to_none(self) -> None:
        assert CronCreate(assistant_id="a", schedule="* * * * *").cron_id is None

    def test_accepts_client_value(self) -> None:
        assert CronCreate(assistant_id="a", schedule="* * * * *", cron_id="nightly").cron_id == "nightly"


class TestCronMetadata:
    """Cron metadata is inherited by every fired run, so it obeys the run's rule
    at create time rather than failing hours later at the first firing."""

    def _create(self, metadata: dict) -> CronCreate:
        return CronCreate(assistant_id="a", schedule="* * * * *", metadata=metadata)

    def test_primitive_values_accepted(self) -> None:
        assert self._create({"tenant": "acme", "retries": 3}).metadata == {"tenant": "acme", "retries": 3}

    def test_nested_value_rejected(self) -> None:
        with pytest.raises(ValidationError, match="must be str/int/float/bool"):
            self._create({"nested": {"a": 1}})

    def test_malformed_key_rejected(self) -> None:
        with pytest.raises(ValidationError, match="must match"):
            self._create({"bad key": "v"})

    def test_cron_id_key_is_reserved(self) -> None:
        with pytest.raises(ValidationError, match="reserved"):
            self._create({"cron_id": "spoofed"})

    def test_one_slot_reserved_for_the_injected_cron_id(self) -> None:
        """31 keys fit; 32 would leave no room for the cron_id stamped at firing."""
        assert len(self._create({f"k{i}": i for i in range(31)}).metadata) == 31
        with pytest.raises(ValidationError, match="exceeds 31 keys"):
            self._create({f"k{i}": i for i in range(32)})

    def test_update_applies_the_same_rule(self) -> None:
        with pytest.raises(ValidationError, match="must be str/int/float/bool"):
            CronUpdate(metadata={"nested": {"a": 1}})
