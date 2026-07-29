"""Unit tests for the shared created_after/created_before search filters."""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from aegra_api.models import RunSearchRequest, ThreadSearchRequest
from aegra_api.models.filters import assume_utc, validate_time_range


class TestAssumeUtc:
    """assume_utc pins a tz so timestamptz binds never depend on server locale."""

    def test_naive_becomes_utc(self) -> None:
        """A naive datetime is interpreted as UTC, not local time."""
        result = assume_utc(datetime(2026, 1, 1, 12, 0, 0))
        assert result.tzinfo == UTC
        assert result.hour == 12

    def test_aware_is_left_alone(self) -> None:
        """An offset-aware datetime keeps its original zone."""
        tz = timezone(timedelta(hours=8))
        original = datetime(2026, 1, 1, 12, 0, 0, tzinfo=tz)
        assert assume_utc(original) is original


class TestValidateTimeRange:
    """validate_time_range rejects windows that can never match."""

    def test_inverted_range_raises(self) -> None:
        """after later than before is a query bug, not an empty result set."""
        after = datetime(2026, 3, 1, tzinfo=UTC)
        before = datetime(2026, 1, 1, tzinfo=UTC)
        with pytest.raises(ValueError, match="created_after must not be later than created_before"):
            validate_time_range(after, before, "created")

    def test_equal_bounds_allowed(self) -> None:
        """An instant-wide window is a legitimate (if narrow) query."""
        moment = datetime(2026, 1, 1, tzinfo=UTC)
        validate_time_range(moment, moment, "created")

    def test_one_sided_bounds_allowed(self) -> None:
        """Either bound may be omitted."""
        moment = datetime(2026, 1, 1, tzinfo=UTC)
        validate_time_range(moment, None, "created")
        validate_time_range(None, moment, "created")
        validate_time_range(None, None, "created")


class TestThreadSearchRequestTimeFilter:
    """ThreadSearchRequest exposes the window and validates it at the boundary."""

    def test_defaults_to_no_window(self) -> None:
        """Omitting both bounds leaves the search unfiltered by time."""
        request = ThreadSearchRequest()
        assert request.created_after is None
        assert request.created_before is None

    def test_naive_input_normalized_to_utc(self) -> None:
        """An ISO string without an offset is read as UTC."""
        request = ThreadSearchRequest(created_after="2026-01-01T00:00:00")
        assert request.created_after is not None
        assert request.created_after.tzinfo == UTC

    def test_offset_input_preserved(self) -> None:
        """An explicit offset is honoured rather than reinterpreted."""
        request = ThreadSearchRequest(created_after="2026-01-01T08:00:00+08:00")
        assert request.created_after is not None
        assert request.created_after.utcoffset() == timedelta(hours=8)

    def test_inverted_window_rejected(self) -> None:
        """An inverted window fails validation instead of returning nothing."""
        with pytest.raises(ValidationError, match="created_after must not be later than created_before"):
            ThreadSearchRequest(created_after="2026-03-01T00:00:00Z", created_before="2026-01-01T00:00:00Z")

    def test_non_datetime_rejected(self) -> None:
        """A non-timestamp value is a 422, not a silent no-op filter."""
        with pytest.raises(ValidationError):
            ThreadSearchRequest(created_after="not-a-timestamp")


class TestPaginationCapIsUniform:
    """Thread and run search share one page cap, matching the other search endpoints."""

    def test_both_accept_the_shared_cap(self) -> None:
        """1000 is the cap used by cron search and the thread-runs list."""
        assert ThreadSearchRequest(limit=1000).limit == 1000
        assert RunSearchRequest(limit=1000).limit == 1000

    def test_both_reject_above_the_cap(self) -> None:
        """Neither model may drift above the shared cap."""
        with pytest.raises(ValidationError):
            ThreadSearchRequest(limit=1001)
        with pytest.raises(ValidationError):
            RunSearchRequest(limit=1001)


class TestRunSearchRequestValidation:
    """RunSearchRequest mirrors the thread window and adds run-specific filters."""

    def test_defaults(self) -> None:
        """Defaults page 20 newest-first with no filters applied."""
        request = RunSearchRequest()
        assert request.limit == 20
        assert request.offset == 0
        assert request.sort_by is None
        assert request.created_after is None
        assert request.assistant_id is None

    def test_naive_input_normalized_to_utc(self) -> None:
        """Naive bounds are read as UTC, same as thread search."""
        request = RunSearchRequest(created_before="2026-01-01T00:00:00")
        assert request.created_before is not None
        assert request.created_before.tzinfo == UTC

    def test_inverted_window_rejected(self) -> None:
        """An inverted window fails validation."""
        with pytest.raises(ValidationError, match="created_after must not be later than created_before"):
            RunSearchRequest(created_after="2026-03-01T00:00:00Z", created_before="2026-01-01T00:00:00Z")

    def test_invalid_status_rejected(self) -> None:
        """An unknown status is rejected rather than matching zero rows."""
        with pytest.raises(ValidationError):
            RunSearchRequest(status="not-a-status")

    def test_valid_status_accepted(self) -> None:
        """A known status passes through."""
        assert RunSearchRequest(status="success").status == "success"

    def test_invalid_sort_by_rejected(self) -> None:
        """sort_by is Literal-bound so getattr can never reach arbitrary attributes."""
        with pytest.raises(ValidationError):
            RunSearchRequest(sort_by="password; DROP TABLE runs --")

    def test_invalid_select_field_rejected(self) -> None:
        """select is Literal-bound to the SDK's RunSelectField values."""
        with pytest.raises(ValidationError):
            RunSearchRequest(select=["run_id", "not_a_column"])

    def test_limit_bounds_enforced(self) -> None:
        """limit is capped so one request can't scan the whole runs table."""
        with pytest.raises(ValidationError):
            RunSearchRequest(limit=1001)
        with pytest.raises(ValidationError):
            RunSearchRequest(limit=0)

    def test_negative_offset_rejected(self) -> None:
        """A negative offset is invalid."""
        with pytest.raises(ValidationError):
            RunSearchRequest(offset=-1)
