"""Shared query-filter primitives for search endpoints."""

from datetime import UTC, datetime
from typing import Annotated

from pydantic import AfterValidator


def assume_utc(value: datetime) -> datetime:
    """Treat a naive datetime as UTC.

    Time columns are timestamptz; asyncpg rejects a naive bind param against
    them, so the tz has to be pinned at the request boundary.
    """
    return value if value.tzinfo else value.replace(tzinfo=UTC)


UtcDatetime = Annotated[datetime, AfterValidator(assume_utc)]


def validate_time_range(after: datetime | None, before: datetime | None, field_prefix: str) -> None:
    """Reject an inverted window at the request boundary.

    An inverted range matches nothing, which reads to the caller as "no data"
    rather than "bad query".
    """
    if after is not None and before is not None and after > before:
        raise ValueError(f"{field_prefix}_after must not be later than {field_prefix}_before")
