"""Pydantic models for cron job endpoints."""

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from aegra_api.models.enums import All, CronSelectField, CronSortBy, Durability, SortOrder
from aegra_api.settings import settings
from aegra_api.models.filters import IdSet, TimeFilters, id_scope
from aegra_api.utils.metadata import CRON_ID_KEY, MAX_KEYS, validate_metadata
from aegra_api.utils.webhooks import WEBHOOK_MAX_LEN, validate_webhook_url

# Field length caps. Keep these conservative; cron metadata is small by nature.
_SCHEDULE_MAX_LEN = 256
_TIMEZONE_MAX_LEN = 64
_STREAM_MODE_MAX_LEN = 64
_STR_FIELD_MAX_LEN = 256

OnRunCompleted = Literal["delete", "keep"]


def _validate_payload_size(model: BaseModel) -> None:
    """Reject payloads whose serialized JSON exceeds the configured cap."""
    cap = settings.cron.CRON_MAX_PAYLOAD_BYTES
    serialized = model.model_dump_json()
    if len(serialized.encode("utf-8")) > cap:
        raise ValueError(f"cron payload exceeds {cap} bytes")


def _validate_cron_metadata(metadata: dict[str, Any] | None) -> None:
    """Hold cron metadata to the rule its fired runs are validated against.

    One slot short of the run cap, because firing stamps ``cron_id`` on top.
    """
    if metadata and CRON_ID_KEY in metadata:
        raise ValueError(f"metadata key {CRON_ID_KEY!r} is reserved")
    validate_metadata(metadata, max_keys=MAX_KEYS - 1)


class CronCreate(BaseModel):
    """Request body for creating a cron job (stateless or thread-bound)."""

    assistant_id: str = Field(..., max_length=_STR_FIELD_MAX_LEN)
    cron_id: str | None = Field(
        None,
        max_length=_STR_FIELD_MAX_LEN,
        description="Client-provided id for idempotent creation; server-generated when omitted.",
    )
    schedule: str = Field(..., max_length=_SCHEDULE_MAX_LEN)
    input: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = Field(None, description="Inherited by every run this schedule fires.")
    config: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    interrupt_before: All | list[str] | None = None
    interrupt_after: All | list[str] | None = None
    webhook: str | None = Field(None, max_length=WEBHOOK_MAX_LEN)
    on_run_completed: OnRunCompleted | None = None
    multitask_strategy: str | None = Field(None, max_length=_STR_FIELD_MAX_LEN)
    end_time: datetime | None = None
    enabled: bool | None = None
    stream_mode: str | list[str] | None = None
    stream_subgraphs: bool | None = None
    timezone: str | None = Field(None, max_length=_TIMEZONE_MAX_LEN)
    stream_resumable: bool | None = None
    durability: Durability | None = None
    checkpoint_during: bool | None = Field(None, deprecated=True, description="DEPRECATED: use `durability`.")

    @model_validator(mode="after")
    def _check(self) -> "CronCreate":
        self.webhook = validate_webhook_url(self.webhook)
        _validate_cron_metadata(self.metadata)
        if isinstance(self.stream_mode, str) and len(self.stream_mode) > _STREAM_MODE_MAX_LEN:
            raise ValueError("stream_mode is too long")
        if self.end_time is not None:
            now = datetime.now(UTC)
            end = self.end_time if self.end_time.tzinfo else self.end_time.replace(tzinfo=UTC)
            if end <= now:
                raise ValueError("end_time must be in the future")
        _validate_payload_size(self)
        return self


class CronResponse(BaseModel):
    """Response model matching the SDK ``Cron`` TypedDict."""

    model_config = ConfigDict(from_attributes=True)

    cron_id: str
    assistant_id: str
    thread_id: str | None = None
    on_run_completed: OnRunCompleted | None = None
    end_time: datetime | None = None
    schedule: str
    created_at: datetime
    updated_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)
    user_id: str | None = None
    next_run_date: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    timezone: str | None = None


class CronUpdate(BaseModel):
    """Request body for updating an existing cron job."""

    schedule: str | None = Field(None, max_length=_SCHEDULE_MAX_LEN)
    end_time: datetime | None = None
    input: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = Field(None, description="Inherited by every run this schedule fires.")
    config: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    webhook: str | None = Field(None, max_length=WEBHOOK_MAX_LEN)
    interrupt_before: All | list[str] | None = None
    interrupt_after: All | list[str] | None = None
    on_run_completed: OnRunCompleted | None = None
    multitask_strategy: str | None = Field(None, max_length=_STR_FIELD_MAX_LEN)
    enabled: bool | None = None
    stream_mode: str | list[str] | None = None
    stream_subgraphs: bool | None = None
    timezone: str | None = Field(None, max_length=_TIMEZONE_MAX_LEN)
    stream_resumable: bool | None = None
    durability: Durability | None = None
    checkpoint_during: bool | None = Field(None, deprecated=True, description="DEPRECATED: use `durability`.")

    @model_validator(mode="after")
    def _check(self) -> "CronUpdate":
        self.webhook = validate_webhook_url(self.webhook)
        _validate_cron_metadata(self.metadata)
        if isinstance(self.stream_mode, str) and len(self.stream_mode) > _STREAM_MODE_MAX_LEN:
            raise ValueError("stream_mode is too long")
        if self.end_time is not None:
            now = datetime.now(UTC)
            end = self.end_time if self.end_time.tzinfo else self.end_time.replace(tzinfo=UTC)
            if end <= now:
                raise ValueError("end_time must be in the future")
        _validate_payload_size(self)
        return self


class CronSearchRequest(TimeFilters):
    """Request body for searching cron jobs."""

    assistant_id: IdSet = id_scope("assistant_id", "Restrict to these assistants.")
    thread_id: IdSet = id_scope("thread_id", "Restrict to these threads.")
    cron_id: IdSet = id_scope("cron_id", "Restrict to these schedules.")
    enabled: bool | None = None
    metadata: dict[str, Any] | None = Field(None, description="Metadata containment match.")
    limit: int = Field(10, ge=1, le=1000)
    offset: int = Field(0, ge=0)
    sort_by: CronSortBy | None = Field(None, description="Sort field; defaults to created_at.")
    sort_order: SortOrder | None = Field(None, description="Sort direction; defaults to desc.")
    select: list[CronSelectField] | None = Field(
        None, description="Return only the listed fields; omit for the full entity."
    )


class CronCountRequest(TimeFilters):
    """Request body for counting cron jobs."""

    assistant_id: IdSet = id_scope("assistant_id", "Restrict to these assistants.")
    thread_id: IdSet = id_scope("thread_id", "Restrict to these threads.")
    cron_id: IdSet = id_scope("cron_id", "Restrict to these schedules.")
    metadata: dict[str, Any] | None = Field(None, description="Metadata containment match.")
