"""Run-related Pydantic models for Agent Protocol"""

import re
from datetime import datetime
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from aegra_api.models.enums import (
    BulkCancelRunsStatus,
    CancelAction,
    DisconnectMode,
    Durability,
    IfNotExists,
    MultitaskStrategy,
    OnCompletionBehavior,
    RunSelectField,
    StreamMode,
)
from aegra_api.utils.status_compat import validate_run_status
from aegra_api.utils.webhooks import WEBHOOK_MAX_LEN, validate_webhook_url

# Constraints for ``RunCreate.metadata`` keys/values, enforced at request
# time so the OpenAPI schema is honest about what reaches OTEL.  Without
# these limits a tenant could submit thousands of keys, megabyte-scale
# values, or nested structures — all of which would either be silently
# dropped by ``merge_run_metadata`` or balloon span size past the OTEL
# collector limits.  Bounds chosen to be generous for legitimate use
# (tenant id, feature flag, environment, sub-agent type, ...) while
# closing the DoS surface.
_METADATA_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_METADATA_MAX_KEYS = 32
_METADATA_MAX_VALUE_LEN = 512


class RunCreate(BaseModel):
    """Request model for creating runs"""

    assistant_id: str = Field(..., description="Assistant to execute")
    input: dict[str, Any] | None = Field(
        None,
        description="Input data for the run. Optional when resuming from a checkpoint.",
    )
    config: dict[str, Any] | None = Field(default_factory=dict, description="Execution config")
    context: dict[str, Any] | None = Field(default_factory=dict, description="Execution context")
    checkpoint: dict[str, Any] | None = Field(
        None,
        description="Checkpoint configuration (e.g., {'checkpoint_id': '...', 'checkpoint_ns': ''})",
    )
    checkpoint_id: str | None = Field(
        None, description="Flat form of `checkpoint.checkpoint_id`; merged into `checkpoint` when given."
    )
    stream: bool = Field(False, description="Enable streaming response")
    stream_mode: StreamMode | list[StreamMode] | None = Field(None, description="Requested stream mode(s)")
    stream_resumable: bool | None = Field(
        None,
        description=(
            "Buffer this run's events so a reconnect with `Last-Event-ID` can replay them. "
            "The buffer is in-memory per run and does not survive a server restart."
        ),
    )
    on_disconnect: DisconnectMode | None = Field(
        None,
        description="Behavior on client disconnect: 'cancel' (default) or 'continue'.",
    )
    on_completion: OnCompletionBehavior | None = Field(
        None,
        description="Behavior after stateless run completes: 'delete' (default) removes the ephemeral thread, 'keep' preserves it.",
    )

    multitask_strategy: MultitaskStrategy | None = Field(
        None,
        description=(
            "How to handle a run started while the thread already has one in flight: "
            "'reject' returns 409, 'interrupt' stops the in-flight run, 'rollback' stops it and "
            "discards its state, 'enqueue' (default) lets both proceed."
        ),
    )
    if_not_exists: IfNotExists | None = Field(
        None,
        description=("When the target thread does not exist: 'create' (default) creates it, 'reject' returns 404."),
    )
    durability: Durability | None = Field(
        None,
        description="When checkpoints are flushed: 'sync', 'async' (default), or 'exit'.",
    )
    checkpoint_during: bool | None = Field(
        None,
        deprecated=True,
        description="DEPRECATED: use `durability`. True maps to 'async', False to 'exit'.",
    )
    after_seconds: int | None = Field(
        None, ge=0, description="Delay execution by this many seconds; the run stays `pending` until due."
    )
    webhook: str | None = Field(
        None,
        max_length=WEBHOOK_MAX_LEN,
        description="URL that receives a POST with the final Run payload once the run reaches a terminal state.",
    )
    feedback_keys: list[str] | None = Field(
        None, description="LangSmith feedback keys; recorded on the run and echoed to tracing."
    )
    langsmith_tracer: dict[str, Any] | None = Field(
        None, description="Client-side LangSmith tracing hints; recorded on the run, not acted on server-side."
    )

    # Human-in-the-loop fields (core HITL functionality)
    command: dict[str, Any] | None = Field(
        None,
        description="Command for resuming interrupted runs with state updates or navigation",
    )
    interrupt_before: str | list[str] | None = Field(
        None,
        description="Nodes to interrupt immediately before they get executed. Use '*' for all nodes.",
    )
    interrupt_after: str | list[str] | None = Field(
        None,
        description="Nodes to interrupt immediately after they get executed. Use '*' for all nodes.",
    )

    # Subgraph configuration
    stream_subgraphs: bool | None = Field(
        False,
        description="Whether to include subgraph events in streaming. When True, includes events from all subgraphs. When False (default when None), excludes subgraph events. Defaults to False for backwards compatibility.",
    )

    # Request metadata (top-level in payload).  Reaches OTEL trace
    # attributes as ``langfuse.trace.metadata.<key>`` (and the
    # OpenInference ``metadata.<key>`` alias on Phoenix targets).  The
    # field is annotated ``dict[str, Any]`` rather than a primitive
    # union so a malformed payload produces one actionable 422 message
    # from ``validate_metadata_shape`` instead of N parallel union-arm
    # errors (one per primitive type Pydantic tries) per offending key.
    metadata: dict[str, Any] | None = Field(
        None,
        description=(
            "Request metadata propagated to OTEL trace attributes "
            "(``langfuse.trace.metadata.<key>``).  Keys must match "
            "``[A-Za-z0-9_-]{1,64}``.  Values must be primitive "
            "(``str``, ``int``, ``float``, ``bool``); string values are "
            "capped at 512 characters.  Maximum 32 keys.  Use this for "
            "filterable attributes (tenant, feature flag, environment, "
            "sub-agent type) rather than payload data."
        ),
    )

    @field_validator("metadata", mode="after")
    @classmethod
    def validate_metadata_shape(
        cls,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Enforce key shape, key count, value type, and string-value length.

        Validation runs entirely here (rather than relying on a primitive
        union on the field type) so each violation produces one clear
        error message instead of N parallel union-arm errors per offending
        key — easier for clients to surface to humans.
        """
        if metadata is None:
            return None
        if len(metadata) > _METADATA_MAX_KEYS:
            raise ValueError(f"metadata exceeds {_METADATA_MAX_KEYS} keys (got {len(metadata)})")
        for key, value in metadata.items():
            if not _METADATA_KEY_RE.match(key):
                raise ValueError(f"metadata key {key!r} must match {_METADATA_KEY_RE.pattern}")
            if not isinstance(value, (str, int, float, bool)):
                raise ValueError(
                    f"metadata value for key {key!r} must be str/int/float/bool, got {type(value).__name__}"
                )
            if isinstance(value, str) and len(value) > _METADATA_MAX_VALUE_LEN:
                raise ValueError(f"metadata value for key {key!r} exceeds {_METADATA_MAX_VALUE_LEN} characters")
        return metadata

    @model_validator(mode="after")
    def normalize(self) -> Self:
        """Fold the SDK's flat/legacy spellings into the canonical fields."""
        self.webhook = validate_webhook_url(self.webhook)

        if self.checkpoint_id:
            self.checkpoint = {**(self.checkpoint or {}), "checkpoint_id": self.checkpoint_id}

        # checkpoint_during predates durability and carries the same meaning:
        # True kept checkpoints during the run, False only at exit.
        if self.durability is None and self.checkpoint_during is not None:
            self.durability = "async" if self.checkpoint_during else "exit"

        # Empty input dict alongside command: drop it for frontend compatibility.
        if self.input is not None and self.command is not None:
            if self.input == {}:
                self.input = None
            else:
                raise ValueError("Cannot specify both 'input' and 'command' - they are mutually exclusive")
        # Checkpoint-only resume keeps input=None so Pregel resumes from next=[...]
        # instead of restarting from __start__ with an empty input.
        if self.input is None and self.command is None and self.checkpoint is None:
            raise ValueError("Must specify at least one of 'input', 'command', or 'checkpoint'")
        return self


class Run(BaseModel):
    """Run entity model

    Status values: pending, running, error, success, timeout, interrupted
    """

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    run_id: str = Field(..., description="Unique identifier for the run.")
    thread_id: str = Field(..., description="Thread this run belongs to.")
    assistant_id: str = Field(..., description="Assistant that is executing this run.")
    status: str = Field(
        "pending", description="Current run status: pending, running, error, success, timeout, or interrupted."
    )
    input: dict[str, Any] | None = Field(
        None, description="Input data provided to the run. None for checkpoint-only resume."
    )
    output: dict[str, Any] | None = Field(
        None, description="Final output produced by the run, or null if not yet complete."
    )
    error_message: str | None = Field(None, description="Error message if the run failed.")
    config: dict[str, Any] | None = Field(
        default_factory=dict, description="Configuration passed to the graph at runtime."
    )
    context: dict[str, Any] | None = Field(
        default_factory=dict, description="Context variables available during execution."
    )
    user_id: str = Field(..., description="Identifier of the user who owns this run.")
    created_at: datetime = Field(..., description="Timestamp when the run was created.")
    updated_at: datetime = Field(..., description="Timestamp when the run was last updated.")
    # First-class in the SDK's Run; reading run["metadata"] must not KeyError.
    metadata: dict[str, Any] = Field(
        default_factory=dict, alias="metadata_dict", description="Metadata supplied when the run was created."
    )
    multitask_strategy: str | None = Field(None, description="Strategy applied for concurrent runs on the same thread.")

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate status conforms to API specification."""
        if not isinstance(v, str):
            raise ValueError(f"Status must be a string, got {type(v)}")
        return validate_run_status(v)

    @field_validator("metadata", mode="before")
    @classmethod
    def default_metadata(cls, v: dict[str, Any] | None) -> dict[str, Any]:
        """Rows written before the column existed read back as NULL."""
        return v or {}


class RunStatus(BaseModel):
    """Simple run status response"""

    run_id: str = Field(..., description="Unique identifier for the run.")
    status: str = Field(..., description="Current run status value.")

    message: str | None = Field(None, description="Optional human-readable status message.")


class RunListRequest(BaseModel):
    """Query parameters for listing a thread's runs."""

    limit: int = Field(10, ge=1, le=1000, description="Maximum rows per page.")
    offset: int = Field(0, ge=0, description="Rows to skip.")
    status: str | None = Field(None, description="Filter by run status.")
    select: list[RunSelectField] | None = Field(
        None, description="Return only the listed fields; omit for the full entity."
    )


class BulkCancelRequest(BaseModel):
    """Request body for POST /runs/cancel.

    Either target explicit runs (`thread_id` plus `run_ids`) or a whole status
    bucket (`status`). One of the two must be given.
    """

    thread_id: str | None = Field(None, description="Thread whose runs should be cancelled.")
    run_ids: list[str] | None = Field(None, min_length=1, description="Explicit runs to cancel.")
    status: BulkCancelRunsStatus | None = Field(
        None, description="Cancel every run in this bucket: 'pending', 'running', or 'all'."
    )

    @model_validator(mode="after")
    def require_a_target(self) -> Self:
        if not self.run_ids and not self.status:
            raise ValueError("Provide run_ids or status to select which runs to cancel")
        if self.run_ids and not self.thread_id:
            raise ValueError("run_ids requires thread_id")
        return self


class BulkCancelResponse(BaseModel):
    """Response body for POST /runs/cancel."""

    cancelled_count: int = Field(..., description="Number of runs transitioned out of an active state.")
    run_ids: list[str] = Field(default_factory=list, description="Runs that were cancelled.")
    action: CancelAction = Field(..., description="Action applied to each run.")
