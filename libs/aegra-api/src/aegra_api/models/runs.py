"""Run-related Pydantic models for Agent Protocol"""

from datetime import datetime
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    Json,
    field_validator,
    model_validator,
)

from aegra_api.models.enums import (
    All,
    BulkCancelRunsStatus,
    CancelAction,
    DisconnectMode,
    Durability,
    IfNotExists,
    MultitaskStrategy,
    OnCompletionBehavior,
    RunSelectField,
    RunSortBy,
    SortOrder,
    StreamMode,
)
from aegra_api.models.filters import IdSet, TimeFilters, id_scope
from aegra_api.utils.metadata import KEY_PATTERN, MAX_KEYS, MAX_VALUE_LEN, validate_metadata
from aegra_api.utils.status_compat import validate_run_status
from aegra_api.utils.webhooks import WEBHOOK_MAX_LEN, validate_webhook_url


class RunCommand(BaseModel):
    """Mirrors the SDK's ``Command``: navigate, update state, or resume.

    Declared instead of a bare dict so a misspelled key is a 422 rather than a
    value ``map_command_to_langgraph`` silently drops — a resume that never
    resumes is indistinguishable from one that did nothing.
    """

    model_config = ConfigDict(extra="forbid")

    goto: str | list[str] | list[dict[str, Any]] | dict[str, Any] | None = Field(
        None, description="Node name(s) to continue at, or Send objects with per-node input."
    )
    update: dict[str, Any] | list[tuple[str, Any]] | None = Field(
        None, description="State updates to merge, or ordered (key, value) pairs."
    )
    resume: Any = Field(None, description="Value handed back to the `interrupt()` that paused the graph.")

    @model_validator(mode="after")
    def require_one(self) -> Self:
        """An empty command would start a run that does nothing."""
        if self.goto is None and self.update is None and "resume" not in self.model_fields_set:
            raise ValueError("command requires at least one of 'goto', 'update', or 'resume'")
        return self


class RunCreate(BaseModel):
    """Request model for creating runs"""

    assistant_id: str = Field(..., description="Assistant to execute")
    run_id: str | None = Field(
        None,
        description="Client-provided id for idempotent creation; server-generated when omitted.",
    )
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
    command: RunCommand | None = Field(
        None,
        description="Command for resuming interrupted runs with state updates or navigation",
    )
    interrupt_before: All | list[str] | None = Field(
        None,
        description="Nodes to interrupt immediately before they get executed. Use '*' for all nodes.",
    )
    interrupt_after: All | list[str] | None = Field(
        None,
        description="Nodes to interrupt immediately after they get executed. Use '*' for all nodes.",
    )

    # Subgraph configuration
    stream_subgraphs: bool | None = Field(
        False,
        description="Whether to include subgraph events in streaming. When True, includes events from all subgraphs. When False (default when None), excludes subgraph events. Defaults to False for backwards compatibility.",
    )

    # Annotated ``dict[str, Any]`` rather than a primitive union so one bad key
    # yields one actionable message instead of N parallel union-arm errors.
    metadata: dict[str, Any] | None = Field(
        None,
        description=(
            "Metadata propagated to OTEL trace attributes "
            f"(``langfuse.trace.metadata.<key>``). Keys match ``{KEY_PATTERN.pattern}``, "
            f"values are primitive, strings cap at {MAX_VALUE_LEN} characters, "
            f"maximum {MAX_KEYS} keys. For filterable attributes, not payload data."
        ),
    )

    @field_validator("metadata", mode="after")
    @classmethod
    def check_metadata(cls, metadata: dict[str, Any] | None) -> dict[str, Any] | None:
        """Same rule crons validate against, so a forwarded value cannot fail here."""
        return validate_metadata(metadata)

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
    # ``validation_alias`` rather than ``alias`` so the ORM attribute name is an
    # input spelling only — an ``alias`` also renames the field on the wire,
    # which FastAPI serializes by default and the SDK does not recognize.
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias="metadata_dict",
        description="Metadata supplied when the run was created.",
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
    # ``Json`` parses the URL-encoded object for us, so a malformed filter is a
    # 422 at the boundary rather than a json.loads deeper in the handler.
    metadata: Json[dict[str, Any]] | None = Field(
        None, description="Metadata containment match, given as a JSON object."
    )
    select: list[RunSelectField] | None = Field(
        None, description="Return only the listed fields; omit for the full entity."
    )


class RunCountRequest(TimeFilters):
    """Filters shared by run search and count, so the two cannot drift."""

    thread_id: IdSet = id_scope("thread_id", "Restrict to these threads.")
    assistant_id: IdSet = id_scope("assistant_id", "Restrict to these assistants.")
    run_id: IdSet = id_scope("run_id", "Restrict to these runs.")
    status: str | None = Field(None, description="Filter by run status.")
    metadata: dict[str, Any] | None = Field(None, description="Metadata containment match.")

    @field_validator("status")
    @classmethod
    def check_status(cls, status: str | None) -> str | None:
        """Reject an unknown status here rather than returning a silently empty page."""
        return validate_run_status(status) if status is not None else None


class RunSearchRequest(RunCountRequest):
    """Request body for searching runs across threads."""

    limit: int = Field(10, ge=1, le=1000, description="Maximum rows per page.")
    offset: int = Field(0, ge=0, description="Rows to skip.")
    sort_by: RunSortBy | None = Field(None, description="Sort field; defaults to created_at.")
    sort_order: SortOrder | None = Field(None, description="Sort direction; defaults to desc.")
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
