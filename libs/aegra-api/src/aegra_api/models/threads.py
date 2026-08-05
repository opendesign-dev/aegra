"""Thread-related Pydantic models for Agent Protocol"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from aegra_api.models.enums import (
    OnConflictBehavior,
    PruneStrategy,
    SortOrder,
    ThreadSelectField,
    ThreadSortBy,
)
from aegra_api.models.filters import IdSet, TimeFilters, id_scope
from aegra_api.utils.status_compat import validate_thread_status

# Cap on ``extract`` paths per search request, per the SDK docstring.
_MAX_EXTRACT_PATHS = 10


class ThreadTTL(BaseModel):
    """Retention policy for a single thread."""

    strategy: PruneStrategy = Field("delete", description="What to do once the thread expires.")
    ttl: float = Field(..., gt=0, description="Lifetime in minutes.")


class SuperstepUpdate(BaseModel):
    """One state write applied while pre-filling a thread."""

    values: dict[str, Any] | list[dict[str, Any]] | None = Field(None, description="State values to write.")
    command: dict[str, Any] | None = Field(None, description="Drive the write as a command instead of values.")
    as_node: str = Field(..., description="Node to attribute the write to.")


class Superstep(BaseModel):
    """A superstep: a batch of state writes applied in order."""

    updates: list[SuperstepUpdate] = Field(..., min_length=1, description="Updates applied in order.")


class ThreadCreate(BaseModel):
    """Request model for creating threads"""

    model_config = ConfigDict(populate_by_name=True)

    metadata: dict[str, Any] | None = Field(None, description="Thread metadata")
    thread_id: str | None = Field(
        None,
        alias="threadId",
        description="Optional client-provided thread ID for idempotent creation",
    )
    if_exists: OnConflictBehavior | None = Field(
        "raise",
        alias="ifExists",
        description="On conflict: 'raise' returns 409, 'do_nothing' returns the existing thread.",
    )
    ttl: ThreadTTL | None = Field(None, description="Retention policy; omit to keep the thread indefinitely.")
    supersteps: list[Superstep] | None = Field(
        None,
        description="Pre-fill the thread with a sequence of state writes (import a transcript, seed a test).",
    )


class ThreadUpdate(BaseModel):
    """Request model for updating threads"""

    metadata: dict[str, Any] | None = Field(None, description="Thread metadata to update")
    ttl: ThreadTTL | None = Field(None, description="Replaces the retention policy; omit to leave it unchanged.")


class Thread(BaseModel):
    """Thread entity model

    Status values: idle, busy, interrupted, error
    """

    model_config = ConfigDict(from_attributes=True)

    thread_id: str = Field(..., description="Unique identifier for the thread.")
    status: str = Field("idle", description="Current thread status: idle, busy, interrupted, or error.")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Arbitrary metadata attached to the thread.")
    user_id: str = Field(..., description="Identifier of the user who owns this thread.")
    created_at: datetime = Field(..., description="Timestamp when the thread was created.")
    updated_at: datetime = Field(..., description="Timestamp when the thread was last updated.")
    # First-class in the SDK's Thread: clients render transcripts straight off
    # these, so omitting them forces a state lookup per row and breaks list views.
    values: dict[str, Any] | None = Field(None, description="Current thread state, null until materialized.")
    interrupts: dict[str, list[dict[str, Any]]] = Field(
        default_factory=dict, description="Pending interrupts grouped by task id."
    )
    config: dict[str, Any] = Field(
        default_factory=dict,
        description="Config the latest run bound this thread to; state reads reload the graph with it.",
    )

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate status conforms to API specification."""
        if not isinstance(v, str):
            raise ValueError(f"Status must be a string, got {type(v)}")
        return validate_thread_status(v)


class ThreadList(BaseModel):
    """Response model for listing threads"""

    threads: list[Thread]
    total: int


class ThreadPruneRequest(BaseModel):
    """Request body for reclaiming thread storage."""

    thread_ids: list[str] = Field(..., min_length=1, description="Threads to prune.")
    strategy: PruneStrategy = Field(
        "delete",
        description="'delete' removes the threads outright; 'keep_latest' drops history but keeps the latest state.",
    )


class ThreadPruneResponse(BaseModel):
    """Response body for POST /threads/prune."""

    pruned_count: int = Field(..., description="Number of threads pruned.")


class ThreadSearchRequest(TimeFilters):
    """Request model for thread search"""

    metadata: dict[str, Any] | None = Field(None, description="Metadata containment match.")
    status: str | None = Field(None, description="Filter by status: idle, busy, interrupted, or error.")
    values: dict[str, Any] | None = Field(
        None, description="State containment match; only matches threads whose state is materialized."
    )
    thread_id: IdSet = id_scope("thread_id", "Restrict to these threads.")
    limit: int = Field(20, le=1000, ge=1, description="Maximum rows per page.")
    offset: int = Field(0, ge=0, description="Rows to skip.")
    sort_by: ThreadSortBy | None = Field(None, description="Sort field; defaults to created_at.")
    sort_order: SortOrder | None = Field(None, description="Sort direction; defaults to desc.")
    select: list[ThreadSelectField] | None = Field(
        None, description="Return only the listed fields; omit for the full entity."
    )
    extract: dict[str, str] | None = Field(
        None,
        max_length=_MAX_EXTRACT_PATHS,
        description=(
            "Alias to dot/bracket path into the thread, e.g. {'last_msg': 'values.messages[-1]'}. "
            "Results land in each row's `extracted` field."
        ),
    )

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str | None) -> str | None:
        """Validate status filter conforms to API specification."""
        if v is not None:
            return validate_thread_status(v)
        return v


class ThreadSearchResponse(BaseModel):
    """Response model for thread search"""

    threads: list[Thread]
    total: int
    limit: int
    offset: int


class ThreadCheckpoint(BaseModel):
    """Checkpoint identifier for thread history"""

    checkpoint_id: str | None = None
    thread_id: str | None = None
    checkpoint_ns: str | None = ""


class ThreadCheckpointPostRequest(BaseModel):
    """Request model for fetching thread checkpoint"""

    checkpoint: ThreadCheckpoint = Field(description="Checkpoint to fetch")
    subgraphs: bool | None = Field(False, description="Include subgraph states")


class ThreadState(BaseModel):
    """Thread state model for history endpoint"""

    values: dict[str, Any] = Field(description="Channel values (messages, etc.)")
    next: list[str] = Field(default_factory=list, description="Next nodes to execute")
    tasks: list[dict[str, Any]] = Field(default_factory=list, description="Tasks to execute")
    interrupts: list[dict[str, Any]] = Field(default_factory=list, description="Interrupt data")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Checkpoint metadata")
    created_at: datetime | None = Field(None, description="Timestamp of state creation")
    checkpoint: ThreadCheckpoint = Field(description="Current checkpoint")
    parent_checkpoint: ThreadCheckpoint | None = Field(None, description="Parent checkpoint")
    checkpoint_id: str | None = Field(None, description="Checkpoint ID (for backward compatibility)")
    parent_checkpoint_id: str | None = Field(None, description="Parent checkpoint ID (for backward compatibility)")


class ThreadStateUpdate(BaseModel):
    """Request model for updating thread state"""

    values: dict[str, Any] | list[dict[str, Any]] | None = Field(
        None, description="The values to update the state with"
    )
    checkpoint: dict[str, Any] | None = Field(None, description="The checkpoint to update the state of")
    checkpoint_id: str | None = Field(None, description="Optional checkpoint ID to update from")
    as_node: str | None = Field(None, description="Update the state as if this node had just executed")
    # Also support query-like parameters for GET-like behavior via POST
    subgraphs: bool | None = Field(False, description="Include states from subgraphs")
    checkpoint_ns: str | None = Field(None, description="Checkpoint namespace")


class ThreadStateUpdateResponse(BaseModel):
    """Response model for thread state update"""

    checkpoint: dict[str, Any] = Field(description="The checkpoint that was created/updated")


class ThreadHistoryRequest(BaseModel):
    """Request model for thread history endpoint"""

    limit: int | None = Field(10, ge=1, le=1000, description="Number of states to return")
    before: dict[str, Any] | str | None = Field(
        None,
        description="Return states before this checkpoint (checkpoint ID string, raw checkpoint dict, or RunnableConfig with 'configurable' key)",
    )
    metadata: dict[str, Any] | None = Field(None, description="Filter by metadata")
    checkpoint: dict[str, Any] | None = Field(None, description="Checkpoint for subgraph filtering")
    subgraphs: bool | None = Field(False, description="Include states from subgraphs")
    checkpoint_ns: str | None = Field(None, description="Checkpoint namespace")
