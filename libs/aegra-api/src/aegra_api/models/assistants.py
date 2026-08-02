"""Assistant-related Pydantic models for Agent Protocol"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from aegra_api.models.enums import (
    AssistantSelectField,
    AssistantSortBy,
    OnConflictBehavior,
    SortOrder,
)
from aegra_api.models.filters import IdSet, TimeFilters, id_scope


class AssistantCreate(BaseModel):
    """Request model for creating assistants"""

    assistant_id: str | None = Field(None, description="Unique assistant identifier (auto-generated if not provided)")
    name: str | None = Field(
        None,
        description="Human-readable assistant name (auto-generated if not provided)",
    )
    description: str | None = Field(None, description="Assistant description")
    config: dict[str, Any] | None = Field(default_factory=dict, description="Assistant configuration")
    context: dict[str, Any] | None = Field(default_factory=dict, description="Assistant context")
    graph_id: str = Field(..., description="LangGraph graph ID from aegra.json")
    metadata: dict[str, Any] | None = Field(
        default_factory=dict, description="Metadata to use for searching and filtering assistants."
    )
    if_exists: OnConflictBehavior | None = Field(
        "raise", description="On conflict: 'raise' returns 409, 'do_nothing' returns the existing assistant."
    )


class Assistant(BaseModel):
    """Assistant entity model"""

    assistant_id: str = Field(..., description="Unique identifier for the assistant.")
    name: str = Field(..., description="Human-readable name of the assistant.")
    description: str | None = Field(None, description="Optional description of the assistant's purpose.")
    config: dict[str, Any] = Field(default_factory=dict, description="Configuration passed to the graph at runtime.")
    context: dict[str, Any] = Field(
        default_factory=dict, description="Context variables available to the graph during execution."
    )
    graph_id: str = Field(..., description="Identifier of the graph this assistant executes.")
    user_id: str = Field(..., description="Identifier of the user who owns this assistant.")
    version: int = Field(..., description="The version of the assistant.")
    metadata: dict[str, Any] = Field(
        default_factory=dict, alias="metadata_dict", description="Arbitrary metadata for searching and filtering."
    )
    created_at: datetime = Field(..., description="Timestamp when the assistant was created.")
    updated_at: datetime = Field(..., description="Timestamp when the assistant was last updated.")

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class AssistantUpdate(BaseModel):
    """Request model for creating assistants"""

    name: str | None = Field(None, description="The name of the assistant (auto-generated if not provided)")
    description: str | None = Field(None, description="The description of the assistant. Defaults to null.")
    config: dict[str, Any] | None = Field(default_factory=dict, description="Configuration to use for the graph.")
    # Must default to None: the service falls back to `request.graph_id or
    # assistant.graph_id`, so any truthy default would let a PATCH that omits
    # the field silently rewrite graph_id.
    graph_id: str | None = Field(None, description="The ID of the graph")
    context: dict[str, Any] | None = Field(
        default_factory=dict,
        description="The context to use for the graph. Useful when graph is configurable.",
    )
    metadata: dict[str, Any] | None = Field(
        default_factory=dict, description="Metadata to use for searching and filtering assistants."
    )


class AssistantList(BaseModel):
    """Response model for listing assistants"""

    assistants: list[Assistant]
    total: int


class AssistantSearchRequest(TimeFilters):
    """Request model for assistant search"""

    assistant_id: IdSet = id_scope("assistant_id", "Restrict to these assistants.")
    name: str | None = Field(None, description="Substring match on name.")
    description: str | None = Field(None, description="Substring match on description.")
    graph_id: str | None = Field(None, description="Exact match on graph id.")
    limit: int = Field(20, le=1000, ge=1, description="Maximum rows per page.")
    offset: int = Field(0, ge=0, description="Rows to skip.")
    metadata: dict[str, Any] | None = Field(default_factory=dict, description="Metadata containment match.")
    sort_by: AssistantSortBy | None = Field(None, description="Sort field; defaults to created_at.")
    sort_order: SortOrder | None = Field(None, description="Sort direction; defaults to desc.")
    select: list[AssistantSelectField] | None = Field(
        None, description="Return only the listed fields; omit for the full entity."
    )


class AssistantVersionsRequest(BaseModel):
    """Request body for listing an assistant's versions."""

    metadata: dict[str, Any] | None = Field(None, description="Metadata containment match on the version.")
    limit: int = Field(10, ge=1, le=1000, description="Maximum rows per page.")
    offset: int = Field(0, ge=0, description="Rows to skip, counting from the newest version.")


class AgentSchemas(BaseModel):
    """Agent schema definitions for client integration.

    Every schema is nullable: a graph that does not annotate one of these
    yields null for it rather than failing the request.
    """

    graph_id: str = Field(..., description="Graph these schemas belong to.")
    input_schema: dict[str, Any] | None = Field(None, description="JSON Schema for graph input.")
    output_schema: dict[str, Any] | None = Field(None, description="JSON Schema for graph output.")
    state_schema: dict[str, Any] | None = Field(None, description="JSON Schema for graph state.")
    config_schema: dict[str, Any] | None = Field(None, description="JSON Schema for graph config.")
    context_schema: dict[str, Any] | None = Field(None, description="JSON Schema for graph context.")
