"""Store-related Pydantic models for Agent Protocol"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class StorePutRequest(BaseModel):
    """Request model for storing items"""

    namespace: list[str] = Field(..., description="Storage namespace")
    key: str = Field(..., description="Item key")
    value: dict[str, Any] = Field(..., description="Item value (must be a JSON object)")
    index: Literal[False] | list[str] | None = Field(
        None,
        description=(
            "Fields to embed for semantic search, as dotted paths into `value`. "
            "`false` skips indexing this item; omit to use the store's configured default."
        ),
    )
    ttl: float | None = Field(None, gt=0, description="Lifetime in minutes; omit to keep the item indefinitely.")

    @field_validator("value", mode="before")
    @classmethod
    def validate_value_is_dict(cls, v: Any) -> dict[str, Any]:
        """Validate that value is a dictionary.

        LangGraph store requires values to be dictionaries for proper
        serialization and search functionality.
        """
        if not isinstance(v, dict):
            raise ValueError(f"Value must be a dictionary (JSON object), got {type(v).__name__}")
        return v


class StoreGetResponse(BaseModel):
    """Response model for getting items"""

    key: str = Field(..., description="The item's key within its namespace.")
    value: Any = Field(..., description="The stored value.")
    namespace: list[str] = Field(..., description="The namespace path where this item is stored.")
    created_at: datetime | None = Field(None, description="When the item was first written.")
    updated_at: datetime | None = Field(None, description="When the item was last written.")


class StoreSearchRequest(BaseModel):
    """Request model for searching store items"""

    namespace_prefix: list[str] = Field(..., description="Namespace prefix to search")
    filter: dict[str, Any] | None = Field(None, description="Optional dictionary of key-value pairs to filter results.")
    query: str | None = Field(None, description="Search query")
    limit: int = Field(20, le=100, ge=1, description="Maximum results")
    offset: int = Field(0, ge=0, description="Results offset")
    refresh_ttl: bool | None = Field(
        None, description="Extend the TTL of matched items; omit to use the store's configured default."
    )


class StoreItem(BaseModel):
    """Store item model"""

    key: str = Field(..., description="The item's key within its namespace.")
    value: Any = Field(..., description="The stored value.")
    namespace: list[str] = Field(..., description="The namespace path where this item is stored.")
    created_at: datetime | None = Field(None, description="When the item was first written.")
    updated_at: datetime | None = Field(None, description="When the item was last written.")
    score: float | None = Field(None, description="Semantic relevance; null unless `query` was given.")


class StoreSearchResponse(BaseModel):
    """Response model for store search"""

    items: list[StoreItem]
    total: int
    limit: int
    offset: int


class StoreDeleteRequest(BaseModel):
    """Request body for deleting store items (SDK-compatible)."""

    namespace: list[str] = Field(..., description="Namespace path of the item to delete.")
    key: str = Field(..., description="Key of the item to delete.")


class StoreListNamespacesRequest(BaseModel):
    """Request model for listing store namespaces"""

    prefix: list[str] | None = Field(None, description="Filter by namespace prefix")
    suffix: list[str] | None = Field(None, description="Filter by namespace suffix")
    max_depth: int | None = Field(None, le=100, ge=1, description="Maximum namespace depth to return")
    limit: int = Field(100, le=1000, ge=1, description="Maximum results")
    offset: int = Field(0, ge=0, description="Results offset")


class StoreListNamespacesResponse(BaseModel):
    """Response model for listing store namespaces"""

    namespaces: list[list[str]]
