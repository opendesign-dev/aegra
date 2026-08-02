from datetime import datetime
from typing import Annotated, Any, Self

from pydantic import AliasChoices, BaseModel, BeforeValidator, ConfigDict, Field, model_validator
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import ColumnElement

# Nullable on purpose: InstrumentedAttribute is covariant, so these also accept NOT NULL columns.
type TextColumn = InstrumentedAttribute[str | None]
type TimeColumn = InstrumentedAttribute[datetime | None]
type JsonColumn = InstrumentedAttribute[dict[str, Any]]

IdSet = Annotated[list[str] | None, BeforeValidator(lambda v: [v] if isinstance(v, str) else v)]


def id_scope(singular: str, description: str) -> Any:
    return Field(None, validation_alias=AliasChoices(singular, f"{singular}s"), description=description)


class TimeRange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    gte: datetime | None = Field(None, description="At or after this instant.")
    gt: datetime | None = Field(None, description="Strictly after this instant.")
    lte: datetime | None = Field(None, description="At or before this instant.")
    lt: datetime | None = Field(None, description="Strictly before this instant.")

    @model_validator(mode="after")
    def require_a_bound(self) -> Self:
        if not self.model_fields_set:
            raise ValueError("a time range needs at least one of 'gte', 'gt', 'lte', 'lt'")
        if self.gte is not None and self.gt is not None:
            raise ValueError("'gte' and 'gt' are mutually exclusive")
        if self.lte is not None and self.lt is not None:
            raise ValueError("'lte' and 'lt' are mutually exclusive")
        return self

    def predicates(self, column: TimeColumn) -> list[ColumnElement[bool]]:
        bounds = (
            (self.gte, column.__ge__),
            (self.gt, column.__gt__),
            (self.lte, column.__le__),
            (self.lt, column.__lt__),
        )
        return [op(value) for value, op in bounds if value is not None]


class TimeFilters(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    created_at: TimeRange | None = Field(None, description="Bound on creation time, e.g. `{'gte': ..., 'lt': ...}`.")
    updated_at: TimeRange | None = Field(None, description="Bound on last-modified time.")

    def time_predicates(self, created: TimeColumn, updated: TimeColumn) -> list[ColumnElement[bool]]:
        ranges = ((self.created_at, created), (self.updated_at, updated))
        return [p for bound, column in ranges if bound for p in bound.predicates(column)]


def in_scope(column: TextColumn, ids: list[str] | None) -> list[ColumnElement[bool]]:
    return [] if ids is None else [column.in_(ids)]


def metadata_scope(column: JsonColumn, metadata: dict[str, Any] | None) -> list[ColumnElement[bool]]:
    return [column.op("@>")(metadata)] if metadata else []
