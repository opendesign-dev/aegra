"""Compile `@auth.on` handler filter dicts into SQLAlchemy JSONB predicates.

A handler may return a filter dict (`Auth.types.FilterType`) to scope a query.
This compiles that dict into a Postgres JSONB predicate over a metadata column.
Filters match against the resource's metadata.

Supported shapes (all matched against the metadata column):
    {"owner": "u1"}                       -> metadata @> {"owner": "u1"}        (eq)
    {"owner": {"$eq": "u1"}}              -> same, explicit
    {"tags": {"$contains": "x"}}          -> metadata->'tags' @> ["x"]          (array contains)
    {"tags": {"$contains": ["x", "y"]}}   -> metadata->'tags' @> ["x", "y"]
    {"$or": [f1, f2, ...]}                -> OR of branches (>= 2, depth <= 2)
    {"$and": [f1, f2, ...]}              -> AND of branches (>= 2)
    multiple top-level keys               -> AND (logical conjunction)

Invalid operators or over-nested filters raise HTTPException(500): a malformed
handler filter is a programmer error, surfaced loudly rather than silently
matching the wrong rows.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import HTTPException
from sqlalchemy import and_, or_
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import ColumnElement

if TYPE_CHECKING:
    from aegra_api.models.auth import User

_MAX_FILTER_DEPTH = 2

# Permissions that lift per-user scoping on a resource's search/count endpoints.
# Free-form elsewhere, but these exact strings are interpreted by the server, so
# they live here rather than being spelled out at each call site.
RUNS_SEARCH_ALL = "runs:search:all"
THREADS_SEARCH_ALL = "threads:search:all"
ASSISTANTS_SEARCH_ALL = "assistants:search:all"
CRONS_SEARCH_ALL = "crons:search:all"

# Owner of deployment-provided rows (the assistants derived from aegra.json), which
# every caller can see alongside its own.
SYSTEM_IDENTITY = "system"


def build_visibility_filters(
    column: InstrumentedAttribute[Any],
    user: User,
    permission: str,
    *,
    shared_identity: str | None = None,
) -> list[ColumnElement[bool]]:
    """Predicates limiting a search to the identities this caller may see.

    Scope is decided by *permission* alone — there is deliberately no request field
    that names other owners, so an unpermitted client cannot even express the
    query. Returns an empty list when the caller may search every identity.

    ``shared_identity`` keeps deployment-owned rows (``user_id = "system"``)
    visible alongside the caller's own.
    """
    if permission in user.permissions:
        return []
    if shared_identity is not None:
        return [or_(column == user.identity, column == shared_identity)]
    return [column == user.identity]


def build_metadata_filter(
    column: InstrumentedAttribute[Any],
    filters: dict[str, Any] | None,
) -> ColumnElement[bool] | None:
    """Compile a handler filter dict into a JSONB predicate over `column`.

    Returns None when there is nothing to filter (so callers can skip adding a
    WHERE clause). `column` is the JSONB metadata column (e.g.
    `AssistantORM.metadata_dict`).

    Aegra compatibility: handlers historically return a nested
    ``{"metadata": {...}}`` envelope to mean "filter on these metadata fields"
    (see examples/jwt_mock_auth_example.py). That envelope is unwrapped and its
    fields merged with any sibling flat constraints before compiling, so both
    ``{"metadata": {"team_id": "t"}}`` and ``{"team_id": "t"}`` filter the same
    way.
    """
    if not filters:
        return None
    filters = _unwrap_metadata_envelope(filters)
    clauses = _compile(column, filters, depth=0)
    if not clauses:
        return None
    return and_(*clauses) if len(clauses) > 1 else clauses[0]


def _unwrap_metadata_envelope(filters: dict[str, Any]) -> dict[str, Any]:
    """Flatten a top-level ``metadata`` envelope into sibling constraints."""
    envelope = filters.get("metadata")
    if not isinstance(envelope, dict):
        return filters
    rest = {k: v for k, v in filters.items() if k != "metadata"}
    return {**envelope, **rest}


def _compile(
    column: InstrumentedAttribute[Any],
    filters: dict[str, Any],
    *,
    depth: int,
) -> list[ColumnElement[bool]]:
    clauses: list[ColumnElement[bool]] = []
    for key, value in filters.items():
        if key in ("$or", "$and"):
            clauses.append(_compile_bool_op(column, key, value, depth=depth))
        else:
            clauses.append(_compile_field(column, key, value))
    return clauses


def _compile_bool_op(
    column: InstrumentedAttribute[Any],
    op: str,
    branches: Any,
    *,
    depth: int,
) -> ColumnElement[bool]:
    if depth >= _MAX_FILTER_DEPTH:
        raise HTTPException(
            500,
            f"Auth handler filter nests deeper than {_MAX_FILTER_DEPTH} levels. "
            "Check the filter returned by your auth handler.",
        )
    if not isinstance(branches, list) or len(branches) < 2:
        raise HTTPException(
            500,
            f"Auth handler filter '{op}' must be a list of at least 2 filter objects. "
            "Check the filter returned by your auth handler.",
        )
    # An empty branch would compile to and_() == TRUE, making OR collapse to TRUE
    # and silently bypass the constraint. Reject it.
    for branch in branches:
        if not isinstance(branch, dict) or not branch:
            raise HTTPException(
                500,
                f"Auth handler filter '{op}' contains an empty or non-dict branch. "
                "Check the filter returned by your auth handler.",
            )
    compiled = [and_(*_compile(column, b, depth=depth + 1)) for b in branches]
    return or_(*compiled) if op == "$or" else and_(*compiled)


def _compile_field(
    column: InstrumentedAttribute[Any],
    key: str,
    value: Any,
) -> ColumnElement[bool]:
    if not isinstance(value, dict):
        # Bare value -> equality via JSONB containment: metadata @> {key: value}.
        return column.op("@>")({key: value})

    if len(value) != 1:
        raise HTTPException(
            500,
            "Auth handler filter value must be a dict with exactly one operator key "
            "($eq or $contains). Check the filter returned by your auth handler.",
        )
    operator, operand = next(iter(value.items()))
    if operator == "$eq":
        return column.op("@>")({key: operand})
    if operator == "$contains":
        items = operand if isinstance(operand, list) else [operand]
        # metadata->'key' @> '[items...]' : the array at `key` contains every item.
        return column[key].op("@>")(items)
    raise HTTPException(
        500,
        f"Auth handler filter operator '{operator}' is not supported. "
        "Use $eq or $contains. Check the filter returned by your auth handler.",
    )
