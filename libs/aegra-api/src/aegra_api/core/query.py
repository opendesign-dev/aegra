"""Shared list-query building blocks: ordering, pagination, field projection.

The four searchable resources (assistants, threads, runs, crons) share these so
sort direction and pagination semantics stay identical across endpoints. Field
validity is enforced by each request model's Literal; this module only assembles.
"""

import re
from collections.abc import Collection, Mapping, Sequence
from typing import Any, Protocol

from fastapi import Response
from pydantic import BaseModel
from sqlalchemy import Select
from sqlalchemy.sql.elements import UnaryExpression

# The SDK omits sort_order to mean descending.
_DESC = "desc"

# Cursor header the SDK reads to populate a search response's ``next``.
NEXT_PAGE_HEADER = "X-Pagination-Next"

# One ``extract`` path segment: either a mapping key or a bracketed list index.
_PATH_SEGMENT = re.compile(r"([^.\[\]]+)|\[(-?\d+)\]")


def build_order_by(column: Any, *, sort_order: str | None, tiebreak: Any) -> list[UnaryExpression[Any]]:
    """Order clause with a unique column appended as tiebreak.

    Duplicate values in the sort column make offset pagination drop or repeat
    rows across pages; a unique trailing column removes the ambiguity.
    """
    ascending = (sort_order or _DESC).lower() != _DESC
    return [column.asc() if ascending else column.desc(), tiebreak.asc()]


def paginate(stmt: Select[Any], *, limit: int, offset: int) -> Select[Any]:
    return stmt.offset(offset).limit(limit)


def project(models: Sequence[BaseModel], fields: Collection[str] | None) -> list[dict[str, Any]]:
    """Reduce each row to ``select``, or return every field when it is unset.

    Both paths go through ``model_dump(by_alias=False)`` so one endpoint cannot
    emit ``metadata`` in one response and ``metadata_dict`` in the next. Rows
    come back as dicts because a projected row lacks required fields and could
    not be reconstructed as a model.
    """
    wanted = set(fields) if fields else None
    return [model.model_dump(mode="json", include=wanted) for model in models]


def _resolve_path(root: Any, path: str) -> Any:
    node = root
    for key, index in _PATH_SEGMENT.findall(path):
        if key:
            if not isinstance(node, Mapping):
                return None
            node = node.get(key)
        else:
            if not isinstance(node, (list, tuple)):
                return None
            position = int(index)
            if not -len(node) <= position < len(node):
                return None
            node = node[position]
        if node is None:
            return None
    return node


def extract_paths(row: Mapping[str, Any], paths: Mapping[str, str]) -> dict[str, Any]:
    """Resolve alias-to-path pairs against a row, e.g. ``values.messages[-1]``.

    Unresolvable paths yield None instead of raising, so one bad alias cannot
    fail an otherwise valid search.
    """
    return {alias: _resolve_path(row, path) for alias, path in paths.items()}


def set_next_page(response: Response, *, offset: int, limit: int, returned: int) -> None:
    """Emit the next-page cursor on a full page; omit it on the last one.

    Offset doubles as the cursor, which saves a count query at the cost of one
    empty request when the total is an exact multiple of the page size.
    """
    if returned < limit:
        return
    response.headers[NEXT_PAGE_HEADER] = str(offset + limit)


class SearchPage(Protocol):
    """The pagination and projection surface every search request shares."""

    limit: int
    offset: int
    select: list[Any] | None


def page(response: Response, rows: Sequence[BaseModel], request: SearchPage) -> list[dict[str, Any]]:
    """Project a search page and advertise its next cursor.

    Kept as one call because the offset/limit behind the cursor must be the pair
    that produced these rows; splitting it across every search endpoint invites a
    mismatch that silently corrupts paging.
    """
    set_next_page(response, offset=request.offset, limit=request.limit, returned=len(rows))
    return project(rows, request.select)
