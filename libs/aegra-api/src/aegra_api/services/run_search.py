"""WHERE-clause construction for /runs/search and /runs/count.

Both endpoints must produce byte-identical predicates — a count that filters
differently from the search it paginates is a silent correctness bug — so the
clause is built once here.
"""

from collections.abc import Callable
from typing import Any

from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import ColumnElement

from aegra_api.core.auth_filters import RUNS_SEARCH_ALL, build_metadata_filter, build_visibility_filters
from aegra_api.core.orm import Run as RunORM
from aegra_api.models import RunSearchRequest, User
from aegra_api.services.langgraph_service import get_langgraph_service
from aegra_api.utils.assistants import resolve_assistant_id


def _match(
    column: InstrumentedAttribute[Any], value: str | list[str], transform: Callable[[str], str] | None = None
) -> ColumnElement[bool]:
    """Equality for a scalar, IN for a list — one filter field, either shape."""
    if isinstance(value, list):
        return column.in_([transform(v) for v in value] if transform else value)
    return column == (transform(value) if transform else value)


def build_run_filters(
    request: RunSearchRequest,
    user: User,
    auth_filters: dict[str, Any] | None,
) -> list[ColumnElement[bool]]:
    """Shared WHERE predicates for /runs/search and /runs/count.

    Scope comes from ``runs:search:all`` on the caller, never from the request —
    there is no field a client can send to widen its own scope.
    """
    where: list[ColumnElement[bool]] = build_visibility_filters(RunORM.user_id, user, RUNS_SEARCH_ALL)
    if request.assistant_id is not None:
        # Registry read hoisted out of _match: it rebuilds the whole graph table and
        # is invariant across the list. Mirrors run creation, so a caller can search
        # by whichever identifier it used to start the run.
        graphs = get_langgraph_service().list_graphs()
        where.append(_match(RunORM.assistant_id, request.assistant_id, lambda gid: resolve_assistant_id(gid, graphs)))
    if request.thread_id is not None:
        where.append(_match(RunORM.thread_id, request.thread_id))
    if request.ids is not None:
        where.append(RunORM.run_id.in_(request.ids))
    if request.status is not None:
        where.append(_match(RunORM.status, request.status))
    if request.metadata:
        where.append(RunORM.metadata_dict.op("@>")(request.metadata))
    if request.created_after is not None:
        where.append(RunORM.created_at >= request.created_after)
    if request.created_before is not None:
        where.append(RunORM.created_at <= request.created_before)
    auth_filter = build_metadata_filter(RunORM.metadata_dict, auth_filters)
    if auth_filter is not None:
        where.append(auth_filter)
    return where
