"""WHERE-clause construction for /runs/search and /runs/count.

Both endpoints must produce byte-identical predicates — a count that filters
differently from the search it paginates is a silent correctness bug — so the
clause is built once here.
"""

from collections.abc import Callable
from typing import Any

from aegra_api.core.auth_filters import build_metadata_filter
from aegra_api.core.orm import Run as RunORM
from aegra_api.models import RunSearchRequest, User
from aegra_api.services.langgraph_service import get_langgraph_service
from aegra_api.utils.assistants import resolve_assistant_id

# Grants /runs/search and /runs/count the whole deployment instead of the caller's
# own runs. Empty by default under noop auth, so the widened scope stays opt-in.
SEARCH_ALL_USERS_PERMISSION = "runs:search:all"


def _resolve_search_assistant_id(assistant_id: str) -> str:
    """Map a graph id in the filter to its canonical assistant id.

    Mirrors run creation so a caller can search by whichever identifier they
    used to start the run.
    """
    return resolve_assistant_id(assistant_id, get_langgraph_service().list_graphs())


def _match(column: Any, value: str | list[str], transform: Callable[[str], str] | None = None) -> Any:
    """Equality for a scalar, IN for a list — one filter field, either shape."""
    if isinstance(value, list):
        return column.in_([transform(v) for v in value] if transform else value)
    return column == (transform(value) if transform else value)


def build_run_filters(
    request: RunSearchRequest,
    user: User,
    auth_filters: dict[str, Any] | None,
    *,
    cross_user: bool,
) -> list[Any]:
    """Shared WHERE predicates for /runs/search and /runs/count.

    ``cross_user`` comes from the caller's permissions, never from the request —
    there is no field a client can send to widen its own scope.
    """
    where: list[Any] = []
    if not cross_user:
        where.append(RunORM.user_id == user.identity)
    if request.assistant_id is not None:
        where.append(_match(RunORM.assistant_id, request.assistant_id, _resolve_search_assistant_id))
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
