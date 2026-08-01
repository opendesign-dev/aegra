"""Every collection query honours ``<resource>:read_all``; nothing else does.

Asserts against the compiled SQL rather than the helper, so a service that
forgets to route through ``read_scope`` is caught here.
"""

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

from aegra_api.models.assistants import AssistantSearchRequest
from aegra_api.models.auth import User
from aegra_api.models.crons import CronCountRequest, CronSearchRequest
from aegra_api.models.threads import ThreadSearchRequest
from aegra_api.services.assistant_service import AssistantService
from aegra_api.services.cron_service import CronService
from aegra_api.services.thread_service import ThreadService


def _user(*permissions: str) -> User:
    return User(identity="alice", permissions=list(permissions))


def _session() -> AsyncMock:
    session = AsyncMock()
    result = Mock()
    result.all.return_value = []
    session.scalars.return_value = result
    session.scalar.return_value = 0
    return session


def _last_sql(session: AsyncMock, method: str) -> str:
    stmt = getattr(session, method).await_args.args[0]
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


def _graphs() -> Mock:
    service = Mock()
    service.list_graphs.return_value = {"test-graph": {}}
    return service


async def _run(resource: str, permissions: tuple[str, ...]) -> tuple[str, str]:
    """Return (search SQL, count SQL) for *resource* under *permissions*."""
    session = _session()
    user = _user(*permissions)

    if resource == "threads":
        service: Any = ThreadService(session, user)
        await service.search(ThreadSearchRequest())
        await service.count(ThreadSearchRequest())
    elif resource == "assistants":
        service = AssistantService(session, user, _graphs())
        await service.search_assistants(AssistantSearchRequest())
        await service.count_assistants(AssistantSearchRequest())
    else:
        service = CronService(session, _graphs())
        await service.search_crons(CronSearchRequest(), user)
        await service.count_crons(CronCountRequest(), user)

    return _last_sql(session, "scalars"), _last_sql(session, "scalar")


@pytest.mark.parametrize("resource", ["threads", "assistants", "crons"])
class TestCollectionQueries:
    async def test_owner_bound_by_default(self, resource: str) -> None:
        search_sql, count_sql = await _run(resource, ())
        assert "'alice'" in search_sql
        assert "'alice'" in count_sql

    async def test_read_all_widens_both(self, resource: str) -> None:
        search_sql, count_sql = await _run(resource, (f"{resource}:read_all",))
        assert "'alice'" not in search_sql
        assert "'alice'" not in count_sql

    async def test_another_resources_permission_does_not_widen(self, resource: str) -> None:
        search_sql, count_sql = await _run(resource, ("runs:read_all", "admin"))
        assert "'alice'" in search_sql
        assert "'alice'" in count_sql


class TestFetchByIdStaysOwnerOnly:
    """read_all covers collections only; one row still belongs to its owner."""

    async def test_thread_fetch_ignores_read_all(self) -> None:
        session = _session()
        session.scalar.return_value = None
        service = ThreadService(session, _user("threads:read_all"))

        with pytest.raises(HTTPException, match="not found"):
            await service.get("t-1")

        assert "'alice'" in _last_sql(session, "scalar")
