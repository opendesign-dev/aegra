"""Owner scoping for every search endpoint.

Scope is decided by a `<resource>:search:all` permission alone. These tests pin
both halves of that contract: the predicate disappears when the permission is
present, and no request field exists through which a client could ask for other
owners' rows.
"""

from typing import Any

import pytest
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import ColumnElement

from aegra_api.core.auth_filters import (
    ASSISTANTS_SEARCH_ALL,
    CRONS_SEARCH_ALL,
    RUNS_SEARCH_ALL,
    SYSTEM_IDENTITY,
    THREADS_SEARCH_ALL,
    build_visibility_filters,
)
from aegra_api.core.orm import Assistant as AssistantORM
from aegra_api.core.orm import Cron as CronORM
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.models import (
    AssistantSearchRequest,
    RunSearchRequest,
    ThreadSearchRequest,
    User,
)
from aegra_api.models.crons import CronCountRequest, CronSearchRequest


def _user(*, permissions: list[str] | None = None) -> User:
    return User(identity="user-1", permissions=permissions or [])


def _sql(predicates: list[ColumnElement[bool]]) -> str:
    return str(select(RunORM.run_id).where(*predicates).compile(dialect=postgresql.dialect()))


# (label, column, permission)
RESOURCES: list[tuple[str, InstrumentedAttribute[Any], str]] = [
    ("runs", RunORM.user_id, RUNS_SEARCH_ALL),
    ("threads", ThreadORM.user_id, THREADS_SEARCH_ALL),
    ("assistants", AssistantORM.user_id, ASSISTANTS_SEARCH_ALL),
    ("crons", CronORM.user_id, CRONS_SEARCH_ALL),
]


class TestBuildVisibilityFilters:
    @pytest.mark.parametrize("label, column, permission", RESOURCES, ids=[r[0] for r in RESOURCES])
    def test_scoped_to_caller_without_permission(
        self, label: str, column: InstrumentedAttribute[Any], permission: str
    ) -> None:
        predicates = build_visibility_filters(column, _user(), permission)
        assert len(predicates) == 1
        assert "user_id = " in _sql(predicates)

    @pytest.mark.parametrize("label, column, permission", RESOURCES, ids=[r[0] for r in RESOURCES])
    def test_permission_drops_ownership_predicate(
        self, label: str, column: InstrumentedAttribute[Any], permission: str
    ) -> None:
        assert build_visibility_filters(column, _user(permissions=[permission]), permission) == []

    @pytest.mark.parametrize("label, column, permission", RESOURCES, ids=[r[0] for r in RESOURCES])
    def test_other_resource_permission_does_not_widen_scope(
        self, label: str, column: InstrumentedAttribute[Any], permission: str
    ) -> None:
        """Each resource has its own gate; holding one must not open another."""
        others = [p for _l, _c, p in RESOURCES if p != permission]
        assert len(build_visibility_filters(column, _user(permissions=others), permission)) == 1

    def test_shared_identity_stays_visible(self) -> None:
        """Deployment-owned assistants are readable by everyone, not just their owner."""
        predicates = build_visibility_filters(
            AssistantORM.user_id, _user(), ASSISTANTS_SEARCH_ALL, shared_identity=SYSTEM_IDENTITY
        )
        # Still one clause, so a later filter cannot accidentally AND away the OR.
        assert len(predicates) == 1
        sql = _sql(predicates)
        assert sql.count("user_id = ") == 2
        assert " OR " in sql

    def test_shared_identity_irrelevant_once_permitted(self) -> None:
        assert (
            build_visibility_filters(
                AssistantORM.user_id,
                _user(permissions=[ASSISTANTS_SEARCH_ALL]),
                ASSISTANTS_SEARCH_ALL,
                shared_identity=SYSTEM_IDENTITY,
            )
            == []
        )


SEARCH_MODELS: list[tuple[str, type[BaseModel]]] = [
    ("RunSearchRequest", RunSearchRequest),
    ("ThreadSearchRequest", ThreadSearchRequest),
    ("AssistantSearchRequest", AssistantSearchRequest),
    ("CronSearchRequest", CronSearchRequest),
    ("CronCountRequest", CronCountRequest),
]


@pytest.mark.parametrize("name, model", SEARCH_MODELS, ids=[m[0] for m in SEARCH_MODELS])
def test_no_owner_field_on_the_request(name: str, model: type[BaseModel]) -> None:
    """Regression: a request field naming other owners would let a client express a
    scope the permission is supposed to be the only gate for."""
    fields = set(model.model_fields)
    assert not fields & {"user_ids", "user_id", "owner", "owners"}, f"{name} exposes an owner field"
