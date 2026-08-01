"""Unit tests for permission-gated read scoping."""

from sqlalchemy import select
from sqlalchemy.sql.elements import ColumnElement

from aegra_api.core.orm import Assistant as AssistantORM
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.scoping import read_all_permission, read_scope
from aegra_api.models.auth import User


def _sql(predicate: ColumnElement[bool]) -> str:
    return str(select(RunORM.run_id).where(predicate).compile(compile_kwargs={"literal_binds": True}))


def _user(*permissions: str) -> User:
    return User(identity="alice", permissions=list(permissions))


class TestReadAllPermission:
    def test_names_are_per_resource(self) -> None:
        assert read_all_permission("runs") == "runs:read_all"
        assert read_all_permission("threads") == "threads:read_all"


class TestReadScope:
    def test_defaults_to_owner_only(self) -> None:
        sql = _sql(read_scope(RunORM.user_id, _user(), resource="runs"))
        assert "user_id = 'alice'" in sql

    def test_unrelated_permission_does_not_widen(self) -> None:
        sql = _sql(read_scope(RunORM.user_id, _user("admin", "threads:read_all"), resource="runs"))
        assert "user_id = 'alice'" in sql

    def test_matching_permission_widens(self) -> None:
        sql = _sql(read_scope(RunORM.user_id, _user("runs:read_all"), resource="runs"))
        assert "alice" not in sql

    def test_include_system_admits_seeded_assistants(self) -> None:
        sql = _sql(read_scope(AssistantORM.user_id, _user(), resource="assistants", include_system=True))
        assert "'alice'" in sql
        assert "'system'" in sql

    def test_widened_assistants_need_no_system_branch(self) -> None:
        sql = _sql(
            read_scope(AssistantORM.user_id, _user("assistants:read_all"), resource="assistants", include_system=True)
        )
        assert "'alice'" not in sql
