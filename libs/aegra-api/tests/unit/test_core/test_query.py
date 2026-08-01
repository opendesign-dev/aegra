"""Unit tests for the shared list-query building blocks.

All four resources go through core.query, so sort direction, tiebreak and the
pagination cursor are asserted once here; per-resource tests only cover their
own filters.
"""

from typing import Any

import pytest
from fastapi import Response
from pydantic import BaseModel, ConfigDict, Field

from aegra_api.core.orm import Assistant as AssistantORM
from aegra_api.core.orm import Cron as CronORM
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.query import (
    NEXT_PAGE_HEADER,
    build_order_by,
    extract_paths,
    paginate,
    project,
    set_next_page,
)


def _clause_names(clauses: list[Any]) -> list[str]:
    return [c.element.key for c in clauses]


class TestBuildOrderBy:
    def test_defaults_to_descending(self) -> None:
        clauses = build_order_by(ThreadORM.created_at, sort_order=None, tiebreak=ThreadORM.thread_id)
        assert _clause_names(clauses) == ["created_at", "thread_id"]
        assert clauses[0].modifier.__name__ == "desc_op"

    def test_sort_order_asc_produces_ascending(self) -> None:
        clauses = build_order_by(ThreadORM.updated_at, sort_order="asc", tiebreak=ThreadORM.thread_id)
        assert _clause_names(clauses)[0] == "updated_at"
        assert clauses[0].modifier.__name__ == "asc_op"

    def test_tiebreak_always_appended_ascending(self) -> None:
        """Offset pagination only stays stable when the sort has a unique tail."""
        clauses = build_order_by(CronORM.next_run_date, sort_order="asc", tiebreak=CronORM.cron_id)
        assert _clause_names(clauses) == ["next_run_date", "cron_id"]
        assert clauses[1].modifier.__name__ == "asc_op"

    def test_orders_by_bound_column_not_raw_string(self) -> None:
        """The sort target must be a bound Column; string building invites injection."""
        from sqlalchemy import Column

        clauses = build_order_by(AssistantORM.name, sort_order="asc", tiebreak=AssistantORM.assistant_id)
        assert isinstance(clauses[0].element, Column)
        assert clauses[0].element.key == "name"
        assert clauses[0].element.table.name == "assistant"

    @pytest.mark.parametrize("resource", [AssistantORM, ThreadORM, CronORM])
    def test_direction_is_consistent_across_resources(self, resource: type[Any]) -> None:
        """Defaults used to diverge (crons sorted asc); all three must be desc now."""
        tiebreak = {"assistant": "assistant_id", "thread": "thread_id", "crons": "cron_id"}[resource.__tablename__]
        clauses = build_order_by(resource.created_at, sort_order=None, tiebreak=getattr(resource, tiebreak))
        assert clauses[0].modifier.__name__ == "desc_op"


class TestPaginate:
    def test_applies_limit_and_offset(self) -> None:
        from sqlalchemy import select

        stmt = paginate(select(ThreadORM), limit=50, offset=100)
        assert stmt._limit == 50
        assert stmt._offset == 100


class _Row(BaseModel):
    a: str
    b: int
    c: str | None = None


class _Aliased(BaseModel):
    """Aliased model, used to pin down the serialized key names."""

    metadata_value: dict[str, Any] = Field(default_factory=dict, alias="metadata_dict")

    model_config = ConfigDict(populate_by_name=True)


class TestProject:
    def test_returns_all_fields_when_no_selection(self) -> None:
        rows = [_Row(a="x", b=1)]
        assert project(rows, None) == [{"a": "x", "b": 1, "c": None}]

    def test_projects_only_requested_fields(self) -> None:
        rows = [_Row(a="x", b=1, c="z")]
        assert project(rows, ["a", "c"]) == [{"a": "x", "c": "z"}]

    def test_empty_field_list_is_treated_as_unset(self) -> None:
        """Projecting an empty object is useless, so an empty select means unset."""
        rows = [_Row(a="x", b=1)]
        assert project(rows, []) == [{"a": "x", "b": 1, "c": None}]

    def test_uses_field_names_not_aliases(self) -> None:
        """Regression: the SDK reads field names; aliases would hide metadata."""
        rows = [_Aliased(metadata_dict={"env": "prod"})]
        assert project(rows, None) == [{"metadata_value": {"env": "prod"}}]

    def test_selection_and_full_output_agree_on_key_names(self) -> None:
        rows = [_Aliased(metadata_dict={"env": "prod"})]
        assert set(project(rows, ["metadata_value"])[0]) <= set(project(rows, None)[0])


class TestExtractPaths:
    def test_resolves_nested_key(self) -> None:
        row = {"values": {"messages": [{"content": "a"}, {"content": "b"}]}}
        assert extract_paths(row, {"first": "values.messages[0].content"}) == {"first": "a"}

    def test_supports_negative_index(self) -> None:
        row = {"values": {"messages": [{"content": "a"}, {"content": "b"}]}}
        assert extract_paths(row, {"last": "values.messages[-1].content"}) == {"last": "b"}

    def test_missing_path_yields_none_instead_of_raising(self) -> None:
        """One bad alias must not fail an otherwise valid search."""
        row = {"values": {"messages": []}}
        assert extract_paths(row, {"gone": "values.nope.deeper", "oob": "values.messages[3]"}) == {
            "gone": None,
            "oob": None,
        }

    def test_index_into_non_list_yields_none(self) -> None:
        assert extract_paths({"values": {"a": 1}}, {"x": "values.a[0]"}) == {"x": None}


class TestSetNextPage:
    def test_sets_cursor_when_page_is_full(self) -> None:
        response = Response()
        set_next_page(response, offset=0, limit=20, returned=20)
        assert response.headers[NEXT_PAGE_HEADER] == "20"

    def test_advances_cursor_by_page_size(self) -> None:
        response = Response()
        set_next_page(response, offset=40, limit=20, returned=20)
        assert response.headers[NEXT_PAGE_HEADER] == "60"

    def test_omits_header_on_last_page(self) -> None:
        """A short page means no next page, which the SDK reads as next=None."""
        response = Response()
        set_next_page(response, offset=0, limit=20, returned=7)
        assert NEXT_PAGE_HEADER not in response.headers
