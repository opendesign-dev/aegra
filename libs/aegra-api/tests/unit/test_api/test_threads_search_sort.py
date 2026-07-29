"""Unit tests for _resolve_sort in /threads/search."""

from aegra_api.api.threads import _resolve_sort
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.models import ThreadSearchRequest


def _col_name(column: object) -> str:
    """Return the ORM column's logical name for assertion comparisons."""
    return getattr(column, "key", None) or getattr(column, "name", "")


class TestResolveSortDefault:
    """With no sort requested, newest-first on created_at."""

    def test_defaults_to_created_at_desc(self) -> None:
        column, asc = _resolve_sort(ThreadSearchRequest())
        assert _col_name(column) == "created_at"
        assert asc is False

    def test_sort_order_alone_does_not_change_the_default(self) -> None:
        """sort_order only applies alongside sort_by."""
        column, asc = _resolve_sort(ThreadSearchRequest(sort_order="asc"))
        assert _col_name(column) == "created_at"
        assert asc is False


class TestResolveSortSdkShape:
    """_resolve_sort honours the SDK-style sort_by / sort_order fields.

    ``sort_by`` is Literal-validated by Pydantic, so unknown values 422 at the
    request boundary and never reach _resolve_sort — the integration suite
    asserts that path (test_search_invalid_sort_by_returns_422).
    """

    def test_sdk_shape_asc(self) -> None:
        column, asc = _resolve_sort(ThreadSearchRequest(sort_by="updated_at", sort_order="asc"))
        assert _col_name(column) == "updated_at"
        assert asc is True

    def test_sdk_shape_desc(self) -> None:
        column, asc = _resolve_sort(ThreadSearchRequest(sort_by="thread_id", sort_order="desc"))
        assert _col_name(column) == "thread_id"
        assert asc is False

    def test_sdk_sort_by_defaults_to_desc(self) -> None:
        """sort_by without sort_order defaults to descending."""
        column, asc = _resolve_sort(ThreadSearchRequest(sort_by="updated_at"))
        assert _col_name(column) == "updated_at"
        assert asc is False

    def test_state_updated_at_maps_to_updated_at(self) -> None:
        """The SDK's state_updated_at has no column of its own; values materialize
        on finalize, which touches updated_at in the same transaction."""
        column, asc = _resolve_sort(ThreadSearchRequest(sort_by="state_updated_at", sort_order="asc"))
        assert _col_name(column) == "updated_at"
        assert asc is True

    def test_returns_real_orm_column(self) -> None:
        """The returned column is the actual ORM attribute, not a string proxy."""
        column, _ = _resolve_sort(ThreadSearchRequest(sort_by="updated_at", sort_order="asc"))
        assert column is ThreadORM.updated_at
