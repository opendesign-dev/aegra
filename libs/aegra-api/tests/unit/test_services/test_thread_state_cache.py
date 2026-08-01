"""Unit tests for the thread latest-state cache."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langgraph.types import Interrupt

from aegra_api.services import thread_state_cache


class _Task:
    def __init__(self, task_id: str, interrupts: tuple[Interrupt, ...]) -> None:
        self.id = task_id
        self.interrupts = interrupts


class _Snapshot:
    def __init__(self, values: dict[str, Any] | None, tasks: tuple[_Task, ...] = ()) -> None:
        self.values = values
        self.tasks = tasks


class TestExtract:
    def test_none_snapshot_yields_no_state(self) -> None:
        assert thread_state_cache.extract(None) == (None, {})

    def test_values_pass_through(self) -> None:
        values, interrupts = thread_state_cache.extract(_Snapshot({"messages": ["hi"]}))
        assert values == {"messages": ["hi"]}
        assert interrupts == {}

    def test_interrupts_group_by_task_and_encode_to_sdk_shape(self) -> None:
        """LangGraph's Interrupt is a dataclass; unencoded it fails dict validation."""
        snapshot = _Snapshot({}, (_Task("task-1", (Interrupt(value={"q": "ok?"}, id="i1"),)),))

        _values, interrupts = thread_state_cache.extract(snapshot)

        assert interrupts == {"task-1": [{"value": {"q": "ok?"}, "id": "i1"}]}

    def test_tasks_without_interrupts_are_skipped(self) -> None:
        snapshot = _Snapshot({}, (_Task("task-1", ()),))
        assert thread_state_cache.extract(snapshot)[1] == {}


class TestMaterialize:
    @pytest.mark.asyncio
    async def test_upsert_carries_a_fingerprint(self) -> None:
        """The digest is what lets an unchanged state skip the write."""
        session = AsyncMock()

        await thread_state_cache.materialize(session, "t-1", values={"a": 1}, interrupts={})

        session.execute.assert_awaited_once()
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_equal_states_produce_equal_fingerprints(self) -> None:
        """Key order must not change the digest, or every write would look new."""
        session = AsyncMock()
        statements = []
        session.execute.side_effect = lambda stmt: statements.append(str(stmt))

        await thread_state_cache.materialize(session, "t-1", values={"a": 1, "b": 2}, interrupts={})
        await thread_state_cache.materialize(session, "t-1", values={"b": 2, "a": 1}, interrupts={})

        assert len(statements) == 2


class TestStore:
    @pytest.mark.asyncio
    async def test_failures_are_swallowed(self) -> None:
        """A cache write must never fail the operation that triggered it."""
        with patch.object(thread_state_cache, "_get_session_maker", side_effect=RuntimeError("no db")):
            await thread_state_cache.store("t-1", values={"a": 1}, interrupts={})


class TestRefresh:
    @pytest.mark.asyncio
    async def test_reads_the_graph_then_stores(self) -> None:
        graph = AsyncMock()
        graph.aget_state.return_value = _Snapshot({"a": 1})

        with patch.object(thread_state_cache, "store", new=AsyncMock()) as store:
            await thread_state_cache.refresh("t-1", graph, {"configurable": {}})

        store.assert_awaited_once_with("t-1", values={"a": 1}, interrupts={})

    @pytest.mark.asyncio
    async def test_unreadable_state_skips_the_write(self) -> None:
        graph = AsyncMock()
        graph.aget_state.side_effect = RuntimeError("checkpointer down")

        with patch.object(thread_state_cache, "store", new=AsyncMock()) as store:
            await thread_state_cache.refresh("t-1", graph, {"configurable": {}})

        store.assert_not_awaited()


class TestRead:
    @pytest.mark.asyncio
    async def test_empty_id_list_skips_the_query(self) -> None:
        session = AsyncMock()

        assert await thread_state_cache.read(session, []) == {}
        session.scalars.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rows_are_keyed_by_thread_id(self) -> None:
        row = MagicMock()
        row.thread_id = "t-1"
        session = AsyncMock()
        result = MagicMock()
        result.all.return_value = [row]
        session.scalars.return_value = result

        assert await thread_state_cache.read(session, ["t-1"]) == {"t-1": row}
