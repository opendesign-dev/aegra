"""Unit tests for static breakpoints (interrupt_before / interrupt_after).

Regression: these were written into the LangGraph *config* dict, which is an
allow-list — the keys were silently dropped and the graph never paused. They are
``astream`` kwargs. A paused run must also finalize as ``interrupted``: a static
breakpoint raises no ``__interrupt__``, so the only evidence is nodes still
queued in the checkpoint.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aegra_api.services.run_executor import _breakpoints, _GraphResult, _is_paused, _read_state


class TestBreakpointNormalization:
    """_breakpoints shapes the API value into what langgraph accepts."""

    def test_none_stays_none(self) -> None:
        """Unset means unset — the kwarg must not be forwarded at all."""
        assert _breakpoints(None) is None

    def test_bare_node_name_is_wrapped(self) -> None:
        assert _breakpoints("agent") == ["agent"]

    def test_list_passes_through(self) -> None:
        assert _breakpoints(["a", "b"]) == ["a", "b"]

    def test_star_is_not_wrapped(self) -> None:
        """langgraph spells 'all nodes' as the literal '*', not ['*']."""
        assert _breakpoints("*") == "*"

    def test_empty_list_stays_empty(self) -> None:
        """An empty list is falsy downstream, so the kwarg is skipped."""
        assert _breakpoints([]) == []


class TestIsPaused:
    """_is_paused covers both ways a run can stop mid-graph."""

    def test_clean_finish_is_not_paused(self) -> None:
        result = _GraphResult()
        assert _is_paused(result) is False

    def test_dynamic_interrupt_is_paused(self) -> None:
        """interrupt() inside a node raises __interrupt__."""
        result = _GraphResult()
        result.has_interrupt = True
        assert _is_paused(result) is True

    def test_static_breakpoint_is_paused(self) -> None:
        """A static breakpoint raises nothing; queued nodes are the only signal.

        Regression: this case used to finalize as 'success', so the caller and the
        webhook were told the run finished while half the graph never ran.
        """
        result = _GraphResult()
        result.pending_nodes = ["respond"]
        assert result.has_interrupt is False
        assert _is_paused(result) is True

    def test_both_signals_is_paused(self) -> None:
        result = _GraphResult()
        result.has_interrupt = True
        result.pending_nodes = ["tools"]
        assert _is_paused(result) is True


class TestReadStatePendingNodes:
    """_read_state surfaces the checkpoint's queued nodes for _is_paused."""

    def _graph(self, snapshot: Any) -> MagicMock:
        graph = MagicMock()
        graph.aget_state = AsyncMock(return_value=snapshot)
        return graph

    @pytest.mark.asyncio
    async def test_reports_queued_nodes(self) -> None:
        snapshot = MagicMock()
        snapshot.values = {}
        snapshot.tasks = ()
        snapshot.next = ("respond",)

        _values, _interrupts, pending = await _read_state(self._graph(snapshot), {}, "run-1")

        assert pending == ["respond"]

    @pytest.mark.asyncio
    async def test_empty_next_means_finished(self) -> None:
        snapshot = MagicMock()
        snapshot.values = {}
        snapshot.tasks = ()
        snapshot.next = ()

        _values, _interrupts, pending = await _read_state(self._graph(snapshot), {}, "run-1")

        assert pending == []

    @pytest.mark.asyncio
    async def test_read_failure_reports_no_pending_nodes(self) -> None:
        """A failed read must not fabricate a pause."""
        graph = MagicMock()
        graph.aget_state = AsyncMock(side_effect=RuntimeError("checkpointer down"))

        values, _interrupts, pending = await _read_state(graph, {}, "run-1")

        assert values is None
        assert pending == []
