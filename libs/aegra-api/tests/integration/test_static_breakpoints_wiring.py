"""Integration test: static breakpoints reach astream as kwargs, not as config keys.

Regression guard for the exact bug shape — the values were placed in the config
dict, which langgraph treats as an allow-list, so they were silently dropped and
no breakpoint ever fired. Asserting on the call kwargs is what catches a relapse;
asserting on config would have passed the whole time it was broken.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest

from aegra_api.services.graph_streaming import stream_graph_events


class _Recorder:
    """Captures the kwargs of the astream call and yields nothing."""

    def __init__(self) -> None:
        self.kwargs: dict[str, Any] = {}
        self.args: tuple[Any, ...] = ()

    def astream(self, *args: Any, **kwargs: Any) -> Any:
        self.args, self.kwargs = args, kwargs

        async def _empty() -> Any:
            return
            yield  # pragma: no cover - never reached, makes this an async generator

        return _empty()


def _graph(recorder: _Recorder) -> MagicMock:
    graph = MagicMock()
    graph.astream = recorder.astream
    graph.get_context_jsonschema = MagicMock(return_value={})
    graph.output_channels = None
    return graph


async def _drain(graph: MagicMock, **kwargs: Any) -> None:
    async for _ in stream_graph_events(
        graph=graph,
        input_data={"messages": []},
        config={"configurable": {"run_id": "r1", "thread_id": "t1"}},
        stream_mode=["values"],
        **kwargs,
    ):
        pass


class TestBreakpointsReachAstream:
    """The kwargs langgraph actually reads must carry the breakpoints."""

    @pytest.mark.asyncio
    async def test_interrupt_before_is_a_kwarg(self) -> None:
        rec = _Recorder()
        await _drain(_graph(rec), interrupt_before=["respond"])
        assert rec.kwargs.get("interrupt_before") == ["respond"]

    @pytest.mark.asyncio
    async def test_interrupt_after_is_a_kwarg(self) -> None:
        rec = _Recorder()
        await _drain(_graph(rec), interrupt_after=["process"])
        assert rec.kwargs.get("interrupt_after") == ["process"]

    @pytest.mark.asyncio
    async def test_star_reaches_astream_unwrapped(self) -> None:
        rec = _Recorder()
        await _drain(_graph(rec), interrupt_before="*")
        assert rec.kwargs.get("interrupt_before") == "*"

    @pytest.mark.asyncio
    async def test_not_placed_in_config(self) -> None:
        """The config dict must stay clean — that is where they used to vanish."""
        rec = _Recorder()
        await _drain(_graph(rec), interrupt_before=["respond"], interrupt_after=["process"])
        config = rec.args[1] if len(rec.args) > 1 else {}
        assert "interrupt_before" not in config
        assert "interrupt_after" not in config

    @pytest.mark.asyncio
    async def test_unset_breakpoints_are_not_forwarded(self) -> None:
        """Omitting them keeps langgraph's default and spares graph types that
        do not accept the kwarg (e.g. the JS remote graph)."""
        rec = _Recorder()
        await _drain(_graph(rec))
        assert "interrupt_before" not in rec.kwargs
        assert "interrupt_after" not in rec.kwargs

    @pytest.mark.asyncio
    async def test_empty_list_is_not_forwarded(self) -> None:
        rec = _Recorder()
        await _drain(_graph(rec), interrupt_before=[])
        assert "interrupt_before" not in rec.kwargs

    @pytest.mark.asyncio
    async def test_durability_still_forwarded_alongside(self) -> None:
        """Breakpoints share the kwargs dict with durability; neither displaces the other."""
        rec = _Recorder()
        await _drain(_graph(rec), durability="sync", interrupt_before=["respond"])
        assert rec.kwargs.get("durability") == "sync"
        assert rec.kwargs.get("interrupt_before") == ["respond"]
