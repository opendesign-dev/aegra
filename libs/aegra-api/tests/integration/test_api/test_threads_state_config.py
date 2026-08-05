"""State reads must load the graph with the config the thread was bound to.

Regression: a factory graph that branches on ``configurable`` compiled a different node
set here than the run did, and LangGraph re-derives ``tasks`` / ``interrupts`` / ``next``
from the loaded nodes — so a thread paused on an interrupt reported none.
"""

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import MagicMock, Mock, patch

from fastapi.testclient import TestClient
from langgraph.types import StateSnapshot

from aegra_api.core.orm import get_session as core_get_session
from tests.fixtures.clients import create_test_app, make_client
from tests.fixtures.database import DummySessionBase, override_get_session_dep
from tests.fixtures.test_helpers import DummyThread

BOUND_CONFIG: dict[str, Any] = {"configurable": {"interrupt_on": {"ask": True}}}


class _Assistant:
    def __init__(self, config: dict[str, Any]) -> None:
        self.assistant_id = "asst-1"
        self.config = config


def _thread(config: dict[str, Any] | None) -> DummyThread:
    thread = DummyThread(
        "thread-1",
        metadata={"graph_id": "graph-1", "assistant_id": "asst-1"},
        user_id="test-user",
        config=config,
    )
    thread.metadata_json = thread.metadata
    return thread


def _snapshot() -> Mock:
    snapshot = Mock(spec=StateSnapshot)
    snapshot.values = {"messages": []}
    snapshot.next = []
    snapshot.tasks = []
    snapshot.interrupts = []
    snapshot.metadata = {}
    snapshot.config = {"configurable": {"checkpoint_id": "cp-1"}}
    snapshot.created_at = "2026-08-05T00:00:00Z"
    snapshot.parent_config = None
    return snapshot


def _agent() -> MagicMock:
    agent = MagicMock()
    agent.aget_state = Mock(return_value=_dummy_awaitable(_snapshot()))
    agent.with_config = Mock(return_value=agent)
    return agent


def _dummy_awaitable(value: Any) -> Any:
    async def _coro() -> Any:
        return value

    return _coro()


def _capturing_get_graph(agent: MagicMock) -> MagicMock:
    mock = MagicMock()

    @asynccontextmanager
    async def async_cm(*_args: Any, **_kwargs: Any) -> Any:
        yield agent

    mock.side_effect = lambda *args, **kwargs: async_cm(*args, **kwargs)
    return mock


def _client(session_cls: type[DummySessionBase]) -> TestClient:
    app = create_test_app(include_runs=False, include_threads=True)
    app.dependency_overrides[core_get_session] = override_get_session_dep(session_cls)
    return make_client(app)


def _read_state(session_cls: type[DummySessionBase]) -> dict[str, Any]:
    """Call the state endpoint and return the config handed to ``get_graph``."""
    agent = _agent()
    get_graph = _capturing_get_graph(agent)
    with patch("aegra_api.api.threads.get_langgraph_service") as get_service:
        get_service.return_value.get_graph = get_graph
        resp = _client(session_cls).get("/threads/thread-1/state")
    assert resp.status_code == 200
    return get_graph.call_args.kwargs["config"]


class TestStateReadUsesBoundConfig:
    def test_replays_the_config_stored_on_the_thread(self) -> None:
        thread = _thread(BOUND_CONFIG)

        class Session(DummySessionBase):
            async def scalar(self, _stmt: Any) -> Any:
                return thread

        config = _read_state(Session)

        assert config["configurable"]["interrupt_on"] == {"ask": True}
        assert config["configurable"]["assistant_id"] == "asst-1"
        assert config["configurable"]["thread_id"] == "thread-1"

    def test_falls_back_to_the_assistant_for_threads_predating_the_column(self) -> None:
        rows: list[Any] = [_thread({}), _Assistant(BOUND_CONFIG)]

        class Session(DummySessionBase):
            async def scalar(self, _stmt: Any) -> Any:
                return rows.pop(0) if rows else None

        config = _read_state(Session)

        assert config["configurable"]["interrupt_on"] == {"ask": True}
        assert config["configurable"]["assistant_id"] == "asst-1"
