"""Unit tests for the config a thread's graph is loaded with."""

from typing import Any
from unittest.mock import AsyncMock

import pytest

from aegra_api.models import User
from aegra_api.services.thread_service import thread_graph_config


class _Thread:
    def __init__(self, metadata: dict[str, Any], config: dict[str, Any] | None = None) -> None:
        self.thread_id = "thread-1"
        self.metadata_json = metadata
        self.config = config or {}


class _Assistant:
    def __init__(self, config: dict[str, Any] | None) -> None:
        self.assistant_id = "asst-1"
        self.config = config


def _user() -> User:
    return User(identity="user-1", scopes=[])


class TestThreadGraphConfig:
    @pytest.mark.asyncio
    async def test_unbound_thread_gets_only_its_own_identity(self) -> None:
        session = AsyncMock()
        thread = _Thread({"graph_id": "graph-1"})

        config = await thread_graph_config(session, thread, _user())

        assert config["configurable"]["thread_id"] == "thread-1"
        assert "assistant_id" not in config["configurable"]
        session.scalar.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_replays_the_config_the_thread_was_bound_with(self) -> None:
        """The stored config is what the run compiled the graph from, so reads reuse it."""
        session = AsyncMock()
        thread = _Thread(
            {"graph_id": "graph-1", "assistant_id": "asst-1"},
            {"recursion_limit": 42, "configurable": {"interrupt_on": {"ask": True}}},
        )

        config = await thread_graph_config(session, thread, _user())

        assert config["recursion_limit"] == 42
        assert config["configurable"]["interrupt_on"] == {"ask": True}
        assert config["configurable"]["assistant_id"] == "asst-1"
        assert config["configurable"]["langgraph_auth_user"].identity == "user-1"
        # No assistant lookup: the thread already carries the config.
        session.scalar.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stored_config_cannot_redirect_to_another_thread(self) -> None:
        session = AsyncMock()
        thread = _Thread(
            {"assistant_id": "asst-1"},
            {"configurable": {"thread_id": "someone-elses", "assistant_id": "asst-evil"}},
        )

        config = await thread_graph_config(session, thread, _user())

        assert config["configurable"]["thread_id"] == "thread-1"
        assert config["configurable"]["assistant_id"] == "asst-1"

    @pytest.mark.asyncio
    async def test_does_not_mutate_the_stored_config(self) -> None:
        session = AsyncMock()
        stored: dict[str, Any] = {"configurable": {"permission": {"ask": "ask"}}}
        thread = _Thread({"assistant_id": "asst-1"}, stored)

        await thread_graph_config(session, thread, _user())

        assert stored == {"configurable": {"permission": {"ask": "ask"}}}

    @pytest.mark.asyncio
    async def test_falls_back_to_the_assistant_for_threads_predating_the_column(self) -> None:
        session = AsyncMock()
        session.scalar.return_value = _Assistant({"configurable": {"interrupt_on": {"ask": True}}})
        thread = _Thread({"assistant_id": "asst-1"}, {})

        config = await thread_graph_config(session, thread, _user())

        assert config["configurable"]["interrupt_on"] == {"ask": True}
        assert config["configurable"]["assistant_id"] == "asst-1"
        session.scalar.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_deleted_assistant_degrades_to_identity_only(self) -> None:
        """A read still has a checkpoint to answer from, so a missing assistant is not fatal."""
        session = AsyncMock()
        session.scalar.return_value = None
        thread = _Thread({"assistant_id": "asst-gone"}, {})

        config = await thread_graph_config(session, thread, _user())

        assert config["configurable"]["thread_id"] == "thread-1"
        assert config["configurable"]["assistant_id"] == "asst-gone"

    @pytest.mark.asyncio
    async def test_assistant_with_null_config_degrades_to_identity_only(self) -> None:
        session = AsyncMock()
        session.scalar.return_value = _Assistant(None)
        thread = _Thread({"assistant_id": "asst-1"}, {})

        config = await thread_graph_config(session, thread, _user())

        assert config["configurable"]["thread_id"] == "thread-1"
        assert config["configurable"]["assistant_id"] == "asst-1"
