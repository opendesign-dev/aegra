"""Unit tests for the MCP tool mapping."""

from typing import Any

import pytest

from aegra_api.services.mcp_server import (
    _EMPTY_INPUT_SCHEMA,
    _normalize_input_schema,
    tool_name_for,
)


class TestToolNameFor:
    def test_plain_name_passes_through(self) -> None:
        assert tool_name_for("weather", "asst-1", set()) == "weather"

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("Weather Bot", "Weather_Bot"),
            ("bot.v2", "bot_v2"),
            ("bot/v2:latest", "bot_v2_latest"),
            ("keep-dashes_and_9", "keep-dashes_and_9"),
        ],
    )
    def test_illegal_characters_become_underscores(self, name: str, expected: str) -> None:
        assert tool_name_for(name, "asst-1", set()) == expected

    def test_surrounding_underscores_are_trimmed(self) -> None:
        assert tool_name_for("  spaced  ", "asst-1", set()) == "spaced"

    def test_name_that_sanitizes_to_nothing_falls_back_to_id(self) -> None:
        assert tool_name_for("中文名", "asst-1", set()) == "asst-1"

    def test_collision_falls_back_to_id(self) -> None:
        taken: set[str] = set()

        first = tool_name_for("Weather Bot", "asst-1", taken)
        second = tool_name_for("Weather.Bot", "asst-2", taken)

        assert first == "Weather_Bot"
        assert second == "asst-2"

    def test_name_is_truncated_to_the_mcp_limit(self) -> None:
        assert len(tool_name_for("a" * 300, "asst-1", set())) == 128

    def test_registers_the_returned_name_so_later_calls_see_it(self) -> None:
        taken: set[str] = set()

        tool_name_for("weather", "asst-1", taken)

        assert "weather" in taken


class TestNormalizeInputSchema:
    def test_object_schema_passes_through(self) -> None:
        schema = {"type": "object", "properties": {"q": {"type": "string"}}}

        assert _normalize_input_schema(schema) is schema

    @pytest.mark.parametrize(
        "schema",
        [None, {}, {"type": "string"}, {"anyOf": [{"type": "object"}]}],
    )
    def test_non_object_schema_degrades_to_empty_object(self, schema: dict[str, Any] | None) -> None:
        assert _normalize_input_schema(schema) == _EMPTY_INPUT_SCHEMA
