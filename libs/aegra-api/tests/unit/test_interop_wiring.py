"""Route registration for the MCP and A2A interop endpoints.

The opt-outs are read at app construction, so the assertion is on which routes
exist — not on a request's status code.
"""

from typing import Any

import pytest
from fastapi import FastAPI

from aegra_api.config import HttpConfig
from aegra_api.main import _include_core_routers, _mount_mcp


def _paths(app: FastAPI) -> set[str]:
    """All route paths, flattening the routers FastAPI includes lazily."""
    found: set[str] = set()

    def walk(routes: list[Any]) -> None:
        for route in routes:
            path = getattr(route, "path", None)
            if path:
                found.add(path)
            nested = getattr(route, "original_router", None) or getattr(route, "routes", None)
            if nested is not None:
                walk(getattr(nested, "routes", nested))

    walk(app.routes)
    return found


def _build(config: HttpConfig | None) -> set[str]:
    app = FastAPI()
    _include_core_routers(app, config)
    _mount_mcp(app, config)
    return _paths(app)


class TestDefaults:
    @pytest.mark.parametrize("config", [None, {}, {"app": "./x.py:app"}])
    def test_both_endpoints_are_on_without_an_opt_out(self, config: HttpConfig | None) -> None:
        paths = _build(config)

        assert "/mcp" in paths
        assert "/a2a/{assistant_id}" in paths
        assert "/.well-known/agent-card.json" in paths


class TestOptOuts:
    def test_disable_mcp_drops_only_the_mcp_route(self) -> None:
        paths = _build({"disable_mcp": True})

        assert "/mcp" not in paths
        assert "/a2a/{assistant_id}" in paths

    def test_disable_a2a_drops_both_a2a_routes(self) -> None:
        paths = _build({"disable_a2a": True})

        assert "/a2a/{assistant_id}" not in paths
        assert "/.well-known/agent-card.json" not in paths
        assert "/mcp" in paths

    def test_both_can_be_disabled_together(self) -> None:
        paths = _build({"disable_mcp": True, "disable_a2a": True})

        assert not {"/mcp", "/a2a/{assistant_id}", "/.well-known/agent-card.json"} & paths
        assert "/assistants" in paths

    @pytest.mark.parametrize("flag", [False, None])
    def test_falsy_flag_keeps_the_endpoints_on(self, flag: bool | None) -> None:
        paths = _build({"disable_mcp": flag, "disable_a2a": flag})  # type: ignore[typeddict-item]

        assert "/mcp" in paths
        assert "/a2a/{assistant_id}" in paths
