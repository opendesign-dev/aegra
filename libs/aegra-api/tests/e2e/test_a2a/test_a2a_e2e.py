"""E2E tests for the A2A protocol surface.

Requires a running server with A2A_ENABLED=true (the default).
"""

import httpx
import pytest

from aegra_api.settings import settings

from .._utils import elog

ROOT_CARD = "/.well-known/agent-card.json"


@pytest.fixture(autouse=True)
def _skip_when_a2a_disabled() -> None:
    if not settings.a2a.A2A_ENABLED:
        pytest.skip("A2A_ENABLED=false; the /a2a routes are intentionally absent")


@pytest.fixture
async def client():
    async with httpx.AsyncClient(base_url=settings.app.SERVER_URL, timeout=15.0) as c:
        yield c


async def _first_graph(client: httpx.AsyncClient) -> str:
    card = (await client.get(ROOT_CARD)).json()
    skills = [s["id"] for s in card.get("skills", [])]
    if not skills:
        pytest.skip("no graphs configured, so no A2A agents to exercise")
    return skills[0]


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_root_card_advertises_every_graph_as_a_skill(client: httpx.AsyncClient) -> None:
    response = await client.get(ROOT_CARD)
    assert response.status_code == 200

    card = response.json()
    elog("A2A root card skills", [s["id"] for s in card["skills"]])
    assert card["name"] == "Aegra"
    assert card["skills"]


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_per_assistant_card_is_served_at_both_discovery_paths(client: httpx.AsyncClient) -> None:
    """Protocol 1.0 clients fetch agent-card.json, 0.3 clients fetch agent.json."""
    graph_id = await _first_graph(client)

    for path in (f"/a2a/{graph_id}/.well-known/agent-card.json", f"/a2a/{graph_id}/.well-known/agent.json"):
        response = await client.get(path)
        assert response.status_code == 200, path
        assert response.json()["name"] == graph_id


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_card_declares_both_protocol_versions(client: httpx.AsyncClient) -> None:
    graph_id = await _first_graph(client)
    card = (await client.get(f"/a2a/{graph_id}")).json()

    interfaces = card.get("supportedInterfaces") or card.get("supported_interfaces") or []
    versions = {i.get("protocolVersion") or i.get("protocol_version") for i in interfaces}
    elog("A2A advertised protocol versions", sorted(v for v in versions if v))
    assert len(versions) >= 2, "platform-era (0.3) and current (1.0) clients must both resolve an endpoint"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_unknown_assistant_returns_404(client: httpx.AsyncClient) -> None:
    assert (await client.get("/a2a/definitely-not-a-graph")).status_code == 404


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_delete_is_405_with_an_allow_header(client: httpx.AsyncClient) -> None:
    """The route exists for platform parity, but JSON-RPC owns cancellation."""
    graph_id = await _first_graph(client)
    response = await client.delete(f"/a2a/{graph_id}")

    assert response.status_code == 405
    assert set(response.headers["allow"].replace(" ", "").split(",")) == {"GET", "POST"}
