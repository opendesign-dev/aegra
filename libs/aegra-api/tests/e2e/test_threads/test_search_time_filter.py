"""E2E tests for the created_after/created_before window on POST /threads/search."""

import uuid
from datetime import datetime, timedelta

import pytest
from httpx import AsyncClient

from aegra_api.settings import settings
from tests.e2e._utils import elog, get_e2e_client


def _parse_created_at(value: str) -> datetime:
    """Parse the API's ISO 8601 created_at into an aware datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iso(moment: datetime) -> str:
    return moment.isoformat()


async def _seed_thread(tag: str) -> dict[str, str]:
    """Create one tagged thread and return its id plus created_at."""
    client = get_e2e_client()
    thread = await client.threads.create(metadata={"time_filter_tag": tag})
    seeded = {"thread_id": thread["thread_id"], "created_at": thread["created_at"]}
    elog(f"Seeded thread for tag {tag}", seeded)
    return seeded


async def _search(body: dict) -> list[dict]:
    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http_client:
        resp = await http_client.post("/threads/search", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_window_is_inclusive_e2e() -> None:
    """A window anchored on the thread's own created_at includes it on both bounds."""
    tag = f"thread-window-in-{uuid.uuid4().hex[:8]}"
    seeded = await _seed_thread(tag)
    created = _parse_created_at(seeded["created_at"])

    rows = await _search(
        {
            "metadata": {"time_filter_tag": tag},
            "created_after": _iso(created),
            "created_before": _iso(created),
            "limit": 100,
        }
    )
    elog("Inclusive window", rows)
    assert [t["thread_id"] for t in rows] == [seeded["thread_id"]]


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_window_excludes_outside_threads_e2e() -> None:
    """Windows on either side of created_at exclude the thread.

    Bounds come from the thread's own timestamp, so no client/database clock
    agreement is assumed.
    """
    tag = f"thread-window-out-{uuid.uuid4().hex[:8]}"
    seeded = await _seed_thread(tag)
    created = _parse_created_at(seeded["created_at"])

    after_window = await _search(
        {
            "metadata": {"time_filter_tag": tag},
            "created_after": _iso(created + timedelta(seconds=5)),
            "limit": 100,
        }
    )
    assert after_window == []

    before_window = await _search(
        {
            "metadata": {"time_filter_tag": tag},
            "created_before": _iso(created - timedelta(seconds=5)),
            "limit": 100,
        }
    )
    assert before_window == []


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_count_honours_window_e2e() -> None:
    """/threads/count applies the same window as /threads/search."""
    tag = f"thread-count-{uuid.uuid4().hex[:8]}"
    seeded = await _seed_thread(tag)
    created = _parse_created_at(seeded["created_at"])

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http_client:
        inside = await http_client.post(
            "/threads/count",
            json={"metadata": {"time_filter_tag": tag}, "created_after": _iso(created)},
        )
        outside = await http_client.post(
            "/threads/count",
            json={
                "metadata": {"time_filter_tag": tag},
                "created_before": _iso(created - timedelta(seconds=5)),
            },
        )

    assert inside.status_code == 200, inside.text
    assert inside.json() == 1
    assert outside.status_code == 200, outside.text
    assert outside.json() == 0


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_inverted_window_returns_422_e2e() -> None:
    """An inverted window is a 422, not a silently empty result."""
    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http_client:
        resp = await http_client.post(
            "/threads/search",
            json={"created_after": "2026-03-01T00:00:00Z", "created_before": "2026-01-01T00:00:00Z"},
        )
    assert resp.status_code == 422, resp.text
