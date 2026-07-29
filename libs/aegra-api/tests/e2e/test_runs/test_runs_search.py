"""E2E tests for POST /runs/search and POST /runs/count against a live server."""

import uuid
from datetime import datetime, timedelta

import pytest
from httpx import AsyncClient

from aegra_api.settings import settings
from tests.e2e._utils import elog, get_e2e_client

# Deterministic, no-LLM graph so these tests never depend on a model provider.
GRAPH_ID = "stress_test"
FAST_INPUT = {"messages": [{"role": "user", "content": '{"delay": 0, "steps": 1}'}]}


def _parse_created_at(value: str) -> datetime:
    """Parse the API's ISO 8601 created_at into an aware datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iso(moment: datetime) -> str:
    return moment.isoformat()


async def _seed_run(tag: str, config_marker: str) -> dict[str, str]:
    """Create an assistant, a thread, and one tagged run; return their ids.

    The assistant's ``config`` carries a unique marker because assistant identity
    is (user_id, graph_id, md5(config)) — two calls with identical config would
    collapse onto one assistant.
    """
    client = get_e2e_client()
    assistant = await client.assistants.create(
        graph_id=GRAPH_ID,
        config={"tags": [config_marker]},
        if_exists="do_nothing",
    )
    thread = await client.threads.create(metadata={"runs_search_tag": tag})
    run = await client.runs.create(
        thread_id=thread["thread_id"],
        assistant_id=assistant["assistant_id"],
        input=FAST_INPUT,
        metadata={"runs_search_tag": tag},
    )
    seeded = {
        "assistant_id": assistant["assistant_id"],
        "thread_id": thread["thread_id"],
        "run_id": run["run_id"],
        "created_at": run["created_at"],
    }
    elog(f"Seeded run for tag {tag}", seeded)
    return seeded


async def _seed_runs_on_one_thread(tag: str, count: int) -> list[str]:
    """Create *count* sequential runs on a single thread, oldest first.

    Runs are awaited one at a time: a thread admits at most one in-flight run,
    so overlapping creates would 409 under the default reject strategy.
    """
    client = get_e2e_client()
    thread = await client.threads.create(metadata={"runs_search_tag": tag})
    thread_id = thread["thread_id"]
    run_ids: list[str] = []
    for _ in range(count):
        run = await client.runs.create(
            thread_id=thread_id,
            assistant_id=GRAPH_ID,
            input=FAST_INPUT,
            metadata={"runs_search_tag": tag},
        )
        run_ids.append(run["run_id"])
        await client.runs.join(thread_id, run["run_id"])
    elog(f"Seeded {count} runs for tag {tag}", run_ids)
    return run_ids


async def _search(body: dict) -> list[dict]:
    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http_client:
        resp = await http_client.post("/runs/search", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _count(body: dict) -> int:
    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http_client:
        resp = await http_client.post("/runs/count", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_finds_run_across_threads_e2e() -> None:
    """A tagged run is found without naming its thread."""
    tag = f"runs-search-{uuid.uuid4().hex[:8]}"
    seeded = await _seed_run(tag, f"marker-{tag}")

    rows = await _search({"metadata": {"runs_search_tag": tag}, "limit": 100})
    elog("Search by metadata", rows)
    assert [r["run_id"] for r in rows] == [seeded["run_id"]]
    assert rows[0]["thread_id"] == seeded["thread_id"]


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_filters_by_assistant_id_e2e() -> None:
    """Two assistants' runs are separable by assistant_id, unlike thread metadata."""
    tag_a = f"assistant-a-{uuid.uuid4().hex[:8]}"
    tag_b = f"assistant-b-{uuid.uuid4().hex[:8]}"
    a = await _seed_run(tag_a, f"marker-{tag_a}")
    b = await _seed_run(tag_b, f"marker-{tag_b}")
    assert a["assistant_id"] != b["assistant_id"], "seed produced one assistant, not two"

    rows_a = await _search({"assistant_id": a["assistant_id"], "limit": 100})
    ids_a = [r["run_id"] for r in rows_a]
    elog("Search by assistant A", ids_a)
    assert a["run_id"] in ids_a
    assert b["run_id"] not in ids_a

    rows_b = await _search({"assistant_id": b["assistant_id"], "limit": 100})
    ids_b = [r["run_id"] for r in rows_b]
    assert b["run_id"] in ids_b
    assert a["run_id"] not in ids_b


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_accepts_graph_id_as_assistant_alias_e2e() -> None:
    """A graph id resolves to its canonical assistant, matching run creation."""
    tag = f"graph-alias-{uuid.uuid4().hex[:8]}"
    client = get_e2e_client()
    # Create the run through the graph-id alias so the canonical assistant is the
    # uuid5-derived one that the same alias resolves to on search.
    thread = await client.threads.create()
    run = await client.runs.create(
        thread_id=thread["thread_id"],
        assistant_id=GRAPH_ID,
        input=FAST_INPUT,
        metadata={"runs_search_tag": tag},
    )

    rows = await _search({"assistant_id": GRAPH_ID, "metadata": {"runs_search_tag": tag}, "limit": 100})
    elog("Search by graph alias", rows)
    assert [r["run_id"] for r in rows] == [run["run_id"]]


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_time_window_is_inclusive_e2e() -> None:
    """A window anchored on the run's own created_at includes it on both bounds."""
    tag = f"window-in-{uuid.uuid4().hex[:8]}"
    seeded = await _seed_run(tag, f"marker-{tag}")
    created = _parse_created_at(seeded["created_at"])

    rows = await _search(
        {
            "metadata": {"runs_search_tag": tag},
            "created_after": _iso(created),
            "created_before": _iso(created),
            "limit": 100,
        }
    )
    elog("Inclusive window", rows)
    assert [r["run_id"] for r in rows] == [seeded["run_id"]]


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_time_window_excludes_outside_runs_e2e() -> None:
    """Windows on either side of the run's created_at exclude it.

    Bounds are derived from the run's own timestamp, so this never depends on
    agreement between the client clock and the database clock.
    """
    tag = f"window-out-{uuid.uuid4().hex[:8]}"
    seeded = await _seed_run(tag, f"marker-{tag}")
    created = _parse_created_at(seeded["created_at"])

    after_window = await _search(
        {
            "metadata": {"runs_search_tag": tag},
            "created_after": _iso(created + timedelta(seconds=5)),
            "limit": 100,
        }
    )
    elog("Window starting after the run", after_window)
    assert after_window == []

    before_window = await _search(
        {
            "metadata": {"runs_search_tag": tag},
            "created_before": _iso(created - timedelta(seconds=5)),
            "limit": 100,
        }
    )
    elog("Window ending before the run", before_window)
    assert before_window == []


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_count_matches_search_e2e() -> None:
    """/runs/count agrees with /runs/search for the same filters."""
    tag = f"count-{uuid.uuid4().hex[:8]}"
    await _seed_run(tag, f"marker-{tag}")

    body = {"metadata": {"runs_search_tag": tag}}
    total = await _count(body)
    rows = await _search({**body, "limit": 100})
    elog("Count vs search", {"count": total, "rows": len(rows)})
    assert total == len(rows) == 1


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_select_projects_fields_e2e() -> None:
    """select trims rows to the requested columns."""
    tag = f"select-{uuid.uuid4().hex[:8]}"
    await _seed_run(tag, f"marker-{tag}")

    rows = await _search({"metadata": {"runs_search_tag": tag}, "select": ["run_id", "assistant_id"], "limit": 100})
    elog("Projected rows", rows)
    assert set(rows[0]) == {"run_id", "assistant_id"}


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_sort_order_e2e() -> None:
    """sort_order flips creation order; the default is newest-first."""
    tag = f"sort-{uuid.uuid4().hex[:8]}"
    created = await _seed_runs_on_one_thread(tag, 3)
    body = {"metadata": {"runs_search_tag": tag}, "sort_by": "created_at", "limit": 100}

    ascending = await _search({**body, "sort_order": "asc"})
    assert [r["run_id"] for r in ascending] == created

    descending = await _search({**body, "sort_order": "desc"})
    assert [r["run_id"] for r in descending] == list(reversed(created))

    default = await _search({"metadata": {"runs_search_tag": tag}, "limit": 100})
    assert [r["run_id"] for r in default] == list(reversed(created))


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_pagination_e2e() -> None:
    """limit/offset walk the result set without gaps or repeats."""
    tag = f"page-{uuid.uuid4().hex[:8]}"
    created = await _seed_runs_on_one_thread(tag, 3)
    body = {"metadata": {"runs_search_tag": tag}, "sort_by": "created_at", "sort_order": "asc"}

    first = await _search({**body, "limit": 2, "offset": 0})
    second = await _search({**body, "limit": 2, "offset": 2})
    elog("Paged", {"first": [r["run_id"] for r in first], "second": [r["run_id"] for r in second]})
    assert [r["run_id"] for r in first] == created[:2]
    assert [r["run_id"] for r in second] == created[2:]


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_status_filter_e2e() -> None:
    """A completed run matches status='success' and not status='pending'."""
    tag = f"status-{uuid.uuid4().hex[:8]}"
    created = await _seed_runs_on_one_thread(tag, 1)
    body = {"metadata": {"runs_search_tag": tag}, "limit": 100}

    successful = await _search({**body, "status": "success"})
    elog("Successful runs", successful)
    assert [r["run_id"] for r in successful] == created

    pending = await _search({**body, "status": "pending"})
    assert pending == []


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_search_inverted_window_returns_422_e2e() -> None:
    """An inverted window is a 422, not a silently empty result."""
    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http_client:
        resp = await http_client.post(
            "/runs/search",
            json={"created_after": "2026-03-01T00:00:00Z", "created_before": "2026-01-01T00:00:00Z"},
        )
    assert resp.status_code == 422, resp.text


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_list_thread_runs_time_window_e2e() -> None:
    """GET /threads/{id}/runs honours the same window."""
    tag = f"list-window-{uuid.uuid4().hex[:8]}"
    seeded = await _seed_run(tag, f"marker-{tag}")
    created = _parse_created_at(seeded["created_at"])
    path = f"/threads/{seeded['thread_id']}/runs"

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http_client:
        inside = await http_client.get(path, params={"created_after": _iso(created)})
        outside = await http_client.get(path, params={"created_before": _iso(created - timedelta(seconds=5))})
        invalid = await http_client.get(
            path,
            params={"created_after": _iso(created), "created_before": _iso(created - timedelta(seconds=5))},
        )

    assert inside.status_code == 200, inside.text
    assert [r["run_id"] for r in inside.json()] == [seeded["run_id"]]
    assert outside.status_code == 200, outside.text
    assert outside.json() == []
    assert invalid.status_code == 422, invalid.text
