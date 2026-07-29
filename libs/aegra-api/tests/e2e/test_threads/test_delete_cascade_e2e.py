"""E2E tests: deleting a thread or assistant leaves no data behind.

Regression, measured against a real database: DELETE /threads dropped the thread
row (cascading to runs and thread_state) but left 12 checkpoints, 9 blobs, and 18
pending writes — and the blobs hold the conversation state, so the data outlived
the delete the caller had asked for.

Uses the deterministic no-LLM ``stress_test`` graph so checkpoint counts are stable.
"""

import pytest
from httpx import AsyncClient

from aegra_api.settings import settings
from tests.e2e._utils import elog, get_e2e_client

GRAPH_ID = "stress_test"
FAST_INPUT = {"messages": [{"role": "user", "content": '{"delay": 0, "steps": 1}'}]}


async def _seed_thread(assistant_id: str, runs: int = 2) -> str:
    """Create a thread and drive enough runs to produce checkpoints."""
    client = get_e2e_client()
    thread = await client.threads.create()
    thread_id = thread["thread_id"]
    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=60.0) as http:
        for _ in range(runs):
            resp = await http.post(
                f"/threads/{thread_id}/runs/wait",
                json={"assistant_id": assistant_id, "input": FAST_INPUT},
            )
            assert resp.status_code == 200, resp.text
    return thread_id


async def _has_state(thread_id: str) -> bool:
    """Whether the checkpointer still answers with state for this thread.

    The only client-visible proxy for the checkpointer tables: history is read
    straight from them, so a non-empty history means rows survive.
    """
    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        resp = await http.get(f"/threads/{thread_id}/history", params={"limit": 10})
    if resp.status_code == 404:
        return False
    assert resp.status_code == 200, resp.text
    return bool(resp.json())


async def _assistant(config_marker: str) -> str:
    client = get_e2e_client()
    assistant = await client.assistants.create(
        graph_id=GRAPH_ID, config={"tags": [config_marker]}, if_exists="do_nothing"
    )
    return assistant["assistant_id"]


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_delete_thread_removes_checkpoint_history_e2e() -> None:
    """DELETE /threads takes the checkpoints with it."""
    assistant_id = await _assistant("delete-cascade-thread")
    thread_id = await _seed_thread(assistant_id)

    assert await _has_state(thread_id), "seed produced no checkpoint history"

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        resp = await http.delete(f"/threads/{thread_id}")
    assert resp.status_code == 200, resp.text

    remaining = await _has_state(thread_id)
    elog("history after thread delete", {"thread_id": thread_id, "has_state": remaining})
    assert not remaining, "checkpoint history survived the thread delete"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_delete_assistant_with_cascade_removes_thread_state_e2e() -> None:
    """delete_threads=true cascades through to the checkpoints too."""
    assistant_id = await _assistant("delete-cascade-assistant")
    thread_id = await _seed_thread(assistant_id)

    assert await _has_state(thread_id), "seed produced no checkpoint history"

    client = get_e2e_client()
    await client.assistants.delete(assistant_id, delete_threads=True)

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        thread_resp = await http.get(f"/threads/{thread_id}")
    assert thread_resp.status_code == 404, "thread should be gone"

    remaining = await _has_state(thread_id)
    elog("history after assistant cascade", {"thread_id": thread_id, "has_state": remaining})
    assert not remaining, "checkpoint history survived the assistant cascade"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_delete_assistant_without_cascade_keeps_the_thread_e2e() -> None:
    """Without the flag the thread stays — the default must not widen.

    Matches the SDK: delete_threads defaults to False, so an assistant delete
    alone is not licence to destroy conversations.
    """
    assistant_id = await _assistant("delete-no-cascade")
    thread_id = await _seed_thread(assistant_id)

    client = get_e2e_client()
    await client.assistants.delete(assistant_id)

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        thread_resp = await http.get(f"/threads/{thread_id}")
    assert thread_resp.status_code == 200, "thread must survive a non-cascading delete"
    assert await _has_state(thread_id), "checkpoint history must survive too"

    # Leave nothing behind for the next run.
    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        await http.delete(f"/threads/{thread_id}")
