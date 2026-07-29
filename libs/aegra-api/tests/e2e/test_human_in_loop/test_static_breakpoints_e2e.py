"""E2E tests for static breakpoints (interrupt_before / interrupt_after).

Uses the deterministic no-LLM ``stress_test`` graph (nodes: process -> respond) so
a pause is provable rather than dependent on what a model decides to do.

Regression: the breakpoint values were written into the LangGraph config dict
instead of passed as astream kwargs, so nothing ever paused; and a paused run
finalized as ``success``, telling callers and webhooks the run had finished.
"""

import pytest
from httpx import AsyncClient

from aegra_api.settings import settings
from tests.e2e._utils import elog, get_e2e_client

GRAPH_ID = "stress_test"
ENTRY_NODE = "process"
SECOND_NODE = "respond"
FAST_INPUT = {"messages": [{"role": "user", "content": '{"delay": 0, "steps": 1}'}]}


async def _assistant_id() -> str:
    client = get_e2e_client()
    assistant = await client.assistants.create(graph_id=GRAPH_ID, if_exists="do_nothing")
    return assistant["assistant_id"]


async def _run_and_snapshot(body: dict) -> dict:
    """Run to completion on a fresh thread, then report run/thread/state."""
    client = get_e2e_client()
    thread = await client.threads.create()
    thread_id = thread["thread_id"]

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=60.0) as http:
        resp = await http.post(f"/threads/{thread_id}/runs/wait", json=body)
        assert resp.status_code == 200, resp.text

    runs = await client.runs.list(thread_id)
    thread_row = await client.threads.get(thread_id)
    state = await client.threads.get_state(thread_id)
    snapshot = {
        "thread_id": thread_id,
        "run_status": runs[0]["status"] if runs else None,
        "thread_status": thread_row["status"],
        "next": list(state.get("next") or []),
    }
    elog("breakpoint snapshot", snapshot)
    return snapshot


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_interrupt_before_pauses_and_reports_interrupted_e2e() -> None:
    """interrupt_before halts the graph and the run finalizes as interrupted."""
    snapshot = await _run_and_snapshot(
        {"assistant_id": await _assistant_id(), "input": FAST_INPUT, "interrupt_before": [SECOND_NODE]}
    )
    assert snapshot["next"] == [SECOND_NODE], "graph did not pause before the node"
    assert snapshot["run_status"] == "interrupted", "a paused run must not report success"
    assert snapshot["thread_status"] == "interrupted"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_interrupt_after_pauses_after_the_node_e2e() -> None:
    """interrupt_after halts once the named node has run."""
    snapshot = await _run_and_snapshot(
        {"assistant_id": await _assistant_id(), "input": FAST_INPUT, "interrupt_after": [ENTRY_NODE]}
    )
    assert snapshot["next"] == [SECOND_NODE]
    assert snapshot["run_status"] == "interrupted"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_interrupt_before_star_pauses_at_entry_e2e() -> None:
    """'*' means every node, so the graph stops before the entry node runs."""
    snapshot = await _run_and_snapshot(
        {"assistant_id": await _assistant_id(), "input": FAST_INPUT, "interrupt_before": "*"}
    )
    assert snapshot["next"] == [ENTRY_NODE]
    assert snapshot["run_status"] == "interrupted"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_no_breakpoint_runs_to_completion_e2e() -> None:
    """Without breakpoints the run finishes — the fix must not pause everything."""
    snapshot = await _run_and_snapshot({"assistant_id": await _assistant_id(), "input": FAST_INPUT})
    assert snapshot["next"] == []
    assert snapshot["run_status"] == "success"
    assert snapshot["thread_status"] == "idle"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_checkpoint_only_resume_completes_a_paused_run_e2e() -> None:
    """A static breakpoint resumes via checkpoint-only run (input=None).

    Unlike a dynamic ``interrupt()``, there is no value to hand back, so the
    resume carries no input and continues from the stored checkpoint.
    """
    assistant_id = await _assistant_id()
    snapshot = await _run_and_snapshot(
        {"assistant_id": assistant_id, "input": FAST_INPUT, "interrupt_before": [SECOND_NODE]}
    )
    assert snapshot["run_status"] == "interrupted"
    thread_id = snapshot["thread_id"]

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=60.0) as http:
        resp = await http.post(
            f"/threads/{thread_id}/runs/wait",
            json={"assistant_id": assistant_id, "input": None, "checkpoint": {}},
        )
    assert resp.status_code == 200, resp.text

    client = get_e2e_client()
    state = await client.threads.get_state(thread_id)
    thread_row = await client.threads.get(thread_id)
    elog("after resume", {"next": state.get("next"), "thread_status": thread_row["status"]})
    assert list(state.get("next") or []) == [], "resume did not drain the queued node"
    assert thread_row["status"] == "idle"
