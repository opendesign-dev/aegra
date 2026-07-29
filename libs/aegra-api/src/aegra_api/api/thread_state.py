"""Thread state and history endpoints.

Split from ``api/threads.py``: those routes own the thread record, these own the
checkpointed graph state behind it — a different backing store (the LangGraph
checkpointer, not the ``threads`` table) and a different failure surface.
"""

import json
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.auth_deps import auth_dependency, get_current_user
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import get_session
from aegra_api.models import (
    ThreadCheckpoint,
    ThreadCheckpointPostRequest,
    ThreadHistoryRequest,
    ThreadState,
    ThreadStateUpdate,
    ThreadStateUpdateResponse,
    User,
)
from aegra_api.models.errors import NOT_FOUND
from aegra_api.services.thread_state_service import ThreadStateService, refresh_materialized_state
from aegra_api.utils.run_utils import strip_pinned_config_keys

router = APIRouter(tags=["Threads"], dependencies=auth_dependency)
logger = structlog.getLogger(__name__)

thread_state_service = ThreadStateService()


@router.get("/threads/{thread_id}/state", response_model=ThreadState, responses={**NOT_FOUND})
async def get_thread_state(
    thread_id: str,
    subgraphs: bool = Query(False, description="Include states from subgraphs"),
    checkpoint_ns: str | None = Query(None, description="Checkpoint namespace to scope lookup"),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> ThreadState:
    """Get the current state of a thread.

    Returns the latest checkpoint's values, pending next nodes, interrupt
    data, and metadata. If the thread has no associated graph yet (no runs
    executed), returns an empty state.
    """
    try:
        stmt = select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user.identity)
        thread = await session.scalar(stmt)
        if not thread:
            raise HTTPException(404, f"Thread '{thread_id}' not found")

        thread_metadata = thread.metadata_json or {}
        graph_id = thread_metadata.get("graph_id")
        if not graph_id:
            logger.info(
                "state GET: no graph_id set for thread %s, returning empty state",
                thread_id,
            )
            empty_checkpoint = ThreadCheckpoint(
                checkpoint_id=None,
                thread_id=thread_id,
                checkpoint_ns="",
            )
            return ThreadState(
                values={},
                next=[],
                tasks=[],
                interrupts=[],
                metadata={},
                created_at=None,
                checkpoint=empty_checkpoint,
                parent_checkpoint=None,
                checkpoint_id=None,
                parent_checkpoint_id=None,
            )

        from aegra_api.services.langgraph_service import (
            create_thread_config,
            get_langgraph_service,
        )

        langgraph_service = get_langgraph_service()
        config: dict[str, Any] = create_thread_config(thread_id, user)
        if checkpoint_ns:
            config["configurable"]["checkpoint_ns"] = checkpoint_ns

        try:
            async with langgraph_service.get_graph(
                graph_id,
                config=config,
                access_context="threads.read",
                user=user,
            ) as agent:
                agent = agent.with_config(config)
                # NOTE: LangGraph only exposes subgraph checkpoints while the run is
                # interrupted. See https://docs.langchain.com/oss/python/langgraph/use-subgraphs#view-subgraph-state
                state_snapshot = await agent.aget_state(config, subgraphs=subgraphs)

                if not state_snapshot:
                    logger.info(
                        "state GET: no checkpoint found for thread %s (checkpoint_ns=%s)",
                        thread_id,
                        checkpoint_ns,
                    )
                    raise HTTPException(404, f"No state found for thread '{thread_id}'")

                thread_state = thread_state_service.convert_snapshot_to_thread_state(
                    state_snapshot, thread_id, subgraphs=subgraphs
                )

                logger.debug(
                    "state GET: thread_id=%s checkpoint_id=%s subgraphs=%s checkpoint_ns=%s",
                    thread_id,
                    thread_state.checkpoint.checkpoint_id,
                    subgraphs,
                    checkpoint_ns,
                )

                return thread_state
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Failed to retrieve latest state for thread '%s'", thread_id)
            raise HTTPException(500, f"Failed to retrieve thread state: {str(e)}") from e

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Unexpected error retrieving latest state for thread '%s'", thread_id)
        raise HTTPException(500, f"Error retrieving thread state: {str(e)}") from e


@router.post("/threads/{thread_id}/state", responses={**NOT_FOUND})
async def update_thread_state(
    thread_id: str,
    request: ThreadStateUpdate,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> ThreadState | ThreadStateUpdateResponse:
    """Update thread state or retrieve it via POST.

    When `values` is provided, creates a new checkpoint with the updated state.
    Use `as_node` to attribute the update to a specific graph node. When
    `values` is null AND `as_node` is not provided, this endpoint acts as a
    POST-based alternative to the GET state endpoint (useful when passing
    complex checkpoint/subgraph parameters in the request body).

    When `values` is null AND `as_node` is provided (e.g. ``as_node="__copy__"``
    as LangGraph Studio sends for "Re-run from here"), this creates a new
    checkpoint derived from the supplied ``checkpoint_id`` without applying
    any state change — used to anchor a subsequent run as a fork of that
    checkpoint rather than of the thread's latest state.
    """
    # GET-shim only fires when body has no mutation or checkpoint targeting.
    if request.values is None and request.as_node is None and request.checkpoint_id is None and not request.checkpoint:
        return await get_thread_state(
            thread_id=thread_id,
            subgraphs=request.subgraphs or False,
            checkpoint_ns=request.checkpoint_ns,
            user=user,
            session=session,
        )

    try:
        stmt = select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user.identity)
        thread = await session.scalar(stmt)
        if not thread:
            raise HTTPException(404, f"Thread '{thread_id}' not found")

        thread_metadata = thread.metadata_json or {}
        graph_id = thread_metadata.get("graph_id")
        if not graph_id:
            raise HTTPException(
                400,
                f"Thread '{thread_id}' has no associated graph. Cannot update state.",
            )

        from aegra_api.services.langgraph_service import (
            create_thread_config,
            get_langgraph_service,
        )

        langgraph_service = get_langgraph_service()
        config: dict[str, Any] = create_thread_config(thread_id, user)

        if request.checkpoint_id:
            config["configurable"]["checkpoint_id"] = request.checkpoint_id
        if request.checkpoint:
            config["configurable"].update(strip_pinned_config_keys(request.checkpoint))
        if request.checkpoint_ns:
            config["configurable"]["checkpoint_ns"] = request.checkpoint_ns

        try:
            async with langgraph_service.get_graph(
                graph_id,
                config=config,
                access_context="threads.update",
                user=user,
            ) as agent:
                # Update state using aupdate_state method
                # This creates a new checkpoint with the updated values
                agent = agent.with_config(config)

                # Handle values - can be dict or list of dicts
                update_values = request.values
                if isinstance(update_values, list):
                    # If it's a list, use the first dict or convert to dict
                    if update_values and isinstance(update_values[0], dict):
                        # Merge all dicts in the list
                        merged = {}
                        for item in update_values:
                            if isinstance(item, dict):
                                merged.update(item)
                        update_values = merged
                    else:
                        update_values = update_values[0] if update_values else None

                # Always pass as_node: without it the graph may resume execution
                # instead of only updating state, which can fail if state doesn't match graph flow.
                try:
                    updated_config = await agent.aupdate_state(config, update_values, as_node=request.as_node)
                except Exception as update_error:
                    logger.exception(
                        "aupdate_state failed for thread %s: %s",
                        thread_id,
                        update_error,
                        exc_info=True,
                    )
                    raise

                # Extract checkpoint info from the updated config
                # aupdate_state returns the updated config dict
                if not isinstance(updated_config, dict):
                    logger.error(
                        "aupdate_state returned non-dict: %s (type: %s)",
                        updated_config,
                        type(updated_config),
                    )
                    raise HTTPException(
                        500,
                        f"Unexpected return type from aupdate_state: {type(updated_config)}",
                    )

                checkpoint_info = {
                    "checkpoint_id": updated_config.get("configurable", {}).get("checkpoint_id"),
                    "thread_id": thread_id,
                    "checkpoint_ns": updated_config.get("configurable", {}).get("checkpoint_ns", ""),
                }

                logger.info(
                    "state POST: updated state for thread %s checkpoint_id=%s",
                    thread_id,
                    checkpoint_info.get("checkpoint_id"),
                )

                await refresh_materialized_state(session, thread, user)
                return ThreadStateUpdateResponse(checkpoint=checkpoint_info)

        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Failed to update state for thread '%s'", thread_id)
            raise HTTPException(500, f"Failed to update thread state: {str(e)}") from e

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Unexpected error updating state for thread '%s'", thread_id)
        raise HTTPException(500, f"Error updating thread state: {str(e)}") from e


@router.get("/threads/{thread_id}/state/{checkpoint_id}", response_model=ThreadState, responses={**NOT_FOUND})
async def get_thread_state_at_checkpoint(
    thread_id: str,
    checkpoint_id: str,
    subgraphs: bool | None = Query(False, description="Include states from subgraphs"),
    checkpoint_ns: str | None = Query(None, description="Checkpoint namespace to scope lookup"),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> ThreadState:
    """Get the thread state at a specific checkpoint.

    Use this to inspect historical state at any point in the thread's
    execution history. Returns 404 if the checkpoint does not exist.
    """
    try:
        stmt = select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user.identity)
        thread = await session.scalar(stmt)
        if not thread:
            raise HTTPException(404, f"Thread '{thread_id}' not found")

        thread_metadata = thread.metadata_json or {}
        graph_id = thread_metadata.get("graph_id")
        if not graph_id:
            raise HTTPException(404, f"Thread '{thread_id}' has no associated graph")

        from aegra_api.services.langgraph_service import (
            create_thread_config,
            get_langgraph_service,
        )

        langgraph_service = get_langgraph_service()

        config: dict[str, Any] = create_thread_config(thread_id, user)
        config["configurable"]["checkpoint_id"] = checkpoint_id
        if checkpoint_ns:
            config["configurable"]["checkpoint_ns"] = checkpoint_ns

        try:
            async with langgraph_service.get_graph(
                graph_id,
                config=config,
                access_context="threads.read",
                user=user,
            ) as agent:
                agent = agent.with_config(config)
                state_snapshot = await agent.aget_state(config, subgraphs=subgraphs or False)

                if not state_snapshot:
                    raise HTTPException(
                        404,
                        f"No state found at checkpoint '{checkpoint_id}' for thread '{thread_id}'",
                    )

                # Convert snapshot to ThreadCheckpoint using service
                thread_checkpoint = thread_state_service.convert_snapshot_to_thread_state(
                    state_snapshot,
                    thread_id,
                    subgraphs=subgraphs or False,
                )

                return thread_checkpoint
        except HTTPException:
            raise
        except Exception as e:
            logger.exception(
                "Failed to retrieve state at checkpoint '%s' for thread '%s'",
                checkpoint_id,
                thread_id,
            )
            raise HTTPException(
                500,
                f"Failed to retrieve state at checkpoint '{checkpoint_id}': {str(e)}",
            ) from e

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error retrieving checkpoint '%s' for thread '%s'", checkpoint_id, thread_id)
        raise HTTPException(500, f"Error retrieving checkpoint '{checkpoint_id}': {str(e)}") from e


@router.post("/threads/{thread_id}/state/checkpoint", response_model=ThreadState, responses={**NOT_FOUND})
async def get_thread_state_at_checkpoint_post(
    thread_id: str,
    request: ThreadCheckpointPostRequest,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> ThreadState:
    """Get the thread state at a specific checkpoint (POST variant).

    Identical to the GET checkpoint endpoint but accepts the checkpoint
    configuration in the request body. Useful when the checkpoint namespace
    contains characters that are awkward in URL paths.
    """
    checkpoint = request.checkpoint
    if not checkpoint.checkpoint_id:
        raise HTTPException(400, "checkpoint_id is required in checkpoint configuration")

    subgraphs = request.subgraphs
    checkpoint_ns = checkpoint.checkpoint_ns if checkpoint.checkpoint_ns else None

    output = await get_thread_state_at_checkpoint(
        thread_id,
        checkpoint.checkpoint_id,
        subgraphs,
        checkpoint_ns,
        user,
        session,
    )
    return output


@router.post("/threads/{thread_id}/history", response_model=list[ThreadState], responses={**NOT_FOUND})
async def get_thread_history_post(
    thread_id: str,
    request: ThreadHistoryRequest,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[ThreadState]:
    """Get the checkpoint history for a thread (POST variant).

    Returns a list of past states ordered from newest to oldest. Use `limit`
    to control how many states are returned and `before` to paginate.
    """
    try:
        limit = request.limit or 10
        if not isinstance(limit, int) or limit < 1 or limit > 1000:
            raise HTTPException(422, "Invalid limit; must be an integer between 1 and 1000")

        before = request.before
        metadata = request.metadata
        checkpoint = request.checkpoint or {}
        subgraphs = bool(request.subgraphs) if request.subgraphs is not None else False
        checkpoint_ns = request.checkpoint_ns

        stmt = select(ThreadORM).where(ThreadORM.thread_id == thread_id, ThreadORM.user_id == user.identity)
        thread = await session.scalar(stmt)
        if not thread:
            raise HTTPException(404, f"Thread '{thread_id}' not found")

        thread_metadata = thread.metadata_json or {}
        graph_id = thread_metadata.get("graph_id")
        if not graph_id:
            logger.info(f"history POST: no graph_id set for thread {thread_id}")
            return []

        from aegra_api.services.langgraph_service import (
            create_thread_config,
            get_langgraph_service,
        )

        langgraph_service = get_langgraph_service()

        config: dict[str, Any] = create_thread_config(thread_id, user)
        if checkpoint:
            cfg_cp = strip_pinned_config_keys(checkpoint)
            if checkpoint_ns is not None:
                cfg_cp.setdefault("checkpoint_ns", checkpoint_ns)
            config["configurable"].update(cfg_cp)
        elif checkpoint_ns is not None:
            config["configurable"]["checkpoint_ns"] = checkpoint_ns

        # Convert `before` to a RunnableConfig for aget_state_history.
        # The SDK sends `before` as either a checkpoint ID string, a raw
        # checkpoint dict, or a full RunnableConfig with a "configurable" key.
        # No thread_id scrub here: aget_state_history reads only checkpoint_id
        # from `before` (the thread comes from the main config, pinned above).
        before_config: dict[str, Any] | None = None
        if isinstance(before, str):
            before_config = {"configurable": {"checkpoint_id": before}}
        elif isinstance(before, dict):
            before_config = before if "configurable" in before else {"configurable": before}

        state_snapshots = []
        kwargs: dict[str, Any] = {
            "limit": limit,
            "before": before_config,
        }
        if metadata is not None:
            kwargs["metadata"] = metadata

        async with langgraph_service.get_graph(
            graph_id,
            config=config,
            access_context="threads.read",
            user=user,
        ) as agent:
            # Some LangGraph versions support subgraphs flag; pass if available
            try:
                async for snapshot in agent.aget_state_history(config, subgraphs=subgraphs, **kwargs):
                    state_snapshots.append(snapshot)
            except TypeError:
                # Fallback if subgraphs not supported in this version
                async for snapshot in agent.aget_state_history(config, **kwargs):
                    state_snapshots.append(snapshot)

        # Convert outside the async with so the graph context is closed first
        thread_states = thread_state_service.convert_snapshots_to_thread_states(state_snapshots, thread_id)

        return thread_states

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error in history POST for thread %s", thread_id)
        msg = str(e).lower()
        if "not found" in msg or "no checkpoint" in msg:
            return []
        raise HTTPException(500, f"Error retrieving thread history: {str(e)}") from e


@router.get("/threads/{thread_id}/history", response_model=list[ThreadState], responses={**NOT_FOUND})
async def get_thread_history_get(
    thread_id: str,
    limit: int = Query(10, ge=1, le=1000, description="Number of states to return"),
    before: str | None = Query(None, description="Return states before this checkpoint ID"),
    subgraphs: bool | None = Query(False, description="Include states from subgraphs"),
    checkpoint_ns: str | None = Query(None, description="Checkpoint namespace"),
    metadata: str | None = Query(None, description="JSON-encoded metadata filter"),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[ThreadState]:
    """Get the checkpoint history for a thread.

    Returns a list of past states ordered from newest to oldest. Use `limit`
    to control how many states are returned and `before` to paginate.
    """
    parsed_metadata: dict[str, Any] | None = None
    if metadata:
        try:
            parsed_metadata = json.loads(metadata)
            if not isinstance(parsed_metadata, dict):
                raise ValueError("metadata must be a JSON object")
        except Exception as e:
            raise HTTPException(422, f"Invalid metadata query param: {e}") from e
    req = ThreadHistoryRequest(
        limit=limit,
        before=before,
        metadata=parsed_metadata,
        checkpoint=None,
        subgraphs=subgraphs,
        checkpoint_ns=checkpoint_ns,
    )
    return await get_thread_history_post(thread_id, req, user, session)
