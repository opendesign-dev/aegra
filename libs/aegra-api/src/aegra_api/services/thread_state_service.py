"""Thread state services: snapshot conversion, and materializing state onto the row."""

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.serializers import GeneralSerializer, LangGraphSerializer
from aegra_api.models.auth import User
from aegra_api.models.threads import ThreadCheckpoint, ThreadState
from aegra_api.services.run_status import materialize_thread_state

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig

logger = structlog.getLogger(__name__)


async def refresh_materialized_state(session: AsyncSession, thread: ThreadORM, user: User) -> None:
    """Best-effort refresh of the thread row's materialized values/interrupts.

    Feeds thread search's ``values`` filter and ``select``. A read failure must
    not fail the state update that triggered it, so errors are swallowed.
    """
    graph_id = (thread.metadata_json or {}).get("graph_id")
    if not graph_id:
        return
    from aegra_api.services.langgraph_service import create_thread_config, get_langgraph_service

    serializer = GeneralSerializer()
    service = get_langgraph_service()
    raw_config = create_thread_config(thread.thread_id, user)
    config = cast("RunnableConfig", raw_config)
    try:
        async with service.get_graph(graph_id, config=raw_config, access_context="threads.read", user=user) as agent:
            state = await agent.aget_state(config)
        values = serializer.serialize(state.values) if state.values is not None else None
        interrupts = LangGraphSerializer().build_interrupts_map(state)
    except Exception as exc:
        logger.warning("Could not materialize thread state", thread_id=thread.thread_id, error=str(exc))
        return
    if isinstance(values, dict):
        await materialize_thread_state(session, thread.thread_id, values, interrupts)
    thread.updated_at = datetime.now(UTC)
    await session.commit()


class ThreadStateService:
    """Service for converting LangGraph snapshots to ThreadState objects"""

    def __init__(self) -> None:
        self.serializer = LangGraphSerializer()

    def convert_snapshot_to_thread_state(self, snapshot: Any, thread_id: str, subgraphs: bool = False) -> ThreadState:
        """Convert a LangGraph snapshot to ThreadState format"""
        try:
            # Extract basic values
            values = getattr(snapshot, "values", {})
            next_nodes = getattr(snapshot, "next", []) or []
            metadata = getattr(snapshot, "metadata", {}) or {}
            created_at = self._extract_created_at(snapshot)

            # Extract tasks and interrupts using serializer
            tasks = self.serializer.extract_tasks_from_snapshot(snapshot)

            # Recursively serialize tasks' state (which might be subgraphs)
            if subgraphs:
                for task in tasks:
                    if "state" in task and task["state"] is not None:
                        try:
                            task["state"] = self.convert_snapshot_to_thread_state(
                                task["state"], thread_id, subgraphs=True
                            )
                        except Exception as e:
                            logger.error(f"Failed to serialize subgraph state for task {task.get('id')}: {e}")
                            task["state"] = None

            interrupts = self.serializer.extract_interrupts_from_snapshot(snapshot)

            # Create checkpoint objects
            current_checkpoint = self._create_checkpoint(snapshot.config, thread_id)
            parent_checkpoint = (
                self._create_checkpoint(snapshot.parent_config, thread_id) if snapshot.parent_config else None
            )

            # Extract checkpoint IDs for backward compatibility
            checkpoint_id = self._extract_checkpoint_id(snapshot.config)
            parent_checkpoint_id = (
                self._extract_checkpoint_id(snapshot.parent_config) if snapshot.parent_config else None
            )

            return ThreadState(
                values=values,
                next=next_nodes,
                tasks=tasks,
                interrupts=interrupts,
                metadata=metadata,
                created_at=created_at,
                checkpoint=current_checkpoint,
                parent_checkpoint=parent_checkpoint,
                checkpoint_id=checkpoint_id,
                parent_checkpoint_id=parent_checkpoint_id,
            )

        except Exception as e:
            logger.error(
                f"Failed to convert snapshot to thread state: {e} "
                f"(thread_id={thread_id}, snapshot_type={type(snapshot).__name__})"
            )
            raise

    def convert_snapshots_to_thread_states(self, snapshots: list[Any], thread_id: str) -> list[ThreadState]:
        """Convert multiple snapshots to ThreadState objects"""
        thread_states = []

        for i, snapshot in enumerate(snapshots):
            try:
                thread_state = self.convert_snapshot_to_thread_state(snapshot, thread_id)
                thread_states.append(thread_state)
            except Exception as e:
                logger.error(f"Failed to convert snapshot in batch: {e} (thread_id={thread_id}, snapshot_index={i})")
                # Continue with other snapshots rather than failing the entire batch
                continue

        return thread_states

    def _extract_created_at(self, snapshot: Any) -> datetime | None:
        """Extract created_at timestamp from snapshot"""
        created_at = getattr(snapshot, "created_at", None)
        if isinstance(created_at, str):
            try:
                return datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            except ValueError:
                logger.warning(f"Invalid created_at format: {created_at}")
                return None
        elif isinstance(created_at, datetime):
            return created_at
        return None

    def _create_checkpoint(self, config: Any, thread_id: str) -> ThreadCheckpoint:
        """Create ThreadCheckpoint from config"""
        if not config or not isinstance(config, dict):
            return ThreadCheckpoint(checkpoint_id=None, thread_id=thread_id, checkpoint_ns="")

        configurable = config.get("configurable", {})
        checkpoint_id = configurable.get("checkpoint_id")
        checkpoint_ns = configurable.get("checkpoint_ns", "")

        return ThreadCheckpoint(
            checkpoint_id=checkpoint_id,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
        )

    def _extract_checkpoint_id(self, config: Any) -> str | None:
        """Extract checkpoint ID from config for backward compatibility"""
        if not config or not isinstance(config, dict):
            return None

        configurable = config.get("configurable", {})
        checkpoint_id = configurable.get("checkpoint_id")
        return str(checkpoint_id) if checkpoint_id is not None else None
