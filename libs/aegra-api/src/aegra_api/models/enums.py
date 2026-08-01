"""Wire enums, each mirroring the same-named Literal in ``langgraph_sdk.schema``.

Kept in one module so an SDK upgrade is a single diff here and every request
model that references these follows automatically.
"""

from typing import Literal, get_args

# --- Entity state ---

RunStatus = Literal["pending", "running", "error", "success", "timeout", "interrupted"]

ThreadStatus = Literal["idle", "busy", "interrupted", "error"]

# Derived from RunStatus rather than spelled out, so a new status cannot be
# missed by whichever set it belongs to.
ACTIVE_RUN_STATUSES = frozenset({"pending", "running"})
TERMINAL_RUN_STATUSES = frozenset(get_args(RunStatus)) - ACTIVE_RUN_STATUSES

# --- Behavior ---

# How to handle a new run while the thread already has one in flight.
MultitaskStrategy = Literal["reject", "interrupt", "rollback", "enqueue"]

DisconnectMode = Literal["cancel", "continue"]

OnConflictBehavior = Literal["raise", "do_nothing"]

# Whether a stateless run's ephemeral thread survives the run.
OnCompletionBehavior = Literal["delete", "keep"]

IfNotExists = Literal["create", "reject"]

# Wildcard selector for interrupt_before / interrupt_after.
All = Literal["*"]
ALL_NODES: str = get_args(All)[0]

# When checkpoints are flushed relative to the step that produced them.
Durability = Literal["sync", "async", "exit"]

CancelAction = Literal["interrupt", "rollback"]

BulkCancelRunsStatus = Literal["pending", "running", "all"]

PruneStrategy = Literal["delete", "keep_latest"]

# --- Ordering and projection ---

SortOrder = Literal["asc", "desc"]

AssistantSortBy = Literal["assistant_id", "graph_id", "name", "created_at", "updated_at"]

ThreadSortBy = Literal["thread_id", "status", "created_at", "updated_at", "state_updated_at"]

CronSortBy = Literal[
    "cron_id",
    "assistant_id",
    "thread_id",
    "created_at",
    "updated_at",
    "next_run_date",
    "end_time",
]

AssistantSelectField = Literal[
    "assistant_id",
    "graph_id",
    "name",
    "description",
    "config",
    "context",
    "created_at",
    "updated_at",
    "metadata",
    "version",
]

ThreadSelectField = Literal[
    "thread_id",
    "created_at",
    "updated_at",
    "metadata",
    "config",
    "context",
    "status",
    "values",
    "interrupts",
]

RunSelectField = Literal[
    "run_id",
    "thread_id",
    "assistant_id",
    "created_at",
    "updated_at",
    "status",
    "metadata",
    "kwargs",
    "multitask_strategy",
]

CronSelectField = Literal[
    "cron_id",
    "assistant_id",
    "thread_id",
    "end_time",
    "schedule",
    "timezone",
    "created_at",
    "updated_at",
    "user_id",
    "payload",
    "next_run_date",
    "metadata",
    "now",
    "on_run_completed",
    "enabled",
]

# --- Streaming ---

StreamMode = Literal[
    "values",
    "messages",
    "updates",
    "events",
    "tasks",
    "checkpoints",
    "debug",
    "custom",
    "messages-tuple",
]

# Thread-level modes; deliberately disjoint from the run-level set above.
ThreadStreamMode = Literal["run_modes", "lifecycle", "state_update"]
