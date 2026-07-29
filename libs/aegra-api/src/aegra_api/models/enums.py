"""Status enums for Aegra API specification."""

from typing import Literal

# Run status enum
RunStatus = Literal[
    "pending",
    "running",
    "error",
    "success",
    "timeout",
    "interrupted",
]

# Thread status enum
ThreadStatus = Literal[
    "idle",
    "busy",
    "interrupted",
    "error",
]

# Single source of truth for terminal run states, so waiters/streaming/executor
# and finalize's compare-and-set can't drift (dropping "timeout" would hang /join).
TERMINAL_RUN_STATUSES: frozenset[str] = frozenset({"success", "error", "interrupted", "timeout"})

# Multitask strategy enum
MultitaskStrategy = Literal[
    "reject",
    "rollback",
    "interrupt",
    "enqueue",
]

# The literals below mirror langgraph-sdk exactly and are the single source of
# truth for these value sets; models/routes reference them instead of inlining.
# tests/contract/test_sdk_contract.py fails if any drifts from the SDK.

# Stream mode enum (SDK StreamMode)
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

# Behavior on client disconnect (SDK DisconnectMode)
DisconnectMode = Literal["cancel", "continue"]

# Behavior after a stateless run completes (SDK OnCompletionBehavior)
OnCompletionBehavior = Literal["delete", "keep"]

# Checkpoint durability (SDK Durability)
Durability = Literal["sync", "async", "exit"]

# Behavior when a resource already exists (SDK OnConflictBehavior; the if_exists param)
OnConflictBehavior = Literal["raise", "do_nothing"]

# Behavior when the target thread is missing (SDK IfNotExists)
IfNotExists = Literal["create", "reject"]

# Run cancel action (SDK CancelAction)
CancelAction = Literal["interrupt", "rollback"]

# Thread prune strategy (SDK PruneStrategy)
PruneStrategy = Literal["delete", "keep_latest"]

# Status filter for bulk run cancel (SDK BulkCancelRunsStatus)
BulkCancelRunsStatus = Literal["pending", "running", "all"]

# Field projections accepted by the ``select`` body param on each search endpoint.
# Must stay value-identical to the SDK: a client sending a field Aegra omits gets
# a 422 on a request the Platform would have served.

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

# Sort keys accepted by each search endpoint (SDK *SortBy). Runs have no SDK
# counterpart; Aegra's own set lives on RunSearchRequest.

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

SortOrder = Literal["asc", "desc"]
