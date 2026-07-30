"""Runtime alignment + perf schema, squashed from five sequential migrations.

Consolidates what shipped as e5b9d2f7a3c1 → a7c3e1f9b2d4 → b8d4f2a1c3e5 →
c9e5a3f1d2b7 → d1f7b3a9c5e2. The revision id below is deliberately the **last**
of that chain, so a database already at head sees no new work; only the four
intermediate ids disappear, which is safe because all five landed in one commit
and were never released in a tag.

What it does, in dependency order:

1. ``runs`` gains ``metadata`` (JSONB), ``multitask_strategy``, ``scheduled_at``
   and ``cancel_requested``; ``thread`` gains ``ttl``. The SDK ``Run`` contract
   needs the first two as real, selectable columns; ``cancel_requested`` makes
   cancellation durable instead of depending on a fire-and-forget pub/sub.
2. ``thread_state`` (1:1 with ``thread``) — the latest materialized state moves
   out of the wide ``thread`` row into a narrow table, backfilled before the old
   ``thread.values``/``interrupts`` columns are dropped. The checkpointer stays
   the source of truth.
3. ``webhook_deliveries`` — a transactional outbox, so a worker crash during the
   retry window can no longer lose a run-completion notification.
4. Index hygiene: composite + GIN indexes for the list/search hot paths, the
   partial unique index enforcing one running run per thread (the ORM has long
   declared it and the worker's claim path catches its IntegrityError, but no
   migration ever created it), a partial index for TTL sweeping, and the removal
   of three single-column indexes that are exact left-prefixes of a composite.

Ordering constraints that shaped the grouping below: the backfill must read
``thread.values`` before step 3 drops it, and the duplicate-``running`` cleanup
must commit before the unique index is built or the build leaves an INVALID
index behind. Everything is ``IF [NOT] EXISTS`` guarded and every concurrent
build sits in an ``autocommit_block``, so the whole migration is idempotent and
resumable — including after an interrupted ``CONCURRENTLY`` build.

Revision ID: d1f7b3a9c5e2
Revises: b88bb61be638
Create Date: 2026-07-19 03:00:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "d1f7b3a9c5e2"
down_revision = "b88bb61be638"
branch_labels = None
depends_on = None

_UQ_NAME = "uq_runs_one_running_per_thread"
_TTL_INDEX = "idx_runs_ttl_sweep"
_TERMINAL = "('success', 'error', 'interrupted', 'timeout')"

_THREAD_HAS_VALUES = sa.text(
    "SELECT 1 FROM information_schema.columns WHERE table_name = 'thread' AND column_name = 'values'"
)

_BACKFILL = sa.text(
    """
    INSERT INTO thread_state (thread_id, "values", interrupts, values_hash, updated_at)
    -- values_hash left NULL: SQL md5(jsonb::text) can't match the app's
    -- md5(json.dumps(sort_keys=True)), so let the first materialize recompute it.
    SELECT t.thread_id, t."values", t.interrupts, NULL, t.updated_at
    FROM thread t
    WHERE (t."values" IS NOT NULL OR t.interrupts IS NOT NULL)
      AND NOT EXISTS (SELECT 1 FROM thread_state ts WHERE ts.thread_id = t.thread_id)
    LIMIT 1000
    """
)

# Keep the most recent running run per thread; demote the rest. Only touches
# threads that already hold >1 running row (the invariant the unique index adds).
_DEDUP_RUNNING = """
    UPDATE runs SET
        status = 'error',
        error_message = COALESCE(error_message, 'superseded: duplicate running run per thread'),
        updated_at = now()
    WHERE run_id IN (
        SELECT run_id FROM (
            SELECT run_id,
                   row_number() OVER (PARTITION BY thread_id ORDER BY created_at DESC, run_id DESC) AS rn
            FROM runs
            WHERE status = 'running'
        ) ranked
        WHERE ranked.rn > 1
    )
"""

_INDEX_CREATE = (
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_thread_user_created ON thread (user_id, created_at DESC)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_thread_created ON runs (thread_id, created_at DESC)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cron_metadata_gin ON crons USING gin (metadata jsonb_path_ops)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_thread_state_values_gin "
    'ON thread_state USING gin ("values" jsonb_path_ops)',
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_scheduled ON runs (status, scheduled_at)",
)
_INDEX_NAMES = (
    "idx_thread_user_created",
    "idx_runs_thread_created",
    "idx_cron_metadata_gin",
    "idx_thread_state_values_gin",
    "idx_runs_scheduled",
)

# (name, create_stmt): dropped on upgrade, restored on downgrade from one source.
_REDUNDANT = (
    ("idx_thread_user", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_thread_user ON thread (user_id)"),
    ("idx_runs_thread_id", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_thread_id ON runs (thread_id)"),
    ("idx_assistant_user", "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_assistant_user ON assistant (user_id)"),
)


def upgrade() -> None:
    # 1. Additive columns and tables, transactionally.
    op.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb")
    op.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS multitask_strategy TEXT")
    op.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMPTZ")
    op.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT false")
    op.execute("ALTER TABLE thread ADD COLUMN IF NOT EXISTS ttl JSONB")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS thread_state (
            thread_id   TEXT PRIMARY KEY REFERENCES thread(thread_id) ON DELETE CASCADE,
            "values"    JSONB,
            interrupts  JSONB,
            values_hash TEXT,
            updated_at  TIMESTAMPTZ DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS webhook_deliveries (
            id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
            run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
            url TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    # Empty table on creation, so a plain (non-concurrent) build costs nothing.
    op.execute("CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_due ON webhook_deliveries (status, next_attempt_at)")

    # 2. Backfill + concurrent builds. Autocommit so batches commit incrementally
    #    and CONCURRENTLY takes only SHARE UPDATE EXCLUSIVE (no write stall).
    with op.get_context().autocommit_block():
        bind = op.get_bind()
        if bind.execute(_THREAD_HAS_VALUES).first():
            while bind.execute(_BACKFILL).rowcount:
                pass
        for stmt in _INDEX_CREATE:
            op.execute(stmt)

    # 3. Only now is it safe to drop what the backfill read.
    op.execute('ALTER TABLE thread DROP COLUMN IF EXISTS "values"')
    op.execute("ALTER TABLE thread DROP COLUMN IF EXISTS interrupts")

    # 4. Resolve the illegal duplicates transactionally; a concurrent unique build
    #    over them would fail and leave an INVALID index.
    op.execute(_DEDUP_RUNNING)

    # 5. Unique invariant, TTL sweep index, and pruning of redundant prefixes.
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_UQ_NAME}")
        op.execute(
            f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {_UQ_NAME} ON runs (thread_id) WHERE status = 'running'"
        )
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_TTL_INDEX} ON runs (updated_at) WHERE status IN {_TERMINAL}"
        )
        for name, _ in _REDUNDANT:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for _, create_stmt in _REDUNDANT:
            op.execute(create_stmt)
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_TTL_INDEX}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_UQ_NAME}")

    op.execute("DROP TABLE IF EXISTS webhook_deliveries")

    # Restore the wide columns from thread_state before dropping it.
    op.execute('ALTER TABLE thread ADD COLUMN IF NOT EXISTS "values" JSONB')
    op.execute("ALTER TABLE thread ADD COLUMN IF NOT EXISTS interrupts JSONB")
    op.execute(
        'UPDATE thread t SET "values" = ts."values", interrupts = ts.interrupts '
        "FROM thread_state ts WHERE ts.thread_id = t.thread_id"
    )
    with op.get_context().autocommit_block():
        for name in _INDEX_NAMES:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
    op.execute("DROP TABLE IF EXISTS thread_state")

    op.execute("ALTER TABLE thread DROP COLUMN IF EXISTS ttl")
    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS cancel_requested")
    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS scheduled_at")
    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS multitask_strategy")
    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS metadata")
