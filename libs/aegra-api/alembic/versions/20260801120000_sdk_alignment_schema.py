"""Schema for SDK data-plane alignment.

Three things, all additive:

1. ``runs.dispatched`` — false while a run is held back by
   ``multitask_strategy="enqueue"``. The drain claims the next queued run with a
   conditional UPDATE on this column, which is what makes hand-off idempotent
   when two runs on the same thread finalize concurrently. Defaults to true so
   every existing row reads as already dispatched.
2. A partial index for that queue lookup, keyed the way the drain queries it.
3. Rebuilds the two GIN indexes ``f2c8a5e13d94`` dropped as unused. They have
   query sites again: ``POST /threads/search`` filters ``values`` against
   ``thread_state``, and cron search filters on ``metadata``.

Every statement is ``IF [NOT] EXISTS`` guarded and the concurrent builds sit in
an autocommit block, so the migration is idempotent and resumable — including
after an interrupted ``CONCURRENTLY`` build, which leaves an INVALID index that
the guarded ``DROP``/rebuild pair clears.

Revision ID: a3d6e0b95f17
Revises: f2c8a5e13d94
Create Date: 2026-08-01 12:00:00.000000
"""

from alembic import op

revision = "a3d6e0b95f17"
down_revision = "f2c8a5e13d94"
branch_labels = None
depends_on = None

_QUEUE_INDEX = "idx_runs_thread_queued"

# (name, build statement) pairs, so upgrade and downgrade share one definition.
_CONCURRENT = (
    (
        _QUEUE_INDEX,
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_QUEUE_INDEX} ON runs (thread_id, created_at) "
        "WHERE dispatched = false",
    ),
    (
        "idx_thread_state_values_gin",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_thread_state_values_gin "
        'ON thread_state USING gin ("values" jsonb_path_ops)',
    ),
    (
        "idx_cron_metadata_gin",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cron_metadata_gin ON crons USING gin (metadata jsonb_path_ops)",
    ),
)


def upgrade() -> None:
    op.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS dispatched BOOLEAN NOT NULL DEFAULT true")

    with op.get_context().autocommit_block():
        for name, build in _CONCURRENT:
            # A previously interrupted CONCURRENTLY build leaves an INVALID index
            # that IF NOT EXISTS would happily keep; drop first so it is rebuilt.
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
            op.execute(build)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for name, _build in _CONCURRENT:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")

    op.execute("ALTER TABLE runs DROP COLUMN IF EXISTS dispatched")
