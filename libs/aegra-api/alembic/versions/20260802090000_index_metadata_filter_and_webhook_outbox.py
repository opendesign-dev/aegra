"""Index the run metadata filter and the webhook outbox sweep.

Two indexes whose query sites exist again:

1. ``idx_runs_metadata_gin`` — ``GET /threads/{id}/runs`` filters ``runs.metadata``
   by JSONB containment. Cron-fired runs inherit the schedule's metadata plus a
   ``cron_id`` key, so "every run this schedule produced" is a containment probe
   on this index.
2. ``idx_webhook_deliveries_due`` — the sweeper claims by
   ``(status, next_attempt_at)``. ``f2c8a5e13d94`` dropped it when delivery was
   removed; delivery is back and the ORM declares the index again, so existing
   databases need it rebuilt.

Every statement is ``IF [NOT] EXISTS`` guarded and the builds sit in an
autocommit block, so the migration is idempotent and resumable — including after
an interrupted ``CONCURRENTLY`` build, which leaves an INVALID index that the
guarded ``DROP``/rebuild pair clears.

Revision ID: b5e1c47a9d38
Revises: a3d6e0b95f17
Create Date: 2026-08-02 09:00:00.000000
"""

from alembic import op

revision = "b5e1c47a9d38"
down_revision = "a3d6e0b95f17"
branch_labels = None
depends_on = None

# (name, build statement) pairs, so upgrade and downgrade share one definition.
_CONCURRENT = (
    (
        "idx_runs_metadata_gin",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_metadata_gin ON runs USING gin (metadata jsonb_path_ops)",
    ),
    (
        "idx_webhook_deliveries_due",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_webhook_deliveries_due "
        "ON webhook_deliveries (status, next_attempt_at)",
    ),
)


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for name, build in _CONCURRENT:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
            op.execute(build)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for name, _build in _CONCURRENT:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
