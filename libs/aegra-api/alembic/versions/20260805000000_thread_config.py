"""Persist the config a thread is bound to.

``thread.config`` is the assistant+request config the latest run merged, stored so
reads can reload the graph the way the run built it. Without it a factory graph that
branches on ``configurable`` compiles a different node set on ``GET /threads/{id}/state``,
and LangGraph — which re-derives ``tasks`` / ``interrupts`` / ``next`` from the loaded
nodes — reports no pending interrupt for a thread that is paused on one.

Matches the SDK's ``Thread.config`` field, already named in ``ThreadSelectField``.
Existing rows default to ``{}``; those threads fall back to the assistant's current
config until their next run writes this column.

Revision ID: c7f2a9b4e610
Revises: b5e1c47a9d38
Create Date: 2026-08-05 00:00:00.000000
"""

from alembic import op

revision = "c7f2a9b4e610"
down_revision = "b5e1c47a9d38"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE thread ADD COLUMN IF NOT EXISTS config JSONB NOT NULL DEFAULT '{}'::jsonb")


def downgrade() -> None:
    op.execute("ALTER TABLE thread DROP COLUMN IF EXISTS config")
