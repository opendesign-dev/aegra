"""cron: track consecutive firing failures

Revision ID: e4a9c2d17b83
Revises: d1f7b3a9c5e2
Create Date: 2026-07-30 12:00:00.000000

Adds ``crons.failure_count``. The scheduler increments it when a firing fails and
resets it on success; once it reaches ``CRON_MAX_CONSECUTIVE_FAILURES`` the cron is
disabled instead of retrying every poll interval forever.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "e4a9c2d17b83"
down_revision = "d1f7b3a9c5e2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "crons",
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
    )


def downgrade() -> None:
    op.drop_column("crons", "failure_count")
