"""create jobs

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", UUID, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("status", sa.Text, nullable=False, server_default="queued"),
        sa.Column("prompt", sa.Text, nullable=False),
        sa.Column("params", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("result", sa.Text),
        sa.Column("error", sa.Text),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer, nullable=False, server_default="3"),
        sa.Column("worker_id", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.clock_timestamp()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("status IN ('queued', 'running', 'done', 'failed')", name="jobs_status_valid"),
        sa.CheckConstraint(
            "status <> 'running' OR (worker_id IS NOT NULL AND heartbeat_at IS NOT NULL)",
            name="jobs_running_has_owner",
        ),
        sa.CheckConstraint("max_attempts > 0", name="jobs_max_attempts_positive"),
    )
    # Partial indexes: claim() only scans queued rows, the reaper only scans running ones.
    # Finished jobs pile up over time but never enter either index.
    op.create_index("jobs_queued_idx", "jobs", ["created_at"], postgresql_where=sa.text("status = 'queued'"))
    op.create_index("jobs_running_idx", "jobs", ["heartbeat_at"], postgresql_where=sa.text("status = 'running'"))


def downgrade() -> None:
    op.drop_table("jobs")
