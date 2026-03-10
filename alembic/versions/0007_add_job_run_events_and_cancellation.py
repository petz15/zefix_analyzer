"""Add job run events and cancellation support.

Revision ID: 0007
Revises: 0006
Create Date: 2026-03-10
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: Union[str, None] = "0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("job_runs", sa.Column("cancel_requested", sa.Boolean(), nullable=False, server_default=sa.false()))

    op.create_table(
        "job_run_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("job_id", sa.Integer(), sa.ForeignKey("job_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False, server_default="info"),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_job_run_events_id", "job_run_events", ["id"])
    op.create_index("ix_job_run_events_job_id", "job_run_events", ["job_id"])


def downgrade() -> None:
    op.drop_index("ix_job_run_events_job_id", table_name="job_run_events")
    op.drop_index("ix_job_run_events_id", table_name="job_run_events")
    op.drop_table("job_run_events")

    op.drop_column("job_runs", "cancel_requested")
