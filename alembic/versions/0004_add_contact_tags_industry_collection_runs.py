"""Add contact fields, tags, industry to companies; add collection_runs table.

Revision ID: 0004
Revises: 0003
Create Date: 2026-03-06
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("companies", sa.Column("contact_name", sa.String(length=256), nullable=True))
    op.add_column("companies", sa.Column("contact_email", sa.String(length=512), nullable=True))
    op.add_column("companies", sa.Column("contact_phone", sa.String(length=64), nullable=True))
    # Comma-separated free-form labels, e.g. "saas,b2b,lead"
    op.add_column("companies", sa.Column("tags", sa.Text(), nullable=True))
    op.add_column("companies", sa.Column("industry", sa.String(length=128), nullable=True))

    op.create_index("ix_companies_industry", "companies", ["industry"])

    op.create_table(
        "collection_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_type", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        # Checkpoint fields for resuming bulk imports
        sa.Column("last_canton", sa.String(length=8), nullable=True),
        sa.Column("last_offset", sa.Integer(), nullable=True),
        sa.Column("stats_json", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_collection_runs_id", "collection_runs", ["id"])
    op.create_index("ix_collection_runs_run_type", "collection_runs", ["run_type"])


def downgrade() -> None:
    op.drop_index("ix_collection_runs_run_type", table_name="collection_runs")
    op.drop_index("ix_collection_runs_id", table_name="collection_runs")
    op.drop_table("collection_runs")
    op.drop_index("ix_companies_industry", table_name="companies")
    op.drop_column("companies", "industry")
    op.drop_column("companies", "tags")
    op.drop_column("companies", "contact_phone")
    op.drop_column("companies", "contact_email")
    op.drop_column("companies", "contact_name")
