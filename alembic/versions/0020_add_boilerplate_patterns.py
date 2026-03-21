"""Add boilerplate_patterns table.

Revision ID: 0020
Revises: 0019
Create Date: 2026-03-21
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0020"
down_revision: Union[str, None] = "0019"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "boilerplate_patterns",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("pattern", sa.Text(), nullable=False),
        sa.Column("description", sa.String(length=512), nullable=True),
        sa.Column("example", sa.Text(), nullable=True),
        sa.Column("match_count", sa.Integer(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("pattern"),
    )
    op.create_index("ix_boilerplate_patterns_id", "boilerplate_patterns", ["id"])
    op.create_index("ix_boilerplate_patterns_active", "boilerplate_patterns", ["active"])


def downgrade() -> None:
    op.drop_index("ix_boilerplate_patterns_active", table_name="boilerplate_patterns")
    op.drop_index("ix_boilerplate_patterns_id", table_name="boilerplate_patterns")
    op.drop_table("boilerplate_patterns")
