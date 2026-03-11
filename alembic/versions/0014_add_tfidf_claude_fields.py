"""Add TF-IDF cluster and Claude classification fields to companies table.

Revision ID: 0014
Revises: 0013
Create Date: 2026-03-11
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0014"
down_revision: Union[str, None] = "0013"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("companies", sa.Column("tfidf_cluster", sa.String(128), nullable=True))
    op.add_column("companies", sa.Column("claude_score", sa.Integer(), nullable=True))
    op.add_column("companies", sa.Column("claude_category", sa.String(128), nullable=True))
    op.add_column("companies", sa.Column("claude_scored_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "claude_scored_at")
    op.drop_column("companies", "claude_category")
    op.drop_column("companies", "claude_score")
    op.drop_column("companies", "tfidf_cluster")
