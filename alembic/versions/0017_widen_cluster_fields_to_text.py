"""Widen tfidf_cluster and purpose_keywords from String(512) to Text.

With max_clusters_per_company=7 and multi-term labels, tfidf_cluster easily
exceeds 512 characters. purpose_keywords similarly has no natural cap.

Revision ID: 0017
Revises: 0016
Create Date: 2026-03-19
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0017"
down_revision: Union[str, None] = "0016"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("companies") as batch_op:
        batch_op.alter_column(
            "tfidf_cluster",
            existing_type=sa.String(512),
            type_=sa.Text(),
            existing_nullable=True,
        )
        batch_op.alter_column(
            "purpose_keywords",
            existing_type=sa.String(512),
            type_=sa.Text(),
            existing_nullable=True,
        )


def downgrade() -> None:
    with op.batch_alter_table("companies") as batch_op:
        batch_op.alter_column(
            "tfidf_cluster",
            existing_type=sa.Text(),
            type_=sa.String(512),
            existing_nullable=True,
        )
        batch_op.alter_column(
            "purpose_keywords",
            existing_type=sa.Text(),
            type_=sa.String(512),
            existing_nullable=True,
        )
