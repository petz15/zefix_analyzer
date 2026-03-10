"""Add extended Zefix detail fields to companies table.

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
    op.add_column("companies", sa.Column("sogc_pub", sa.Text(), nullable=True))
    op.add_column("companies", sa.Column("capital_nominal", sa.String(64), nullable=True))
    op.add_column("companies", sa.Column("capital_currency", sa.String(16), nullable=True))
    op.add_column("companies", sa.Column("head_offices", sa.Text(), nullable=True))
    op.add_column("companies", sa.Column("further_head_offices", sa.Text(), nullable=True))
    op.add_column("companies", sa.Column("branch_offices", sa.Text(), nullable=True))
    op.add_column("companies", sa.Column("has_taken_over", sa.Text(), nullable=True))
    op.add_column("companies", sa.Column("was_taken_over_by", sa.Text(), nullable=True))
    op.add_column("companies", sa.Column("audit_companies", sa.Text(), nullable=True))
    op.add_column("companies", sa.Column("old_names", sa.Text(), nullable=True))
    op.add_column("companies", sa.Column("cantonal_excerpt_web", sa.String(1024), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "cantonal_excerpt_web")
    op.drop_column("companies", "old_names")
    op.drop_column("companies", "audit_companies")
    op.drop_column("companies", "was_taken_over_by")
    op.drop_column("companies", "has_taken_over")
    op.drop_column("companies", "branch_offices")
    op.drop_column("companies", "further_head_offices")
    op.drop_column("companies", "head_offices")
    op.drop_column("companies", "capital_currency")
    op.drop_column("companies", "capital_nominal")
    op.drop_column("companies", "sogc_pub")
