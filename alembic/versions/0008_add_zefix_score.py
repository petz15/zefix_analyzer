"""Add zefix_score column to companies table.

Revision ID: 0008
Revises: 0007
Create Date: 2026-03-10
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: Union[str, None] = "0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("companies", sa.Column("zefix_score", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "zefix_score")
