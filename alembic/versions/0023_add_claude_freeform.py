"""Add claude_freeform column to companies.

Revision ID: 0023
Revises: 0022
Create Date: 2026-03-22
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0023"
down_revision: Union[str, None] = "0022"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("companies", sa.Column("claude_freeform", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "claude_freeform")
