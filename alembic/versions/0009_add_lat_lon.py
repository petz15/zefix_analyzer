"""Add geocoded lat/lon columns to companies table.

Revision ID: 0009
Revises: 0008
Create Date: 2026-03-10
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: Union[str, None] = "0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("companies", sa.Column("lat", sa.Float(), nullable=True))
    op.add_column("companies", sa.Column("lon", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "lon")
    op.drop_column("companies", "lat")
