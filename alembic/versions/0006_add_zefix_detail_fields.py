"""Add Zefix administrative detail fields to companies table.

Revision ID: 0006
Revises: 0005
Create Date: 2026-03-10
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("companies", sa.Column("ehraid", sa.String(64), nullable=True))
    op.add_column("companies", sa.Column("chid", sa.String(32), nullable=True))
    op.add_column("companies", sa.Column("legal_seat_id", sa.Integer(), nullable=True))
    op.add_column("companies", sa.Column("legal_form_id", sa.Integer(), nullable=True))
    op.add_column("companies", sa.Column("legal_form_uid", sa.String(64), nullable=True))
    op.add_column("companies", sa.Column("legal_form_short_name", sa.String(32), nullable=True))
    op.add_column("companies", sa.Column("sogc_date", sa.String(32), nullable=True))
    op.add_column("companies", sa.Column("deletion_date", sa.String(32), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "deletion_date")
    op.drop_column("companies", "sogc_date")
    op.drop_column("companies", "legal_form_short_name")
    op.drop_column("companies", "legal_form_uid")
    op.drop_column("companies", "legal_form_id")
    op.drop_column("companies", "legal_seat_id")
    op.drop_column("companies", "chid")
    op.drop_column("companies", "ehraid")
