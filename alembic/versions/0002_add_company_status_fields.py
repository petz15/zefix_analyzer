"""Add google search results, match score, review_status, proposal_status to companies.

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-06
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("companies", sa.Column("google_search_results_raw", sa.Text(), nullable=True))
    op.add_column("companies", sa.Column("website_match_score", sa.Integer(), nullable=True))
    op.add_column("companies", sa.Column("review_status", sa.String(length=32), nullable=True))
    op.add_column("companies", sa.Column("proposal_status", sa.String(length=32), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "proposal_status")
    op.drop_column("companies", "review_status")
    op.drop_column("companies", "website_match_score")
    op.drop_column("companies", "google_search_results_raw")
