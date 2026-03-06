"""Add indexes on frequently-filtered company columns.

Revision ID: 0003
Revises: 0002
Create Date: 2026-03-06
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index("ix_companies_canton", "companies", ["canton"])
    op.create_index("ix_companies_review_status", "companies", ["review_status"])
    op.create_index("ix_companies_proposal_status", "companies", ["proposal_status"])
    op.create_index("ix_companies_website_checked_at", "companies", ["website_checked_at"])
    op.create_index("ix_companies_website_match_score", "companies", ["website_match_score"])


def downgrade() -> None:
    op.drop_index("ix_companies_website_match_score", table_name="companies")
    op.drop_index("ix_companies_website_checked_at", table_name="companies")
    op.drop_index("ix_companies_proposal_status", table_name="companies")
    op.drop_index("ix_companies_review_status", table_name="companies")
    op.drop_index("ix_companies_canton", table_name="companies")
