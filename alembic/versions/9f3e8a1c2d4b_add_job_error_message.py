"""add job error message

Revision ID: 9f3e8a1c2d4b
Revises: 81299037b661
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "9f3e8a1c2d4b"
down_revision: Union[str, Sequence[str], None] = "81299037b661"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("error_message", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("jobs", "error_message")
