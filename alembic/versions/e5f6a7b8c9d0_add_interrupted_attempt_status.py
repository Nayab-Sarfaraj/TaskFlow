"""add interrupted job attempt status

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
"""

from typing import Sequence, Union

from alembic import op


revision: str = "e5f6a7b8c9d0"
down_revision: Union[str, Sequence[str], None] = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TYPE job_attempt_status ADD VALUE IF NOT EXISTS 'interrupted'"
    )


def downgrade() -> None:
    # PostgreSQL does not support removing an enum value directly.
    pass
