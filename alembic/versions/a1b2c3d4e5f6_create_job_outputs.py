"""create job outputs

Revision ID: a1b2c3d4e5f6
Revises: 4c7d2e9a1f10
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "4c7d2e9a1f10"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_outputs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("operation_key", sa.String(length=255), nullable=False),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("operation_key"),
    )


def downgrade() -> None:
    op.drop_table("job_outputs")
