"""track the attempt that currently owns a job

Revision ID: 4c7d2e9a1f10
Revises: 9f3e8a1c2d4b
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "4c7d2e9a1f10"
down_revision: Union[str, Sequence[str], None] = "9f3e8a1c2d4b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("current_attempt_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_jobs_current_attempt_id",
        "jobs",
        "job_attempts",
        ["current_attempt_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint("fk_jobs_current_attempt_id", "jobs", type_="foreignkey")
    op.drop_column("jobs", "current_attempt_id")
