"""add job type to job outputs

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "f6a7b8c9d0e1"
down_revision: Union[str, Sequence[str], None] = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    job_type = postgresql.ENUM(
        "video",
        "email",
        "report",
        "scraping",
        name="job_type",
        create_type=False,
    )
    op.add_column(
        "job_outputs",
        sa.Column("job_type", job_type, nullable=True),
    )
    op.execute(
        sa.text(
            """
            UPDATE job_outputs AS outputs
            SET job_type = jobs.job_type
            FROM jobs
            WHERE jobs.id = outputs.job_id
            """
        )
    )
    op.alter_column("job_outputs", "job_type", nullable=False)


def downgrade() -> None:
    op.drop_column("job_outputs", "job_type")
