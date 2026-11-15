"""add correlation fields for distributed logging"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "9a1b2c3d4e5f"
down_revision: Union[str, Sequence[str], None] = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("request_id", sa.String(length=100), nullable=True))
    op.create_index("ix_jobs_request_id", "jobs", ["request_id"])
    op.add_column("outbox_events", sa.Column("request_id", sa.String(length=100), nullable=True))
    op.add_column("outbox_events", sa.Column("client_id", sa.String(length=100), nullable=True))


def downgrade() -> None:
    op.drop_column("outbox_events", "client_id")
    op.drop_column("outbox_events", "request_id")
    op.drop_index("ix_jobs_request_id", table_name="jobs")
    op.drop_column("jobs", "request_id")
