"""change outbox_events client_id to integer

Revision ID: a648fc26209e
Revises: 1167a76638e2
Create Date: 2026-11-16 19:12:37.133138

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a648fc26209e'
down_revision: Union[str, Sequence[str], None] = '1167a76638e2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column(
        "outbox_events",
        "client_id",
        existing_type=sa.VARCHAR(length=100),
        type_=sa.Integer(),
        existing_nullable=True,
        postgresql_using="client_id::integer",
    )
    op.create_foreign_key(
        "fk_outbox_events_client_id",
        "outbox_events",
        "clients",
        ["client_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("fk_outbox_events_client_id", "outbox_events", type_="foreignkey")
    op.alter_column(
        "outbox_events",
        "client_id",
        existing_type=sa.Integer(),
        type_=sa.VARCHAR(length=100),
        existing_nullable=True,
    )
