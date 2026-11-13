from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete, func, select, update
from src.models.outbox_events import OutboxEvent


async def fetch_unpublished_events(db: AsyncSession):
    statement = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.created_at)
        .limit(100)
        .with_for_update(skip_locked=True)
    )
    result = await db.execute(statement)
    return result.scalars().all()
