
from src.repositories import outbox_event as outbox_event_repository

from sqlalchemy.ext.asyncio import AsyncSession

async def fetch_unpublished_events(db: AsyncSession):
    return await outbox_event_repository.fetch_unpublished_events(db)
    
