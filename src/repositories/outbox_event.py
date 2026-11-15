from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete, func, select, update
from src.models.outbox_events import OutboxEvent
from src.models.jobs import Job


async def fetch_unpublished_events(db: AsyncSession, batch_size: int = 100):
    if batch_size <= 0:
        return []

    high_priority_count = max(1, (batch_size * 70) // 100)
    low_priority_count = batch_size - high_priority_count
    # Select one unpublished outbox event per job before applying the priority
    # limits. This prevents duplicate outbox rows for one job consuming slots.
    unique_events = (
        select(func.min(OutboxEvent.id).label("event_id"))
        .where(OutboxEvent.published_at.is_(None))
        .group_by(OutboxEvent.aggregate_id)
        .subquery()
    )

    def priority_statement(descending: bool, batch_size: int):
        priority_order = Job.priority.desc() if descending else Job.priority.asc()
        return (
            select(OutboxEvent)
            .join(Job, Job.id == OutboxEvent.aggregate_id)
            .join(unique_events, unique_events.c.event_id == OutboxEvent.id)
            .order_by(priority_order, OutboxEvent.created_at, OutboxEvent.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True, of=OutboxEvent)
        )

    high_result = await db.execute(
        priority_statement(
            descending=True,
            batch_size=high_priority_count,
        )
    )
    low_result = await db.execute(
        priority_statement(
            descending=False,
            batch_size=low_priority_count,
        )
    )

    events = []
    seen_job_ids = set()
    for event in [*high_result.scalars().all(), *low_result.scalars().all()]:
        if event.aggregate_id in seen_job_ids:
            continue
        seen_job_ids.add(event.aggregate_id)
        events.append(event)
    return events
