import asyncio
import json
import logging
from datetime import datetime, timezone

from src.db.database import SessionLocal
from src.services import outbox_event as outbox_event_service
from src.config.redis import get_redis
from src.logging_config import configure_root_logger

from sqlalchemy import delete, func, select, update
from src.models.jobs import Job, JobStatus
from src.models.outbox_events import OutboxEvent

logger = logging.getLogger(__name__)


async def run_scheduler_cycle(limit: int = 100) -> int:
    dispatched = 0
    for _ in range(limit):
        async with SessionLocal() as db:
            try:
                statement = (
                    select(Job)
                    .where(
                        Job.scheduled_at <= datetime.now(timezone.utc),
                        Job.status == JobStatus.RETRYING,
                    )
                    .order_by(Job.created_at)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                result = await db.execute(statement)
                job = result.scalar_one_or_none()

                if job is None:
                    break

                job_id = job.id
                scheduled_at = job.scheduled_at
                payload = job.payload

                update_job_statement = (
                    update(Job)
                    .where(
                        Job.status == JobStatus.RETRYING,
                        Job.id == job_id,
                    )
                    .values(status=JobStatus.QUEUED)
                )
                update_result = await db.execute(update_job_statement)
                if update_result.rowcount != 1:
                    await db.rollback()
                    logger.warning(
                        "Skipping job %s: it is no longer retrying",
                        job_id,
                    )
                    continue

                new_event = OutboxEvent(
                    aggregate_id=job_id,
                    event_type="job.created",
                    payload=payload,
                )
                db.add(new_event)
                await db.commit()
                dispatched += 1
                logger.info(
                    "Requeued job %s scheduled for %s and created outbox event %s",
                    job_id,
                    scheduled_at,
                    new_event.id,
                )
            except Exception:
                await db.rollback()
                logger.exception("Scheduler failed while requeueing a job")
    return dispatched


async def scheduler():
    while True:
        await run_scheduler_cycle()
        await asyncio.sleep(1)


if __name__ == "__main__":
    configure_root_logger()
    logger.info("Starting  scheduler")
    asyncio.run(scheduler())
