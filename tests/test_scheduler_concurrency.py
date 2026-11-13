import asyncio
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from src.models.jobs import JobStatus
from src.models.outbox_events import OutboxEvent
from src.scheduler.scheduler import run_scheduler_cycle
from tests.test_phase5_acceptance import cleanup_jobs, create_job


@pytest.mark.asyncio
async def test_two_schedulers_create_only_one_dispatch_event(db_session):
    job = await create_job(db_session, status=JobStatus.RETRYING)
    job.scheduled_at = datetime.now(timezone.utc)
    await db_session.commit()

    results = await asyncio.gather(run_scheduler_cycle(), run_scheduler_cycle())
    events = (
        (
            await db_session.execute(
                select(OutboxEvent).where(OutboxEvent.aggregate_id == job.id)
            )
        )
        .scalars()
        .all()
    )

    assert sum(results) == 1
    assert len(events) == 1
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_scheduler_restart_dispatches_pending_retry(db_session):
    job = await create_job(db_session, status=JobStatus.RETRYING)
    job.scheduled_at = datetime.now(timezone.utc)
    await db_session.commit()

    first_run = await run_scheduler_cycle()
    second_run_after_restart = await run_scheduler_cycle()
    await db_session.refresh(job)

    assert first_run == 1
    assert second_run_after_restart == 0
    assert job.status == JobStatus.QUEUED
    await cleanup_jobs(db_session, [job.id])
