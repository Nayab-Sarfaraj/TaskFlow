import httpx
import pytest
from datetime import datetime, timezone
from sqlalchemy import select, update

from src.main import app
from src.models.job_attempts import JobAttempt, JobAttemptStatus
from src.models.jobs import Job, JobStatus
from src.publisher.main import (
    REDIS_CONSUMER_GROUP,
    REDIS_STREAM,
    publish_pending_events_once,
)
from src.scheduler.scheduler import run_scheduler_cycle
from src.workers.groups import create_group
from src.workers.lifecycle import register_worker
from src.workers.main import process_messages


@pytest.mark.asyncio
async def test_retrying_job_is_requeued_and_later_completes(integration_redis):
    """Exercise real worker, scheduler, publisher, and worker retry cycles."""
    await create_group(REDIS_STREAM, REDIS_CONSUMER_GROUP, integration_redis)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/jobs/",
            headers={"X-API-Key": "test_key_abc123", "Idempotency-Key": "retry-flow"},
            json={
                "job_type": "report",
                "payload": {
                    "report_type": "retry-flow",
                    "format": "pdf",
                    # The report handler raises a retryable TimeoutError before sleeping.
                    "execution_time_seconds": 121,
                },
            },
        )
    assert response.status_code == 201
    job_id = response.json()["id"]

    worker_id = "integration-worker-retry"
    await register_worker(worker_id, concurrency_limit=1)
    await publish_pending_events_once()
    first_delivery = await integration_redis.xreadgroup(
        REDIS_CONSUMER_GROUP, worker_id, {REDIS_STREAM: ">"}, count=1
    )
    await process_messages(first_delivery, worker_id, integration_redis)

    from src.db.database import SessionLocal

    async with SessionLocal() as db:
        failed_job = await db.get(Job, job_id)
        first_attempt = await db.scalar(
            select(JobAttempt).where(JobAttempt.job_id == job_id)
        )
        assert failed_job.status == JobStatus.RETRYING
        assert first_attempt.status == JobAttemptStatus.FAILED

        # Model a corrected upstream report request, then let the actual
        # scheduler, publisher, and worker execute the retry lifecycle.
        await db.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                payload={
                    "report_type": "retry-flow",
                    "format": "pdf",
                    "execution_time_seconds": 0,
                },
                scheduled_at=datetime.now(timezone.utc),
            )
        )
        await db.commit()

    assert await run_scheduler_cycle() == 1
    await publish_pending_events_once()
    retry_delivery = await integration_redis.xreadgroup(
        REDIS_CONSUMER_GROUP, worker_id, {REDIS_STREAM: ">"}, count=1
    )
    await process_messages(retry_delivery, worker_id, integration_redis)

    async with SessionLocal() as db:
        completed_job = await db.get(Job, job_id)
        attempts = (
            await db.scalars(
                select(JobAttempt)
                .where(JobAttempt.job_id == job_id)
                .order_by(JobAttempt.attempt_number)
            )
        ).all()
        assert completed_job.status == JobStatus.COMPLETED
        assert [attempt.status for attempt in attempts] == [
            JobAttemptStatus.FAILED,
            JobAttemptStatus.SUCCEEDED,
        ]
