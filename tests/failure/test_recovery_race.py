import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, update

from src.models.job_attempts import JobAttempt, JobAttemptStatus
from src.models.jobs import Job, JobStatus, JobType
from src.models.workers import Worker, WorkerStatus
from src.workers.stale_delector import abandoned_job_detecton


@pytest.mark.asyncio
async def test_two_recovery_cycles_only_transition_an_expired_lease_once():
    from src.db.database import SessionLocal

    async with SessionLocal() as db:
        worker = Worker(id="failed-worker", status=WorkerStatus.OFFLINE, concurrency_limit=1)
        job = Job(
            job_type=JobType.REPORT,
            payload={"report_type": "recovery-race", "format": "pdf"},
            client_id="client_001",
            status=JobStatus.PROCESSING,
            attempt_count=1,
            max_attempts=4,
            lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
        db.add_all((worker, job))
        await db.flush()
        attempt = JobAttempt(
            job_id=job.id,
            worker_id=worker.id,
            attempt_number=1,
            status=JobAttemptStatus.PROCESSING,
        )
        db.add(attempt)
        await db.flush()
        job.current_attempt_id = attempt.id
        await db.commit()
        job_id = job.id

    recovery_tasks = [
        asyncio.create_task(abandoned_job_detecton()),
        asyncio.create_task(abandoned_job_detecton()),
    ]
    try:
        for _ in range(50):
            async with SessionLocal() as db:
                job = await db.get(Job, job_id)
                if job.status == JobStatus.RETRYING:
                    break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("competing recovery daemons did not recover the job")
    finally:
        for task in recovery_tasks:
            task.cancel()
        await asyncio.gather(*recovery_tasks, return_exceptions=True)

    async with SessionLocal() as db:
        job = await db.get(Job, job_id)
        attempt = await db.scalar(select(JobAttempt).where(JobAttempt.job_id == job_id))
        assert job.status == JobStatus.RETRYING
        assert attempt.status == JobAttemptStatus.INTERRUPTED
