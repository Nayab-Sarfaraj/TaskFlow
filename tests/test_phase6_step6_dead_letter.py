from datetime import datetime, timezone

import pytest
from sqlalchemy import select, update

from src.models.job_attempts import JobAttempt, JobAttemptStatus
from src.models.jobs import Job, JobStatus
from src.scheduler.scheduler import run_scheduler_cycle
from src.workers import main as worker_module
from tests.test_phase5_acceptance import (
    FakeRedis,
    cleanup_jobs,
    create_job,
    ensure_workers,
    worker_message,
)
from tests.test_phase6_step5_attempts import process_failure, requeue_job


async def get_attempts(session, job_id):
    return (
        (
            await session.execute(
                select(JobAttempt)
                .where(JobAttempt.job_id == job_id)
                .order_by(JobAttempt.attempt_number)
            )
        )
        .scalars()
        .all()
    )


@pytest.mark.asyncio
async def test_retry_budget_exhausted_creates_terminal_failed_job(
    db_session, monkeypatch
):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["dead-letter-budget"])

    for attempt_number in range(1, 5):
        await process_failure(
            monkeypatch,
            job.id,
            "dead-letter-budget",
            TimeoutError(f"timeout-{attempt_number}"),
        )
        if attempt_number < 4:
            await requeue_job(db_session, job.id)

    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.scheduled_at is None
    assert len(await get_attempts(db_session, job.id)) == 4
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_permanent_error_on_first_attempt_is_failed(db_session, monkeypatch):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["dead-letter-permanent"])

    await process_failure(
        monkeypatch,
        job.id,
        "dead-letter-permanent",
        ValueError("permanent error"),
    )
    await db_session.refresh(job)
    attempts = await get_attempts(db_session, job.id)

    assert job.status == JobStatus.FAILED
    assert job.scheduled_at is None
    assert len(attempts) == 1
    assert attempts[0].status == JobAttemptStatus.FAILED
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_failed_job_retains_payload_error_and_attempt_history(
    db_session, monkeypatch
):
    payload = {"document": "important", "customer_id": 42}
    job = await create_job(db_session, payload=payload)
    await ensure_workers(db_session, ["dead-letter-inspection"])

    await process_failure(
        monkeypatch,
        job.id,
        "dead-letter-inspection",
        ValueError("invalid document"),
    )
    await db_session.refresh(job)
    attempts = await get_attempts(db_session, job.id)

    assert job.payload == payload
    assert job.error_message == "invalid document"
    assert len(attempts) == 1
    assert attempts[0].error_message == "invalid document"
    assert attempts[0].finished_at is not None
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_scheduler_ignores_failed_job(db_session):
    job = await create_job(db_session, status=JobStatus.FAILED)
    job.scheduled_at = datetime.now(timezone.utc)
    await db_session.commit()

    assert await run_scheduler_cycle() == 0
    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_duplicate_redis_message_for_failed_job_does_not_execute(
    db_session, monkeypatch
):
    job = await create_job(db_session, status=JobStatus.FAILED)
    await ensure_workers(db_session, ["dead-letter-redis"])
    redis = FakeRedis()

    async def should_not_execute(_):
        raise AssertionError("failed job was executed")

    monkeypatch.setattr(worker_module.asyncio, "sleep", should_not_execute)
    await worker_module.process_messages(
        worker_message(job.id, "failed-1"), "dead-letter-redis", redis
    )
    await db_session.refresh(job)

    assert job.status == JobStatus.FAILED
    assert not await get_attempts(db_session, job.id)
    assert redis.acks == [("job_events", "workers", "failed-1")]
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_stale_attempt_cannot_mark_job_failed(db_session):
    job = await create_job(db_session, status=JobStatus.PROCESSING)
    await ensure_workers(db_session, ["dead-letter-stale"])
    attempt = JobAttempt(
        job_id=job.id,
        worker_id="dead-letter-stale",
        attempt_number=1,
        status=JobAttemptStatus.PROCESSING,
    )
    db_session.add(attempt)
    await db_session.flush()
    job.current_attempt_id = attempt.id
    await db_session.commit()

    result = await db_session.execute(
        update(Job)
        .where(
            Job.id == job.id,
            Job.status == JobStatus.PROCESSING,
            Job.current_attempt_id == attempt.id + 1,
        )
        .values(status=JobStatus.FAILED)
    )
    await db_session.commit()
    await db_session.refresh(job)

    assert result.rowcount == 0
    assert job.status == JobStatus.PROCESSING
    await cleanup_jobs(db_session, [job.id])
