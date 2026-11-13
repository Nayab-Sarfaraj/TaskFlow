import asyncio
from datetime import datetime, timezone

import pytest
from sqlalchemy import select, update

from src.models.job_attempts import JobAttempt, JobAttemptStatus
from src.models.jobs import Job, JobStatus
from src.scheduler.scheduler import run_scheduler_cycle
from src.workers import main as worker_module
from tests.test_phase5_acceptance import (
    cleanup_jobs,
    create_job,
    ensure_workers,
    worker_message,
)


async def process_failure(monkeypatch, job_id, worker_id, error):
    async def fail_handler(_):
        raise error

    monkeypatch.setattr(worker_module.asyncio, "sleep", fail_handler)
    await worker_module.process_messages(worker_message(job_id), worker_id, _redis())


async def process_success(monkeypatch, job_id, worker_id):
    async def successful_handler(_):
        return None

    monkeypatch.setattr(worker_module.asyncio, "sleep", successful_handler)
    await worker_module.process_messages(worker_message(job_id), worker_id, _redis())


def _redis():
    from tests.test_phase5_acceptance import FakeRedis

    return FakeRedis()


async def requeue_job(session, job_id):
    await session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(status=JobStatus.QUEUED, scheduled_at=datetime.now(timezone.utc))
    )
    await session.commit()


async def attempts_for(session, job_id):
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
async def test_job_fails_three_times_then_succeeds_on_attempt_four(
    db_session, monkeypatch
):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["step5-worker"])

    for _ in range(3):
        await process_failure(
            monkeypatch, job.id, "step5-worker", TimeoutError("timeout")
        )
        await requeue_job(db_session, job.id)

    await process_success(monkeypatch, job.id, "step5-worker")
    await db_session.refresh(job)
    attempts = await attempts_for(db_session, job.id)

    assert job.status == JobStatus.COMPLETED
    assert job.attempt_count == 4
    assert [attempt.attempt_number for attempt in attempts] == [1, 2, 3, 4]
    assert all(attempt.status == JobAttemptStatus.FAILED for attempt in attempts[:3])
    assert attempts[3].status == JobAttemptStatus.SUCCEEDED
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_job_fails_four_times_and_is_permanently_failed(db_session, monkeypatch):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["step5-failing-worker"])

    for attempt_number in range(1, 5):
        await process_failure(
            monkeypatch,
            job.id,
            "step5-failing-worker",
            TimeoutError(f"timeout {attempt_number}"),
        )
        if attempt_number < 4:
            await requeue_job(db_session, job.id)

    await db_session.refresh(job)
    attempts = await attempts_for(db_session, job.id)

    assert job.status == JobStatus.FAILED
    assert job.attempt_count == 4
    assert len(attempts) == 4
    assert all(attempt.status == JobAttemptStatus.FAILED for attempt in attempts)
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_permanent_failure_on_attempt_one_does_not_retry(db_session, monkeypatch):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["step5-permanent-worker"])

    await process_failure(
        monkeypatch,
        job.id,
        "step5-permanent-worker",
        ValueError("permanent failure"),
    )
    await db_session.refresh(job)
    attempts = await attempts_for(db_session, job.id)

    assert job.status == JobStatus.FAILED
    assert job.attempt_count == 1
    assert len(attempts) == 1
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_two_workers_create_only_one_attempt(db_session, monkeypatch):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["step5-worker-a", "step5-worker-b"])

    async def no_sleep(_):
        return None

    monkeypatch.setattr(worker_module.asyncio, "sleep", no_sleep)
    await asyncio.gather(
        worker_module.process_messages(
            worker_message(job.id, "a-0"), "step5-worker-a", _redis()
        ),
        worker_module.process_messages(
            worker_message(job.id, "b-0"), "step5-worker-b", _redis()
        ),
    )
    await db_session.refresh(job)
    attempts = await attempts_for(db_session, job.id)

    assert len(attempts) == 1
    assert job.attempt_count == 1
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_scheduler_requeue_does_not_increment_attempt_count(db_session):
    job = await create_job(db_session, status=JobStatus.RETRYING)
    job.attempt_count = 2
    job.scheduled_at = datetime.now(timezone.utc)
    await db_session.commit()

    assert await run_scheduler_cycle() == 1
    await db_session.refresh(job)

    assert job.status == JobStatus.QUEUED
    assert job.attempt_count == 2
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_failed_attempt_and_parent_transition_roll_back_together(db_session):
    job = await create_job(db_session, status=JobStatus.PROCESSING)
    await ensure_workers(db_session, ["step5-rollback-worker"])
    attempt = JobAttempt(
        job_id=job.id,
        worker_id="step5-rollback-worker",
        attempt_number=1,
        status=JobAttemptStatus.PROCESSING,
    )
    db_session.add(attempt)
    await db_session.flush()
    job.current_attempt_id = attempt.id
    await db_session.commit()

    try:
        await db_session.execute(
            update(JobAttempt)
            .where(JobAttempt.id == attempt.id)
            .values(status=JobAttemptStatus.FAILED, error_message="boom")
        )
        await db_session.execute(
            update(Job)
            .where(Job.id == job.id, Job.current_attempt_id == -1)
            .values(status=JobStatus.FAILED)
        )
        raise RuntimeError("parent transition rejected")
    except RuntimeError:
        await db_session.rollback()

    await db_session.refresh(job)
    await db_session.refresh(attempt)
    assert job.status == JobStatus.PROCESSING
    assert attempt.status == JobAttemptStatus.PROCESSING
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_retry_after_worker_restart_continues_attempt_number(
    db_session, monkeypatch
):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["step5-worker-before", "step5-worker-after"])

    await process_failure(
        monkeypatch, job.id, "step5-worker-before", TimeoutError("restart")
    )
    await requeue_job(db_session, job.id)
    await process_success(monkeypatch, job.id, "step5-worker-after")

    await db_session.refresh(job)
    attempts = await attempts_for(db_session, job.id)
    assert job.status == JobStatus.COMPLETED
    assert [attempt.attempt_number for attempt in attempts] == [1, 2]
    assert attempts[0].worker_id == "step5-worker-before"
    assert attempts[1].worker_id == "step5-worker-after"
    await cleanup_jobs(db_session, [job.id])
