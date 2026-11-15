import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.models.job_attempts import JobAttempt, JobAttemptStatus
from src.models.jobs import Job, JobStatus, JobType
from src.workers import stale_delector
from tests.test_phase5_acceptance import cleanup_jobs, create_job, ensure_workers


async def run_one_recovery_scan(monkeypatch):
    async def stop_after_scan(_):
        raise asyncio.CancelledError

    monkeypatch.setattr(stale_delector.asyncio, "sleep", stop_after_scan)
    task = asyncio.create_task(stale_delector.abandoned_job_detecton())
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_expired_attempt_is_interrupted_without_incrementing_count(
    db_session, monkeypatch
):
    job = await create_job(db_session, status=JobStatus.PROCESSING)
    job.attempt_count = 1
    job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    await ensure_workers(db_session, ["phase7-worker-a"])
    attempt = JobAttempt(
        job_id=job.id,
        worker_id="phase7-worker-a",
        attempt_number=1,
        status=JobAttemptStatus.PROCESSING,
    )
    db_session.add(attempt)
    await db_session.flush()
    job.current_attempt_id = attempt.id
    await db_session.commit()

    await run_one_recovery_scan(monkeypatch)
    await db_session.refresh(job)
    await db_session.refresh(attempt)

    assert job.status == JobStatus.RETRYING
    assert job.attempt_count == 1
    assert job.current_attempt_id is None
    assert job.lease_expires_at is None
    assert attempt.status == JobAttemptStatus.INTERRUPTED
    assert attempt.finished_at is not None
    assert attempt.worker_id == "phase7-worker-a"
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_expired_final_attempt_becomes_failed(db_session, monkeypatch):
    job = await create_job(db_session, status=JobStatus.PROCESSING)
    job.job_type = JobType.VIDEO
    job.attempt_count = 3
    job.max_attempts = 3
    job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    await ensure_workers(db_session, ["phase7-worker-c"])
    attempt = JobAttempt(
        job_id=job.id,
        worker_id="phase7-worker-c",
        attempt_number=3,
        status=JobAttemptStatus.PROCESSING,
    )
    db_session.add(attempt)
    await db_session.flush()
    job.current_attempt_id = attempt.id
    await db_session.commit()

    await run_one_recovery_scan(monkeypatch)
    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.attempt_count == 3
    assert (
        len(
            (
                await db_session.execute(
                    select(JobAttempt).where(JobAttempt.job_id == job.id)
                )
            )
            .scalars()
            .all()
        )
        == 1
    )
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_concurrent_recovery_only_one_can_lock_job(db_session, db_engine):
    job = await create_job(db_session, status=JobStatus.PROCESSING)
    job.attempt_count = 1
    job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    await ensure_workers(db_session, ["phase7-concurrent"])
    attempt = JobAttempt(
        job_id=job.id,
        worker_id="phase7-concurrent",
        attempt_number=1,
        status=JobAttemptStatus.PROCESSING,
    )
    db_session.add(attempt)
    await db_session.flush()
    job.current_attempt_id = attempt.id
    await db_session.commit()

    sessions = async_sessionmaker(db_engine, expire_on_commit=False)
    first = sessions()
    second = sessions()
    try:
        first_job = (
            await first.execute(
                select(Job)
                .where(Job.id == job.id, Job.status == JobStatus.PROCESSING)
                .with_for_update(skip_locked=True)
            )
        ).scalar_one()
        second_job = (
            await second.execute(
                select(Job)
                .where(Job.id == job.id, Job.status == JobStatus.PROCESSING)
                .with_for_update(skip_locked=True)
            )
        ).scalar_one_or_none()

        assert first_job.id == job.id
        assert second_job is None
        await first.rollback()
        await second.rollback()
    finally:
        await first.close()
        await second.close()
        await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_stale_worker_cannot_complete_after_recovery(db_session, monkeypatch):
    job = await create_job(db_session, status=JobStatus.PROCESSING)
    job.attempt_count = 1
    job.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    await ensure_workers(db_session, ["phase7-stale"])
    attempt = JobAttempt(
        job_id=job.id,
        worker_id="phase7-stale",
        attempt_number=1,
        status=JobAttemptStatus.PROCESSING,
    )
    db_session.add(attempt)
    await db_session.flush()
    job.current_attempt_id = attempt.id
    await db_session.commit()

    await run_one_recovery_scan(monkeypatch)
    result = await db_session.execute(
        update(Job)
        .where(
            Job.id == job.id,
            Job.status == JobStatus.PROCESSING,
            Job.current_attempt_id == attempt.id,
            Job.lease_expires_at > datetime.now(timezone.utc),
        )
        .values(status=JobStatus.COMPLETED)
    )
    await db_session.commit()
    await db_session.refresh(job)

    assert result.rowcount == 0
    assert job.status == JobStatus.RETRYING
    await cleanup_jobs(db_session, [job.id])
