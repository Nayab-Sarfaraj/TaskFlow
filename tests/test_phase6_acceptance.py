import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from sqlalchemy import select

from src.models.job_attempts import JobAttempt, JobAttemptStatus
from src.models.jobs import Job, JobStatus, JobType
from src.models.outbox_events import OutboxEvent
from src.scheduler.scheduler import run_scheduler_cycle
from src.workers import main as worker_module
from tests.test_phase5_acceptance import (
    FakeRedis,
    cleanup_jobs,
    create_job,
    ensure_workers,
    worker_message,
)


async def run_failing_worker(monkeypatch, job_id, worker_id, error):
    async def fail_sleep(_):
        raise error

    monkeypatch.setattr(worker_module.asyncio, "sleep", fail_sleep)
    redis = FakeRedis()
    await worker_module.process_messages(worker_message(job_id), worker_id, redis)
    return redis


@pytest.mark.asyncio
async def test_retryable_timeout_moves_job_to_retrying(db_session, monkeypatch):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["phase6-timeout"])

    await run_failing_worker(
        monkeypatch, job.id, "phase6-timeout", TimeoutError("timeout")
    )
    await db_session.refresh(job)
    attempt = (
        await db_session.execute(select(JobAttempt).where(JobAttempt.job_id == job.id))
    ).scalar_one()

    assert job.status == JobStatus.RETRYING
    assert job.scheduled_at > datetime.now(timezone.utc)
    assert attempt.status == JobAttemptStatus.FAILED
    assert attempt.finished_at is not None
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_permanent_http_400_fails_job(db_session, monkeypatch):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["phase6-http"])
    error = httpx.HTTPStatusError(
        "bad request",
        request=httpx.Request("GET", "https://example.test"),
        response=httpx.Response(400),
    )

    await run_failing_worker(monkeypatch, job.id, "phase6-http", error)
    await db_session.refresh(job)

    assert job.status == JobStatus.FAILED
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_scheduler_ignores_future_retry(db_session):
    job = await create_job(db_session, status=JobStatus.RETRYING)
    job.scheduled_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await db_session.commit()

    assert await run_scheduler_cycle() == 0
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_scheduler_requeues_due_retry_and_creates_one_event(db_session):
    job = await create_job(db_session, status=JobStatus.RETRYING)
    job.scheduled_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    await db_session.commit()

    assert await run_scheduler_cycle() == 1
    await db_session.refresh(job)
    events = (
        (
            await db_session.execute(
                select(OutboxEvent).where(OutboxEvent.aggregate_id == job.id)
            )
        )
        .scalars()
        .all()
    )
    assert job.status == JobStatus.QUEUED
    assert len(events) == 1
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_early_redis_message_does_not_execute_job(db_session, monkeypatch):
    job = await create_job(db_session)
    job.scheduled_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    await db_session.commit()
    await ensure_workers(db_session, ["phase6-early"])
    redis = FakeRedis()

    async def should_not_run(_):
        raise AssertionError("early job was executed")

    monkeypatch.setattr(worker_module.asyncio, "sleep", should_not_run)
    await worker_module.process_messages(worker_message(job.id), "phase6-early", redis)
    await db_session.refresh(job)

    assert job.status == JobStatus.QUEUED
    assert (
        not (
            await db_session.execute(
                select(JobAttempt).where(JobAttempt.job_id == job.id)
            )
        )
        .scalars()
        .all()
    )
    assert redis.acks
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_null_scheduled_at_job_is_executable(db_session, monkeypatch):
    job = await create_job(db_session)
    job.scheduled_at = None
    await db_session.commit()
    await ensure_workers(db_session, ["phase6-null-schedule"])
    redis = FakeRedis()

    async def no_sleep(_):
        return None

    monkeypatch.setattr(worker_module.asyncio, "sleep", no_sleep)
    await worker_module.process_messages(
        worker_message(job.id), "phase6-null-schedule", redis
    )
    await db_session.refresh(job)

    assert job.status == JobStatus.COMPLETED
    assert job.attempt_count == 1
    await cleanup_jobs(db_session, [job.id])
