import asyncio
from datetime import datetime, timezone

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.db.database import get_db
from src.main import app
from src.models.job_attempts import JobAttempt, JobAttemptStatus
from src.models.jobs import Job, JobStatus, JobType
from src.models.outbox_events import OutboxEvent
from src.models.workers import Worker
from src.workers import main as worker_module
from tests.test_phase5_acceptance import (
    FakeRedis,
    cleanup_jobs,
    ensure_workers,
    worker_message,
)


CLIENT_ID = "client_001"
HEADERS = {"x-api-key": "test_key_abc123"}


@pytest_asyncio.fixture
async def api_client(db_engine):
    sessions = async_sessionmaker(db_engine, expire_on_commit=False)

    async def override_get_db():
        async with sessions() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.pop(get_db, None)


async def create_client_job(session, status=JobStatus.FAILED):
    job = Job(
        client_id=CLIENT_ID,
        job_type=JobType.REPORT,
        payload={"source": "manual-retry-test"},
        status=status,
        scheduled_at=datetime.now(timezone.utc),
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


@pytest.mark.asyncio
async def test_retry_failed_job_queues_new_execution(api_client, db_session):
    job = await create_client_job(db_session)
    response = await api_client.post(f"/api/v1/jobs/{job.id}/retry", headers=HEADERS)

    assert response.status_code == 200
    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED
    assert job.current_attempt_id is None
    events = (
        (
            await db_session.execute(
                select(OutboxEvent).where(OutboxEvent.aggregate_id == job.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(events) == 1
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [JobStatus.COMPLETED, JobStatus.PROCESSING])
async def test_retry_ineligible_job_returns_conflict(api_client, db_session, status):
    job = await create_client_job(db_session, status=status)
    response = await api_client.post(f"/api/v1/jobs/{job.id}/retry", headers=HEADERS)
    assert response.status_code == 409
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_retry_nonexistent_job_returns_not_found(api_client):
    response = await api_client.post("/api/v1/jobs/999999999/retry", headers=HEADERS)
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_concurrent_retry_requests_only_one_succeeds(api_client, db_session):
    job = await create_client_job(db_session)
    responses = await asyncio.gather(
        *[
            api_client.post(f"/api/v1/jobs/{job.id}/retry", headers=HEADERS)
            for _ in range(2)
        ]
    )

    assert sorted(response.status_code for response in responses) == [200, 409]
    events = (
        (
            await db_session.execute(
                select(OutboxEvent).where(OutboxEvent.aggregate_id == job.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(events) == 1
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_manual_retry_preserves_attempt_history(api_client, db_session):
    job = await create_client_job(db_session)
    await ensure_workers(db_session, ["manual-retry-history"])
    attempt = JobAttempt(
        job_id=job.id,
        worker_id="manual-retry-history",
        attempt_number=1,
        status=JobAttemptStatus.FAILED,
        error_message="previous error",
        finished_at=datetime.now(timezone.utc),
    )
    db_session.add(attempt)
    await db_session.commit()

    response = await api_client.post(f"/api/v1/jobs/{job.id}/retry", headers=HEADERS)
    assert response.status_code == 200
    attempts = (
        (
            await db_session.execute(
                select(JobAttempt).where(JobAttempt.job_id == job.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(attempts) == 1
    assert attempts[0].error_message == "previous error"
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_retry_then_worker_execution_records_new_attempt(
    api_client, db_session, monkeypatch
):
    job = await create_client_job(db_session)
    await ensure_workers(db_session, ["manual-retry-worker"])
    response = await api_client.post(f"/api/v1/jobs/{job.id}/retry", headers=HEADERS)
    assert response.status_code == 200

    async def no_sleep(_):
        return None

    monkeypatch.setattr(worker_module.asyncio, "sleep", no_sleep)
    await worker_module.process_messages(
        worker_message(job.id), "manual-retry-worker", FakeRedis()
    )
    attempts = (
        (
            await db_session.execute(
                select(JobAttempt).where(JobAttempt.job_id == job.id)
            )
        )
        .scalars()
        .all()
    )
    assert len(attempts) == 1
    assert attempts[0].status == JobAttemptStatus.SUCCEEDED
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_outbox_failure_rolls_manual_retry_back(
    api_client, db_session, monkeypatch
):
    job = await create_client_job(db_session)

    class FailingOutbox:
        def __init__(self, **kwargs):
            raise RuntimeError("outbox insert failed")

    monkeypatch.setattr("src.repositories.jobs.OutboxEvent", FailingOutbox)
    response = await api_client.post(f"/api/v1/jobs/{job.id}/retry", headers=HEADERS)

    assert response.status_code == 500
    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_manual_retry_after_exhaustion_starts_new_cycle(
    api_client, db_session, monkeypatch
):
    job = await create_client_job(db_session)
    await ensure_workers(db_session, ["manual-cycle-worker"])
    attempts = [
        JobAttempt(
            job_id=job.id,
            worker_id="manual-cycle-worker",
            attempt_number=number,
            status=JobAttemptStatus.FAILED,
            error_message=f"failure-{number}",
            finished_at=datetime.now(timezone.utc),
        )
        for number in range(1, 5)
    ]
    db_session.add_all(attempts)
    job.attempt_count = 4
    await db_session.commit()

    response = await api_client.post(f"/api/v1/jobs/{job.id}/retry", headers=HEADERS)
    assert response.status_code == 200
    await db_session.refresh(job)
    assert job.attempt_count == 0
    assert job.retry_cycle_start == 4

    async def no_sleep(_):
        return None

    monkeypatch.setattr(worker_module.asyncio, "sleep", no_sleep)
    await worker_module.process_messages(
        worker_message(job.id), "manual-cycle-worker", FakeRedis()
    )
    await db_session.refresh(job)
    history = (
        (
            await db_session.execute(
                select(JobAttempt)
                .where(JobAttempt.job_id == job.id)
                .order_by(JobAttempt.attempt_number)
            )
        )
        .scalars()
        .all()
    )
    assert job.attempt_count == 1
    assert history[-1].attempt_number == 5
    await cleanup_jobs(db_session, [job.id])
