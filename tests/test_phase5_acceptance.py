import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.db.database import Base
from src.main import app
from src.models.job_attempts import JobAttempt, JobAttemptStatus
from src.models.job_outputs import JobOutput
from src.models.jobs import Job, JobStatus, JobType
from src.models.outbox_events import OutboxEvent
from src.models.workers import Worker
from src.repositories.outbox_event import fetch_unpublished_events
from src.workers import main as worker_module


API_HEADERS = {"x-api-key": "test_key_abc123"}


def key(prefix="phase5"):
    return f"{prefix}-{uuid4().hex}"


def request_body(payload=None):
    return {
        "job_type": "report",
        "payload": payload or {"value": key("payload")},
        "priority": 5,
        "max_retries": 1,
    }


@pytest_asyncio.fixture
async def client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as value:
        yield value


async def job_ids_for_key(session, idempotency_key):
    result = await session.execute(
        select(Job.id).where(Job.idempotency_key == idempotency_key)
    )
    return list(result.scalars())


async def cleanup_jobs(session, job_ids):
    if not job_ids:
        return
    await session.execute(delete(JobOutput).where(JobOutput.job_id.in_(job_ids)))
    await session.execute(
        update(Job)
        .where(Job.id.in_(job_ids))
        .values(current_attempt_id=None)
    )
    await session.execute(delete(JobAttempt).where(JobAttempt.job_id.in_(job_ids)))
    await session.execute(delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(job_ids)))
    await session.execute(delete(Job).where(Job.id.in_(job_ids)))
    await session.commit()


async def create_job(session, *, status=JobStatus.QUEUED, payload=None):
    job = Job(
        job_type=JobType.REPORT,
        payload=payload or {"value": key("job")},
        client_id=key("client"),
        status=status,
        max_attempts=3,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def ensure_workers(session, worker_ids):
    existing = set(
        (await session.execute(select(Worker.id).where(Worker.id.in_(worker_ids))))
        .scalars()
    )
    for worker_id in set(worker_ids) - existing:
        session.add(Worker(id=worker_id))
    await session.commit()


class FakeRedis:
    def __init__(self, *, fail_ack=False):
        self.acks = []
        self.xadd_calls = []
        self.fail_ack = fail_ack

    async def xadd(self, name, fields):
        self.xadd_calls.append((name, fields))
        return f"redis-{len(self.xadd_calls)}-0"

    async def xack(self, stream, group, message_id):
        if self.fail_ack:
            raise RuntimeError("simulated XACK failure")
        self.acks.append((stream, group, message_id))
        return 1


def worker_message(job_id, message_id="1-0"):
    return [("job_events", [(message_id, {"event": json.dumps({
        "job_id": job_id,
        "event_type": "job.created",
    })})])]


async def run_worker_message(monkeypatch, job_id, redis, *message_ids):
    async def no_sleep(_):
        return None

    monkeypatch.setattr(worker_module.asyncio, "sleep", no_sleep)
    await asyncio.gather(*[
        worker_module.process_messages(
            worker_message(job_id, message_id), f"worker-{index}", redis
        )
        for index, message_id in enumerate(message_ids)
    ])


@pytest.mark.asyncio
async def test_1_duplicate_api_requests(client, db_session):
    idempotency_key = key("same-request")
    body = request_body()
    responses = await asyncio.gather(*[
        client.post("/api/v1/jobs/", json=body, headers={**API_HEADERS, "Idempotency-Key": idempotency_key})
        for _ in range(2)
    ])
    assert all(response.is_success for response in responses)
    assert responses[0].json()["id"] == responses[1].json()["id"]
    ids = await job_ids_for_key(db_session, idempotency_key)
    assert len(ids) == 1
    result = await db_session.execute(
        select(OutboxEvent).where(OutboxEvent.aggregate_id == ids[0])
    )
    assert len(result.scalars().all()) == 1
    await cleanup_jobs(db_session, ids)


@pytest.mark.asyncio
async def test_2_same_key_different_payload(client, db_session):
    idempotency_key = key("different-payload")
    first = await client.post("/api/v1/jobs/", json=request_body({"a": 1}), headers={**API_HEADERS, "Idempotency-Key": idempotency_key})
    second = await client.post("/api/v1/jobs/", json=request_body({"a": 2}), headers={**API_HEADERS, "Idempotency-Key": idempotency_key})
    assert first.is_success
    assert second.status_code == 409
    ids = await job_ids_for_key(db_session, idempotency_key)
    assert len(ids) == 1
    await cleanup_jobs(db_session, ids)


@pytest.mark.asyncio
async def test_3_twenty_concurrent_api_submissions(client, db_session):
    idempotency_key = key("twenty")
    body = request_body()
    responses = await asyncio.gather(*[
        client.post("/api/v1/jobs/", json=body, headers={**API_HEADERS, "Idempotency-Key": idempotency_key})
        for _ in range(20)
    ])
    assert all(response.is_success for response in responses)
    assert len({response.json()["id"] for response in responses}) == 1
    ids = await job_ids_for_key(db_session, idempotency_key)
    assert len(ids) == 1
    result = await db_session.execute(select(OutboxEvent).where(OutboxEvent.aggregate_id == ids[0]))
    assert len(result.scalars().all()) == 1
    await cleanup_jobs(db_session, ids)


async def publish_batch(engine, redis, count=100):
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        events = await fetch_unpublished_events(session)
        for event in events[:count]:
            await redis.xadd("job_events", {"event": json.dumps({"job_id": event.aggregate_id, "event_id": event.id})})
            event.published_at = datetime.now(timezone.utc)
        await session.commit()


@pytest.mark.asyncio
async def test_4_concurrent_outbox_publishers(db_engine, db_session):
    jobs = [await create_job(db_session) for _ in range(10)]
    for job in jobs:
        db_session.add(OutboxEvent(aggregate_id=job.id, event_type="job.created", payload=job.payload))
    await db_session.commit()
    redis = FakeRedis()
    await asyncio.gather(publish_batch(db_engine, redis), publish_batch(db_engine, redis))
    result = await db_session.execute(select(OutboxEvent).where(OutboxEvent.aggregate_id.in_([j.id for j in jobs])))
    events = result.scalars().all()
    assert all(event.published_at is not None for event in events)
    assert len(redis.xadd_calls) == 10
    await cleanup_jobs(db_session, [job.id for job in jobs])


@pytest.mark.asyncio
async def test_5_duplicate_redis_delivery_executes_once(db_session, monkeypatch):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["worker-0", "worker-1"])
    redis = FakeRedis()
    await redis.xadd("job_events", {"event": json.dumps({"job_id": job.id})})
    await redis.xadd("job_events", {"event": json.dumps({"job_id": job.id})})
    await run_worker_message(monkeypatch, job.id, redis, "1-0", "2-0")
    attempts = (await db_session.execute(select(JobAttempt).where(JobAttempt.job_id == job.id))).scalars().all()
    outputs = (await db_session.execute(select(JobOutput).where(JobOutput.job_id == job.id))).scalars().all()
    assert len(attempts) == 1
    assert len(outputs) == 1
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_6_terminal_jobs_are_skipped_and_acknowledged(db_session, monkeypatch):
    jobs = [await create_job(db_session, status=status) for status in (JobStatus.COMPLETED, JobStatus.CANCELLED)]
    redis = FakeRedis()
    await run_worker_message(monkeypatch, jobs[0].id, redis, "1-0")
    await run_worker_message(monkeypatch, jobs[1].id, redis, "2-0")
    assert not (await db_session.execute(select(JobAttempt).where(JobAttempt.job_id.in_([j.id for j in jobs])))).scalars().all()
    assert not (await db_session.execute(select(JobOutput).where(JobOutput.job_id.in_([j.id for j in jobs])))).scalars().all()
    assert len(redis.acks) == 2
    await cleanup_jobs(db_session, [job.id for job in jobs])


@pytest.mark.asyncio
async def test_7_concurrent_worker_claims(db_session, monkeypatch):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["worker-0", "worker-1"])
    redis = FakeRedis()
    await run_worker_message(monkeypatch, job.id, redis, "1-0", "2-0")
    await db_session.refresh(job)
    attempts = (await db_session.execute(select(JobAttempt).where(JobAttempt.job_id == job.id))).scalars().all()
    assert job.status == JobStatus.COMPLETED
    assert job.attempt_count == 1
    assert len(attempts) == 1
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_8_stale_worker_cannot_complete_newer_attempt(db_session):
    job = await create_job(db_session, status=JobStatus.PROCESSING)
    job_id = job.id
    await ensure_workers(db_session, ["old", "new"])
    old = JobAttempt(job_id=job.id, worker_id="old", attempt_number=1, status=JobAttemptStatus.PROCESSING)
    new = JobAttempt(job_id=job.id, worker_id="new", attempt_number=2, status=JobAttemptStatus.PROCESSING)
    db_session.add_all([old, new])
    await db_session.flush()
    job.current_attempt_id = new.id
    await db_session.commit()
    stale = await db_session.execute(update(Job).where(Job.id == job.id, Job.status == JobStatus.PROCESSING, Job.current_attempt_id == old.id).values(status=JobStatus.COMPLETED))
    assert stale.rowcount == 0
    await db_session.rollback()
    await cleanup_jobs(db_session, [job_id])


@pytest.mark.asyncio
async def test_9_ack_failure_does_not_change_completed_state(db_session, monkeypatch):
    job = await create_job(db_session)
    await ensure_workers(db_session, ["worker-0", "worker-1"])
    redis = FakeRedis(fail_ack=True)
    await run_worker_message(monkeypatch, job.id, redis, "1-0")
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert len((await db_session.execute(select(JobOutput).where(JobOutput.job_id == job.id))).scalars().all()) == 1
    redis.fail_ack = False
    await run_worker_message(monkeypatch, job.id, redis, "2-0")
    assert len((await db_session.execute(select(JobAttempt).where(JobAttempt.job_id == job.id))).scalars().all()) == 1
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_10_output_insertion_is_idempotent(db_engine, db_session):
    job = await create_job(db_session)
    operation_key = key(f"job-{job.id}-output")
    sessions = async_sessionmaker(db_engine, expire_on_commit=False)

    async def insert_output(result):
        async with sessions() as session:
            statement = insert(JobOutput).values(job_id=job.id, operation_key=operation_key, result=result).on_conflict_do_nothing(index_elements=["operation_key"])
            await session.execute(statement)
            await session.commit()

    await asyncio.gather(insert_output({"winner": 1}), insert_output({"winner": 2}))
    outputs = (await db_session.execute(select(JobOutput).where(JobOutput.operation_key == operation_key))).scalars().all()
    assert len(outputs) == 1
    assert outputs[0].result in ({"winner": 1}, {"winner": 2})
    await cleanup_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_11_output_and_completion_are_atomic(db_session):
    job = await create_job(db_session, status=JobStatus.PROCESSING)
    await ensure_workers(db_session, ["atomic"])
    attempt = JobAttempt(job_id=job.id, worker_id="atomic", attempt_number=1, status=JobAttemptStatus.PROCESSING)
    db_session.add(attempt)
    await db_session.flush()
    job.current_attempt_id = attempt.id
    await db_session.commit()
    try:
        db_session.add(JobOutput(job_id=job.id, operation_key=key("atomic"), result={"ok": True}))
        await db_session.execute(update(Job).where(Job.id == job.id, Job.current_attempt_id == attempt.id).values(status=JobStatus.COMPLETED))
        raise RuntimeError("simulated pre-commit failure")
    except RuntimeError:
        await db_session.rollback()
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert not (await db_session.execute(select(JobOutput).where(JobOutput.job_id == job.id))).scalars().all()
    await cleanup_jobs(db_session, [job.id])
