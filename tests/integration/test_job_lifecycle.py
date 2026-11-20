import json

import httpx
import pytest
from sqlalchemy import func, select

from src.main import app
from src.models.job_attempts import JobAttempt, JobAttemptStatus
from src.models.job_outputs import JobOutput
from src.models.jobs import Job, JobStatus
from src.models.outbox_events import OutboxEvent
from src.publisher.main import (
    REDIS_CONSUMER_GROUP,
    REDIS_STREAM,
    publish_pending_events_once,
)
from src.workers.groups import create_group
from src.workers.lifecycle import register_worker
from src.workers.main import process_messages


def report_payload(execution_time_seconds: float = 0) -> dict:
    return {
        "report_type": "integration-lifecycle",
        "format": "pdf",
        "execution_time_seconds": execution_time_seconds,
    }


async def submit_report(client: httpx.AsyncClient, key: str, payload: dict) -> dict:
    response = await client.post(
        "/api/v1/jobs/",
        headers={
            "X-API-Key": "test_key_abc123",
            "Idempotency-Key": key,
            "X-Request-ID": f"integration-{key}",
        },
        json={"job_type": "report", "payload": payload},
    )
    assert response.status_code == 201, response.text
    return response.json()


@pytest.mark.asyncio
async def test_api_to_outbox_to_redis_to_worker_completion(integration_redis):
    await create_group(REDIS_STREAM, REDIS_CONSUMER_GROUP, integration_redis)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        submitted = await submit_report(client, "lifecycle-job", report_payload())

    from src.db.database import SessionLocal

    async with SessionLocal() as db:
        job = await db.get(Job, submitted["id"])
        outbox = await db.scalar(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == submitted["id"])
        )
        assert job.status == JobStatus.QUEUED
        assert outbox is not None and outbox.published_at is None

    assert await publish_pending_events_once() == [outbox.id]
    stream_entries = await integration_redis.xrange(REDIS_STREAM)
    assert len(stream_entries) == 1
    assert json.loads(stream_entries[0][1]["event"])["job_id"] == str(submitted["id"])

    worker_id = "integration-worker-lifecycle"
    await register_worker(worker_id, concurrency_limit=1)
    messages = await integration_redis.xreadgroup(
        REDIS_CONSUMER_GROUP, worker_id, {REDIS_STREAM: ">"}, count=1
    )
    await process_messages(messages, worker_id, integration_redis)

    async with SessionLocal() as db:
        job = await db.get(Job, submitted["id"])
        attempts = (
            await db.scalars(
                select(JobAttempt).where(JobAttempt.job_id == submitted["id"])
            )
        ).all()
        output = await db.scalar(
            select(JobOutput).where(JobOutput.job_id == submitted["id"])
        )
        assert job.status == JobStatus.COMPLETED
        assert [(attempt.attempt_number, attempt.status) for attempt in attempts] == [
            (1, JobAttemptStatus.SUCCEEDED)
        ]
        assert output is not None


@pytest.mark.asyncio
async def test_duplicate_redis_delivery_does_not_duplicate_side_effects(
    integration_redis,
):
    await create_group(REDIS_STREAM, REDIS_CONSUMER_GROUP, integration_redis)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        submitted = await submit_report(client, "duplicate-delivery", report_payload())

    await publish_pending_events_once()
    worker_id = "integration-worker-duplicate"
    await register_worker(worker_id, concurrency_limit=1)
    first_delivery = await integration_redis.xreadgroup(
        REDIS_CONSUMER_GROUP, worker_id, {REDIS_STREAM: ">"}, count=1
    )
    await process_messages(first_delivery, worker_id, integration_redis)

    duplicate_payload = {
        "job_id": str(submitted["id"]),
        "event_type": "job.created",
        "event_id": "duplicate",
        "request_id": "duplicate-delivery",
        "client_id": "client_001",
    }
    await integration_redis.xadd(REDIS_STREAM, {"event": json.dumps(duplicate_payload)})
    duplicate_delivery = await integration_redis.xreadgroup(
        REDIS_CONSUMER_GROUP, worker_id, {REDIS_STREAM: ">"}, count=1
    )
    await process_messages(duplicate_delivery, worker_id, integration_redis)

    from src.db.database import SessionLocal

    async with SessionLocal() as db:
        assert (
            await db.scalar(
                select(func.count())
                .select_from(JobAttempt)
                .where(JobAttempt.job_id == submitted["id"])
            )
            == 1
        )
        assert (
            await db.scalar(
                select(func.count())
                .select_from(JobOutput)
                .where(JobOutput.job_id == submitted["id"])
            )
            == 1
        )
