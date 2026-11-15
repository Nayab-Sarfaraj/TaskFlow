import asyncio
import json

import httpx
import pytest
from sqlalchemy import func, select

from src.main import app
from src.models.job_attempts import JobAttempt
from src.models.job_outputs import JobOutput
from src.models.jobs import Job, JobStatus
from src.workers.groups import create_group
from src.workers.lifecycle import register_worker
from src.workers.main import REDIS_CONSUMER_GROUP, REDIS_STREAM, process_messages


@pytest.mark.asyncio
async def test_two_workers_cannot_claim_one_job_twice(integration_redis):
    await create_group(REDIS_STREAM, REDIS_CONSUMER_GROUP, integration_redis)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/jobs/",
            headers={"X-API-Key": "test_key_abc123", "Idempotency-Key": "claim-race"},
            json={
                "job_type": "report",
                "payload": {
                    "report_type": "claim-race",
                    "format": "pdf",
                    "execution_time_seconds": 0.2,
                },
            },
        )
    assert response.status_code == 201
    job_id = response.json()["id"]

    event = json.dumps(
        {
            "job_id": str(job_id),
            "event_id": "claim-race",
            "event_type": "job.created",
            "request_id": "claim-race",
            "client_id": "client_001",
        }
    )
    await integration_redis.xadd(REDIS_STREAM, {"event": event})
    await integration_redis.xadd(REDIS_STREAM, {"event": event})
    worker_ids = ("integration-worker-a", "integration-worker-b")
    for worker_id in worker_ids:
        await register_worker(worker_id, concurrency_limit=1)

    first = await integration_redis.xreadgroup(
        REDIS_CONSUMER_GROUP, worker_ids[0], {REDIS_STREAM: ">"}, count=1
    )
    second = await integration_redis.xreadgroup(
        REDIS_CONSUMER_GROUP, worker_ids[1], {REDIS_STREAM: ">"}, count=1
    )
    await asyncio.gather(
        process_messages(first, worker_ids[0], integration_redis),
        process_messages(second, worker_ids[1], integration_redis),
    )

    from src.db.database import SessionLocal

    async with SessionLocal() as db:
        job = await db.get(Job, job_id)
        assert job.status == JobStatus.COMPLETED
        assert await db.scalar(
            select(func.count()).select_from(JobAttempt).where(JobAttempt.job_id == job_id)
        ) == 1
        assert await db.scalar(
            select(func.count()).select_from(JobOutput).where(JobOutput.job_id == job_id)
        ) == 1
