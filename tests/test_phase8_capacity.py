import asyncio
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from redis.asyncio import Redis
from sqlalchemy import func, select

from src.models.jobs import Job, JobStatus
from src.repositories import jobs as jobs_repository
from src.services import rate_limiter
from src.main import app
from tests.test_phase5_acceptance import (
    API_HEADERS,
    cleanup_jobs,
    key,
    request_body,
)


@pytest_asyncio.fixture
async def client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as value:
        yield value


async def clear_rate_limit(client_id: str) -> None:
    redis = Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    try:
        await redis.delete(f"rate_limit:jobs:{client_id}")
    finally:
        await redis.aclose()


@pytest.mark.asyncio
async def test_job_rate_limit_accepts_60_and_rejects_61(
    client, db_session, monkeypatch
):
    client_id = "client_001"
    await clear_rate_limit(client_id)
    monkeypatch.setattr(jobs_repository, "MAX_PENDING_JOBS", 100_000)

    job_ids = []
    try:
        responses = await asyncio.gather(
            *[
                client.post(
                    "/api/v1/jobs/",
                    json=request_body(),
                    headers={
                        **API_HEADERS,
                        "Idempotency-Key": f"rate-{uuid4().hex}",
                    },
                )
                for _ in range(60)
            ]
        )
        assert all(response.status_code == 201 for response in responses)
        job_ids.extend(response.json()["id"] for response in responses)

        rejected = await client.post(
            "/api/v1/jobs/",
            json=request_body(),
            headers={**API_HEADERS, "Idempotency-Key": f"rate-{uuid4().hex}"},
        )
        assert rejected.status_code == 429
        assert rejected.headers.get("Retry-After")
    finally:
        await cleanup_jobs(db_session, job_ids)
        await clear_rate_limit(client_id)


@pytest.mark.asyncio
async def test_clients_have_independent_rate_limits(client, db_session, monkeypatch):
    monkeypatch.setattr(rate_limiter, "RATE_LIMIT", 1)
    monkeypatch.setattr(rate_limiter, "WINDOW_MS", 60_000)
    monkeypatch.setattr(jobs_repository, "MAX_PENDING_JOBS", 100_000)
    job_ids = []
    try:
        first = await client.post(
            "/api/v1/jobs/",
            json=request_body(),
            headers={**API_HEADERS, "Idempotency-Key": key("client-1")},
        )
        second = await client.post(
            "/api/v1/jobs/",
            json=request_body(),
            headers={
                "x-api-key": "test_key_def456",
                "Idempotency-Key": key("client-2"),
            },
        )
        assert first.status_code == 201
        assert second.status_code == 201
        job_ids.extend([first.json()["id"], second.json()["id"]])
    finally:
        await cleanup_jobs(db_session, job_ids)
        await clear_rate_limit("client_001")
        await clear_rate_limit("client_002")


@pytest.mark.asyncio
async def test_rate_limit_window_resets(client, db_session, monkeypatch):
    client_id = "client_001"
    await clear_rate_limit(client_id)
    monkeypatch.setattr(rate_limiter, "RATE_LIMIT", 1)
    monkeypatch.setattr(rate_limiter, "WINDOW_MS", 1_000)
    clock = [1_000.0]
    monkeypatch.setattr(rate_limiter.time, "time", lambda: clock[0])
    monkeypatch.setattr(jobs_repository, "MAX_PENDING_JOBS", 100_000)
    job_ids = []
    try:
        first = await client.post(
            "/api/v1/jobs/",
            json=request_body(),
            headers={**API_HEADERS, "Idempotency-Key": key("window-1")},
        )
        blocked = await client.post(
            "/api/v1/jobs/",
            json=request_body(),
            headers={**API_HEADERS, "Idempotency-Key": key("window-2")},
        )
        assert first.status_code == 201
        assert blocked.status_code == 429
        job_ids.append(first.json()["id"])

        clock[0] += 2
        after_reset = await client.post(
            "/api/v1/jobs/",
            json=request_body(),
            headers={**API_HEADERS, "Idempotency-Key": key("window-3")},
        )
        assert after_reset.status_code == 201
        job_ids.append(after_reset.json()["id"])
    finally:
        await cleanup_jobs(db_session, job_ids)
        await clear_rate_limit(client_id)


@pytest.mark.asyncio
async def test_pending_capacity_admits_only_one_concurrent_submission(
    client, db_session, monkeypatch
):
    await clear_rate_limit("client_001")
    pending_count = await db_session.scalar(
        select(func.count(Job.id)).where(
            Job.status.in_([JobStatus.QUEUED, JobStatus.RETRYING])
        )
    )
    monkeypatch.setattr(jobs_repository, "MAX_PENDING_JOBS", pending_count + 1)

    responses = await asyncio.gather(
        *[
            client.post(
                "/api/v1/jobs/",
                json=request_body(),
                headers={
                    **API_HEADERS,
                    "Idempotency-Key": f"capacity-{uuid4().hex}",
                },
            )
            for _ in range(20)
        ]
    )
    accepted = [response for response in responses if response.status_code == 201]
    rejected = [response for response in responses if response.status_code == 503]
    job_ids = [response.json()["id"] for response in accepted]
    try:
        assert len(accepted) == 1
        assert len(rejected) == 19
        assert all(response.headers.get("Retry-After") for response in rejected)
    finally:
        await cleanup_jobs(db_session, job_ids)
        await clear_rate_limit("client_001")

    # Capacity is available again after the admitted job is removed.
    available = await client.post(
        "/api/v1/jobs/",
        json=request_body(),
        headers={**API_HEADERS, "Idempotency-Key": f"capacity-after-{uuid4().hex}"},
    )
    assert available.status_code == 201
    await cleanup_jobs(db_session, [available.json()["id"]])
