import asyncio
import os
from datetime import datetime, timedelta, timezone
import pytest
import pytest_asyncio
import httpx
from redis.asyncio import Redis
from sqlalchemy import delete, select

from src.main import app
from src.db.database import engine
from src.models.client import Client, ClientStatus
from src.models.api_key import APIKey
from src.models.jobs import Job
from src.models.job_attempts import JobAttempt
from src.models.job_outputs import JobOutput
from src.models.outbox_events import OutboxEvent
from src.services.api_key_service import create_api_key


ADMIN_KEY = "test_concurrency_admin_secret"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


@pytest_asyncio.fixture
async def setup_multi_clients(db_session, monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", ADMIN_KEY)

    clients = []
    raw_keys = []
    api_keys = []

    # Create 5 distinct clients with API keys
    for i in range(5):
        c = Client(name=f"Concurrent Client {i}", status=ClientStatus.ACTIVE)
        db_session.add(c)
        await db_session.commit()
        await db_session.refresh(c)
        clients.append(c)

        key_obj, raw_key = await create_api_key(
            db_session, client_id=c.id, name=f"Key Client {i}"
        )
        api_keys.append(key_obj)
        raw_keys.append(raw_key)

    client_ids = [c.id for c in clients]

    yield {
        "clients": clients,
        "client_ids": client_ids,
        "api_keys": api_keys,
        "raw_keys": raw_keys,
    }

    # Teardown: Clean up DB tables and Redis rate-limiting keys
    job_ids = list(
        (await db_session.scalars(select(Job.id).where(Job.client_id.in_(client_ids)))).all()
    )
    if job_ids:
        await db_session.execute(delete(JobOutput).where(JobOutput.job_id.in_(job_ids)))
        await db_session.execute(delete(JobAttempt).where(JobAttempt.job_id.in_(job_ids)))
        await db_session.execute(delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(job_ids)))
        await db_session.execute(delete(Job).where(Job.id.in_(job_ids)))

    await db_session.execute(delete(APIKey).where(APIKey.client_id.in_(client_ids)))
    await db_session.execute(delete(Client).where(Client.id.in_(client_ids)))
    await db_session.commit()

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        for cid in client_ids:
            await redis.delete(f"rate_limit:jobs:{cid}")
    finally:
        await redis.aclose()


@pytest_asyncio.fixture
async def api_client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# 1. 100 concurrent authenticated requests across multiple clients:
#    - Every request resolves the correct client_id
#    - No cross-client data leakage
#    - No unexpected database connection-pool exhaustion
@pytest.mark.asyncio
async def test_100_concurrent_requests_resolve_correct_client_and_no_leakage(
    api_client, setup_multi_clients, db_session
):
    clients = setup_multi_clients["clients"]
    raw_keys = setup_multi_clients["raw_keys"]

    num_clients = len(clients)  # 5
    reqs_per_client = 20
    total_requests = num_clients * reqs_per_client  # 100

    # Build 100 concurrent request coroutines (20 requests per each of the 5 clients)
    async def make_request(client_idx: int, req_idx: int):
        resp = await api_client.post(
            "/api/v1/jobs/",
            headers={"X-API-Key": raw_keys[client_idx]},
            json={
                "job_type": "report",
                "payload": {"client_idx": client_idx, "req_idx": req_idx},
                "priority": 5,
                "max_retries": 2,
            },
        )
        return client_idx, req_idx, resp

    tasks = [
        make_request(c_idx, r_idx)
        for c_idx in range(num_clients)
        for r_idx in range(reqs_per_client)
    ]

    # Execute all 100 requests concurrently
    results = await asyncio.gather(*tasks)

    # Verification 1: No unexpected database connection-pool exhaustion (all 100 succeed with 201)
    created_jobs_by_client = {c.id: [] for c in clients}
    for c_idx, r_idx, resp in results:
        assert resp.status_code == 201, f"Request failed: {resp.status_code} {resp.text}"
        data = resp.json()
        job_id = data["id"]
        expected_client_id = clients[c_idx].id
        created_jobs_by_client[expected_client_id].append(job_id)

    # Verification 2: Every request resolves the correct client_id in the database
    for c in clients:
        job_ids = created_jobs_by_client[c.id]
        assert len(job_ids) == reqs_per_client
        db_jobs = (
            await db_session.scalars(select(Job).where(Job.id.in_(job_ids)))
        ).all()
        assert len(db_jobs) == reqs_per_client
        for j in db_jobs:
            assert j.client_id == c.id

    # Verification 3: No cross-client data leakage in listings
    for c_idx, c in enumerate(clients):
        list_resp = await api_client.get(
            "/api/v1/jobs/?limit=100",
            headers={"X-API-Key": raw_keys[c_idx]},
        )
        assert list_resp.status_code == 200
        items = list_resp.json()["items"]
        returned_ids = {item["id"] for item in items}

        # Must contain this client's jobs
        expected_ids = set(created_jobs_by_client[c.id])
        assert expected_ids.issubset(returned_ids)

        # Must NOT contain any other client's jobs
        for other_client in clients:
            if other_client.id != c.id:
                other_ids = set(created_jobs_by_client[other_client.id])
                assert returned_ids.isdisjoint(other_ids)

    # Verification 4: Cross-client GET lookups return 404 (no cross-client leakage)
    cross_tasks = []
    for c_idx, c in enumerate(clients):
        # Pick another client's job to attempt retrieving
        other_c_idx = (c_idx + 1) % num_clients
        other_job_id = created_jobs_by_client[clients[other_c_idx].id][0]
        cross_tasks.append(
            api_client.get(
                f"/api/v1/jobs/{other_job_id}",
                headers={"X-API-Key": raw_keys[c_idx]},
            )
        )

    cross_responses = await asyncio.gather(*cross_tasks)
    for cross_resp in cross_responses:
        assert cross_resp.status_code == 404
        assert cross_resp.json()["detail"] == "Job not found."


# 2. Concurrent authentication doesn't create excessive last_used_at writes
@pytest.mark.asyncio
async def test_concurrent_authentication_does_not_create_excessive_last_used_at_writes(
    api_client, setup_multi_clients, db_session
):
    client = setup_multi_clients["clients"][0]
    raw_key = setup_multi_clients["raw_keys"][0]
    key_obj = setup_multi_clients["api_keys"][0]

    # Initial request sets and commits last_used_at
    initial_resp = await api_client.post(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key},
        json={"job_type": "report", "payload": {"init": True}},
    )
    assert initial_resp.status_code == 201

    await db_session.refresh(key_obj)
    initial_last_used = key_obj.last_used_at
    assert initial_last_used is not None

    # Execute 100 concurrent authenticated requests with the same API key
    tasks = [
        api_client.get(
            "/api/v1/jobs/",
            headers={"X-API-Key": raw_key},
        )
        for _ in range(100)
    ]
    responses = await asyncio.gather(*tasks)

    # All 100 requests authenticate successfully without connection issues
    assert all(r.status_code == 200 for r in responses)

    # Verify last_used_at was NOT modified by the 100 requests due to the 5-minute throttle
    await db_session.refresh(key_obj)
    assert key_obj.last_used_at == initial_last_used


# 3. Concurrent revocation requests behave correctly
@pytest.mark.asyncio
async def test_concurrent_revocation_requests_behave_correctly(
    api_client, setup_multi_clients, db_session
):
    client = setup_multi_clients["clients"][1]
    raw_key = setup_multi_clients["raw_keys"][1]
    key_obj = setup_multi_clients["api_keys"][1]

    # Key is currently active
    resp_before = await api_client.get(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key},
    )
    assert resp_before.status_code == 200

    # Send 20 concurrent revocation requests for the same key simultaneously
    revoke_tasks = [
        api_client.delete(
            f"/api/v1/clients/{client.id}/keys/{key_obj.id}",
            headers={"X-Admin-Key": ADMIN_KEY},
        )
        for _ in range(20)
    ]
    revoke_responses = await asyncio.gather(*revoke_tasks)

    # Every concurrent request returns 204 No Content without deadlocks or errors
    assert all(r.status_code == 204 for r in revoke_responses)

    # The key is now revoked in the database
    await db_session.refresh(key_obj)
    assert key_obj.revoked_at is not None

    # Subsequent authentication immediately fails with 401 Unauthorized
    resp_after = await api_client.get(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key},
    )
    assert resp_after.status_code == 401
    assert resp_after.json()["detail"] == "Invalid API key"
