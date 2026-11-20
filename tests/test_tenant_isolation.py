import os
from datetime import datetime, timezone
import pytest
import pytest_asyncio
import httpx
from sqlalchemy import select, delete

from src.main import app
from src.models.client import Client, ClientStatus
from src.models.jobs import Job, JobStatus, JobType
from src.models.job_attempts import JobAttempt
from src.models.job_outputs import JobOutput
from src.models.outbox_events import OutboxEvent
from src.models.api_key import APIKey
from src.services.api_key_service import create_api_key


@pytest_asyncio.fixture
async def test_clients_and_keys(db_session):
    # Setup Client A
    client_a = Client(name="Tenant Client A", status=ClientStatus.ACTIVE)
    db_session.add(client_a)
    # Setup Client B
    client_b = Client(name="Tenant Client B", status=ClientStatus.ACTIVE)
    db_session.add(client_b)
    await db_session.commit()
    await db_session.refresh(client_a)
    await db_session.refresh(client_b)

    # Create API keys
    _, raw_key_a = await create_api_key(db_session, client_id=client_a.id, name="Key A")
    _, raw_key_b = await create_api_key(db_session, client_id=client_b.id, name="Key B")

    yield {
        "client_a": client_a,
        "client_b": client_b,
        "key_a": raw_key_a,
        "key_b": raw_key_b,
        "headers_a": {"X-API-Key": raw_key_a},
        "headers_b": {"X-API-Key": raw_key_b},
    }

    # Cleanup
    job_ids = list(
        (
            await db_session.scalars(
                select(Job.id).where(Job.client_id.in_([client_a.id, client_b.id]))
            )
        ).all()
    )
    if job_ids:
        await db_session.execute(delete(JobOutput).where(JobOutput.job_id.in_(job_ids)))
        await db_session.execute(
            delete(JobAttempt).where(JobAttempt.job_id.in_(job_ids))
        )
        await db_session.execute(
            delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(job_ids))
        )
        await db_session.execute(delete(Job).where(Job.id.in_(job_ids)))
    await db_session.execute(
        delete(APIKey).where(APIKey.client_id.in_([client_a.id, client_b.id]))
    )
    await db_session.execute(
        delete(Client).where(Client.id.in_([client_a.id, client_b.id]))
    )
    await db_session.commit()


@pytest_asyncio.fixture
async def api_client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# 1. Client A can create and retrieve its jobs
@pytest.mark.asyncio
async def test_client_a_can_create_and_retrieve_its_jobs(
    api_client, test_clients_and_keys, db_session
):
    client_a = test_clients_and_keys["client_a"]
    headers_a = test_clients_and_keys["headers_a"]

    # Create job
    create_resp = await api_client.post(
        "/api/v1/jobs/",
        headers=headers_a,
        json={
            "job_type": "report",
            "payload": {"data": "client_a_data"},
            "priority": 5,
            "max_retries": 2,
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    job_id = create_resp.json()["id"]

    # Verify directly in DB that ownership belongs to Client A
    job_db = await db_session.get(Job, job_id)
    assert job_db is not None
    assert job_db.client_id == client_a.id

    # Retrieve job
    get_resp = await api_client.get(f"/api/v1/jobs/{job_id}", headers=headers_a)
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == job_id
    assert get_resp.json()["job_type"] == "report"


# 2. Client B cannot retrieve A's jobs
@pytest.mark.asyncio
async def test_client_b_cannot_retrieve_a_jobs(api_client, test_clients_and_keys):
    headers_a = test_clients_and_keys["headers_a"]
    headers_b = test_clients_and_keys["headers_b"]

    # Client A creates job
    create_resp = await api_client.post(
        "/api/v1/jobs/",
        headers=headers_a,
        json={"job_type": "report", "payload": {"data": "private_to_a"}},
    )
    assert create_resp.status_code == 201
    job_a_id = create_resp.json()["id"]

    # Client B tries to retrieve Client A's job -> 404 Not Found
    get_resp_b = await api_client.get(f"/api/v1/jobs/{job_a_id}", headers=headers_b)
    assert get_resp_b.status_code == 404
    assert get_resp_b.json()["detail"] == "Job not found."


# 3. Client B cannot cancel or retry A's jobs
@pytest.mark.asyncio
async def test_client_b_cannot_cancel_or_retry_a_jobs(
    api_client, test_clients_and_keys, db_session
):
    headers_a = test_clients_and_keys["headers_a"]
    headers_b = test_clients_and_keys["headers_b"]

    # Client A creates job
    create_resp = await api_client.post(
        "/api/v1/jobs/",
        headers=headers_a,
        json={"job_type": "report", "payload": {"data": "test_retry_cancel"}},
    )
    assert create_resp.status_code == 201
    job_a_id = create_resp.json()["id"]

    # Set job status to failed so it would be eligible for retry if owned
    job_db = await db_session.get(Job, job_a_id)
    job_db.status = JobStatus.FAILED
    await db_session.commit()

    # Client B attempts retry on Client A's job -> 404 Not Found
    retry_resp_b = await api_client.post(
        f"/api/v1/jobs/{job_a_id}/retry", headers=headers_b
    )
    assert retry_resp_b.status_code == 404
    assert retry_resp_b.json()["detail"] == "Failed job not found for this client."

    # Client B attempts cancel on Client A's job -> 404 Not Found
    cancel_resp_b = await api_client.post(
        f"/api/v1/jobs/{job_a_id}/cancel", headers=headers_b
    )
    assert cancel_resp_b.status_code == 404

    # Client A can successfully retry its own failed job -> 200 OK
    retry_resp_a = await api_client.post(
        f"/api/v1/jobs/{job_a_id}/retry", headers=headers_a
    )
    assert retry_resp_a.status_code == 200
    assert retry_resp_a.json()["status"] == "queued"


# 4. Client B cannot access A's job attempts or results
@pytest.mark.asyncio
async def test_client_b_cannot_access_a_job_attempts_or_results(
    api_client, test_clients_and_keys, db_session
):
    headers_a = test_clients_and_keys["headers_a"]
    headers_b = test_clients_and_keys["headers_b"]

    # Client A creates job
    create_resp = await api_client.post(
        "/api/v1/jobs/",
        headers=headers_a,
        json={"job_type": "report", "payload": {"data": "attempts_results_test"}},
    )
    assert create_resp.status_code == 201
    job_a_id = create_resp.json()["id"]

    # Client B attempts to fetch attempts or results sub-resources -> 404
    attempts_resp_b = await api_client.get(
        f"/api/v1/jobs/{job_a_id}/attempts", headers=headers_b
    )
    assert attempts_resp_b.status_code == 404

    results_resp_b = await api_client.get(
        f"/api/v1/jobs/{job_a_id}/results", headers=headers_b
    )
    assert results_resp_b.status_code == 404

    # Furthermore, getting the job itself returns 404, preventing leaking any attempt/result metadata
    get_resp_b = await api_client.get(f"/api/v1/jobs/{job_a_id}", headers=headers_b)
    assert get_resp_b.status_code == 404


# 5. Job listings contain only authenticated client's jobs
@pytest.mark.asyncio
async def test_job_listings_contain_only_authenticated_client_jobs(
    api_client, test_clients_and_keys
):
    headers_a = test_clients_and_keys["headers_a"]
    headers_b = test_clients_and_keys["headers_b"]

    # Client A creates 2 jobs
    resp_a1 = await api_client.post(
        "/api/v1/jobs/",
        headers=headers_a,
        json={"job_type": "report", "payload": {"idx": 1}},
    )
    resp_a2 = await api_client.post(
        "/api/v1/jobs/",
        headers=headers_a,
        json={"job_type": "report", "payload": {"idx": 2}},
    )
    job_a1_id = resp_a1.json()["id"]
    job_a2_id = resp_a2.json()["id"]

    # Client B creates 1 job
    resp_b1 = await api_client.post(
        "/api/v1/jobs/",
        headers=headers_b,
        json={"job_type": "report", "payload": {"idx": 3}},
    )
    job_b1_id = resp_b1.json()["id"]

    # Listing for Client A contains only A's jobs
    list_a = await api_client.get("/api/v1/jobs/", headers=headers_a)
    assert list_a.status_code == 200
    ids_a = [item["id"] for item in list_a.json()["items"]]
    assert job_a1_id in ids_a
    assert job_a2_id in ids_a
    assert job_b1_id not in ids_a

    # Listing for Client B contains only B's jobs
    list_b = await api_client.get("/api/v1/jobs/", headers=headers_b)
    assert list_b.status_code == 200
    ids_b = [item["id"] for item in list_b.json()["items"]]
    assert job_b1_id in ids_b
    assert job_a1_id not in ids_b
    assert job_a2_id not in ids_b


# 6. Forged client_id cannot change job ownership
@pytest.mark.asyncio
async def test_forged_client_id_cannot_change_job_ownership(
    api_client, test_clients_and_keys, db_session
):
    client_a = test_clients_and_keys["client_a"]
    client_b = test_clients_and_keys["client_b"]
    headers_b = test_clients_and_keys["headers_b"]
    headers_a = test_clients_and_keys["headers_a"]

    # Client B sends a job submission with forged client_id matching Client A
    forged_resp = await api_client.post(
        f"/api/v1/jobs/?client_id={client_a.id}",
        headers={**headers_b, "X-Client-ID": str(client_a.id)},
        json={
            "job_type": "report",
            "payload": {"data": "forged_attempt"},
            "client_id": client_a.id,
        },
    )
    assert forged_resp.status_code == 201
    job_id = forged_resp.json()["id"]

    # Verify directly in DB that job was assigned to Client B, NOT Client A
    job_db = await db_session.get(Job, job_id)
    assert job_db.client_id == client_b.id

    # Client A cannot access this job (404)
    get_resp_a = await api_client.get(f"/api/v1/jobs/{job_id}", headers=headers_a)
    assert get_resp_a.status_code == 404

    # Client B can access this job (200)
    get_resp_b = await api_client.get(f"/api/v1/jobs/{job_id}", headers=headers_b)
    assert get_resp_b.status_code == 200


# 7. Both clients can use the same idempotency key independently
@pytest.mark.asyncio
async def test_both_clients_can_use_same_idempotency_key_independently(
    api_client, test_clients_and_keys
):
    headers_a = test_clients_and_keys["headers_a"]
    headers_b = test_clients_and_keys["headers_b"]
    shared_key = "cross-tenant-shared-idempotency-key-xyz"

    resp_a = await api_client.post(
        "/api/v1/jobs/",
        headers={**headers_a, "Idempotency-Key": shared_key},
        json={"job_type": "report", "payload": {"data": "client_a_work"}},
    )
    assert resp_a.status_code == 201

    resp_b = await api_client.post(
        "/api/v1/jobs/",
        headers={**headers_b, "Idempotency-Key": shared_key},
        json={"job_type": "report", "payload": {"data": "client_b_work"}},
    )
    assert resp_b.status_code == 201

    # Distinct jobs were created for each tenant without conflict
    assert resp_a.json()["id"] != resp_b.json()["id"]
