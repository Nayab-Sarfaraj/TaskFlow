import os
from datetime import datetime, timezone
import pytest
import pytest_asyncio
import httpx
from sqlalchemy import select, delete

from src.main import app
from src.db.database import get_db, SessionLocal
from src.models.client import Client, ClientStatus
from src.models.jobs import Job, JobStatus, JobType
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
    }

    # Cleanup
    job_ids = list((await db_session.scalars(select(Job.id).where(Job.client_id.in_([client_a.id, client_b.id])))).all())
    if job_ids:
        await db_session.execute(delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(job_ids)))
        await db_session.execute(delete(Job).where(Job.id.in_(job_ids)))
    await db_session.execute(delete(APIKey).where(APIKey.client_id.in_([client_a.id, client_b.id])))
    await db_session.execute(delete(Client).where(Client.id.in_([client_a.id, client_b.id])))
    await db_session.commit()


@pytest_asyncio.fixture
async def api_client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
async def test_tenant_isolation_scenarios(api_client, test_clients_and_keys, db_session):
    client_a = test_clients_and_keys["client_a"]
    client_b = test_clients_and_keys["client_b"]
    headers_a = {"X-API-Key": test_clients_and_keys["key_a"]}
    headers_b = {"X-API-Key": test_clients_and_keys["key_b"]}

    # 1. Client A submits a job -> belongs to Client A
    submit_resp_a = await api_client.post(
        "/api/v1/jobs/",
        headers=headers_a,
        json={
            "job_type": "report",
            "payload": {"data": "client_a_report"},
            "priority": 5,
            "max_retries": 2,
        },
    )
    assert submit_resp_a.status_code == 201, submit_resp_a.text
    job_a_data = submit_resp_a.json()
    job_a_id = job_a_data["id"]

    # Verify directly in DB that job belongs to client_a.id
    job_in_db = await db_session.get(Job, job_a_id)
    assert job_in_db is not None
    assert job_in_db.client_id == client_a.id

    # 2. Client A retrieves its job -> 200 OK
    get_resp_a = await api_client.get(f"/api/v1/jobs/{job_a_id}", headers=headers_a)
    assert get_resp_a.status_code == 200
    assert get_resp_a.json()["id"] == job_a_id

    # 3. Client B retrieves Client A's job -> 404 NOT FOUND
    get_resp_b = await api_client.get(f"/api/v1/jobs/{job_a_id}", headers=headers_b)
    assert get_resp_b.status_code == 404
    assert get_resp_b.json()["detail"] == "Job not found."

    # 4. Client B attempts to retry Client A's job -> 404 NOT FOUND
    # First set job status to failed so it would be retryable if owned
    job_in_db.status = JobStatus.FAILED
    await db_session.commit()

    retry_resp_b = await api_client.post(f"/api/v1/jobs/{job_a_id}/retry", headers=headers_b)
    assert retry_resp_b.status_code == 404
    assert retry_resp_b.json()["detail"] == "Failed job not found for this client."

    # But Client A can retry it -> 200 OK
    retry_resp_a = await api_client.post(f"/api/v1/jobs/{job_a_id}/retry", headers=headers_a)
    assert retry_resp_a.status_code == 200
    assert retry_resp_a.json()["status"] == "queued"

    # 5. Client A lists jobs -> only Client A's jobs returned
    # Submit a job for Client B
    submit_resp_b = await api_client.post(
        "/api/v1/jobs/",
        headers=headers_b,
        json={
            "job_type": "report",
            "payload": {"data": "client_b_report"},
            "priority": 5,
            "max_retries": 2,
        },
    )
    assert submit_resp_b.status_code == 201
    job_b_id = submit_resp_b.json()["id"]

    list_resp_a = await api_client.get("/api/v1/jobs/", headers=headers_a)
    assert list_resp_a.status_code == 200
    items_a = list_resp_a.json()["items"]
    ids_a = [item["id"] for item in items_a]
    assert job_a_id in ids_a
    assert job_b_id not in ids_a

    list_resp_b = await api_client.get("/api/v1/jobs/", headers=headers_b)
    assert list_resp_b.status_code == 200
    items_b = list_resp_b.json()["items"]
    ids_b = [item["id"] for item in items_b]
    assert job_b_id in ids_b
    assert job_a_id not in ids_b

    # 6. Forged client_id in payload or query is ignored
    # Client B sends a request attempting to forge client_id as client A's id
    forged_resp = await api_client.post(
        "/api/v1/jobs/",
        headers=headers_b,
        json={
            "job_type": "report",
            "payload": {"data": "forged_data"},
            "client_id": client_a.id,  # Forged parameter
        },
    )
    assert forged_resp.status_code == 201
    forged_job_id = forged_resp.json()["id"]
    forged_job_db = await db_session.get(Job, forged_job_id)
    # Must still belong to Client B, NOT Client A!
    assert forged_job_db.client_id == client_b.id

    # 7. Two clients reuse the same idempotency key -> No cross-client conflict
    shared_key = "tenant-shared-idempotency-key"
    idempotent_resp_a = await api_client.post(
        "/api/v1/jobs/",
        headers={**headers_a, "Idempotency-Key": shared_key},
        json={"job_type": "report", "payload": {"data": "payload_a"}},
    )
    assert idempotent_resp_a.status_code == 201

    idempotent_resp_b = await api_client.post(
        "/api/v1/jobs/",
        headers={**headers_b, "Idempotency-Key": shared_key},
        json={"job_type": "report", "payload": {"data": "payload_b"}},
    )
    assert idempotent_resp_b.status_code == 201
    assert idempotent_resp_a.json()["id"] != idempotent_resp_b.json()["id"]
