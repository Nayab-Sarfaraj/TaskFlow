import httpx
import pytest
from sqlalchemy import func, select

from src.main import app
from src.models.jobs import Job
from src.models.outbox_events import OutboxEvent


@pytest.mark.asyncio
async def test_same_client_and_idempotency_key_create_one_job_and_outbox_event():
    payload = {
        "job_type": "report",
        "payload": {
            "report_type": "idempotency-integration",
            "format": "pdf",
            "execution_time_seconds": 0,
        },
    }
    headers = {
        "X-API-Key": "test_key_abc123",
        "Idempotency-Key": "integration-idempotency-key",
        "X-Request-ID": "integration-idempotency-request",
    }
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.post("/api/v1/jobs/", headers=headers, json=payload)
        replay = await client.post("/api/v1/jobs/", headers=headers, json=payload)

    assert first.status_code == 201
    assert replay.status_code == 201
    assert first.json()["id"] == replay.json()["id"]

    from src.db.database import SessionLocal

    async with SessionLocal() as db:
        job_id = first.json()["id"]
        assert (
            await db.scalar(
                select(func.count()).select_from(Job).where(Job.id == job_id)
            )
            == 1
        )
        assert (
            await db.scalar(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.aggregate_id == job_id)
            )
            == 1
        )
