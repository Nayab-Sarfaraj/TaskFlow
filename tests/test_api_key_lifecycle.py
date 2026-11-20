import os
from datetime import datetime, timedelta, timezone
import pytest
import pytest_asyncio
import httpx
from sqlalchemy import delete, select

from src.main import app
from src.models.client import Client, ClientStatus
from src.models.api_key import APIKey
from src.models.jobs import Job
from src.models.job_attempts import JobAttempt
from src.models.job_outputs import JobOutput
from src.models.outbox_events import OutboxEvent
from src.services.api_key_service import create_api_key


ADMIN_KEY = "test_lifecycle_admin_secret"


@pytest_asyncio.fixture
async def setup_client_with_keys(db_session, monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", ADMIN_KEY)

    client = Client(name="Lifecycle Client", status=ClientStatus.ACTIVE)
    db_session.add(client)
    await db_session.commit()
    await db_session.refresh(client)

    # Key 1
    key_1, raw_key_1 = await create_api_key(
        db_session, client_id=client.id, name="Active Key 1"
    )
    # Key 2
    key_2, raw_key_2 = await create_api_key(
        db_session, client_id=client.id, name="Active Key 2"
    )
    # Expired Key (expires 1 hour in the past)
    expired_time = datetime.now(timezone.utc) - timedelta(hours=1)
    key_expired, raw_key_expired = await create_api_key(
        db_session,
        client_id=client.id,
        name="Expired Key",
        expires_at=expired_time,
    )

    yield {
        "client": client,
        "key_1": key_1,
        "raw_key_1": raw_key_1,
        "key_2": key_2,
        "raw_key_2": raw_key_2,
        "key_expired": key_expired,
        "raw_key_expired": raw_key_expired,
    }

    # Cleanup
    job_ids = list(
        (
            await db_session.scalars(select(Job.id).where(Job.client_id == client.id))
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
    await db_session.execute(delete(APIKey).where(APIKey.client_id == client.id))
    await db_session.execute(delete(Client).where(Client.id == client.id))
    await db_session.commit()


@pytest_asyncio.fixture
async def api_client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# 1. Two active keys authenticate successfully
@pytest.mark.asyncio
async def test_two_active_keys_authenticate_successfully(
    api_client, setup_client_with_keys
):
    raw_key_1 = setup_client_with_keys["raw_key_1"]
    raw_key_2 = setup_client_with_keys["raw_key_2"]

    # Key 1 submits job successfully
    resp_1 = await api_client.post(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key_1},
        json={"job_type": "report", "payload": {"source": "key_1"}},
    )
    assert resp_1.status_code == 201
    assert resp_1.json()["id"] is not None

    # Key 2 submits job successfully
    resp_2 = await api_client.post(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key_2},
        json={"job_type": "report", "payload": {"source": "key_2"}},
    )
    assert resp_2.status_code == 201
    assert resp_2.json()["id"] is not None


# 2. Revoking one key doesn't affect the other
@pytest.mark.asyncio
async def test_revoking_one_key_does_not_affect_the_other(
    api_client, setup_client_with_keys
):
    client = setup_client_with_keys["client"]
    key_1 = setup_client_with_keys["key_1"]
    raw_key_2 = setup_client_with_keys["raw_key_2"]

    # Revoke Key 1
    revoke_resp = await api_client.delete(
        f"/api/v1/clients/{client.id}/keys/{key_1.id}",
        headers={"X-Admin-Key": ADMIN_KEY},
    )
    assert revoke_resp.status_code == 204

    # Key 2 continues working unimpeded
    resp_2 = await api_client.post(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key_2},
        json={"job_type": "report", "payload": {"source": "key_2_after_key_1_revoked"}},
    )
    assert resp_2.status_code == 201

    list_resp = await api_client.get(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key_2},
    )
    assert list_resp.status_code == 200


# 3. Revoked keys immediately fail subsequent authentication
@pytest.mark.asyncio
async def test_revoked_keys_immediately_fail_subsequent_authentication(
    api_client, setup_client_with_keys
):
    client = setup_client_with_keys["client"]
    key_1 = setup_client_with_keys["key_1"]
    raw_key_1 = setup_client_with_keys["raw_key_1"]

    # Revoke Key 1
    revoke_resp = await api_client.delete(
        f"/api/v1/clients/{client.id}/keys/{key_1.id}",
        headers={"X-Admin-Key": ADMIN_KEY},
    )
    assert revoke_resp.status_code == 204

    # Immediate failure on subsequent requests
    resp = await api_client.post(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key_1},
        json={"job_type": "report", "payload": {"source": "should_fail"}},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid API key"

    get_resp = await api_client.get(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key_1},
    )
    assert get_resp.status_code == 401
    assert get_resp.json()["detail"] == "Invalid API key"


# 4. Repeated revocation returns 204
@pytest.mark.asyncio
async def test_repeated_revocation_returns_204(
    api_client, setup_client_with_keys, db_session
):
    client = setup_client_with_keys["client"]
    key_2 = setup_client_with_keys["key_2"]
    headers = {"X-Admin-Key": ADMIN_KEY}

    # First revocation -> 204
    resp1 = await api_client.delete(
        f"/api/v1/clients/{client.id}/keys/{key_2.id}",
        headers=headers,
    )
    assert resp1.status_code == 204

    db_key = await db_session.get(APIKey, key_2.id)
    await db_session.refresh(db_key)
    revoked_timestamp = db_key.revoked_at
    assert revoked_timestamp is not None

    # Repeated revocation -> 204
    resp2 = await api_client.delete(
        f"/api/v1/clients/{client.id}/keys/{key_2.id}",
        headers=headers,
    )
    assert resp2.status_code == 204

    # Timestamp is not mutated by repeated revocation
    await db_session.refresh(db_key)
    assert db_key.revoked_at == revoked_timestamp


# 5. Expired keys cannot authenticate
@pytest.mark.asyncio
async def test_expired_keys_cannot_authenticate(api_client, setup_client_with_keys):
    raw_key_expired = setup_client_with_keys["raw_key_expired"]

    # Submitting job with expired key -> 401
    resp = await api_client.post(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key_expired},
        json={"job_type": "report", "payload": {"source": "expired"}},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid API key"

    # Listing jobs with expired key -> 401
    get_resp = await api_client.get(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_key_expired},
    )
    assert get_resp.status_code == 401
    assert get_resp.json()["detail"] == "Invalid API key"


# 6. API key listings never expose hashes or secrets
@pytest.mark.asyncio
async def test_api_key_listings_never_expose_hashes_or_secrets(
    api_client, setup_client_with_keys
):
    client = setup_client_with_keys["client"]
    raw_key_1 = setup_client_with_keys["raw_key_1"]

    # List keys using admin key
    response = await api_client.get(
        f"/api/v1/clients/{client.id}/keys",
        headers={"X-Admin-Key": ADMIN_KEY},
    )
    assert response.status_code == 200
    keys = response.json()
    assert len(keys) >= 3

    for k in keys:
        # Safe public metadata fields must be present
        assert "id" in k
        assert "name" in k
        assert "key_prefix" in k
        assert "created_at" in k
        assert "expires_at" in k
        assert "revoked_at" in k
        assert "last_used_at" in k

        # Sensitive secrets and hashes MUST NEVER be exposed
        assert "key_hash" not in k
        assert "api_key" not in k
        assert "raw_key" not in k
        assert "secret" not in k
        assert "hashed_key" not in k

        # Key prefix should only be short prefix e.g. "tf_live_XXXX"
        assert k["key_prefix"].startswith("tf_live_")
        assert len(k["key_prefix"]) < len(raw_key_1)
