import os
from datetime import datetime, timezone
import pytest
import pytest_asyncio
import httpx
from sqlalchemy import delete, select

from src.main import app
from src.models.client import Client, ClientStatus
from src.models.api_key import APIKey
from src.models.jobs import Job
from src.models.outbox_events import OutboxEvent
from src.services.api_key_service import create_api_key


ADMIN_KEY = "test_phase10_admin_secret"


@pytest_asyncio.fixture
async def setup_client_with_keys(db_session, monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", ADMIN_KEY)

    client_a = Client(name="Phase10 Client A", status=ClientStatus.ACTIVE)
    client_b = Client(name="Phase10 Client B", status=ClientStatus.ACTIVE)
    db_session.add(client_a)
    db_session.add(client_b)
    await db_session.commit()
    await db_session.refresh(client_a)
    await db_session.refresh(client_b)

    # Client A gets 3 keys
    key_a1, raw_a1 = await create_api_key(
        db_session, client_id=client_a.id, name="Production Key"
    )
    key_a2, raw_a2 = await create_api_key(
        db_session, client_id=client_a.id, name="Staging Key"
    )
    key_a3, raw_a3 = await create_api_key(
        db_session, client_id=client_a.id, name="CI/CD Key"
    )

    # Client B gets 1 key
    key_b1, raw_b1 = await create_api_key(
        db_session, client_id=client_b.id, name="Client B Key"
    )

    yield {
        "client_a": client_a,
        "client_b": client_b,
        "keys_a": [key_a1, key_a2, key_a3],
        "raw_a": [raw_a1, raw_a2, raw_a3],
        "key_a1": key_a1,
        "key_a2": key_a2,
        "key_a3": key_a3,
        "raw_a1": raw_a1,
        "raw_a2": raw_a2,
        "raw_a3": raw_a3,
        "key_b1": key_b1,
        "raw_b1": raw_b1,
    }

    # Cleanup jobs, outbox_events, keys, clients
    job_ids = list(
        (
            await db_session.scalars(
                select(Job.id).where(Job.client_id.in_([client_a.id, client_b.id]))
            )
        ).all()
    )
    if job_ids:
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


# Checklist 1: List multiple API keys for one client
@pytest.mark.asyncio
async def test_list_multiple_api_keys_for_one_client(
    api_client, setup_client_with_keys
):
    client_a = setup_client_with_keys["client_a"]

    response = await api_client.get(
        f"/api/v1/clients/{client_a.id}/keys",
        headers={"X-Admin-Key": ADMIN_KEY},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert len(data) == 3

    names = {item["name"] for item in data}
    assert names == {"Production Key", "Staging Key", "CI/CD Key"}


# Checklist 2: Verify response never contains key_hash
@pytest.mark.asyncio
async def test_verify_response_never_contains_key_hash(
    api_client, setup_client_with_keys
):
    client_a = setup_client_with_keys["client_a"]
    raw_a1 = setup_client_with_keys["raw_a1"]

    response = await api_client.get(
        f"/api/v1/clients/{client_a.id}/keys",
        headers={"X-Admin-Key": ADMIN_KEY},
    )
    assert response.status_code == 200
    data = response.json()

    for item in data:
        # Expected safe metadata fields only
        assert "id" in item
        assert "name" in item
        assert "key_prefix" in item
        assert "created_at" in item
        assert "expires_at" in item
        assert "revoked_at" in item
        assert "last_used_at" in item

        # Critical security check: Never expose key_hash or raw keys
        assert "key_hash" not in item
        assert "api_key" not in item
        assert "raw_key" not in item
        assert "secret" not in item

        # Verify key_prefix is only the short identifier prefix, not the full key
        assert item["key_prefix"].startswith("tf_live_")
        assert len(item["key_prefix"]) < len(raw_a1)


# Checklist 3: Revoke key → authentication returns 401
@pytest.mark.asyncio
async def test_revoke_key_authentication_returns_401(
    api_client, setup_client_with_keys, db_session
):
    client_a = setup_client_with_keys["client_a"]
    key_a1 = setup_client_with_keys["key_a1"]
    raw_a1 = setup_client_with_keys["raw_a1"]

    # Revoke key_a1 using admin credentials
    revoke_resp = await api_client.delete(
        f"/api/v1/clients/{client_a.id}/keys/{key_a1.id}",
        headers={"X-Admin-Key": ADMIN_KEY},
    )
    assert revoke_resp.status_code == 204

    # 1. Calling job submission with the revoked key returns 401
    job_resp = await api_client.post(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_a1},
        json={"job_type": "report", "payload": {"test": "revoked"}},
    )
    assert job_resp.status_code == 401
    assert job_resp.json()["detail"] == "Invalid API key"

    # 2. Calling job listing with the revoked key returns 401
    list_resp = await api_client.get(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_a1},
    )
    assert list_resp.status_code == 401
    assert list_resp.json()["detail"] == "Invalid API key"


# Checklist 4: Other active keys continue working
@pytest.mark.asyncio
async def test_other_active_keys_continue_working(api_client, setup_client_with_keys):
    client_a = setup_client_with_keys["client_a"]
    key_a1 = setup_client_with_keys["key_a1"]
    raw_a1 = setup_client_with_keys["raw_a1"]
    raw_a2 = setup_client_with_keys["raw_a2"]

    # Revoke key_a1
    revoke_resp = await api_client.delete(
        f"/api/v1/clients/{client_a.id}/keys/{key_a1.id}",
        headers={"X-Admin-Key": ADMIN_KEY},
    )
    assert revoke_resp.status_code == 204

    # Key a1 is dead for job operations (401)
    fail_resp = await api_client.post(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_a1},
        json={"job_type": "report", "payload": {"test": "revoked_key_job"}},
    )
    assert fail_resp.status_code == 401

    # Key a2 is still alive and working for job operations (201 Created)
    job_resp = await api_client.post(
        "/api/v1/jobs/",
        headers={"X-API-Key": raw_a2},
        json={"job_type": "report", "payload": {"test": "active_key_test"}},
    )
    assert job_resp.status_code == 201


# Checklist 5: Revoke same key twice → both requests return 204
@pytest.mark.asyncio
async def test_revoke_same_key_twice_both_requests_return_204(
    api_client, setup_client_with_keys, db_session
):
    client_a = setup_client_with_keys["client_a"]
    key_a2 = setup_client_with_keys["key_a2"]
    headers = {"X-Admin-Key": ADMIN_KEY}

    # First revocation -> 204
    resp1 = await api_client.delete(
        f"/api/v1/clients/{client_a.id}/keys/{key_a2.id}",
        headers=headers,
    )
    assert resp1.status_code == 204

    db_key = await db_session.get(APIKey, key_a2.id)
    await db_session.refresh(db_key)
    first_timestamp = db_key.revoked_at
    assert first_timestamp is not None

    # Second revocation -> 204
    resp2 = await api_client.delete(
        f"/api/v1/clients/{client_a.id}/keys/{key_a2.id}",
        headers=headers,
    )
    assert resp2.status_code == 204

    # Timestamp remains unchanged
    await db_session.refresh(db_key)
    assert db_key.revoked_at == first_timestamp


# Checklist 6: Revoke another client's key → 404
@pytest.mark.asyncio
async def test_revoke_another_clients_key_returns_404(
    api_client, setup_client_with_keys, db_session
):
    client_b = setup_client_with_keys["client_b"]
    key_a1 = setup_client_with_keys["key_a1"]
    headers = {"X-Admin-Key": ADMIN_KEY}

    # Attempt to revoke Client A's key using Client B's client_id in path
    response = await api_client.delete(
        f"/api/v1/clients/{client_b.id}/keys/{key_a1.id}",
        headers=headers,
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "API key not found."

    # Key A was not revoked by the unauthorized attempt
    db_key = await db_session.get(APIKey, key_a1.id)
    await db_session.refresh(db_key)
    assert db_key.revoked_at is None


# Checklist 7: No admin credentials → 401
@pytest.mark.asyncio
async def test_no_admin_credentials_returns_401(api_client, setup_client_with_keys):
    client_a = setup_client_with_keys["client_a"]
    key_a3 = setup_client_with_keys["key_a3"]

    # 1. No credentials header
    resp_no_auth = await api_client.delete(
        f"/api/v1/clients/{client_a.id}/keys/{key_a3.id}"
    )
    assert resp_no_auth.status_code == 401
    assert resp_no_auth.json()["detail"] == "Invalid admin API key"

    # 2. Invalid admin key
    resp_bad_auth = await api_client.delete(
        f"/api/v1/clients/{client_a.id}/keys/{key_a3.id}",
        headers={"X-Admin-Key": "incorrect_admin_token"},
    )
    assert resp_bad_auth.status_code == 401
    assert resp_bad_auth.json()["detail"] == "Invalid admin API key"


# Admin-only client registration and key generation tests
@pytest.mark.asyncio
async def test_admin_only_client_registration_and_key_generation(
    api_client, setup_client_with_keys, db_session
):
    client_a = setup_client_with_keys["client_a"]

    # 1. Register client without admin key -> 401
    resp_reg_fail = await api_client.post(
        "/api/v1/clients/",
        json={"name": "Unauthorized Client Corp"},
    )
    assert resp_reg_fail.status_code == 401

    # 2. Register client with admin key -> 201
    resp_reg_ok = await api_client.post(
        "/api/v1/clients/",
        headers={"X-Admin-Key": ADMIN_KEY},
        json={"name": "Authorized Client Corp"},
    )
    assert resp_reg_ok.status_code == 201
    new_client_id = resp_reg_ok.json()["id"]

    try:
        # 3. Generate key without admin key -> 401
        resp_key_fail = await api_client.post(
            f"/api/v1/clients/{new_client_id}/keys",
            json={"name": "Unauthorized Key"},
        )
        assert resp_key_fail.status_code == 401

        # 4. Generate key with admin key -> 201
        resp_key_ok = await api_client.post(
            f"/api/v1/clients/{new_client_id}/keys",
            headers={"X-Admin-Key": ADMIN_KEY},
            json={"name": "Authorized Key"},
        )
        assert resp_key_ok.status_code == 201
        assert "api_key" in resp_key_ok.json()
        new_key_id = resp_key_ok.json()["id"]

        # 5. GET /api/v1/clients/{client_id} without admin key -> 401
        get_client_fail = await api_client.get(f"/api/v1/clients/{new_client_id}")
        assert get_client_fail.status_code == 401

        # 6. GET /api/v1/clients/{client_id} with admin key -> 200
        get_client_ok = await api_client.get(
            f"/api/v1/clients/{new_client_id}",
            headers={"X-Admin-Key": ADMIN_KEY},
        )
        assert get_client_ok.status_code == 200
        assert get_client_ok.json()["id"] == new_client_id

        # 7. GET /api/v1/clients/{client_id}/keys without admin key -> 401
        get_keys_fail = await api_client.get(f"/api/v1/clients/{new_client_id}/keys")
        assert get_keys_fail.status_code == 401

        # 8. GET /api/v1/clients/{client_id}/keys with admin key -> 200
        get_keys_ok = await api_client.get(
            f"/api/v1/clients/{new_client_id}/keys",
            headers={"X-Admin-Key": ADMIN_KEY},
        )
        assert get_keys_ok.status_code == 200
        assert len(get_keys_ok.json()) == 1

        # 9. DELETE /api/v1/clients/{client_id}/keys/{key_id} without admin key -> 401
        del_key_fail = await api_client.delete(
            f"/api/v1/clients/{new_client_id}/keys/{new_key_id}"
        )
        assert del_key_fail.status_code == 401

        # 10. DELETE /api/v1/clients/{client_id}/keys/{key_id} with admin key -> 204
        del_key_ok = await api_client.delete(
            f"/api/v1/clients/{new_client_id}/keys/{new_key_id}",
            headers={"X-Admin-Key": ADMIN_KEY},
        )
        assert del_key_ok.status_code == 204
    finally:
        # Cleanup newly created client and keys
        await db_session.execute(
            delete(APIKey).where(APIKey.client_id == new_client_id)
        )
        await db_session.execute(delete(Client).where(Client.id == new_client_id))
        await db_session.commit()
