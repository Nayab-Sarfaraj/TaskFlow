import hashlib
import hmac
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

load_dotenv()

from src.db.database import get_db
from src.models.api_key import APIKey
from src.models.client import Client, ClientStatus


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
admin_key_header = APIKeyHeader(name="X-Admin-Key", auto_error=False)


async def get_current_client(
    supplied_key: Optional[str] = Depends(api_key_header),
    db: AsyncSession = Depends(get_db),
) -> Client:
    if not supplied_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    key_hash = hashlib.sha256(supplied_key.encode("utf-8")).hexdigest()
    result = await db.execute(
        select(APIKey, Client)
        .join(Client, APIKey.client_id == Client.id)
        .where(APIKey.key_hash == key_hash)
    )
    row = result.one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    api_key, client = row
    now = datetime.now(timezone.utc)
    if api_key.revoked_at is not None or (
        api_key.expires_at is not None and api_key.expires_at <= now
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    if client.status != ClientStatus.ACTIVE:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Client is suspended",
        )

    # Throttle writes; an existing request transaction may commit this update.
    if (
        api_key.last_used_at is None
        or now - api_key.last_used_at >= timedelta(minutes=5)
    ):
        api_key.last_used_at = now
    return client


async def check_is_admin(
    x_admin_key: Optional[str] = Depends(admin_key_header),
) -> None:
    configured_key = os.getenv("ADMIN_API_KEY")
    if not configured_key or not x_admin_key or not hmac.compare_digest(
        x_admin_key, configured_key
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
