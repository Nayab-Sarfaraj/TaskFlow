import hashlib
import secrets
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.api_key import APIKey
from src.models.client import Client, ClientStatus


class ClientNotFoundError(Exception):
    pass


class ClientSuspendedError(Exception):
    pass


class APIKeyNotFoundError(Exception):
    pass


KeyNotFoundError = APIKeyNotFoundError


async def create_api_key(
    db: AsyncSession,
    client_id: int,
    name: str,
    expires_at: Optional[datetime] = None,
) -> tuple[APIKey, str]:
    try:
        client = await db.get(Client, client_id)
        if client is None:
            raise ClientNotFoundError
        if client.status != ClientStatus.ACTIVE:
            raise ClientSuspendedError

        identifier = secrets.token_hex(4)
        secret = secrets.token_urlsafe(32)
        raw_key = f"tf_live_{identifier}_{secret}"
        key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
        key_prefix = f"tf_live_{identifier}"

        api_key = APIKey(
            client_id=client_id,
            name=name,
            key_hash=key_hash,
            key_prefix=key_prefix,
            expires_at=expires_at,
        )
        db.add(api_key)
        await db.commit()
        await db.refresh(api_key)
        return api_key, raw_key
    except (ClientNotFoundError, ClientSuspendedError):
        raise
    except Exception:
        await db.rollback()
        raise


async def list_api_keys(
    db: AsyncSession,
    client_id: int,
) -> list[APIKey]:
    try:
        client = await db.get(Client, client_id)
        if client is None:
            raise ClientNotFoundError

        result = await db.execute(
            select(APIKey)
            .where(APIKey.client_id == client_id)
            .order_by(APIKey.created_at.desc())
        )
        return list(result.scalars().all())
    except (ClientNotFoundError, APIKeyNotFoundError):
        raise
    except Exception:
        await db.rollback()
        raise


async def revoke_api_key(
    db: AsyncSession,
    client_id: int,
    key_id: int,
) -> APIKey:
    try:
        result = await db.execute(
            select(APIKey)
            .where(
                APIKey.id == key_id,
                APIKey.client_id == client_id,
            )
            .with_for_update()
        )
        api_key = result.scalar_one_or_none()
        if api_key is None:
            raise APIKeyNotFoundError

        if api_key.revoked_at is None:
            api_key.revoked_at = datetime.now(timezone.utc)
            await db.commit()
            await db.refresh(api_key)

        return api_key
    except (ClientNotFoundError, APIKeyNotFoundError):
        raise
    except Exception:
        await db.rollback()
        raise
