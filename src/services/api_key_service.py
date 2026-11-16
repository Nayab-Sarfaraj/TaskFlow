import hashlib
import secrets
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.models.api_key import APIKey
from src.models.client import Client, ClientStatus


class ClientNotFoundError(Exception):
    pass


class ClientSuspendedError(Exception):
    pass


async def create_api_key(
    db: AsyncSession,
    client_id: int,
    name: str,
    expires_at: Optional[datetime] = None,
) -> tuple[APIKey, str]:
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
    try:
        db.add(api_key)
        await db.commit()
        await db.refresh(api_key)
    except Exception:
        await db.rollback()
        raise

    return api_key, raw_key
