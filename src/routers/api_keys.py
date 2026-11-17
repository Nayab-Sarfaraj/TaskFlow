from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.database import get_db
from src.dependencies.client import check_is_admin
from src.schemas.api_key import (
    APIKeyCreate,
    APIKeyCreateResponse,
    APIKeyResponse,
)
from src.services.api_key_service import (
    APIKeyNotFoundError,
    ClientNotFoundError,
    ClientSuspendedError,
    KeyNotFoundError,
    create_api_key,
    list_api_keys,
    revoke_api_key,
)

router = APIRouter()


@router.post(
    "/{client_id}/keys",
    response_model=APIKeyCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register_api_key(
    client_id: int,
    body: APIKeyCreate,
    db: AsyncSession = Depends(get_db),
    _admin: None = Depends(check_is_admin),
):
    try:
        api_key, raw_key = await create_api_key(
            db=db,
            client_id=client_id,
            name=body.name,
            expires_at=body.expires_at,
        )
    except ClientNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found.",
        )
    except ClientSuspendedError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Suspended clients cannot receive API keys.",
        )

    return APIKeyCreateResponse(
        id=api_key.id,
        client_id=api_key.client_id,
        name=api_key.name,
        api_key=raw_key,
        key_prefix=api_key.key_prefix,
        created_at=api_key.created_at,
        expires_at=api_key.expires_at,
    )


@router.get(
    "/{client_id}/keys",
    response_model=list[APIKeyResponse],
    status_code=status.HTTP_200_OK,
)
async def list_client_api_keys(
    client_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: None = Depends(check_is_admin),
):
    try:
        return await list_api_keys(db=db, client_id=client_id)
    except ClientNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found.",
        )
    except (APIKeyNotFoundError, KeyNotFoundError):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found.",
        )


@router.delete(
    "/{client_id}/keys/{key_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def revoke_api_key_endpoint(
    client_id: int,
    key_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: None = Depends(check_is_admin),
):
    try:
        await revoke_api_key(db=db, client_id=client_id, key_id=key_id)
    except (APIKeyNotFoundError, KeyNotFoundError):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found.",
        )
