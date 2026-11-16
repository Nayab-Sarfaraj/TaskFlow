from fastapi import APIRouter,status,HTTPException,Depends
from src.schemas.client import register_client_request,register_client_response
from sqlalchemy.ext.asyncio import AsyncSession
from src.services import client as client_service
from src.db.database import get_db
from src.exceptions import ClientRegisterationError
from src.dependencies.client import check_is_admin


router =APIRouter()

@router.post("/",status_code=status.HTTP_201_CREATED,response_model=register_client_response)
async def register_client(
    body: register_client_request,
    db: AsyncSession = Depends(get_db),
    _admin: None = Depends(check_is_admin),
):
    try:
        return await client_service.register_client(body,db)
    except ClientRegisterationError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Client could not be registered. Please try again later.",
        ) from exc


@router.get(
    "/{client_id}",
    status_code=status.HTTP_200_OK,
    response_model=register_client_response,
)
async def get_client(
    client_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: None = Depends(check_is_admin),
):
    client = await client_service.get_client_by_id(client_id, db)
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found.",
        )
    return client


