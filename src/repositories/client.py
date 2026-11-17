from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from src.schemas.client import register_client_request,register_client_response
from src.models.client import Client,ClientStatus
from src.exceptions import ClientRegisterationError
async def register_client(body:register_client_request,db:AsyncSession):
    try:
            new_client=Client(name=body.name)
            db.add(new_client)
            await db.commit()
            await db.refresh(new_client)
            return new_client
    except Exception as e:
            await db.rollback()
            raise ClientRegisterationError from e


async def get_client_by_id(client_id: int, db: AsyncSession) -> Optional[Client]:
    return await db.get(Client, client_id)

