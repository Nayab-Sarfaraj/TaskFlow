from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from src.schemas.client import register_client_request,register_client_response
from src.repositories import client as client_repository

async def register_client(body:register_client_request,db:AsyncSession):
    return await client_repository.register_client(body,db)


async def get_client_by_id(client_id: int, db: AsyncSession):
    return await client_repository.get_client_by_id(client_id, db)
