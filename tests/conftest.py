import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.db import database as application_database


@pytest_asyncio.fixture(autouse=True)
async def reset_application_engine():
    """Prevent pooled asyncpg connections leaking between integration tests."""
    yield
    await application_database.engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def db_engine():
    url = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://nayab:taskflow@localhost:5435/taskflow",
    )
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect():
            pass
    except Exception as exc:
        await engine.dispose()
        pytest.skip(f"PostgreSQL is unavailable: {exc}")
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine):
    session_factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
