"""Fixtures for real, isolated TaskFlow integration infrastructure.

Run only with TASKFLOW_INTEGRATION=1 and a database whose name contains
``test``.  This guard prevents lifecycle tests from truncating development data.
"""

import os
from contextlib import asynccontextmanager
from typing import Awaitable, Callable

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from sqlalchemy import text


INTEGRATION_ENABLED = os.getenv("TASKFLOW_INTEGRATION") == "1"
DATABASE_URL = os.getenv("DATABASE_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_STREAM = os.getenv("TASKFLOW_REDIS_STREAM", "taskflow:integration:jobs")
REDIS_GROUP = os.getenv(
    "TASKFLOW_REDIS_CONSUMER_GROUP", "taskflow-integration-workers"
)


def pytest_collection_modifyitems(config, items):
    if INTEGRATION_ENABLED:
        if "test" not in DATABASE_URL.rsplit("/", 1)[-1].lower():
            raise pytest.UsageError(
                "Integration DATABASE_URL must target a database with 'test' in its name"
            )
        return

    skip = pytest.mark.skip(
        reason="set TASKFLOW_INTEGRATION=1 with isolated PostgreSQL and Redis"
    )
    for item in items:
        item.add_marker(skip)


@pytest_asyncio.fixture
async def integration_redis():
    client = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except Exception as exc:
        await client.aclose()
        pytest.skip(f"Integration Redis is unavailable: {exc}")
    yield client
    await client.aclose()


@pytest_asyncio.fixture(autouse=True)
async def clean_integration_state(integration_redis):
    from src.db.database import SessionLocal

    async with SessionLocal() as db:
        await db.execute(
            text(
                "TRUNCATE TABLE job_outputs, job_attempts, outbox_events, workers, jobs "
                "RESTART IDENTITY CASCADE"
            )
        )
        await db.commit()
    await integration_redis.delete(REDIS_STREAM)
    yield
    await integration_redis.delete(REDIS_STREAM)
    from src.config.redis import close_redis

    await close_redis()


async def eventually(
    assertion: Callable[[], Awaitable[bool]], timeout_seconds: float = 10.0
) -> None:
    """Bounded polling helper for components that advance asynchronously."""
    import asyncio

    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        if await assertion():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before the integration timeout")
        await asyncio.sleep(0.1)
