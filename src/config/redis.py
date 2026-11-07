import os
from typing import Optional

from dotenv import load_dotenv
from redis.asyncio import Redis

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

redis_client: Optional[Redis] = None


def get_redis() -> Redis:
    """Return the shared async Redis client."""
    global redis_client

    if redis_client is None:
        redis_client = Redis.from_url(
            REDIS_URL,
            decode_responses=True,
        )

    return redis_client


async def close_redis() -> None:
    """Close the shared Redis client during application shutdown."""
    global redis_client

    if redis_client is not None:
        await redis_client.aclose()
        redis_client = None
