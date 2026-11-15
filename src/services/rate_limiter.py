import math
import time
import uuid

from fastapi import HTTPException
from redis.asyncio import Redis

RATE_LIMIT = 60
WINDOW_MS = 60_000

RATE_LIMIT_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)

local count = redis.call('ZCARD', key)

if count >= limit then
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retry_ms = tonumber(oldest[2]) + window - now
    return {0, retry_ms}
end

redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, window)

return {1, 0}
"""


async def check_rate_limit(redis: Redis, client_id: str):
    key = f"rate_limit:jobs:{client_id}"

    now_ms = int(time.time() * 1000)

    allowed, retry_ms = await redis.eval(
        RATE_LIMIT_SCRIPT,
        1,
        key,
        now_ms,
        WINDOW_MS,
        RATE_LIMIT,
        str(uuid.uuid4()),
    )

    if not allowed:
        retry_after = max(1, math.ceil(retry_ms / 1000))

        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Maximum 60 jobs per minute.",
            headers={"Retry-After": str(retry_after)},
        )