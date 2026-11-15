import asyncio
import json
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from src.db.database import SessionLocal
from src.services import outbox_event as outbox_event_service
from src.config.redis import get_redis
from src.logging_config import configure_root_logger

logger = logging.getLogger(__name__)
load_dotenv()
redis_client = get_redis()
REDIS_STREAM = "job_events"
REDIS_CONSUMER_GROUP = "workers"
REDIS_MAX_IN_FLIGHT = int(os.getenv("REDIS_MAX_IN_FLIGHT", "6"))


async def publish_event(stream: str, event: dict):
    message_id = await redis_client.xadd(
        name=stream,
        fields={"event": json.dumps(event)},
    )
    return message_id


async def available_publish_capacity(redis) -> int:
    pending_summary = await redis.xpending(
        REDIS_STREAM,
        REDIS_CONSUMER_GROUP,
    )
    pending_count = int(pending_summary.get("pending", 0))
    return max(0, REDIS_MAX_IN_FLIGHT - pending_count)


async def publisher():
    while True:
        async with SessionLocal() as db:
            try:
                available_capacity = await available_publish_capacity(redis_client)

                if available_capacity <= 0:
                    logger.info(
                        "Redis in-flight limit reached (%s); delaying outbox publishing",
                        REDIS_MAX_IN_FLIGHT,
                    )
                    events = []
                else:
                    events = await outbox_event_service.fetch_unpublished_events(
                        db,
                        batch_size=min(100, available_capacity),
                    )

                published_event_ids = []
                for event in events:
                    payload = {
                        "job_id": str(event.aggregate_id),
                        "event_id": str(event.id),
                        "event_type": event.event_type,
                    }
                    message_id = await publish_event(REDIS_STREAM, payload)
                    logger.info(
                        "Published outbox event %s for job %s to Redis as %s",
                        event.id,
                        event.aggregate_id,
                        message_id,
                    )
                    event.published_at = datetime.now(timezone.utc)
                    published_event_ids.append(event.id)

                await db.commit()
                if published_event_ids:
                    logger.info(
                        "Marked %d outbox event(s) as published: %s",
                        len(published_event_ids),
                        published_event_ids,
                    )
            except Exception as e:
                await db.rollback()
                logger.exception("Outbox publisher failed")

        await asyncio.sleep(1)


if __name__ == "__main__":
    configure_root_logger()
    logger.info("Starting outbox publisher")
    asyncio.run(publisher())
