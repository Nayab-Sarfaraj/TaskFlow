import asyncio
import json
import logging
from datetime import datetime, timezone

from src.db.database import SessionLocal
from src.services import outbox_event as outbox_event_service
from src.config.redis import get_redis
from src.logging_config import configure_root_logger

logger = logging.getLogger(__name__)
redis_client = get_redis()


async def publish_event(stream: str, event: dict):
    message_id = await redis_client.xadd(
        name=stream,
        fields={"event": json.dumps(event)},
    )
    return message_id


async def publisher():
    while True:
        async with SessionLocal() as db:
            try:
                events = await outbox_event_service.fetch_unpublished_events(db)

                published_event_ids = []
                for event in events:
                    payload = {
                        "job_id": str(event.aggregate_id),
                        "event_id": str(event.id),
                        "event_type": event.event_type,
                    }
                    message_id = await publish_event("job_events", payload)
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
