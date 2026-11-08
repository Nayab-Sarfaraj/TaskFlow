import asyncio
import json
import logging
import os
from src.workers.groups import  create_group
from src.config.redis import close_redis
from src.db.database import SessionLocal
from sqlalchemy import update
from src.models.jobs import Job, JobStatus
from src.models.job_attempts import JobAttempt,JobAttemptStatus
from datetime import datetime,timezone
from dotenv import load_dotenv
from redis.asyncio import Redis
from src.logging_config import configure_root_logger

logger = logging.getLogger(__name__)

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

WORKER_ID = os.getenv("WORKER_ID")
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "3"))
if WORKER_CONCURRENCY < 1:
    raise ValueError("WORKER_CONCURRENCY must be greater than zero")


async def process_messages(messages,worker_id,redis_client):
    for stream_name, entries in messages:
        for message_id, fields in entries:
            job_id = None
            attempt_number = None
            processing_started = False
            try:
                event = json.loads(fields["event"])
                job_id = int(event["job_id"])
                event_type = event["event_type"]

                async with SessionLocal() as db:
                    statement = (
                        update(Job)
                        .where(
                            Job.id == job_id,
                            Job.status == JobStatus.QUEUED,
                        )
                        .values(
                            status=JobStatus.PROCESSING,
                            attempt_count=Job.attempt_count + 1,
                            started_at=datetime.now(timezone.utc)
                        )
                        .returning(Job)
                    )
                    result = await db.execute(statement)
                    updated_job = result.scalar_one_or_none()

                    if updated_job is None:
                        await db.rollback()
                        logger.info(
                            "Worker %s skipping job %s: it is not queued or does not exist",
                            worker_id,
                            job_id,
                        )
                        await redis_client.xack(stream_name, "workers", message_id)
                        continue
                    else:
                        attempt_number = updated_job.attempt_count
                        db.add(
                            JobAttempt(
                                job_id=updated_job.id,
                                attempt_number=attempt_number,
                                status=JobAttemptStatus.PROCESSING,
                                worker_id=worker_id,
                            )
                        )
                        await db.commit()
                        processing_started = True

                logger.info(
                    "Worker %s started job %s (attempt %s, event %s)",
                    worker_id,
                    job_id,
                    attempt_number,
                    event_type,
                )

                # TODO: replace this with the actual job handler.
                await asyncio.sleep(2)

                async with SessionLocal() as db:
                    attempt_statement = (
                        update(JobAttempt)
                        .where(
                            JobAttempt.job_id == job_id,
                            JobAttempt.attempt_number == attempt_number,
                            JobAttempt.status == JobAttemptStatus.PROCESSING,
                        )
                        .values(
                            status=JobAttemptStatus.SUCCEEDED,
                            finished_at=datetime.now(timezone.utc),
                        )
                    )
                    attempt_result = await db.execute(attempt_statement)

                    job_statement = (
                        update(Job).where(Job.id==job_id,Job.status==JobStatus.PROCESSING).values(status=JobStatus.COMPLETED,completed_at=datetime.now(timezone.utc))
                    )
                    job_result = await db.execute(job_statement)

                    if attempt_result.rowcount != 1 or job_result.rowcount != 1:
                        raise RuntimeError(
                            "Success update did not update exactly one attempt and job"
                        )

                    await db.commit()
                logger.info(
                    "Worker %s completed job %s (attempt %s, event %s)",
                    worker_id,
                    job_id,
                    attempt_number,
                    event_type,
                )

                # A missing/non-queued job is intentionally acknowledged because
                # there is no valid work left for this message.
                try:
                    await redis_client.xack(stream_name, "workers", message_id)
                except Exception:
                    # The job is already durably complete. An ACK failure must
                    # not turn it into FAILED; leave the message pending for
                    # later recovery/reconciliation.
                    logger.exception(
                        "Worker %s failed to ACK completed job %s (message %s)",
                        worker_id,
                        job_id,
                        message_id,
                    )

            except Exception as exc:
                logger.exception(
                    "Worker %s failed processing job %s (message %s)",
                    worker_id,
                    job_id,
                    message_id,
                )

                if job_id is None:
                    continue

                # If the initial transaction never committed, there is no
                # durable processing attempt to mark as failed. Do not ACK.
                if not processing_started:
                    logger.error(
                        "Worker %s could not durably start job %s; "
                        "leaving message %s pending",
                        worker_id,
                        job_id,
                        message_id,
                    )
                    continue

                try:
                    error_message = str(exc)
                    async with SessionLocal() as db:
                        attempt_statement = (
                            update(JobAttempt)
                            .where(
                                JobAttempt.job_id == job_id,
                                JobAttempt.status == JobAttemptStatus.PROCESSING,
                            )
                            .values(
                                status=JobAttemptStatus.FAILED,
                                error_message=error_message,
                                finished_at=datetime.now(timezone.utc),
                            )
                        )
                        attempt_result = await db.execute(attempt_statement)

                        job_statement = (
                            update(Job)
                            .where(
                                Job.id == job_id,
                                Job.status == JobStatus.PROCESSING,
                            )
                            .values(
                                status=JobStatus.FAILED,
                                error_message=error_message,
                            )
                        )
                        job_result = await db.execute(job_statement)

                        if attempt_result.rowcount != 1 or job_result.rowcount != 1:
                            raise RuntimeError(
                                "Failure update did not update exactly one attempt and job"
                            )

                        await db.commit()

                    # Acknowledge only after both failure updates commit.
                    await redis_client.xack(stream_name, "workers", message_id)
                    logger.error(
                        "Worker %s marked job %s (attempt %s) as failed",
                        worker_id,
                        job_id,
                        attempt_number,
                    )
                except Exception:
                    logger.exception(
                        "Worker %s failed to persist failure for job %s; "
                        "leaving message pending",
                        worker_id,
                        job_id,
                    )

async def execute_job(messages,semaphore,worker_id,redis_client):
    try:
        await process_messages(messages,worker_id,redis_client)
    finally:
        semaphore.release()
    


async def worker(worker_id:int,redis_client):
    consumer=f"worker_{worker_id}"
    semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)
    active_tasks = set()
    
    while True:
        await semaphore.acquire()
        try:
            messages =await redis_client.xreadgroup(
            groupname="workers",
            consumername=consumer,
            streams={"job_events": ">"},
            count=1,
            block=5000
        )
            if not messages:
                semaphore.release()
                continue
            if messages:
                task =asyncio.create_task(
                    execute_job(messages,semaphore,worker_id,redis_client)
                )
                # this is to keep the track of active tasks okay and the task is discarded once the task is completed
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)
               
        except asyncio.CancelledError :
            semaphore.release()
            raise
        except Exception:
            semaphore.release()
            logger.exception("Worker %s failed", consumer)
            await asyncio.sleep(1)


async def main():
    
    client = Redis.from_url(
        REDIS_URL,
        decode_responses=True
    )
    try:
        await create_group(stream="job_events",group="workers",redis_client=client)
        # await asyncio.gather(*(worker(i+1) for i in range(WORKER_COUNT)))
        await worker(WORKER_ID,redis_client=client)
    finally:
        await close_redis()
if __name__ == "__main__":
    configure_root_logger()
    logger.info(
        "Starting worker %s with concurrency limit %s",
        WORKER_ID,
        WORKER_CONCURRENCY,
    )
    asyncio.run(main())
