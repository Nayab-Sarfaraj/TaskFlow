import asyncio
import json
import logging
from src.workers.groups import redis_client, group_exits, create_group
from src.db.database import SessionLocal
from sqlalchemy import update
from src.models.jobs import Job, JobStatus
from src.models.job_attempts import JobAttempt,JobAttemptStatus
from datetime import datetime,timezone

logger = logging.getLogger(__name__)

async def process_messages(messages):
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
                            "Skipping job %s because it was not queued or does not exist",
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
                                worker_id="1",
                            )
                        )
                        await db.commit()
                        processing_started = True

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
                    "Processing %s for job %s",
                    event_type,
                    job_id,
                )

                # A missing/non-queued job is intentionally acknowledged because
                # there is no valid work left for this message.
                await redis_client.xack(stream_name, "workers", message_id)

            except Exception as exc:
                logger.exception("Failed to process message %s", message_id)

                if job_id is None:
                    continue

                # If the initial transaction never committed, there is no
                # durable processing attempt to mark as failed. Do not ACK.
                if not processing_started:
                    logger.error(
                        "Job %s was not durably started; leaving message %s pending",
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
                except Exception:
                    logger.exception(
                        "Failed to persist failure for job %s; leaving message pending",
                        job_id,
                    )

async def worker():
    await create_group(stream="job_events",group="workers")
    while True:
        messages =await redis_client.xreadgroup(
        groupname="workers",
        consumername="worker_1",
        streams={"job_events": ">"},
        count=1,
        block=5000
    )
        # print("got the job")
        if messages:
            await process_messages(messages)



if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info("Starting outbox publisher")
    asyncio.run(worker())
