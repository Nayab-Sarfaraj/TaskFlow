import asyncio
import json
import logging
import os
import signal
import sys
from src.workers.groups import create_group
from src.config.redis import close_redis
from src.db.database import SessionLocal, engine
from sqlalchemy import or_, select, update,func,text
from src.models.jobs import Job, JobStatus, JobType
from src.models.job_attempts import JobAttempt, JobAttemptStatus
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timezone
from dotenv import load_dotenv
from redis.asyncio import Redis
from src.logging_config import configure_root_logger
from src.models.job_outputs import JobOutput
from src.workers.email import send_bulk_email
from src.workers.video import process_video
from src.workers.report import process_report
from src.workers.scraping import process_scraping
from src.config.retry import RetryPolicy
from src.utils.failure_classifier import is_retryable
from src.utils.failure_classifier import RetryableEmailError
from src.utils.retry_backoff import (
    calculate_next_execution_time,
    get_delay,
    should_retry,
)
from src.workers.lifecycle import (
    get_worker_id,
    register_worker,
    heart_beat_look,
    HeartBeatError,
    WorkerRegisterationError,
    lease_renewal,
)
from src.workers.stale_delector import abandoned_job_detecton, detect_stale_worker
import signal
from src.models.workers import Worker, WorkerStatus

logger = logging.getLogger(__name__)

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

WORKER_ID = os.getenv("WORKER_ID",1)
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "3"))
WORKER_SHUTDOWN_GRACE_PERIOD = float(
    os.getenv("WORKER_SHUTDOWN_GRACE_PERIOD", "30")
)
MAX_JOB_EXECUTION_SECONDS = float(
    os.getenv("MAX_JOB_EXECUTION_SECONDS", "300")
)
PENDING_RECOVERY_INTERVAL = 15
PENDING_IDLE_TIMEOUT_MS = 60_000
if WORKER_CONCURRENCY < 1:
    raise ValueError("WORKER_CONCURRENCY must be greater than zero")

shutdown_signal=asyncio.Event()


class ExecutionOwnershipLost(Exception):
    pass

async def handle_job(job: Job, job_attempt: JobAttempt):
    async def run_handler():
        if job.job_type == JobType.EMAIL:
            return await send_bulk_email(job=job, job_attempt=job_attempt)
        if job.job_type == JobType.VIDEO:
            return await process_video(job=job, job_attempt=job_attempt)
        if job.job_type == JobType.REPORT:
            return await process_report(job=job, job_attempt=job_attempt)
        if job.job_type == JobType.SCRAPING:
            return await process_scraping(job=job, job_attempt=job_attempt)

        # Placeholder for other job handlers until they are implemented.
        await asyncio.sleep(120)

    return await asyncio.wait_for(
        run_handler(),
        timeout=MAX_JOB_EXECUTION_SECONDS,
    )


async def process_messages(messages, worker_id, redis_client):
    for stream_name, entries in messages:
        for message_id, fields in entries:
            job_id = None
            attempt_number = None
            historical_attempt_number = None
            attempt_id = None
            processing_started = False
            operation_key = None
            job_type = None
            lease_renewal_task = None
            job_task = None
            execution_result = None
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
                            or_(
                                Job.scheduled_at.is_(None),
                                Job.scheduled_at <= datetime.now(timezone.utc),
                            ),
                        )
                        .values(
                            status=JobStatus.PROCESSING,
                            attempt_count=Job.attempt_count + 1,
                            started_at=datetime.now(timezone.utc),
                            lease_expires_at=func.now()+text("INTERVAL '60 seconds'")
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
                        job_type = updated_job.job_type
                        attempt_number = updated_job.attempt_count
                        historical_attempt_number = (
                            updated_job.retry_cycle_start + attempt_number
                        )
                        attempt = JobAttempt(
                            job_id=updated_job.id,
                            attempt_number=historical_attempt_number,
                            status=JobAttemptStatus.PROCESSING,
                            worker_id=worker_id,
                        )
                        db.add(attempt)
                        await db.flush()
                        attempt_id = attempt.id
                        await db.execute(
                            update(Job)
                            .where(Job.id == job_id, Job.status == JobStatus.PROCESSING)
                            .values(current_attempt_id=attempt_id)
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
                lease_renewal_task=asyncio.create_task(lease_renewal(job_id=job_id,attempt_id=attempt_id))
                job_task = asyncio.create_task(
                    handle_job(job=updated_job, job_attempt=attempt)
                )

                done, pending = await asyncio.wait(
                    {job_task, lease_renewal_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if lease_renewal_task in done:
                    # Renewal ended: ownership was lost or retry policy failed.
                    try:
                        lease_renewal_task.result()
                    except Exception as exc:
                        raise ExecutionOwnershipLost from exc
                    job_task.cancel()
                    await asyncio.gather(job_task, return_exceptions=True)
                    raise ExecutionOwnershipLost

                if job_task in done:
                    # Job completed first.
                    execution_result = await job_task
                    lease_renewal_task.cancel()
                    await asyncio.gather(
                        lease_renewal_task,
                        return_exceptions=True,
                    )

                async with SessionLocal() as db:
                    attempt_statement = (
                        update(JobAttempt)
                        .where(
                            JobAttempt.job_id == job_id,
                            JobAttempt.attempt_number == historical_attempt_number,
                            JobAttempt.id == attempt_id,
                            JobAttempt.status == JobAttemptStatus.PROCESSING,
                        )
                        .values(
                            status=JobAttemptStatus.SUCCEEDED,
                            finished_at=datetime.now(timezone.utc),
                        )
                    )
                    attempt_result = await db.execute(attempt_statement)

                    job_statement = (
                        update(Job)
                        .where(
                            Job.id == job_id,
                            Job.status == JobStatus.PROCESSING,
                            Job.current_attempt_id == attempt_id,
                            Job.lease_expires_at>func.now()
                        )
                        .values(
                            status=JobStatus.COMPLETED,
                            completed_at=datetime.now(timezone.utc),
                            lease_expires_at=None
                        )
                    )
                    job_result = await db.execute(job_statement)

                    if attempt_result.rowcount != 1 or job_result.rowcount != 1:

                        raise ExecutionOwnershipLost(
                            f"Worker {worker_id} lost ownership of job {job_id}"
                        )

                    operation_key = f"job:{job_id}:output"
                    add_output_statement = (
                        insert(JobOutput)
                        .values(
                            job_id=job_id,
                            job_type=job_type,
                            operation_key=operation_key,
                            result=execution_result
                            or {"message": "Job completed"},
                        )
                        .on_conflict_do_nothing(index_elements=["operation_key"])
                    )
                    await db.execute(add_output_statement)

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
            except ExecutionOwnershipLost:
                logger.warning(
                    "Worker %s lost execution ownership of job %s",
                    worker_id,
                    job_id,
                )
                if lease_renewal_task is not None:
                        lease_renewal_task.cancel()
                        await asyncio.gather(
                                            lease_renewal_task,
                                            return_exceptions=True,
                                        )
                # Do not mark the job as FAILED.
                # Leave this message pending for later reconciliation.
                continue

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
                    policy = RetryPolicy(job_type=job_type)
                    is_retryable_error = is_retryable(exception=exc)
                    error_message = str(exc)
                    if is_retryable_error:
                        can_retry = should_retry(
                            attempt_number=attempt_number, retry_policy=policy
                        )
                        if can_retry:
                            delay = get_delay(
                                attempt_number=attempt_number, retry_policy=policy
                            )
                            next_execution_time = calculate_next_execution_time(
                                delay=delay
                            )
                            async with SessionLocal() as db:
                                attempt_statement = (
                                    update(JobAttempt)
                                    .where(
                                        JobAttempt.job_id == job_id,
                                        JobAttempt.id == attempt_id,
                                        JobAttempt.attempt_number
                                        == historical_attempt_number,
                                        JobAttempt.status
                                        == JobAttemptStatus.PROCESSING,
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
                                        Job.current_attempt_id == attempt_id,
                                        Job.lease_expires_at>func.now()
                                    )
                                    .values(
                                        status=JobStatus.RETRYING,
                                        error_message=error_message,
                                        scheduled_at=next_execution_time,
                                        lease_expires_at=None
                                    )
                                )
                                job_result = await db.execute(job_statement)

                                if (
                                    attempt_result.rowcount != 1
                                    or job_result.rowcount != 1
                                ):
                                    raise RuntimeError(
                                        "Failure update did not update exactly one attempt and job"
                                    )

                                await db.commit()

                        else:
                            partial_email_completion = isinstance(
                                exc, RetryableEmailError
                            )
                            async with SessionLocal() as db:
                                attempt_statement = (
                                    update(JobAttempt)
                                    .where(
                                        JobAttempt.job_id == job_id,
                                        JobAttempt.id == attempt_id,
                                        JobAttempt.attempt_number
                                        == historical_attempt_number,
                                        JobAttempt.status
                                        == JobAttemptStatus.PROCESSING,
                                        
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
                                        Job.current_attempt_id == attempt_id,
                                        Job.lease_expires_at > func.now(),  
                                    )
                                    .values(
                                        status=(
                                            JobStatus.COMPLETED
                                            if partial_email_completion
                                            else JobStatus.FAILED
                                        ),
                                        error_message=(
                                            None
                                            if partial_email_completion
                                            else error_message
                                        ),
                                        scheduled_at=None,
                                        lease_expires_at=None
                                    )
                                )
                                job_result = await db.execute(job_statement)

                                if (
                                    attempt_result.rowcount != 1
                                    or job_result.rowcount != 1
                                ):
                                    raise RuntimeError(
                                        "Failure update did not update exactly one attempt and job"
                                    )

                                if (
                                    partial_email_completion
                                    and job_type == JobType.EMAIL
                                ):
                                    await db.execute(
                                        insert(JobOutput)
                                        .values(
                                            job_id=job_id,
                                            job_type=job_type,
                                            operation_key=f"job:{job_id}:output",
                                            result=exc.result,
                                        )
                                        .on_conflict_do_update(
                                            index_elements=["operation_key"],
                                            set_={
                                                "job_type": job_type,
                                                "result": exc.result,
                                            },
                                        )
                                    )

                                await db.commit()

                    else:
                        async with SessionLocal() as db:
                            attempt_statement = (
                                update(JobAttempt)
                                .where(
                                    JobAttempt.job_id == job_id,
                                    JobAttempt.id == attempt_id,
                                    JobAttempt.attempt_number
                                    == historical_attempt_number,
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
                                    Job.current_attempt_id == attempt_id,
                                    Job.lease_expires_at > func.now(),

                                )
                                .values(
                                    status=JobStatus.FAILED,
                                    error_message=error_message,
                                    scheduled_at=None,
                                    lease_expires_at=None
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
                    logger.info(
                        "Worker %s finalized job %s (attempt %s) after delivery handling",
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
            finally:
                for task in (job_task, lease_renewal_task):
                    if task is not None and not task.done():
                        task.cancel()
                tasks = tuple(
                    task
                    for task in (job_task, lease_renewal_task)
                    if task is not None
                )
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)


async def recover_pending_messages(redis_client, worker_id):
    """Inspect and reclaim abandoned messages from the workers group."""
    consumer = f"recovery_{worker_id}"
    while True:
        try:
            pending = await redis_client.xpending_range(
                "job_events", "workers", min="-", max="+", count=100
            )
            if pending:
                logger.info("Found %s pending job_events messages", len(pending))

            for pending_entry in pending:
                message_id = pending_entry["message_id"]
                idle_ms = pending_entry["time_since_delivered"]
                if idle_ms < PENDING_IDLE_TIMEOUT_MS:
                    continue

                raw_messages = await redis_client.xrange(
                    "job_events", min=message_id, max=message_id, count=1
                )
                if not raw_messages:
                    continue
                candidate_id, candidate_fields = raw_messages[0]
                try:
                    event = json.loads(candidate_fields["event"])
                    candidate_job_id = int(event["job_id"])
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    logger.exception(
                        "Malformed pending event in Redis message %s; skipping",
                        candidate_id,
                    )
                    continue
                async with SessionLocal() as db:
                    result = await db.execute(
                        select(Job).where(Job.id == candidate_job_id)
                    )
                    candidate_job = result.scalar_one_or_none()

                if (
                    candidate_job is not None
                    and candidate_job.status == JobStatus.PROCESSING
                    and candidate_job.lease_expires_at is not None
                    and candidate_job.lease_expires_at > datetime.now(timezone.utc)
                ):
                    logger.info(
                        "Skipping active message %s for job %s",
                        candidate_id,
                        candidate_job_id,
                    )
                    continue

                claimed = await redis_client.xclaim(
                    "job_events",
                    "workers",
                    consumer,
                    PENDING_IDLE_TIMEOUT_MS,
                    [candidate_id],
                )
                if not claimed:
                    continue
                message_id, fields = claimed[0]
                try:
                    job_id = candidate_job_id
                    async with SessionLocal() as db:
                        result = await db.execute(
                            select(Job).where(Job.id == job_id)
                        )
                        job = result.scalar_one_or_none()

                    if True:
                        if job is None:
                            logger.error(
                                "Reclaimed message %s references missing job %s",
                                message_id,
                                job_id,
                            )
                            await redis_client.xack(
                                "job_events", "workers", message_id
                            )
                        elif job.status in (
                            JobStatus.COMPLETED,
                            JobStatus.FAILED,
                            JobStatus.CANCELLED,
                        ):
                            await redis_client.xack(
                                "job_events", "workers", message_id
                            )
                        elif job.status == JobStatus.PROCESSING:
                            if job.lease_expires_at is not None and (
                                job.lease_expires_at > datetime.now(timezone.utc)
                            ):
                                logger.info(
                                    "Leaving message %s pending; job %s lease is valid",
                                    message_id,
                                    job_id,
                                )
                            else:
                                logger.info(
                                    "Leaving expired job %s for abandoned-job recovery",
                                    job_id,
                                )
                        elif job.status == JobStatus.RETRYING:
                            await redis_client.xack(
                                "job_events", "workers", message_id
                            )
                        elif job.status == JobStatus.QUEUED:
                            if job.scheduled_at is None or (
                                job.scheduled_at <= datetime.now(timezone.utc)
                            ):
                                await redis_client.xadd(
                                    "job_events",
                                    {"event": fields["event"]},
                                )
                                await redis_client.xack(
                                    "job_events", "workers", message_id
                                )
                            else:
                                logger.info(
                                    "Leaving message %s pending; job %s is not yet eligible",
                                    message_id,
                                    job_id,
                                )
                except Exception:
                    logger.exception(
                        "Failed PostgreSQL reconciliation for message %s",
                        message_id,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Pending Redis message recovery failed")

        await asyncio.sleep(PENDING_RECOVERY_INTERVAL)


async def execute_job(messages, semaphore, worker_id, redis_client):
    try:
        await process_messages(messages, worker_id, redis_client)
    finally:
        semaphore.release()


async def worker(
    worker_id: int,
    redis_client,
    shutdown_event: asyncio.Event,
):
    consumer = f"worker_{worker_id}"
    semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)
    active_tasks = set()

    while True:
        # FIX: Do not block forever on the concurrency semaphore during
        # shutdown. A long-running job can hold the only slot while this
        # loop waits for another one; waiting on both lets shutdown stop
        # consumption immediately.
        acquire_task = asyncio.create_task(semaphore.acquire())
        shutdown_wait_task = asyncio.create_task(shutdown_event.wait())
        done, _ = await asyncio.wait(
            {acquire_task, shutdown_wait_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if shutdown_wait_task in done:
            acquire_task.cancel()
            await asyncio.gather(acquire_task, return_exceptions=True)
            break
        shutdown_wait_task.cancel()
        await asyncio.gather(shutdown_wait_task, return_exceptions=True)

        if shutdown_event.is_set():
            semaphore.release()
            break
        try:
            messages = await redis_client.xreadgroup(
                groupname="workers",
                consumername=consumer,
                streams={"job_events": ">"},
                count=1,
                block=5000,
            )
            if not messages:
                semaphore.release()
                continue
            if shutdown_event.is_set():
                # Leave already-delivered messages pending for recovery.
                semaphore.release()
                break
            if messages:
                task = asyncio.create_task(
                    execute_job(messages, semaphore, worker_id, redis_client)
                )
                # this is to keep the track of active tasks okay and the task is discarded once the task is completed
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)

        except asyncio.CancelledError:
            semaphore.release()
            raise
        except Exception:
            semaphore.release()
            logger.exception("Worker %s failed", consumer)
            await asyncio.sleep(1)

    if active_tasks:
        logger.info(
            "Worker %s draining %s active task(s) for up to %s seconds",
            worker_id,
            len(active_tasks),
            WORKER_SHUTDOWN_GRACE_PERIOD,
        )
        done, pending = await asyncio.wait(
            tuple(active_tasks),
            timeout=WORKER_SHUTDOWN_GRACE_PERIOD,
        )
        if pending:
            logger.warning(
                "Worker %s grace period expired with %s task(s) still active",
                worker_id,
                len(pending),
            )
            for task in pending:
                task.cancel()
            _, still_running = await asyncio.wait(pending, timeout=5)
            if still_running:
                logger.error(
                    "Worker %s has %s task(s) that ignored cancellation",
                    worker_id,
                    len(still_running),
                )
        if done:
            await asyncio.gather(*done, return_exceptions=True)



async def begin_shutdown(signum,worker_id):
    logger.info("Received %s; requesting graceful shutdown", signum)

    async with SessionLocal() as db:
        try:
            await db.execute(
                update(Worker)
                .where(
                    Worker.id == worker_id,
                    Worker.status == WorkerStatus.ACTIVE,
                )
                .values(status=WorkerStatus.DRAINING)
            )
            await db.commit()
        except Exception:
            logger.exception(
                "Failed to mark worker %s as DRAINING during shutdown",
                worker_id,
            )


async def main():
    worker_id=get_worker_id()
    client = Redis.from_url(REDIS_URL, decode_responses=True)
    heartbeat_task=None
    failure_count=0
    abandoned_job_detecton_task=None
    detect_stale_worker_task=None
    pending_recovery_task=None
    worker_task = None
    shutdown_status_task = None
    shutdown_wait_task = None
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()
    shutdown_started = False

    def request_shutdown(signum, worker_id):
        nonlocal shutdown_started
        if shutdown_started:
            logger.warning("Shutdown already in progress; ignoring signal %s", signum)
            return
        shutdown_started = True
        logger.info("Received %s; requesting graceful shutdown", signal.Signals(signum).name)
        shutdown_event.set()
        # FIX: track the status update so cleanup cannot race ahead of it.
        nonlocal shutdown_status_task
        shutdown_status_task = asyncio.create_task(
            begin_shutdown(signum=signum, worker_id=worker_id)
        )
        
            

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, request_shutdown, sig,worker_id)

    try:
        await create_group(stream="job_events", group="workers", redis_client=client)
        await register_worker(worker_id=worker_id,concurrency_limit=WORKER_CONCURRENCY)

        # await asyncio.gather(*(worker(i+1) for i in range(WORKER_COUNT)))

        heartbeat_task=asyncio.create_task(heart_beat_look(worker_id=worker_id))
        detect_stale_worker_task=asyncio.create_task(detect_stale_worker())
        abandoned_job_detecton_task=asyncio.create_task(abandoned_job_detecton())
        pending_recovery_task=asyncio.create_task(
            recover_pending_messages(client, worker_id)
        )

        worker_task = asyncio.create_task(
            worker(
                worker_id,
                redis_client=client,
                shutdown_event=shutdown_event,
            )
        )
        # FIX: monitor both independent shutdown and worker failure.
        shutdown_wait_task = asyncio.create_task(shutdown_event.wait())
        done, _ = await asyncio.wait(
            {worker_task, heartbeat_task, shutdown_wait_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if heartbeat_task in done:
            # FIX: a failed heartbeat must stop new job consumption.
            try:
                heartbeat_task.result()
            except Exception:
                logger.exception("Worker heartbeat task failed; requesting shutdown")
            else:
                logger.error(
                    "Worker heartbeat task stopped unexpectedly; requesting shutdown"
                )
            request_shutdown(signal.SIGTERM, worker_id)
            await worker_task
        elif worker_task in done:
            worker_task.result()
        else:
            await worker_task
        if shutdown_status_task is not None:
            await shutdown_status_task
    except Exception as e:
        if isinstance(e,WorkerRegisterationError):
           sys.exit(1)
        if isinstance(e,HeartBeatError):
            sys.exit(1)
    finally:
        if shutdown_wait_task is not None and not shutdown_wait_task.done():
            shutdown_wait_task.cancel()
            await asyncio.gather(shutdown_wait_task, return_exceptions=True)
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
        if worker_task is not None and not worker_task.done():
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            await asyncio.gather(
                heartbeat_task,
                return_exceptions=True,
            )
        if detect_stale_worker_task is not None:
            detect_stale_worker_task.cancel()
            await asyncio.gather(
                            detect_stale_worker_task,
                            return_exceptions=True,
                        )
        if abandoned_job_detecton_task is not None:
            abandoned_job_detecton_task.cancel()
            await asyncio.gather(
                        abandoned_job_detecton_task,
                        return_exceptions=True,
                    )
        if pending_recovery_task is not None:
            pending_recovery_task.cancel()
            await asyncio.gather(
                pending_recovery_task,
                return_exceptions=True,
            )
        try:
            async with SessionLocal() as db:
                await db.execute(
                    update(Worker)
                    .where(
                        Worker.id == worker_id,
                        Worker.status.in_(
                            (WorkerStatus.ACTIVE, WorkerStatus.DRAINING)
                        ),
                    )
                    .values(status=WorkerStatus.OFFLINE)
                )
                await db.commit()
        except Exception:
            logger.exception(
                "Failed to mark worker %s as OFFLINE during shutdown",
                worker_id,
            )
        await close_redis()
        await engine.dispose()


if __name__ == "__main__":
    configure_root_logger()
    logger.info(
        "Starting worker %s with concurrency limit %s",
        WORKER_ID,
        WORKER_CONCURRENCY,
    )
    asyncio.run(main())
