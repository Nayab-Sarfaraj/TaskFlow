import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import func, or_, select, text, update

from src.db.database import SessionLocal
from src.config.retry import RetryPolicy
from src.models.workers import Worker, WorkerStatus
from src.models.jobs import Job, JobStatus
from src.models.job_attempts import JobAttempt, JobAttemptStatus

DETECTION_INTERVAL = 15
HEART_BEAT_TIMEOUT = 30
JOB_LEASE_DURATION = 60
RECOVERY_BATCH_SIZE = 100

logger = logging.getLogger(__name__)


async def detect_stale_worker():
    while True:
        try:
            async with SessionLocal() as db:
                null_heartbeat_result = await db.execute(
                    select(Worker.id)
                    .where(
                        Worker.status.in_((WorkerStatus.ACTIVE, WorkerStatus.DRAINING)),
                        Worker.last_heartbeat.is_(None),
                    )
                    .limit(RECOVERY_BATCH_SIZE)
                )
                for worker_id in null_heartbeat_result.scalars():
                    logger.error(
                        "Worker %s has an inconsistent NULL heartbeat",
                        worker_id,
                    )

                statement = (
                    update(Worker)
                    .where(
                        Worker.status.in_((WorkerStatus.ACTIVE, WorkerStatus.DRAINING)),
                        Worker.last_heartbeat.is_not(None),
                        Worker.last_heartbeat
                        < func.now() - text(f"INTERVAL '{HEART_BEAT_TIMEOUT} seconds'"),
                    )
                    .values(status=WorkerStatus.OFFLINE)
                    .returning(Worker.id)
                )
                result = await db.execute(statement)
                stale_worker_ids = result.scalars().all()
                await db.commit()
                for worker_id in stale_worker_ids:
                    logger.warning(
                        "Worker %s marked OFFLINE after stale heartbeat",
                        worker_id,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger(__name__).exception(
                "Stale worker detection failed",
            )

        await asyncio.sleep(DETECTION_INTERVAL)


async def abandoned_job_detecton():
    while True:
        for _ in range(RECOVERY_BATCH_SIZE):
            job_id = None
            try:
                async with SessionLocal() as db:
                    result = await db.execute(
                        select(Job)
                        .where(
                            Job.status == JobStatus.PROCESSING,
                            Job.lease_expires_at.is_not(None),
                            Job.lease_expires_at <= func.now(),
                        )
                        .order_by(Job.id)
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    job = result.scalar_one_or_none()
                    if job is None:
                        break

                    job_id = job.id
                    if job.current_attempt_id is None:
                        logger.error(
                            "Job %s has no current attempt; skipping recovery", job.id
                        )
                        await db.rollback()
                        continue

                    attempt_result = await db.execute(
                        update(JobAttempt)
                        .where(
                            JobAttempt.id == job.current_attempt_id,
                            JobAttempt.job_id == job.id,
                            JobAttempt.status == JobAttemptStatus.PROCESSING,
                        )
                        .values(
                            status=JobAttemptStatus.INTERRUPTED,
                            finished_at=datetime.now(timezone.utc),
                            error_message="Job lease expired",
                        )
                    )
                    if attempt_result.rowcount != 1:
                        logger.error(
                            "Job %s has invalid current attempt %s; skipping recovery",
                            job.id,
                            job.current_attempt_id,
                        )
                        await db.rollback()
                        continue

                    retry_policy = RetryPolicy(job_type=job.job_type)
                    next_status = (
                        JobStatus.RETRYING
                        if job.attempt_count < retry_policy.max_attempts
                        else JobStatus.FAILED
                    )
                    await db.execute(
                        update(Job)
                        .where(Job.id == job.id, Job.status == JobStatus.PROCESSING)
                        .values(
                            status=next_status,
                            current_attempt_id=None,
                            lease_expires_at=None,
                            scheduled_at=(
                                datetime.now(timezone.utc)
                                if next_status == JobStatus.RETRYING
                                else None
                            ),
                            error_message="Job lease expired",
                        )
                    )
                    await db.commit()
                    logger.warning(
                        "Recovered abandoned job %s as %s", job.id, next_status.value
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Recovery failed for abandoned job %s", job_id)

        try:
            async with SessionLocal() as db:
                result = await db.execute(
                    select(Job.id)
                    .where(
                        Job.status == JobStatus.PROCESSING,
                        Job.lease_expires_at.is_(None),
                    )
                    .limit(RECOVERY_BATCH_SIZE)
                )
                for job_id in result.scalars():
                    logger.error("Job %s has an inconsistent NULL lease", job_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Abandoned job detection scan failed")

        await asyncio.sleep(DETECTION_INTERVAL)


async def abandoned_job_detecton_legacy():
    while True:
        try:
            async with SessionLocal() as db:
                null_lease_result = await db.execute(
                    select(Job.id)
                    .where(
                        Job.status == JobStatus.PROCESSING,
                        Job.lease_expires_at.is_(None),
                    )
                    .limit(RECOVERY_BATCH_SIZE)
                )
                for job_id in null_lease_result.scalars():
                    logger.error("Job %s has an inconsistent NULL lease", job_id)

                result = await db.execute(
                    select(Job)
                    .where(
                        Job.status == JobStatus.PROCESSING,
                        Job.lease_expires_at.is_not(None),
                        Job.lease_expires_at <= func.now(),
                    )
                    .limit(RECOVERY_BATCH_SIZE)
                )
                expired_job_ids = [job.id for job in result.scalars().all()]

            for job_id in expired_job_ids:
                try:
                    async with SessionLocal() as db:
                        async with db.begin():
                            result = await db.execute(
                                select(Job)
                                .where(
                                    Job.id == job_id,
                                    Job.status == JobStatus.PROCESSING,
                                    Job.lease_expires_at.is_not(None),
                                    Job.lease_expires_at <= func.now(),
                                )
                                .with_for_update(skip_locked=True)
                            )
                            job = result.scalar_one_or_none()
                            if job is None:
                                continue

                            if job.current_attempt_id is None:
                                logger.error(
                                    "Job %s has no current attempt; skipping recovery",
                                    job.id,
                                )
                                continue

                            attempt_result = await db.execute(
                                update(JobAttempt)
                                .where(
                                    JobAttempt.id == job.current_attempt_id,
                                    JobAttempt.job_id == job.id,
                                    JobAttempt.status == JobAttemptStatus.PROCESSING,
                                )
                                .values(
                                    status=JobAttemptStatus.INTERRUPTED,
                                    finished_at=datetime.now(timezone.utc),
                                    error_message="Job lease expired",
                                )
                            )
                            if attempt_result.rowcount != 1:
                                logger.error(
                                    "Job %s has invalid current attempt %s; skipping recovery",
                                    job.id,
                                    job.current_attempt_id,
                                )
                                continue

                            retry_policy = RetryPolicy(job_type=job.job_type)
                            next_status = (
                                JobStatus.RETRYING
                                if job.attempt_count < retry_policy.max_attempts
                                else JobStatus.FAILED
                            )
                            await db.execute(
                                update(Job)
                                .where(
                                    Job.id == job.id,
                                    Job.status == JobStatus.PROCESSING,
                                )
                                .values(
                                    status=next_status,
                                    current_attempt_id=None,
                                    lease_expires_at=None,
                                    scheduled_at=(
                                        datetime.now(timezone.utc)
                                        if next_status == JobStatus.RETRYING
                                        else None
                                    ),
                                    error_message="Job lease expired",
                                )
                            )
                            logger.warning(
                                "Recovered abandoned job %s as %s",
                                job.id,
                                next_status.value,
                            )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Recovery failed for abandoned job %s",
                        job_id,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger(__name__).exception(
                "Abandoned job detection failed",
            )

        await asyncio.sleep(DETECTION_INTERVAL)
