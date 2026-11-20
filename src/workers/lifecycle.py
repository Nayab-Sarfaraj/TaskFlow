import asyncio
import logging
import os
import uuid

from sqlalchemy import func, text, update

from src.db.database import SessionLocal
from src.models.workers import Worker, WorkerStatus
from src.models.jobs import Job, JobStatus

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 10
JOB_LEASE_DURATION = int(os.getenv("JOB_LEASE_DURATION_SECONDS", "60"))
FAILURE_THRESHOLD = 3
JOB_LEASE_RENEW_INTERVAL = 15


class WorkerRegisterationError(Exception):
    pass


class HeartBeatError(Exception):
    pass


def get_worker_id():
    return f"worker-{uuid.uuid4().hex[:16]}"


async def register_worker(worker_id: str, concurrency_limit: int):
    async with SessionLocal() as db:
        try:
            new_worker = Worker(
                id=worker_id,
                concurrency_limit=concurrency_limit,
                status=WorkerStatus.ACTIVE,
                last_heartbeat=func.now(),
            )
            db.add(new_worker)
            await db.commit()
            logger.info("Registered worker %s", worker_id)
        except Exception as e:
            logger.warning(
                "Worker registration failed",
                extra={
                    "service_name": "worker",
                    "worker_id": worker_id,
                    "error_category": type(e).__name__,
                },
            )

            await db.rollback()
            raise WorkerRegisterationError


async def heart_beat_look(worker_id: str):
    failure_count = 0
    while True:
        try:
            async with SessionLocal() as db:
                update_statement = (
                    update(Worker)
                    .where(
                        Worker.id == worker_id,
                        Worker.status.in_((WorkerStatus.ACTIVE, WorkerStatus.DRAINING)),
                    )
                    .values(last_heartbeat=func.now())
                )
                result = await db.execute(update_statement)
                await db.commit()
                if result.rowcount != 1:
                    logger.warning(
                        "Worker %s is no longer active or draining",
                        worker_id,
                    )
                    return
                failure_count = 0
        except asyncio.CancelledError:
            raise

        except Exception as exc:
            failure_count = failure_count + 1
            logger.exception(
                "Heartbeat failed",
                extra={
                    "service_name": "worker",
                    "worker_id": worker_id,
                    "error_category": type(exc).__name__,
                },
            )
        finally:
            if failure_count >= FAILURE_THRESHOLD:
                raise HeartBeatError

        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def lease_renewal(job_id, attempt_id):
    while True:
        async with SessionLocal() as db:
            try:
                statement = (
                    update(Job)
                    .where(
                        Job.id == job_id,
                        Job.current_attempt_id == attempt_id,
                        Job.status == JobStatus.PROCESSING,
                        Job.lease_expires_at.is_not(None),
                        Job.lease_expires_at > func.now(),
                    )
                    .values(
                        lease_expires_at=func.now()
                        + text(f"INTERVAL '{JOB_LEASE_DURATION} seconds'")
                    )
                )
                result = await db.execute(statement)
                await db.commit()
                if result.rowcount != 1:
                    logger.warning(
                        "Lease for job %s is no longer active",
                        job_id,
                    )
                    return

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Lease renewal failed for job %s, attempt %s",
                    job_id,
                    attempt_id,
                )

        await asyncio.sleep(JOB_LEASE_RENEW_INTERVAL)
