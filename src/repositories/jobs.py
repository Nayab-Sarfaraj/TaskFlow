import os
from datetime import datetime, timezone

from src.models.jobs import Job
from src.models.job_attempts import JobAttempt
from src.models.outbox_events import OutboxEvent
from sqlalchemy.ext.asyncio import AsyncSession
from src.schemas.jobs import job_submission_request
from src.exceptions import (
    IdempotencyConflictError,
    JobCreationError,
    JobRetryConflictError,
    JobRetryError,
    QueueCapacityExceeded,
)
from typing import Optional
from sqlalchemy.exc import IntegrityError
from sqlalchemy import delete, func, select, text, update
from src.models.jobs import JobStatus, JobType
from src.utils.hash import generate_request_fingerprint
from src.config.retry import RetryPolicy


MAX_PENDING_JOBS = int(os.getenv("MAX_PENDING_JOBS", "1000"))
PENDING_CAPACITY_LOCK_KEY = "taskflow:pending-job-capacity"


async def _lock_and_check_pending_capacity(db: AsyncSession) -> None:
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
        {"lock_key": PENDING_CAPACITY_LOCK_KEY},
    )
    pending_count = await db.scalar(
        select(func.count(Job.id)).where(
            Job.status.in_([JobStatus.QUEUED, JobStatus.RETRYING])
        )
    )
    if pending_count >= MAX_PENDING_JOBS:
        raise QueueCapacityExceeded


async def create_job(
    db: AsyncSession,
    job: job_submission_request,
    client_id: int,
    idempotency_key: Optional[str] = None,
    request_id: Optional[str] = None,
):
    fingerprint = generate_request_fingerprint(job)
    try:
        if idempotency_key is not None:
            existing_job = await db.scalar(
                select(Job).where(
                    Job.client_id == client_id,
                    Job.idempotency_key == idempotency_key,
                )
            )
            if existing_job is not None:
                if existing_job.request_fingerprint == fingerprint:
                    existing_job._taskflow_created_new = False
                    return existing_job
                raise IdempotencyConflictError

        await _lock_and_check_pending_capacity(db)

        # Recheck after acquiring the lock so concurrent identical requests
        # cannot both pass the initial idempotency lookup.
        if idempotency_key is not None:
            existing_job = await db.scalar(
                select(Job).where(
                    Job.client_id == client_id,
                    Job.idempotency_key == idempotency_key,
                )
            )
            if existing_job is not None:
                if existing_job.request_fingerprint == fingerprint:
                    existing_job._taskflow_created_new = False
                    return existing_job
                raise IdempotencyConflictError

        new_job = Job(
            job_type=job.job_type,
            payload=job.payload,
            priority=job.priority,
            max_attempts=job.max_retries + 1,
            client_id=client_id,
            request_id=request_id,
            request_fingerprint=fingerprint,
        )

        if job.scheduled_at is not None:
            new_job.scheduled_at = job.scheduled_at
        new_job.idempotency_key = idempotency_key

        db.add(new_job)
        await db.flush()

        outbox_event = OutboxEvent(
            aggregate_id=new_job.id,
            event_type="job.created",
            payload=new_job.payload,
            request_id=request_id,
            client_id=client_id,
        )

        db.add(outbox_event)
        await db.commit()
        await db.refresh(new_job)
        new_job._taskflow_created_new = True

        return new_job

    except QueueCapacityExceeded:
        await db.rollback()
        raise
    except IdempotencyConflictError:
        await db.rollback()
        raise
    except IntegrityError as exc:
        await db.rollback()

        if idempotency_key is not None:
            statement = select(Job).where(
                Job.client_id == client_id,
                Job.idempotency_key == idempotency_key,
            )
            result = await db.execute(statement)
            existing_job = result.scalar_one_or_none()

            if existing_job and existing_job.request_fingerprint == fingerprint:
                existing_job._taskflow_created_new = False
                return existing_job

            if existing_job:
                raise IdempotencyConflictError from exc

        raise JobCreationError from exc
    except Exception as exc:
        await db.rollback()
        raise JobCreationError from exc


async def get_client_jobs(
    db: AsyncSession,
    client_id: int,
    limit: int,
    offset: int,
    status: Optional[JobStatus] = None,
    job_type: Optional[JobType] = None,
):
    filters = [Job.client_id == client_id]
    if status is not None:
        filters.append(Job.status == status)
    if job_type is not None:
        filters.append(Job.job_type == job_type)

    count_statement = select(func.count()).select_from(Job).where(*filters)
    total_result = await db.execute(count_statement)
    total = total_result.scalar_one()

    statement = (
        select(Job)
        .where(*filters)
        .order_by(Job.created_at.desc(), Job.id.desc())
        .offset(offset)
        .limit(limit)
    )

    result = await db.execute(statement)
    jobs = result.scalars().all()
    return jobs, total


async def get_job(db: AsyncSession, job_id: int, client_id: int):
    statement = select(Job).where(
        Job.id == job_id,
        Job.client_id == client_id,
    )
    result = await db.execute(statement)
    return result.scalar_one_or_none()


async def start_manual_retry(
    db: AsyncSession,
    job_id: int,
    client_id: int,
):
    try:
        await _lock_and_check_pending_capacity(db)
        statement = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.client_id == client_id,
                Job.status == JobStatus.FAILED,
            )
            .values(
                status=JobStatus.QUEUED,
                scheduled_at=datetime.now(timezone.utc),
                error_message=None,
                current_attempt_id=None,
                attempt_count=0,
                retry_cycle_start=(
                    select(func.coalesce(func.max(JobAttempt.attempt_number), 0))
                    .where(JobAttempt.job_id == job_id)
                    .scalar_subquery()
                ),
            )
            .returning(Job)
        )
        result = await db.execute(statement)
        updated_job = result.scalar_one_or_none()

        if updated_job is None:
            await db.rollback()
            existing_job_statement = select(Job).where(
                Job.id == job_id,
                Job.client_id == client_id,
            )
            existing_job_result = await db.execute(existing_job_statement)
            existing_job = existing_job_result.scalar_one_or_none()
            if existing_job is not None:
                raise JobRetryConflictError
            return None

        db.add(
            OutboxEvent(
                aggregate_id=updated_job.id,
                event_type="job.created",
                payload=updated_job.payload,
                request_id=updated_job.request_id,
                client_id=updated_job.client_id,
            )
        )
        await db.commit()
        await db.refresh(updated_job)
        return updated_job
    except JobRetryConflictError:
        raise
    except QueueCapacityExceeded:
        await db.rollback()
        raise
    except Exception as exc:
        await db.rollback()
        raise JobRetryError from exc
