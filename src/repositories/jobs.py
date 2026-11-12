from src.models.jobs import Job 
from src.models.outbox_events import OutboxEvent
from sqlalchemy.ext.asyncio import AsyncSession
from src.schemas.jobs import job_submission_request
from src.exceptions import JobCreationError, IdempotencyConflictError
from typing import Optional
from sqlalchemy.exc import IntegrityError
from sqlalchemy import delete, func, select, update
from src.models.jobs import JobStatus,JobType
from src.utils.hash import generate_request_fingerprint
async def create_job(db: AsyncSession, job: job_submission_request, client_id: str,idempotency_key:Optional[str]=None):
    fingerprint = generate_request_fingerprint(job)
    try:
        new_job = Job(
            job_type=job.job_type,
            payload=job.payload,
            priority=job.priority,
            max_attempts=job.max_retries + 1,
            client_id=client_id,
            request_fingerprint=fingerprint
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
        )

        db.add(outbox_event)
        await db.commit()
        await db.refresh(new_job)

        return new_job

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
                return existing_job

            if existing_job:
                raise IdempotencyConflictError from exc

        raise JobCreationError from exc
    except Exception as exc:
        await db.rollback()
        raise JobCreationError from exc


async def get_client_jobs(db: AsyncSession, client_id: str,limit:int,offset:int,status:Optional[JobStatus]=None,job_type:Optional[JobType]=None,):
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


async def get_job(db: AsyncSession, job_id: int, client_id: str):
    statement = select(Job).where(
        Job.id == job_id,
        Job.client_id == client_id,
    )
    result = await db.execute(statement)
    return result.scalar_one_or_none()
    
