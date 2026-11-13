from sqlalchemy.ext.asyncio import AsyncSession
from src.schemas.jobs import job_submission_request, get_client_jobs_response
from src.repositories import jobs as job_repository
from typing import Optional
from src.exceptions import JobRetryNotFoundError
from src.models.jobs import JobStatus, JobType


async def create_job(
    db: AsyncSession,
    job: job_submission_request,
    client_id: str,
    idempotency_key: Optional[str] = None,
):

    return await job_repository.create_job(db, job, client_id, idempotency_key)


async def get_client_jobs(
    db: AsyncSession,
    client_id: str,
    limit: int,
    offset: int,
    status: Optional[JobStatus] = None,
    job_type: Optional[JobType] = None,
):

    jobs, total = await job_repository.get_client_jobs(
        db,
        client_id,
        limit=limit,
        offset=offset,
        status=status,
        job_type=job_type,
    )
    return get_client_jobs_response(
        items=jobs,
        offset=offset,
        limit=limit,
        total=total,
    )


async def get_job(db: AsyncSession, job_id: int, client_id: str):
    return await job_repository.get_job(db, job_id=job_id, client_id=client_id)


async def start_manual_retry(
    job_id: int,
    client_id: str,
    db: AsyncSession,
):
    updated_job = await job_repository.start_manual_retry(
        db,
        job_id=job_id,
        client_id=client_id,
    )
    if updated_job is None:
        raise JobRetryNotFoundError
    return updated_job
