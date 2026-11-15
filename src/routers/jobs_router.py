import os
from typing import AsyncIterator

from dotenv import load_dotenv
from fastapi import APIRouter, status, Header, HTTPException, Depends, Request
from redis.asyncio import Redis
from src.schemas.jobs import (
    job_submission_request,
    job_response,
    get_client_jobs_response,
)
from src.dependencies.client import authenticate_client
from src.services.rate_limiter import check_rate_limit
from sqlalchemy.ext.asyncio import AsyncSession
from src.db.database import get_db
from src.services import jobs as job_service
from src.exceptions import (
    IdempotencyConflictError,
    JobCreationError,
    JobRetryConflictError,
    JobRetryError,
    JobRetryNotFoundError,
    QueueCapacityExceeded,
)
from src.models.jobs import JobStatus, JobType
from typing import Optional
from fastapi import Query
from src.logging_config import set_log_context

router = APIRouter()
load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


async def get_rate_limit_redis() -> AsyncIterator[Redis]:
    """Create and close a Redis client on the request event loop."""
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        yield redis_client
    finally:
        await redis_client.aclose()


# router
@router.post(
    "/",
    response_model=job_response,
    status_code=status.HTTP_201_CREATED,
)
async def submit_job(
    request: Request,
    job_request: job_submission_request,
    idempotency_key: Optional[str] = Header(default=None),
    client: dict = Depends(authenticate_client),
    db: AsyncSession = Depends(get_db),
    redis_client=Depends(get_rate_limit_redis),
):
    try:

        client_id = client["client_id"]
        set_log_context(service_name="api", client_id=client_id)
        await check_rate_limit(redis_client, client_id)
        request_id = getattr(request.state, "request_id", None)
        return await job_service.create_job(db, job_request, client_id, idempotency_key, request_id)
    except IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key was already used with different parameters.",
        )
    except QueueCapacityExceeded as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Pending job capacity is currently exhausted.",
            headers={"Retry-After": str(exc.retry_after_seconds)},
        )
    except JobCreationError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Job could not be created.",
        )


@router.get(
    "/", status_code=status.HTTP_200_OK, response_model=get_client_jobs_response
)
async def get_client_jobs(
    limit: int = Query(ge=0, le=100, default=10),
    offset: int = Query(default=0, ge=0),
    status: Optional[JobStatus] = None,
    job_type: Optional[JobType] = None,
    client: dict = Depends(authenticate_client),
    db: AsyncSession = Depends(get_db),
):

    set_log_context(service_name="api", client_id=client["client_id"])
    return await job_service.get_client_jobs(
        db,
        client_id=client["client_id"],
        limit=limit,
        offset=offset,
        status=status,
        job_type=job_type,
    )


@router.get(
    "/{job_id}",
    status_code=status.HTTP_200_OK,
    response_model=job_response,
)
async def get_job(
    job_id: int,
    client: dict = Depends(authenticate_client),
    db: AsyncSession = Depends(get_db),
):
    set_log_context(service_name="api", client_id=client["client_id"], job_id=str(job_id))
    job = await job_service.get_job(
        db,
        job_id=job_id,
        client_id=client["client_id"],
    )
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found.",
        )
    return job


@router.post(
    "/{id}/retry",
    status_code=status.HTTP_200_OK,
    response_model=job_response,
)
async def start_manual_retry(
    id: int,
    client: dict = Depends(authenticate_client),
    db: AsyncSession = Depends(get_db),
):
    try:
        set_log_context(service_name="api", client_id=client["client_id"], job_id=str(id))
        return await job_service.start_manual_retry(
            id,
            client_id=client["client_id"],
            db=db,
        )
    except JobRetryNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Failed job not found for this client.",
        )
    except JobRetryConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Job is not eligible for manual retry.",
        )
    except QueueCapacityExceeded as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Pending job capacity is currently exhausted.",
            headers={"Retry-After": str(exc.retry_after_seconds)},
        )
    except JobRetryError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Job could not be retried.",
        )
