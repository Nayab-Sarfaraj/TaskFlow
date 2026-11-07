from fastapi import APIRouter,status,Header,HTTPException,Depends
from src.schemas.jobs import job_submission_request,job_response,get_client_jobs_response
from src.dependencies.client import authenticate_client
from sqlalchemy.ext.asyncio import AsyncSession
from src.db.database import get_db
from src.services import jobs as job_service
from src.exceptions import JobCreationError, IdempotencyConflictError
from src.models.jobs import JobStatus,JobType
from typing import Optional
from fastapi import Query
router = APIRouter()



# router
@router.post(
    "/",
    response_model=job_response,
    status_code=status.HTTP_201_CREATED,
)
async def submit_job(
    job_request: job_submission_request,
    idempotency_key:Optional[str]=Header(default=None),
    client: dict = Depends(authenticate_client),
    db: AsyncSession = Depends(get_db),
):
    try:
        print(idempotency_key)
        client_id = client["client_id"]
        return await job_service.create_job(db, job_request, client_id,idempotency_key)
    except IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key was already used with different parameters.",
        )
    except JobCreationError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Job could not be created.",
        )
    

@router.get("/",status_code=status.HTTP_200_OK,response_model=get_client_jobs_response)
async def get_client_jobs(limit:int=Query(ge=0,le=100,default=10),offset:int=Query(default=0,ge=0),status:Optional[JobStatus]=None,job_type:Optional[JobType]=None,client: dict = Depends(authenticate_client),
    db: AsyncSession = Depends(get_db)):

    return await job_service.get_client_jobs(db,client_id=client["client_id"],limit=limit,offset=offset,status=status,job_type=job_type)


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