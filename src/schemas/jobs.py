from datetime import datetime, timezone
from typing import Any
from pydantic import BaseModel, Field, field_validator, ConfigDict
from src.models.jobs import JobType, JobStatus
from typing import Optional, List


class job_submission_request(BaseModel):
    job_type: JobType
    payload: dict[str, Any]
    priority: int = Field(default=5, ge=1, le=10)
    max_retries: int = Field(default=3, ge=0, le=10)
    scheduled_at: Optional[datetime] = None

    @field_validator("scheduled_at")
    @classmethod
    def valdidate_scheduled_at(cls, value):
        if value is not None:
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("scheduled_at must be timezone-aware")

            if value < datetime.now(timezone.utc):
                raise ValueError("scheduled_at cannot be in the past")

        return value


class job_response(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    job_type: JobType
    status: JobStatus
    payload: dict[str, Any]
    priority: int
    max_attempts: int
    scheduled_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


class get_client_jobs_response(BaseModel):
    items: List[job_response]
    offset: int
    limit: int
    total: int
