from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, Literal

from pydantic import BaseModel, Field

from src.models.job_attempts import JobAttempt
from src.models.jobs import Job, JobType


REPORT_MAX_SIZE_BYTES = int(os.getenv("REPORT_MAX_SIZE_BYTES", str(50 * 1024 * 1024)))
REPORT_MAX_EXECUTION_SECONDS = float(os.getenv("REPORT_MAX_EXECUTION_SECONDS", "120"))


class ReportJobPayload(BaseModel):
    report_type: str = Field(default="generic", min_length=1, max_length=100)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    format: Literal["pdf", "html", "csv"] = "pdf"
    generated_size_bytes: int = Field(default=0, ge=0)
    execution_time_seconds: float = Field(default=0, ge=0)


async def process_report(job: Job, job_attempt: JobAttempt) -> dict[str, str]:
    """Validate report limits and return a simulated report reference."""
    if job.job_type != JobType.REPORT:
        raise ValueError("process_report requires a REPORT job")

    payload = ReportJobPayload.model_validate(job.payload)
    if payload.generated_size_bytes > REPORT_MAX_SIZE_BYTES:
        raise ValueError(
            "generated report exceeds the configured maximum size "
            f"({REPORT_MAX_SIZE_BYTES} bytes)"
        )
    if payload.execution_time_seconds > REPORT_MAX_EXECUTION_SECONDS:
        raise TimeoutError(
            "report execution exceeds the configured maximum duration "
            f"({REPORT_MAX_EXECUTION_SECONDS} seconds)"
        )

    await asyncio.wait_for(asyncio.sleep(0), timeout=REPORT_MAX_EXECUTION_SECONDS)
    return {
        "report_reference": (
            f"report://jobs/{job.id}/attempts/{job_attempt.id}/"
            f"output.{payload.format}"
        )
    }
