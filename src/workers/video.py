from __future__ import annotations

import asyncio
import os
import re
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator

from src.models.job_attempts import JobAttempt
from src.models.jobs import Job, JobType
from src.utils.ssrf import validate_safe_url


MAX_VIDEO_SIZE_MB = int(os.getenv("MAX_VIDEO_SIZE_MB", "100"))
VIDEO_MAX_DOWNLOAD_BYTES = MAX_VIDEO_SIZE_MB * 1024 * 1024
VIDEO_FFMPEG_CONCURRENCY = int(
    os.getenv("MAX_VIDEO_CONCURRENCY", os.getenv("VIDEO_FFMPEG_CONCURRENCY", "2"))
)
if VIDEO_FFMPEG_CONCURRENCY < 1:
    raise ValueError("VIDEO_FFMPEG_CONCURRENCY must be greater than zero")

FFMPEG_SEMAPHORE = asyncio.Semaphore(VIDEO_FFMPEG_CONCURRENCY)
RESOLUTION_PATTERN = re.compile(r"^(\d{2,5})x(\d{2,5})$")


class VideoJobPayload(BaseModel):
    url: HttpUrl
    resolution: str
    download_size_bytes: int = Field(default=0, ge=0)
    generate_thumbnail: bool = False

    @field_validator("resolution")
    @classmethod
    def validate_resolution(cls, value: str) -> str:
        match = RESOLUTION_PATTERN.fullmatch(value.strip().lower())
        if match is None:
            raise ValueError("resolution must use WIDTHxHEIGHT format")

        width, height = (int(part) for part in match.groups())
        if width < 144 or height < 144:
            raise ValueError("resolution must be at least 144x144")
        return f"{width}x{height}"


async def process_video(job: Job, job_attempt: JobAttempt) -> dict[str, Optional[str]]:
    """Validate a video job and return simulated output references.

    Actual downloading and FFmpeg execution are intentionally not performed.
    The semaphore and validation represent the production safety boundaries.
    """
    if job.job_type != JobType.VIDEO:
        raise ValueError("process_video requires a VIDEO job")

    payload = VideoJobPayload.model_validate(job.payload)
    await validate_safe_url(str(payload.url))
    if payload.download_size_bytes > VIDEO_MAX_DOWNLOAD_BYTES:
        raise ValueError(
            "video download exceeds the configured maximum size "
            f"({VIDEO_MAX_DOWNLOAD_BYTES} bytes)"
        )

    async with FFMPEG_SEMAPHORE:
        await asyncio.sleep(0)
        output_reference = f"video://jobs/{job.id}/attempts/{job_attempt.id}/output.mp4"
        thumbnail_reference = (
            f"video://jobs/{job.id}/attempts/{job_attempt.id}/thumbnail.jpg"
            if payload.generate_thumbnail
            else None
        )

    return {
        "video_reference": output_reference,
        "thumbnail_reference": thumbnail_reference,
    }
