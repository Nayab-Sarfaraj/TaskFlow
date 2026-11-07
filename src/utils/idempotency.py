from src.models.jobs import Job
from src.schemas.jobs import job_submission_request


def is_request_parameter_matches(
    existing_job: Job,
    request: job_submission_request,
) -> bool:
    """Return True when a retry has the same parameters as the original request."""
    if existing_job is None:
        return False

    # When scheduled_at is omitted, the database assigns the timestamp.
    # Therefore, compare it only when the client explicitly supplied one.
    scheduled_at_matches = (
        request.scheduled_at is None
        or existing_job.scheduled_at == request.scheduled_at
    )

    return (
        existing_job.job_type == request.job_type
        and existing_job.payload == request.payload
        and existing_job.priority == request.priority
        and existing_job.max_attempts == request.max_retries + 1
        and scheduled_at_matches
    )
