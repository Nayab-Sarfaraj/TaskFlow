import hashlib
import json

from src.schemas.jobs import job_submission_request


def generate_request_fingerprint(
    request: job_submission_request,
) -> str:
    data = request.model_dump(mode="json")

    canonical_json = json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()