import json
import os
import sys
import urllib.error
import urllib.request
from uuid import uuid4


API_BASE_URL = os.getenv("TASKFLOW_API_URL", "http://127.0.0.1:8000")
API_KEY = os.getenv("TASKFLOW_API_KEY", "test_key_abc123")
JOB_TYPES = ("video", "email", "report", "scraping")
JOB_COUNT = 20


def main() -> int:
    batch_id = str(uuid4())
    endpoint = f"{API_BASE_URL.rstrip('/')}/api/v1/jobs/"
    successful = 0

    for index in range(1, JOB_COUNT + 1):
        job = {
            "job_type": JOB_TYPES[(index - 1) % len(JOB_TYPES)],
            "payload": {
                "batch_id": batch_id,
                "job_number": index,
                "description": f"Test job {index} from batch {batch_id}",
            },
            "priority": (index % 10) + 1,
            "max_retries": 3,
        }
        request = urllib.request.Request(
            endpoint,
            data=json.dumps(job).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-API-Key": API_KEY,
                "Idempotency-Key": f"{batch_id}-{index:02d}",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                result = json.loads(response.read().decode("utf-8"))
                successful += 1
                print(
                    f"[{index:02d}/{JOB_COUNT}] "
                    f"Created job id={result.get('id')} "
                    f"status={result.get('status')}"
                )
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            print(
                f"[{index:02d}/{JOB_COUNT}] Request failed: "
                f"HTTP {error.code}: {detail}",
                file=sys.stderr,
            )
        except (urllib.error.URLError, TimeoutError) as error:
            print(
                f"[{index:02d}/{JOB_COUNT}] Could not reach {endpoint}: {error}",
                file=sys.stderr,
            )

    print(f"Finished: {successful}/{JOB_COUNT} jobs created.")
    return 0 if successful == JOB_COUNT else 1


if __name__ == "__main__":
    raise SystemExit(main())
