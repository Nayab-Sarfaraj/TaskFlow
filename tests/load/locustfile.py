"""TaskFlow submission workload for Locust.

Run this only against isolated TaskFlow infrastructure.  Each virtual user
submits durable report jobs; a small percentage repeats its previous
idempotency key to exercise API deduplication without creating more work.
"""

import itertools
import os
import uuid

from locust import HttpUser, constant_pacing, task


JOB_PATH = "/api/v1/jobs/"
REPORT_DURATION_SECONDS = float(os.getenv("TASKFLOW_LOAD_REPORT_DURATION_SECONDS", "2"))
LOAD_CLIENT_COUNT = int(os.getenv("TASKFLOW_LOAD_CLIENT_COUNT", "1000"))
TARGET_RPS = float(os.getenv("TASKFLOW_LOAD_TARGET_RPS", "25"))
EXPECTED_USERS = int(os.getenv("TASKFLOW_LOAD_EXPECTED_USERS", "25"))

if LOAD_CLIENT_COUNT < 1:
    raise ValueError("TASKFLOW_LOAD_CLIENT_COUNT must be at least 1")
if TARGET_RPS <= 0:
    raise ValueError("TASKFLOW_LOAD_TARGET_RPS must be positive")
if EXPECTED_USERS < 1:
    raise ValueError("TASKFLOW_LOAD_EXPECTED_USERS must be at least 1")

# Locust applies wait_time once per virtual user. This creates an aggregate
# request rate close to TARGET_RPS once EXPECTED_USERS are running.
REQUEST_INTERVAL_SECONDS = EXPECTED_USERS / TARGET_RPS
_user_numbers = itertools.count(1)


class TaskFlowUser(HttpUser):
    """A client that generates both unique and duplicate job submissions."""

    wait_time = constant_pacing(REQUEST_INTERVAL_SECONDS)

    def on_start(self):
        self.last_submission = None
        client_number = (next(_user_numbers) - 1) % LOAD_CLIENT_COUNT + 1
        self.client_id = f"load_client_{client_number:04d}"
        self.api_key = f"load_test_key_{client_number:04d}"

    def _submit(self, idempotency_key: str, request_id: str):
        payload = {
            "job_type": "report",
            "payload": {
                "report_type": "load-test",
                "format": "pdf",
                "execution_time_seconds": REPORT_DURATION_SECONDS,
                "parameters": {"load_request_id": request_id},
            },
            "priority": 5,
            "max_retries": 3,
        }
        with self.client.post(
            JOB_PATH,
            json=payload,
            headers={
                "X-API-Key": self.api_key,
                "X-Request-ID": request_id,
                "Idempotency-Key": idempotency_key,
            },
            name="POST /api/v1/jobs [unique]",
            catch_response=True,
        ) as response:
            if response.status_code != 201:
                response.failure(f"unexpected status {response.status_code}")
                return None
            response.success()
            return response.json()

    @task(9)
    def submit_unique_job(self):
        token = uuid.uuid4().hex
        self.last_submission = (f"load-{token}", f"load-request-{token}")
        self._submit(*self.last_submission)

    @task(1)
    def replay_last_submission(self):
        if self.last_submission is None:
            return
        idempotency_key, request_id = self.last_submission
        with self.client.post(
            JOB_PATH,
            json={
                "job_type": "report",
                "payload": {
                    "report_type": "load-test",
                    "format": "pdf",
                    "execution_time_seconds": REPORT_DURATION_SECONDS,
                    "parameters": {"load_request_id": request_id},
                },
                "priority": 5,
                "max_retries": 3,
            },
            headers={
                "X-API-Key": self.api_key,
                "X-Request-ID": request_id,
                "Idempotency-Key": idempotency_key,
            },
            name="POST /api/v1/jobs [idempotent replay]",
            catch_response=True,
        ) as response:
            if response.status_code != 201:
                response.failure(f"unexpected status {response.status_code}")
            else:
                response.success()
