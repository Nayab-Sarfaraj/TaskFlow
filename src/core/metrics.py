"""Low-cardinality Prometheus metrics for TaskFlow.

Set ``PROMETHEUS_MULTIPROC_DIR`` to a shared writable directory when the API,
publisher, scheduler, and workers run as separate processes. The API endpoint
then aggregates the per-process metric files.
"""

import os
from pathlib import Path
from prometheus_client import Counter, Gauge, Histogram

PROMETHEUS_MULTIPROC_DIR = os.environ.setdefault(
    "PROMETHEUS_MULTIPROC_DIR", "/tmp/taskflow-prometheus"
)
if PROMETHEUS_MULTIPROC_DIR:
    Path(PROMETHEUS_MULTIPROC_DIR).mkdir(parents=True, exist_ok=True)

jobs_submitted_total = Counter(
    "taskflow_jobs_submitted_total",
    "Total number of jobs submitted",
    ("job_type",),
)
jobs_completed_total = Counter(
    "taskflow_jobs_completed_total",
    "Total number of jobs completed",
    ("job_type",),
)
jobs_failed_total = Counter(
    "taskflow_jobs_failed_total",
    "Total number of jobs failed",
    ("job_type", "error_category"),
)
jobs_retried_total = Counter(
    "taskflow_jobs_retried_total",
    "Total number of jobs retried",
    ("job_type",),
)
job_duration_seconds = Histogram(
    "taskflow_job_duration_seconds",
    "Job execution duration in seconds",
    ("job_type",),
)
job_queue_wait_seconds = Histogram(
    "taskflow_job_queue_wait_seconds",
    "Time jobs spend waiting before execution in seconds",
    ("job_type",),
)
active_jobs = Gauge(
    "taskflow_active_jobs",
    "Number of actively executing jobs",
    ("job_type",),
)
queue_depth = Gauge(
    "taskflow_queue_depth",
    "Number of jobs by queue status",
    ("status",),
)
worker_heartbeat_age_seconds = Gauge(
    "taskflow_worker_heartbeat_age_seconds",
    "Age of the latest worker heartbeat in seconds",
    ("worker_id",),
)


def job_type_value(job_type) -> str:
    """Return a stable metric label for enum or string job types."""
    return getattr(job_type, "value", job_type)
