"""Low-cardinality Prometheus metrics for TaskFlow.

Set ``PROMETHEUS_MULTIPROC_DIR`` to a shared writable directory when the API,
publisher, scheduler, and workers run as separate processes. The API endpoint
then aggregates the per-process metric files.
"""

import os
from pathlib import Path

PROMETHEUS_MULTIPROC_DIR = os.environ.setdefault(
    "PROMETHEUS_MULTIPROC_DIR", "/tmp/taskflow-prometheus"
)
if PROMETHEUS_MULTIPROC_DIR:
    Path(PROMETHEUS_MULTIPROC_DIR).mkdir(parents=True, exist_ok=True)

# prometheus_client reads this setting during import. It must be configured
# before Counter/Gauge/Histogram are imported by worker processes.
from prometheus_client import Counter, Gauge, Histogram

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
    multiprocess_mode="livesum",
)
queue_depth = Gauge(
    "taskflow_queue_depth",
    "Number of jobs by queue status",
    ("status",),
    multiprocess_mode="livesum",
)
worker_heartbeat_age_seconds = Gauge(
    "taskflow_worker_heartbeat_age_seconds",
    "Age of the latest worker heartbeat in seconds",
    ("worker_id",),
    multiprocess_mode="livesum",
)


def job_type_value(job_type) -> str:
    """Return a stable metric label for enum or string job types."""
    return getattr(job_type, "value", job_type)


# Pre-create low-cardinality series so a fresh multiprocess scrape exposes the
# complete TaskFlow metric surface even before the first job of each type.
for _job_type in ("email", "video", "report", "scraping"):
    jobs_submitted_total.labels(job_type=_job_type).inc(0)
    jobs_completed_total.labels(job_type=_job_type).inc(0)
    active_jobs.labels(job_type=_job_type).set(0)

for _status in (
    "postgres_pending",
    "redis_stream_backlog",
):
    queue_depth.labels(status=_status).set(0)
