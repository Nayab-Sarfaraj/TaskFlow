import logging
import os
import time
import uuid

os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", "/tmp/taskflow-prometheus")

from fastapi import FastAPI, Request
from fastapi.responses import Response

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from prometheus_client import multiprocess
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from src.core.metrics import worker_heartbeat_age_seconds
from src.db.database import SessionLocal
from src.models.workers import Worker, WorkerStatus
from src.routers.health import router as health_router
from src.routers.jobs_router import router as job_router
from src.logging_config import configure_uvicorn_logging, log_context

configure_uvicorn_logging()

app = FastAPI()
logger = logging.getLogger(__name__)


async def update_worker_heartbeat_ages() -> None:
    """Refresh heartbeat age from PostgreSQL, the authoritative heartbeat store."""
    try:
        async with SessionLocal() as db:
            heartbeat_age = func.extract(
                "epoch", func.now() - Worker.last_heartbeat
            ).label("heartbeat_age")
            result = await db.execute(
                select(Worker.id, heartbeat_age).where(
                    Worker.status.in_((WorkerStatus.ACTIVE, WorkerStatus.DRAINING)),
                    Worker.last_heartbeat.is_not(None),
                )
            )
            for worker_id, age_seconds in result:
                worker_heartbeat_age_seconds.labels(worker_id=worker_id).set(
                    max(0.0, float(age_seconds))
                )
    except SQLAlchemyError as exc:
        # Metrics remain scrapeable when PostgreSQL is unavailable; readiness
        # communicates the dependency failure separately.
        logger.warning(
            "Worker heartbeat metric refresh failed",
            extra={"service_name": "api", "error_category": type(exc).__name__},
        )


@app.get("/metrics", include_in_schema=False)
async def metrics():
    await update_worker_heartbeat_ages()
    multiprocess_dir = os.getenv(
        "PROMETHEUS_MULTIPROC_DIR", "/tmp/taskflow-prometheus"
    )
    if multiprocess_dir:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        content = generate_latest(registry)
    else:
        content = generate_latest()
    return Response(content=content, media_type=CONTENT_TYPE_LATEST)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = request_id
    started = time.perf_counter()
    with log_context(request_id=request_id, service_name="api"):
        try:
            response = await call_next(request)
            logger.info("HTTP request completed", extra={"status_code": response.status_code, "method": request.method, "path": request.url.path, "duration_ms": round((time.perf_counter() - started) * 1000, 2)})
        except Exception as exc:
            logger.exception("HTTP request failed", extra={"error_category": type(exc).__name__, "method": request.method, "path": request.url.path})
            raise
    response.headers["X-Request-ID"] = request_id
    return response


@app.get("/")
def read_root():
    return {"status": "running"}


app.include_router(router=job_router, prefix="/api/v1/jobs")
app.include_router(router=health_router, prefix="/health")
