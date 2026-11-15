import logging
import os
import time
import uuid

os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", "/tmp/taskflow-prometheus")

from fastapi import FastAPI, Request
from fastapi.responses import Response

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from prometheus_client import multiprocess
from src.routers.jobs_router import router as job_router
from src.logging_config import configure_uvicorn_logging, log_context

configure_uvicorn_logging()

app = FastAPI()
logger = logging.getLogger(__name__)


@app.get("/metrics", include_in_schema=False)
def metrics():
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
