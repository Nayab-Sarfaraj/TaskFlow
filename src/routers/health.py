"""Unauthenticated operational health endpoints.

Liveness answers whether this API process can serve HTTP.  Readiness checks the
durable job store only: Redis is deliberately not a readiness dependency because
PostgreSQL can still accept and persist jobs while Redis is unavailable.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from src.db.database import SessionLocal, engine
from src.models.workers import Worker, WorkerStatus


router = APIRouter(tags=["health"])
logger = logging.getLogger(__name__)

WORKER_HEARTBEAT_TIMEOUT_SECONDS = 30


@router.get("/live", summary="API liveness")
async def live() -> dict[str, str]:
    """Return success whenever the API process is able to handle requests."""
    return {"status": "ok"}


@router.get("/ready", summary="API readiness")
async def ready() -> dict[str, str]:
    """Check PostgreSQL without making Redis a job-submission dependency."""
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        logger.warning(
            "PostgreSQL readiness check failed",
            extra={"service_name": "api", "error_category": type(exc).__name__},
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="PostgreSQL is unavailable",
        ) from None

    return {"status": "ok", "database": "ok"}


def _worker_health(worker: Worker, now: datetime) -> dict[str, Any]:
    heartbeat = worker.last_heartbeat
    if heartbeat is not None and heartbeat.tzinfo is None:
        heartbeat = heartbeat.replace(tzinfo=timezone.utc)

    heartbeat_age_seconds = (
        round(max(0.0, (now - heartbeat).total_seconds()), 3)
        if heartbeat is not None
        else None
    )
    is_current = (
        heartbeat_age_seconds is not None
        and heartbeat_age_seconds <= WORKER_HEARTBEAT_TIMEOUT_SECONDS
    )
    healthy = worker.status in (WorkerStatus.ACTIVE, WorkerStatus.DRAINING) and is_current

    return {
        "worker_id": worker.id,
        "status": worker.status.value,
        "health": "healthy" if healthy else "unhealthy",
        "last_heartbeat": heartbeat.isoformat() if heartbeat is not None else None,
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "concurrency_limit": worker.concurrency_limit,
    }


@router.get("/workers", summary="Registered worker health")
async def workers() -> dict[str, Any]:
    """Report each registered worker without affecting API readiness."""
    try:
        async with SessionLocal() as db:
            result = await db.execute(select(Worker).order_by(Worker.id))
            registered_workers = result.scalars().all()
    except SQLAlchemyError as exc:
        logger.warning(
            "Worker health check failed",
            extra={"service_name": "api", "error_category": type(exc).__name__},
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="PostgreSQL is unavailable",
        ) from None

    now = datetime.now(timezone.utc)
    worker_statuses = [_worker_health(worker, now) for worker in registered_workers]
    healthy_count = sum(worker["health"] == "healthy" for worker in worker_statuses)
    overall_status = "ok" if healthy_count else "degraded"

    return {
        "status": overall_status,
        "heartbeat_timeout_seconds": WORKER_HEARTBEAT_TIMEOUT_SECONDS,
        "registered_workers": len(worker_statuses),
        "healthy_workers": healthy_count,
        "workers": worker_statuses,
    }
