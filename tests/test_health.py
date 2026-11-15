from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from sqlalchemy.exc import OperationalError

from src.main import app
import src.main as main_module
from src.models.workers import Worker, WorkerStatus
from src.routers import health


class HealthyEngine:
    @asynccontextmanager
    async def connect(self):
        yield HealthyConnection()


class HealthyConnection:
    async def execute(self, statement):
        return None


class UnavailableEngine:
    @asynccontextmanager
    async def connect(self):
        raise OperationalError("SELECT 1", {}, Exception("database unavailable"))
        yield


@pytest.mark.asyncio
async def test_liveness_does_not_require_dependencies():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_readiness_only_checks_postgres(monkeypatch):
    monkeypatch.setattr(health, "engine", HealthyEngine())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}


@pytest.mark.asyncio
async def test_readiness_returns_503_when_postgres_is_unavailable(monkeypatch):
    monkeypatch.setattr(health, "engine", UnavailableEngine())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["detail"] == "PostgreSQL is unavailable"


def test_worker_health_uses_the_recovery_heartbeat_timeout():
    now = datetime.now(timezone.utc)
    active = Worker(
        id="worker-current",
        status=WorkerStatus.ACTIVE,
        last_heartbeat=now - timedelta(seconds=10),
        concurrency_limit=3,
    )
    stale = Worker(
        id="worker-stale",
        status=WorkerStatus.ACTIVE,
        last_heartbeat=now - timedelta(seconds=31),
        concurrency_limit=1,
    )

    assert health._worker_health(active, now)["health"] == "healthy"
    assert health._worker_health(stale, now)["health"] == "unhealthy"


@pytest.mark.asyncio
async def test_worker_endpoint_reports_healthy_and_stale_workers(monkeypatch):
    now = datetime.now(timezone.utc)
    workers = [
        Worker(
            id="worker-running",
            status=WorkerStatus.ACTIVE,
            last_heartbeat=now - timedelta(seconds=5),
            concurrency_limit=3,
        ),
        Worker(
            id="worker-dead",
            status=WorkerStatus.ACTIVE,
            last_heartbeat=now - timedelta(seconds=31),
            concurrency_limit=1,
        ),
    ]

    class Result:
        def scalars(self):
            return self

        def all(self):
            return workers

    class DatabaseSession:
        async def execute(self, statement):
            return Result()

    @asynccontextmanager
    async def session_factory():
        yield DatabaseSession()

    monkeypatch.setattr(health, "SessionLocal", session_factory)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health/workers")

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "ok"
    assert body["healthy_workers"] == 1
    assert {item["worker_id"]: item["health"] for item in body["workers"]} == {
        "worker-running": "healthy",
        "worker-dead": "unhealthy",
    }


@pytest.mark.asyncio
async def test_heartbeat_metric_uses_the_database_heartbeat_age(monkeypatch):
    observed = []

    class Metric:
        def labels(self, **labels):
            self.worker_id = labels["worker_id"]
            return self

        def set(self, value):
            observed.append((self.worker_id, value))

    class DatabaseSession:
        async def execute(self, statement):
            return [("worker-one", 4.25)]

    @asynccontextmanager
    async def session_factory():
        yield DatabaseSession()

    monkeypatch.setattr(main_module, "SessionLocal", session_factory)
    monkeypatch.setattr(main_module, "worker_heartbeat_age_seconds", Metric())

    await main_module.update_worker_heartbeat_ages()

    assert observed == [("worker-one", 4.25)]
