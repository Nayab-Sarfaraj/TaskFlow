from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import delete, select

from src.models.job_attempts import JobAttemptStatus
from src.models.job_outputs import JobOutput
from src.models.jobs import Job, JobStatus, JobType
from src.models.outbox_events import OutboxEvent
from src.publisher import main as publisher_module
from src.repositories.outbox_event import fetch_unpublished_events
from src.utils.failure_classifier import RetryableEmailError
from src.utils.ssrf import validate_safe_url
from src.workers import email as email_module
from src.workers import report as report_module
from src.workers import scraping as scraping_module
from src.workers import video as video_module


async def create_test_job(session, job_type, payload, priority=5):
    job = Job(
        job_type=job_type,
        payload=payload,
        client_id=f"phase8-{uuid4().hex}",
        status=JobStatus.PROCESSING,
        priority=priority,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def cleanup_test_jobs(session, job_ids):
    await session.execute(delete(JobOutput).where(JobOutput.job_id.in_(job_ids)))
    await session.execute(delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(job_ids)))
    await session.execute(delete(Job).where(Job.id.in_(job_ids)))
    await session.commit()


@pytest.mark.asyncio
async def test_email_results_persist_and_retry_only_failed_recipient(
    db_session, monkeypatch
):
    job = await create_test_job(
        db_session,
        JobType.EMAIL,
        {
            "recipients": ["accepted@example.com", "retry@example.com"],
            "subject": "Test",
            "body": "Body",
        },
    )
    attempt = SimpleNamespace(id=1, attempt_number=1)
    sent = []

    async def first_send(*, recipient, subject, body):
        sent.append(recipient)
        if recipient == "retry@example.com":
            raise ConnectionError("connection refused")

    monkeypatch.setattr(email_module, "_send_email", first_send)
    with pytest.raises(RetryableEmailError):
        await email_module.send_bulk_email(job, attempt)

    async def second_send(*, recipient, subject, body):
        sent.append(recipient)

    monkeypatch.setattr(email_module, "_send_email", second_send)
    result = await email_module.send_bulk_email(
        job, SimpleNamespace(id=2, attempt_number=2)
    )

    assert result == {"total": 2, "accepted": 2, "failed": 0, "uncertain": 0}
    assert sent == ["accepted@example.com", "retry@example.com", "retry@example.com"]
    outputs = (
        await db_session.execute(select(JobOutput).where(JobOutput.job_id == job.id))
    ).scalars().all()
    assert {output.result["status"] for output in outputs} == {"succeeded"}
    await cleanup_test_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_invalid_email_payload_is_rejected_without_output(db_session):
    job = await create_test_job(
        db_session,
        JobType.EMAIL,
        {"recipients": ["not-an-email"], "subject": "", "body": ""},
    )
    with pytest.raises(ValidationError):
        await email_module.send_bulk_email(job, SimpleNamespace(id=1, attempt_number=1))
    outputs = (
        await db_session.execute(select(JobOutput).where(JobOutput.job_id == job.id))
    ).scalars().all()
    assert outputs == []
    await cleanup_test_jobs(db_session, [job.id])


@pytest.mark.asyncio
async def test_handler_limits_and_ssrf_protection(db_session, monkeypatch):
    video_job = await create_test_job(
        db_session,
        JobType.VIDEO,
        {"url": "https://example.com/video", "resolution": "1280x720", "download_size_bytes": 1},
    )
    monkeypatch.setattr(video_module, "VIDEO_MAX_DOWNLOAD_BYTES", 0)
    with pytest.raises(ValueError):
        await video_module.process_video(video_job, SimpleNamespace(id=1))

    report_job = await create_test_job(
        db_session,
        JobType.REPORT,
        {"report_type": "sales", "parameters": {}, "generated_size_bytes": 1},
    )
    monkeypatch.setattr(report_module, "REPORT_MAX_SIZE_BYTES", 0)
    with pytest.raises(ValueError):
        await report_module.process_report(report_job, SimpleNamespace(id=1))

    with pytest.raises(ValueError):
        await validate_safe_url("http://127.0.0.1/internal")

    scrape_job = await create_test_job(
        db_session,
        JobType.SCRAPING,
        {"urls": ["http://127.0.0.1/internal", "http://127.0.0.1/other"]},
    )
    result = await scraping_module.process_scraping(scrape_job, SimpleNamespace(id=1))
    assert result["total"] == 2
    assert all(page["status"] == "failed" for page in result["pages"])
    await cleanup_test_jobs(db_session, [video_job.id, report_job.id, scrape_job.id])


@pytest.mark.asyncio
async def test_outbox_priority_selection_deduplicates_jobs(db_session):
    jobs = []
    for priority in range(1, 101):
        job = await create_test_job(
            db_session,
            JobType.REPORT,
            {"value": priority},
            priority=priority,
        )
        job.status = JobStatus.QUEUED
        db_session.add(OutboxEvent(aggregate_id=job.id, event_type="job.created", payload=job.payload))
        jobs.append(job)
    db_session.add(OutboxEvent(aggregate_id=jobs[0].id, event_type="job.created", payload=jobs[0].payload))
    await db_session.commit()

    events = await fetch_unpublished_events(db_session)
    priorities = {
        job.id: job.priority
        for job in (
            await db_session.execute(
                select(Job).where(Job.id.in_([event.aggregate_id for event in events]))
            )
        ).scalars()
    }
    returned_priorities = [priorities[event.aggregate_id] for event in events]
    assert len({event.aggregate_id for event in events}) == len(events)
    assert returned_priorities[: min(70, len(returned_priorities))] == sorted(
        returned_priorities[: min(70, len(returned_priorities))], reverse=True
    )
    await cleanup_test_jobs(db_session, [job.id for job in jobs])


@pytest.mark.asyncio
async def test_redis_publish_capacity_is_bounded(monkeypatch):
    class PendingRedis:
        async def xpending(self, stream, group):
            return {"pending": 5}

    monkeypatch.setattr(publisher_module, "REDIS_MAX_IN_FLIGHT", 6)
    assert await publisher_module.available_publish_capacity(PendingRedis()) == 1
