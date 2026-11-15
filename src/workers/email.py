from __future__ import annotations

import asyncio
from email.message import EmailMessage
import re
import logging
import os
import smtplib
import socket
from typing import List

from pydantic import BaseModel, Field, ValidationError, field_validator

from src.models.job_attempts import JobAttempt
from src.models.jobs import Job, JobType
from src.db.database import SessionLocal
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from src.models.job_outputs import JobOutput
from src.utils.failure_classifier import RetryableEmailError

logger = logging.getLogger(__name__)
EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
SMTP_FROM = os.getenv("SMTP_FROM", "no-reply@taskflow.local")
MAX_EMAIL_RECIPIENTS = int(os.getenv("MAX_EMAIL_RECIPIENTS", "100"))


class EmailJobPayload(BaseModel):
    """Validated payload required by an email job."""

    recipients: List[str] = Field(min_length=1, max_length=MAX_EMAIL_RECIPIENTS)
    subject: str = Field(min_length=1, max_length=998)
    body: str = Field(min_length=1)

    @field_validator("recipients")
    @classmethod
    def validate_recipients(cls, recipients: List[str]) -> List[str]:
        cleaned_recipients = [recipient.strip() for recipient in recipients]
        if any(not recipient for recipient in cleaned_recipients):
            raise ValueError("recipients must not contain empty addresses")
        if any(
            not EMAIL_PATTERN.fullmatch(recipient) for recipient in cleaned_recipients
        ):
            raise ValueError("recipients must contain valid email addresses")
        normalized_recipients = [
            recipient.casefold() for recipient in cleaned_recipients
        ]
        if len(normalized_recipients) != len(set(normalized_recipients)):
            raise ValueError("recipients must not contain duplicates")
        return cleaned_recipients

    @field_validator("subject", "body")
    @classmethod
    def validate_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("subject and body must not be empty")
        return value


async def send_bulk_email(job: Job, job_attempt: JobAttempt) -> dict[str, int]:
    """Send one email per recipient and return an aggregate delivery result."""
    if job.job_type != JobType.EMAIL:
        raise ValueError("send_bulk_email requires an EMAIL job")

    try:
        payload = EmailJobPayload.model_validate(job.payload)
    except ValidationError as exc:
        # Do not log the payload itself because it may contain addresses or
        # message content; the validation details are sufficient for diagnosis.
        logger.warning(
            "Invalid email payload for job %s: %s",
            job.id,
            exc.errors(),
        )
        raise

    result = {
        "total": len(payload.recipients),
        "accepted": 0,
        "failed": 0,
        "uncertain": 0,
    }
    retryable_failures = 0

    for recipient in payload.recipients:
        operation_key = f"email:{job.id}:{recipient}:output"

        async with SessionLocal() as db:
            existing = await db.scalar(
                select(JobOutput).where(JobOutput.operation_key == operation_key)
            )
            if existing is not None:
                existing_status = existing.result.get("status")
                if existing_status == "succeeded":
                    logger.info(
                        "Skipping already completed email delivery for job %s to %s",
                        job.id,
                        recipient,
                    )
                    result["accepted"] += 1
                    continue
                if existing_status in {"sending", "uncertain"}:
                    if existing_status == "sending":
                        await db.execute(
                            update(JobOutput)
                            .where(JobOutput.operation_key == operation_key)
                            .values(
                                result={
                                    **existing.result,
                                    "status": "uncertain",
                                    "error": "Worker stopped during SMTP delivery",
                                }
                            )
                        )
                        await db.commit()
                    logger.warning(
                        "Skipping uncertain email delivery for job %s to %s",
                        job.id,
                        recipient,
                    )
                    result["uncertain"] += 1
                    continue
                if existing_status == "failed" and not existing.result.get(
                    "retryable", False
                ):
                    result["failed"] += 1
                    continue

            await db.execute(
                insert(JobOutput)
                .values(
                    job_id=job.id,
                    job_type=job.job_type,
                    operation_key=operation_key,
                    result={
                        "recipient": recipient,
                        "status": "pending",
                        "attempt": job_attempt.attempt_number,
                    },
                )
                .on_conflict_do_nothing(index_elements=["operation_key"])
            )
            await db.commit()

            # Persist the sending fence before SMTP. If the worker crashes
            # after SMTP accepts the message, a retry will mark it uncertain
            # instead of sending the same message again.
            await db.execute(
                update(JobOutput)
                .where(JobOutput.operation_key == operation_key)
                .values(
                    result={
                        "recipient": recipient,
                        "status": "sending",
                        "attempt": job_attempt.attempt_number,
                    }
                )
            )
            await db.commit()

        try:
            await _send_email(
                recipient=recipient,
                subject=payload.subject,
                body=payload.body,
            )
        except Exception as exc:
            delivery_status, retryable = _classify_delivery_failure(exc)
            logger.exception(
                "Email delivery failed for job %s to %s",
                job.id,
                recipient,
            )
            async with SessionLocal() as db:
                await db.execute(
                    update(JobOutput)
                    .where(JobOutput.operation_key == operation_key)
                    .values(
                        result={
                            "recipient": recipient,
                            "status": delivery_status,
                            "attempt": job_attempt.attempt_number,
                            "error": "smtp_delivery_failed",
                            "retryable": retryable,
                        }
                    )
                )
                await db.commit()
            if delivery_status == "uncertain":
                result["uncertain"] += 1
            else:
                result["failed"] += 1
            if retryable:
                retryable_failures += 1
            continue

        async with SessionLocal() as db:
            await db.execute(
                update(JobOutput)
                .where(JobOutput.operation_key == operation_key)
                .values(
                    result={
                        "recipient": recipient,
                        "status": "succeeded",
                        "attempt": job_attempt.attempt_number,
                        "retryable": False,
                    }
                )
            )
            await db.commit()

        result["accepted"] += 1

    if retryable_failures > 0:
        raise RetryableEmailError(
            "Some email recipients require retry",
            result=result,
        )

    return result


def _classify_delivery_failure(exception: Exception) -> tuple[str, bool]:
    """Return (recipient status, retryable) for an SMTP delivery error."""
    if isinstance(exception, (TimeoutError, socket.timeout)):
        return "uncertain", False

    smtp_code = getattr(exception, "smtp_code", None)
    if smtp_code is None:
        smtp_code = getattr(exception, "code", None)
    if smtp_code is not None:
        if 400 <= smtp_code < 500:
            return "failed", True
        if smtp_code >= 500:
            return "failed", False

    if isinstance(
        exception,
        (
            ConnectionError,
            smtplib.SMTPConnectError,
            smtplib.SMTPServerDisconnected,
        ),
    ):
        return "failed", True

    return "failed", False


async def _send_email(*, recipient: str, subject: str, body: str) -> None:
    """Send one message through the configured SMTP server."""

    def send() -> None:
        message = EmailMessage()
        message["From"] = SMTP_FROM
        message["To"] = recipient
        message["Subject"] = subject
        message.set_content(body)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as smtp:
            smtp.send_message(message)

    await asyncio.to_thread(send)
