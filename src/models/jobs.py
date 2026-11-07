from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional

from src.db.database import Base
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum as SAEnum,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

if TYPE_CHECKING:
    from src.models.job_attempts import JobAttempt


class JobType(str, Enum):
    VIDEO = "video"
    EMAIL = "email"
    REPORT = "report"
    SCRAPING = "scraping"


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint("attempt_count >= 0", name="ck_jobs_attempt_count_nonnegative"),
        CheckConstraint("max_attempts > 0", name="ck_jobs_max_attempts_positive"),
        CheckConstraint("priority >= 0", name="ck_jobs_priority_nonnegative"),
        Index("ix_jobs_status_scheduled_at", "status", "scheduled_at"),
        UniqueConstraint(
            "client_id",
            "idempotency_key",
            name="uq_client_idempotency",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_type: Mapped[JobType] = mapped_column(
        SAEnum(
            JobType,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
            name="job_type",
        ),
        nullable=False,
    )
    payload: Mapped[Dict[str, object]] = mapped_column(JSONB, nullable=False)
    result: Mapped[Optional[Dict[str, object]]] = mapped_column(JSONB, nullable=True)
    client_id: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        SAEnum(
            JobStatus,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
            name="job_status",
        ),
        default=JobStatus.QUEUED,
        server_default=text("'queued'"),
        nullable=False,
    )
    priority: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default=text("0"),
        nullable=False,
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default=text("0"),
        nullable=False,
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        default=3,
        server_default=text("3"),
        nullable=False,
    )
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
        nullable=False,
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    attempts: Mapped[List["JobAttempt"]] = relationship(back_populates="job")
    request_fingerprint: Mapped[Optional[str]] = mapped_column(
    String(64),
    nullable=True,
)