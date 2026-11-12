from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.database import Base

if TYPE_CHECKING:
    from src.models.jobs import Job


class JobAttemptStatus(str, Enum):
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class JobAttempt(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="uq_job_attempt_number"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"), nullable=False)
    worker_id: Mapped[str] = mapped_column(ForeignKey("workers.id"), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[JobAttemptStatus] = mapped_column(
        SAEnum(
            JobAttemptStatus,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
            name="job_attempt_status",
        ),
        nullable=False,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
        nullable=False,
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    job: Mapped["Job"] = relationship(
        back_populates="attempts",
        foreign_keys=[job_id],
    )
