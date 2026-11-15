from __future__ import annotations

from typing import Dict

from sqlalchemy import Enum as SAEnum, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.db.database import Base
from src.models.jobs import JobType


class JobOutput(Base):
    __tablename__ = "job_outputs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        ForeignKey("jobs.id"),
        nullable=False,
    )
    job_type: Mapped[JobType] = mapped_column(
        SAEnum(
            JobType,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
            name="job_type",
            create_type=False,
        ),
        nullable=False,
    )
    operation_key: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        nullable=False,
    )
    result: Mapped[Dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
    )
