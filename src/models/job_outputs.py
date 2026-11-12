from __future__ import annotations

from typing import Dict

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.db.database import Base


class JobOutput(Base):
    __tablename__ = "job_outputs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        ForeignKey("jobs.id"),
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
