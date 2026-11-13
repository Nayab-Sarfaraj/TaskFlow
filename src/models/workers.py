from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum as SAEnum,
    Integer,
    String,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from src.db.database import Base


class WorkerStatus(str, Enum):
    ACTIVE = "active"
    DRAINING = "draining"
    OFFLINE = "offline"


class Worker(Base):
    __tablename__ = "workers"
    __table_args__ = (
        CheckConstraint(
            "concurrency_limit > 0",
            name="ck_workers_concurrency_limit_positive",
        ),
    )

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    status: Mapped[WorkerStatus] = mapped_column(
        SAEnum(
            WorkerStatus,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
            name="worker_status",
        ),
        default=WorkerStatus.ACTIVE,
        server_default=text("'active'"),
        nullable=False,
    )
    last_heartbeat: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
        nullable=False,
    )
    concurrency_limit: Mapped[int] = mapped_column(
        Integer,
        default=1,
        server_default=text("1"),
        nullable=False,
    )
