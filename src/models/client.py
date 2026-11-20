from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Optional, List

from sqlalchemy import (
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
    func,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.database import Base


class ClientStatus(str, Enum):
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"


if TYPE_CHECKING:
    from src.models.jobs import Job
if TYPE_CHECKING:
    from src.models.api_key import APIKey


class Client(Base):
    __tablename__ = "clients"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    status: Mapped[ClientStatus] = mapped_column(
        SAEnum(ClientStatus, name="client_status"),
        default=ClientStatus.ACTIVE,
        server_default="ACTIVE",
        nullable=False,
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
    jobs: Mapped[List["Job"]] = relationship(
        back_populates="client", cascade="all,delete-orphan"
    )

    api_keys: Mapped[List["APIKey"]] = relationship(
        back_populates="client", cascade="all,delete-orphan"
    )
