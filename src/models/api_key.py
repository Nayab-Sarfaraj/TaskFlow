from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Optional,List

from sqlalchemy import (
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
    func,
    String
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.database import Base



if TYPE_CHECKING:
    from src.models.client import Client

class APIKey(Base):
    __tablename__ = "api_keys"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key_hash: Mapped[str] = mapped_column(
    String(64),
    unique=True,
    index=True,
    nullable=False,
)
    client_id: Mapped[int] = mapped_column(
    ForeignKey("clients.id"),
    nullable=False,
)
    key_prefix:Mapped[str]=mapped_column(String,nullable=False)
    name:Mapped[str]=mapped_column(String,nullable=False)
    expires_at: Mapped[Optional[datetime]] = mapped_column(
            DateTime(timezone=True),nullable=True   
        )
    revoked_at: Mapped[Optional[datetime]] = mapped_column(
           DateTime(timezone=True),nullable=True   
        )
    last_used_at: Mapped[Optional[datetime]] = mapped_column(
           DateTime(timezone=True),nullable=True   
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
    client: Mapped["Client"] = relationship(
    back_populates="api_keys"
)
   
   