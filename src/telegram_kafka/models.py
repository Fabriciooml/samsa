from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Index, String, Text, UniqueConstraint, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class AdminUser(Base):
    __tablename__ = "admin_user"

    id: Mapped[int] = mapped_column(primary_key=True)
    password_hash: Mapped[str] = mapped_column(Text)


class Bot(Base):
    __tablename__ = "bot"
    __table_args__ = (
        Index("uq_bot_active_name", "name", unique=True, postgresql_where=text("archived_at IS NULL")),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String(120))
    token_encrypted: Mapped[str] = mapped_column(Text)
    token_hint: Mapped[str] = mapped_column(String(16))
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class Route(Base):
    __tablename__ = "route"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    topic: Mapped[str] = mapped_column(String(249))
    bot_id: Mapped[str] = mapped_column(ForeignKey("bot.id"))
    chat_id: Mapped[int] = mapped_column(BigInteger)
    template: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class RouteStartOffset(Base):
    __tablename__ = "route_start_offset"

    route_id: Mapped[str] = mapped_column(ForeignKey("route.id"), primary_key=True)
    partition: Mapped[int] = mapped_column(primary_key=True)
    start_offset: Mapped[int] = mapped_column(BigInteger)


class DeliveryFailure(Base):
    __tablename__ = "delivery_failure"
    __table_args__ = (
        UniqueConstraint("route_id", "topic", "partition", "offset", "category"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    route_id: Mapped[str] = mapped_column(ForeignKey("route.id"))
    topic: Mapped[str] = mapped_column(String(249))
    partition: Mapped[int] = mapped_column()
    offset: Mapped[int] = mapped_column(BigInteger)
    category: Mapped[str] = mapped_column(String(60))
    detail: Mapped[str] = mapped_column(Text)
    rendered_text: Mapped[str | None] = mapped_column(Text)
    retry_state: Mapped[str] = mapped_column(String(30), default="open")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
