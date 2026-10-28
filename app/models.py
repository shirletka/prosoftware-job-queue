import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    type_annotation_map = {datetime: DateTime(timezone=True)}


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True, server_default=text("gen_random_uuid()"))
    status: Mapped[str] = mapped_column(Text, server_default=JobStatus.QUEUED.value)
    prompt: Mapped[str] = mapped_column(Text)
    params: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    result: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, server_default="3")
    worker_id: Mapped[str | None] = mapped_column(Text)
    # clock_timestamp(), not now(): jobs inserted in one transaction still get distinct times and stay FIFO.
    created_at: Mapped[datetime] = mapped_column(server_default=func.clock_timestamp())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now())
    claimed_at: Mapped[datetime | None]
    heartbeat_at: Mapped[datetime | None]
    finished_at: Mapped[datetime | None]

    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'done', 'failed')",
            name="jobs_status_valid",
        ),
        CheckConstraint(
            "status <> 'running' OR (worker_id IS NOT NULL AND heartbeat_at IS NOT NULL)",
            name="jobs_running_has_owner",
        ),
        CheckConstraint("max_attempts > 0", name="jobs_max_attempts_positive"),
        Index("jobs_queued_idx", "created_at", postgresql_where=text("status = 'queued'")),
        Index("jobs_running_idx", "heartbeat_at", postgresql_where=text("status = 'running'")),
    )
