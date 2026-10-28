import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .models import JobStatus


class JobCreate(BaseModel):
    prompt: str = Field(min_length=1, max_length=20_000)
    params: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1, le=20)


class JobComplete(BaseModel):
    result: str = Field(max_length=100_000)


class JobFail(BaseModel):
    error: str = Field(min_length=1, max_length=4000)


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: JobStatus
    prompt: str
    params: dict[str, Any]
    result: str | None
    error: str | None
    attempts: int
    max_attempts: int
    worker_id: str | None
    created_at: datetime
    claimed_at: datetime | None
    heartbeat_at: datetime | None
    finished_at: datetime | None
