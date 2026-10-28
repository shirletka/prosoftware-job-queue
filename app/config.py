from datetime import timedelta

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg://jobs:jobs@localhost:55432/jobs"
    worker_tokens: str = Field(default="", description="worker-1:token,worker-2:token")
    lease_timeout_seconds: int = Field(default=600, gt=0)
    max_runtime_seconds: int = Field(default=3600, gt=0)
    reaper_interval_seconds: float = Field(default=30, gt=0)

    @property
    def lease_timeout(self) -> timedelta:
        return timedelta(seconds=self.lease_timeout_seconds)

    @property
    def max_runtime(self) -> timedelta:
        return timedelta(seconds=self.max_runtime_seconds)

    @property
    def workers_by_token(self) -> dict[str, str]:
        workers = {}
        for pair in filter(None, (p.strip() for p in self.worker_tokens.split(","))):
            worker_id, sep, token = pair.partition(":")
            if not sep or not worker_id or not token:
                raise ValueError(f"WORKER_TOKENS entry must look like 'worker-id:token', got {pair!r}")
            workers[token] = worker_id
        return workers
