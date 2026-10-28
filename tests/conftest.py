import os
import uuid
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings
from app.main import create_app

DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "postgresql+psycopg://jobs:jobs@localhost:55432/jobs_test")
ROOT = Path(__file__).parent.parent

W1 = {"Authorization": "Bearer token-1"}
W2 = {"Authorization": "Bearer token-2"}


@pytest.fixture(scope="session", autouse=True)
def database() -> None:
    url = make_url(DATABASE_URL)
    admin = url.set(drivername="postgresql", database="postgres").render_as_string(hide_password=False)
    with psycopg.connect(admin, autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", [url.database]).fetchone():
            conn.execute(f'CREATE DATABASE "{url.database}"')

    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", DATABASE_URL)
    # Up, down and up again: the downgrade path gets exercised on every run, not only when someone needs it.
    command.upgrade(config, "head")
    command.downgrade(config, "base")
    command.upgrade(config, "head")


@pytest.fixture
async def sessions():
    engine = create_async_engine(DATABASE_URL, pool_size=20)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE jobs"))
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
async def client(sessions):
    settings = Settings(database_url=DATABASE_URL, worker_tokens="worker-1:token-1,worker-2:token-2")
    app = create_app(settings, run_reaper=False)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    await app.state.engine.dispose()


async def backdate(
    sessions: async_sessionmaker[AsyncSession],
    job_id: uuid.UUID | str,
    *,
    minutes: float,
    column: str = "heartbeat_at",
) -> None:
    """Pretend `column` (the last heartbeat by default) happened `minutes` ago."""
    assert column in {"heartbeat_at", "claimed_at", "created_at"}
    async with sessions.begin() as s:
        await s.execute(
            text(f"UPDATE jobs SET {column} = now() - make_interval(secs => :s) WHERE id = :id"),
            {"s": minutes * 60, "id": job_id},
        )
