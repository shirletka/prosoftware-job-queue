import asyncio
import hmac
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from . import queue
from .config import Settings
from .schemas import JobComplete, JobCreate, JobFail, JobOut

log = logging.getLogger(__name__)


async def reaper_loop(sessions: async_sessionmaker[AsyncSession], settings: Settings) -> None:
    while True:
        try:
            async with sessions.begin() as session:
                stale = await queue.requeue_stale(session, settings.lease_timeout, settings.max_runtime)
                for job_id, new_status in stale:
                    log.warning("job %s lease expired, now %s", job_id, new_status)
        except Exception:
            log.exception("reaper iteration failed")
        await asyncio.sleep(settings.reaper_interval_seconds)


def create_app(settings: Settings | None = None, *, run_reaper: bool = True) -> FastAPI:
    settings = settings or Settings()
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    workers_by_token = settings.workers_by_token
    if not workers_by_token:
        log.warning("WORKER_TOKENS is empty: every worker request will get 401")

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        reaper = asyncio.create_task(reaper_loop(sessions, settings)) if run_reaper else None
        yield
        if reaper:
            reaper.cancel()
            with suppress(asyncio.CancelledError):
                await reaper
        await engine.dispose()

    app = FastAPI(title="Job queue", lifespan=lifespan)
    app.state.engine = engine

    def current_worker(authorization: Annotated[str | None, Header()] = None) -> str:
        scheme, _, token = (authorization or "").partition(" ")
        if scheme.lower() == "bearer" and token:
            for known, worker_id in workers_by_token.items():
                if hmac.compare_digest(known.encode(), token.strip().encode()):
                    return worker_id
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Worker token required", {"WWW-Authenticate": "Bearer"})

    Worker = Annotated[str, Depends(current_worker)]

    @app.exception_handler(queue.JobNotFound)
    async def not_found(_: Request, exc: queue.JobNotFound) -> JSONResponse:
        return JSONResponse({"detail": f"Job {exc.args[0]} not found"}, status.HTTP_404_NOT_FOUND)

    @app.exception_handler(queue.JobConflict)
    async def conflict(_: Request, exc: queue.JobConflict) -> JSONResponse:
        if exc.status == "running":
            detail = "Job is held by another worker"
        else:
            detail = f"Job is {exc.status}, expected running"
        return JSONResponse({"detail": detail, "status": exc.status}, status.HTTP_409_CONFLICT)

    # Each handler commits inside its own body, before the response leaves.
    # With a yield-dependency the commit could land after the worker already got the job
    # and called /complete on a claim that is not visible yet.

    @app.post("/jobs", status_code=status.HTTP_201_CREATED)
    async def create_job(body: JobCreate) -> JobOut:
        async with sessions.begin() as s:
            job = await queue.enqueue(s, body.prompt, body.params, body.max_attempts)
        return JobOut.model_validate(job)

    @app.post("/jobs/claim", response_model=JobOut, responses={204: {"description": "Queue is empty"}})
    async def claim_job(worker: Worker) -> JobOut | Response:
        async with sessions.begin() as s:
            job = await queue.claim(s, worker)
        if job is None:
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        return JobOut.model_validate(job)

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: uuid.UUID) -> JobOut:
        async with sessions() as s:
            return JobOut.model_validate(await queue.get(s, job_id))

    @app.post("/jobs/{job_id}/heartbeat")
    async def heartbeat_job(job_id: uuid.UUID, worker: Worker) -> JobOut:
        async with sessions.begin() as s:
            job = await queue.heartbeat(s, job_id, worker)
        return JobOut.model_validate(job)

    @app.post("/jobs/{job_id}/complete")
    async def complete_job(job_id: uuid.UUID, body: JobComplete, worker: Worker) -> JobOut:
        async with sessions.begin() as s:
            job = await queue.complete(s, job_id, worker, body.result)
        return JobOut.model_validate(job)

    @app.post("/jobs/{job_id}/fail")
    async def fail_job(job_id: uuid.UUID, body: JobFail, worker: Worker) -> JobOut:
        async with sessions.begin() as s:
            job = await queue.fail(s, job_id, worker, body.error)
        return JobOut.model_validate(job)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return {"status": "ok"}

    return app
