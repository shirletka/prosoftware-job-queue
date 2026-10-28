"""All state transitions of a job. Each function is one SQL statement, so a transition either happens or not."""

import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import case, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Job, JobStatus


class JobNotFound(Exception):
    pass


class JobConflict(Exception):
    def __init__(self, job: Job) -> None:
        # Copied out of the ORM object: it expires when the failed transaction rolls back.
        self.status = job.status
        self.worker_id = job.worker_id
        super().__init__(f"job {job.id} is {job.status}, owner {job.worker_id}")


async def enqueue(session: AsyncSession, prompt: str, params: dict[str, Any], max_attempts: int) -> Job:
    job = Job(prompt=prompt, params=params, max_attempts=max_attempts)
    session.add(job)
    await session.flush()
    await session.refresh(job)
    return job


async def get(session: AsyncSession, job_id: uuid.UUID) -> Job:
    job = await session.get(Job, job_id, populate_existing=True)
    if job is None:
        raise JobNotFound(job_id)
    return job


async def claim(session: AsyncSession, worker_id: str) -> Job | None:
    # SKIP LOCKED: a row another transaction is claiming right now is invisible to us,
    # so concurrent workers take different jobs instead of waiting on the same one.
    next_job = (
        select(Job.id)
        .where(Job.status == JobStatus.QUEUED)
        .order_by(Job.created_at)
        .limit(1)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    stmt = (
        update(Job)
        .where(Job.id == next_job)
        .values(
            status=JobStatus.RUNNING,
            worker_id=worker_id,
            attempts=Job.attempts + 1,
            claimed_at=func.now(),
            heartbeat_at=func.now(),
            updated_at=func.now(),
        )
        .returning(Job)
    )
    return await session.scalar(stmt)


async def _update_owned(session: AsyncSession, job_id: uuid.UUID, worker_id: str, **values: Any) -> Job:
    # The worker must still hold the lease. If the reaper gave the job to someone else,
    # a late complete() from the old worker must not overwrite the new attempt.
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.RUNNING, Job.worker_id == worker_id)
        .values(updated_at=func.now(), **values)
        .returning(Job)
    )
    job = await session.scalar(stmt)
    if job is None:
        raise JobConflict(await get(session, job_id))
    return job


async def heartbeat(session: AsyncSession, job_id: uuid.UUID, worker_id: str) -> Job:
    return await _update_owned(session, job_id, worker_id, heartbeat_at=func.now())


async def complete(session: AsyncSession, job_id: uuid.UUID, worker_id: str, result: str) -> Job:
    return await _update_owned(
        session, job_id, worker_id, status=JobStatus.DONE, result=result, error=None, finished_at=func.now()
    )


async def fail(session: AsyncSession, job_id: uuid.UUID, worker_id: str, error: str) -> Job:
    return await _update_owned(session, job_id, worker_id, status=JobStatus.FAILED, error=error, finished_at=func.now())


async def requeue_stale(
    session: AsyncSession, lease_timeout: timedelta, max_runtime: timedelta
) -> list[tuple[uuid.UUID, str]]:
    """Put stuck jobs back in the queue; give up after max_attempts.

    Stuck means the worker stopped sending heartbeats (it died), or the job has been running
    for longer than any job should (the worker is alive but hangs on a provider call).
    """
    lease_expired = Job.heartbeat_at < func.now() - lease_timeout
    too_long = Job.claimed_at < func.now() - max_runtime
    stale = (
        select(Job.id)
        .where(Job.status == JobStatus.RUNNING, or_(lease_expired, too_long))
        .with_for_update(skip_locked=True)
    )
    exhausted = Job.attempts >= Job.max_attempts
    stmt = (
        update(Job)
        .where(Job.id.in_(stale))
        .values(
            status=case((exhausted, JobStatus.FAILED.value), else_=JobStatus.QUEUED.value),
            error=func.concat(
                case((lease_expired, "lease expired on "), else_="ran longer than allowed on "),
                Job.worker_id,
                " after attempt ",
                Job.attempts,
            ),
            finished_at=case((exhausted, func.now())),
            worker_id=None,
            claimed_at=None,
            heartbeat_at=None,
            updated_at=func.now(),
        )
        .returning(Job.id, Job.status)
    )
    return [(row.id, row.status) for row in await session.execute(stmt)]
