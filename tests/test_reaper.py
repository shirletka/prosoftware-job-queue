import asyncio
from contextlib import suppress
from datetime import timedelta

import pytest

from app import queue
from app.config import Settings
from app.main import reaper_loop

from .conftest import backdate

LEASE = timedelta(minutes=10)
MAX_RUNTIME = timedelta(hours=1)


async def claimed_job(sessions, *, max_attempts: int = 3):
    async with sessions.begin() as s:
        job = await queue.enqueue(s, "фраза", {}, max_attempts)
        await queue.claim(s, "w1")
    return job.id


async def reap(sessions):
    async with sessions.begin() as s:
        return await queue.requeue_stale(s, LEASE, MAX_RUNTIME)


async def test_stale_job_goes_back_to_the_queue(sessions):
    job_id = await claimed_job(sessions)
    await backdate(sessions, job_id, minutes=11)

    assert await reap(sessions) == [(job_id, "queued")]

    async with sessions() as s:
        job = await queue.get(s, job_id)
    assert job.status == "queued"
    assert job.worker_id is None
    assert job.heartbeat_at is None
    assert job.error == "lease expired on w1 after attempt 1"


async def test_job_inside_the_lease_is_left_alone(sessions):
    job_id = await claimed_job(sessions)
    await backdate(sessions, job_id, minutes=9)

    assert await reap(sessions) == []


async def test_requeued_job_can_be_taken_by_another_worker_and_the_old_one_loses_it(sessions):
    job_id = await claimed_job(sessions)
    await backdate(sessions, job_id, minutes=11)
    await reap(sessions)

    async with sessions.begin() as s:
        job = await queue.claim(s, "w2")
    assert (job.id, job.worker_id, job.attempts) == (job_id, "w2", 2)

    with pytest.raises(queue.JobConflict) as conflict:
        async with sessions.begin() as s:
            await queue.complete(s, job_id, "w1", "late result")
    assert conflict.value.worker_id == "w2"


async def test_job_fails_after_max_attempts(sessions):
    job_id = await claimed_job(sessions, max_attempts=1)
    await backdate(sessions, job_id, minutes=11)

    assert await reap(sessions) == [(job_id, "failed")]

    async with sessions() as s:
        job = await queue.get(s, job_id)
    assert job.status == "failed"
    assert job.finished_at is not None


async def test_heartbeat_keeps_a_long_job_alive(sessions):
    job_id = await claimed_job(sessions)
    await backdate(sessions, job_id, minutes=30)

    async with sessions.begin() as s:
        await queue.heartbeat(s, job_id, "w1")

    assert await reap(sessions) == []


async def test_finished_jobs_are_never_requeued(sessions):
    job_id = await claimed_job(sessions)
    async with sessions.begin() as s:
        await queue.complete(s, job_id, "w1", "ok")
    await backdate(sessions, job_id, minutes=60)

    assert await reap(sessions) == []


async def test_alive_but_hanging_job_is_requeued_after_max_runtime(sessions):
    job_id = await claimed_job(sessions)
    await backdate(sessions, job_id, minutes=61, column="claimed_at")
    await backdate(sessions, job_id, minutes=0.5)  # heartbeats still arrive

    assert await reap(sessions) == [(job_id, "queued")]
    async with sessions() as s:
        assert (await queue.get(s, job_id)).error == "ran longer than allowed on w1 after attempt 1"


async def test_reaper_loop_runs_in_the_background(sessions):
    job_id = await claimed_job(sessions)
    await backdate(sessions, job_id, minutes=11)

    task = asyncio.create_task(reaper_loop(sessions, Settings(reaper_interval_seconds=0.05)))
    try:
        for _ in range(40):
            await asyncio.sleep(0.05)
            async with sessions() as s:
                if (await queue.get(s, job_id)).status == "queued":
                    break
        else:
            pytest.fail("the reaper loop did not requeue the stale job")
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
