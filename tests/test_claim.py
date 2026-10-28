import asyncio

from app import queue

from .conftest import backdate


async def enqueue_many(sessions, n: int) -> list:
    async with sessions.begin() as s:
        return [(await queue.enqueue(s, f"фраза {i}", {}, 3)).id for i in range(n)]


async def test_claim_takes_the_oldest_queued_job(sessions):
    first, second = await enqueue_many(sessions, 2)
    # Make the row inserted first the newest one, so physical row order cannot pass for FIFO.
    await backdate(sessions, first, minutes=-1, column="created_at")

    async with sessions.begin() as s:
        job = await queue.claim(s, "w1")

    assert job.id == second
    assert job.status == "running"
    assert job.worker_id == "w1"
    assert job.attempts == 1
    assert job.claimed_at is not None and job.heartbeat_at is not None


async def test_jobs_created_in_one_transaction_keep_their_order(sessions):
    created = await enqueue_many(sessions, 5)

    claimed = []
    for _ in created:
        async with sessions.begin() as s:
            claimed.append((await queue.claim(s, "w1")).id)

    assert claimed == created


async def test_claim_on_empty_queue_returns_nothing(sessions):
    async with sessions.begin() as s:
        assert await queue.claim(s, "w1") is None


async def test_locked_job_is_skipped_not_waited_for(sessions):
    first, second = await enqueue_many(sessions, 2)

    async with sessions() as slow:
        await slow.begin()
        held = await queue.claim(slow, "w1")  # row lock is held until this transaction ends

        async with sessions.begin() as fast:
            # Without SKIP LOCKED this call would block on the first row until `slow` commits.
            other = await asyncio.wait_for(queue.claim(fast, "w2"), timeout=2)

        assert (held.id, other.id) == (first, second)
        await slow.rollback()

    async with sessions() as s:
        assert (await queue.get(s, first)).status == "queued", "rolled back claim leaves the job in the queue"


async def test_concurrent_workers_never_get_the_same_job(sessions):
    await enqueue_many(sessions, 300)

    async def drain(worker_id: str) -> list:
        taken = []
        while True:
            async with sessions.begin() as s:
                job = await queue.claim(s, worker_id)
            if job is None:
                return taken
            taken.append(job.id)

    per_worker = await asyncio.gather(*(drain(f"w{i}") for i in range(10)))
    taken = [job_id for ids in per_worker for job_id in ids]

    assert len(taken) == 300
    assert len(set(taken)) == 300
    assert sum(1 for ids in per_worker if ids) > 1, "the claims really ran in parallel"
