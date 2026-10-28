"""Worker: claims a job, pretends to generate something, reports the result.

API_URL=http://localhost:8000 WORKER_TOKEN=... python -m worker
"""

import asyncio
import logging
import os
import random
import signal
from contextlib import suppress
from typing import Any

import httpx

log = logging.getLogger("worker")

API_URL = os.environ.get("API_URL", "http://localhost:8000")
TOKEN = os.environ["WORKER_TOKEN"]
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL_SECONDS", "1"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "60"))
JOB_SECONDS = (float(os.environ.get("JOB_MIN_SECONDS", "1")), float(os.environ.get("JOB_MAX_SECONDS", "4")))
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0.1"))
REPORT_ATTEMPTS = 6


class LeaseLost(Exception):
    pass


async def generate(job: dict[str, Any]) -> str:
    seconds = random.uniform(*JOB_SECONDS)
    await asyncio.sleep(seconds)
    if random.random() < FAIL_RATE:
        raise RuntimeError("simulated provider error")
    voice = job["params"].get("voice", "default")
    return f"[{voice}, {seconds:.1f} s] {job['prompt']}"


async def keep_lease(client: httpx.AsyncClient, job_id: str) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            response = await client.post(f"/jobs/{job_id}/heartbeat")
        except httpx.HTTPError as exc:
            # One missed heartbeat is fine, the lease is much longer than the interval.
            log.warning("heartbeat for %s failed: %s", job_id, exc)
            continue
        if response.status_code == 409:
            raise LeaseLost(job_id)


async def report(client: httpx.AsyncClient, job_id: str, action: str, body: dict[str, str]) -> bool:
    """Deliver the outcome, riding out an API restart. If it never gets through, the lease expires, the job reruns."""
    for attempt in range(REPORT_ATTEMPTS):
        try:
            response = await client.post(f"/jobs/{job_id}/{action}", json=body)
        except httpx.HTTPError as exc:
            log.warning("%s for %s failed: %s", action, job_id, exc)
        else:
            if response.is_success:
                return True
            if response.status_code == 409:
                log.warning("%s was taken away before we finished: %s", job_id, response.json()["detail"])
                return False
            log.warning("%s for %s: HTTP %s %s", action, job_id, response.status_code, response.text[:200])
            if response.status_code < 500:
                return False
        await asyncio.sleep(min(2**attempt, 30))
    log.error("gave up reporting %s for %s, the job will be retried after its lease expires", action, job_id)
    return False


async def process(client: httpx.AsyncClient, job: dict[str, Any]) -> None:
    job_id = job["id"]
    log.info("claimed %s (attempt %s)", job_id, job["attempts"])
    heartbeat = asyncio.create_task(keep_lease(client, job_id))
    work = asyncio.create_task(generate(job))
    try:
        await asyncio.wait({heartbeat, work}, return_when=asyncio.FIRST_COMPLETED)
        if not work.done():
            work.cancel()
            heartbeat.result()  # re-raises LeaseLost
        result = work.result()
    except LeaseLost:
        log.warning("lost lease on %s, dropping the result", job_id)
        return
    except Exception as exc:
        # str(TimeoutError()) is empty, and the API rejects an empty error.
        error = f"{type(exc).__name__}: {exc}"[:4000]
        if await report(client, job_id, "fail", {"error": error}):
            log.info("failed %s: %s", job_id, error)
        return
    finally:
        heartbeat.cancel()

    if await report(client, job_id, "complete", {"result": result}):
        log.info("done %s: %s", job_id, result)


async def pause(stopping: asyncio.Event, seconds: float) -> None:
    """Sleep that ends early on SIGTERM, so `docker stop` does not wait out a 30 s backoff."""
    with suppress(TimeoutError):
        await asyncio.wait_for(stopping.wait(), seconds)


async def main() -> None:
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopping.set)

    headers = {"Authorization": f"Bearer {TOKEN}"}
    async with httpx.AsyncClient(base_url=API_URL, headers=headers, timeout=10) as client:
        backoff = POLL_INTERVAL
        while not stopping.is_set():
            try:
                response = await client.post("/jobs/claim")
                response.raise_for_status()
            except httpx.HTTPError as exc:
                log.warning("claim failed: %s, retrying in %.0fs", exc, backoff)
                await pause(stopping, backoff)
                backoff = min(backoff * 2, 30)
                continue
            backoff = POLL_INTERVAL
            if response.status_code == 204:
                await pause(stopping, POLL_INTERVAL)
                continue
            # SIGTERM lets the current job finish; the next claim simply does not happen.
            await process(client, response.json())
    log.info("stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(main())
