import asyncio
import uuid

import pytest

from .conftest import W1, W2


async def create(client, prompt: str = "Сәлем", **body) -> dict:
    response = await client.post("/jobs", json={"prompt": prompt, **body})
    assert response.status_code == 201
    return response.json()


async def test_job_lifecycle(client):
    job = await create(client, prompt="Сәлем, әлем", params={"voice": "aigerim", "speed": 1.1})
    assert job["status"] == "queued"
    assert job["attempts"] == 0

    claimed = (await client.post("/jobs/claim", headers=W1)).json()
    assert (claimed["id"], claimed["status"], claimed["worker_id"]) == (job["id"], "running", "worker-1")

    done = await client.post(f"/jobs/{job['id']}/complete", json={"result": "s3://audio/a.mp3"}, headers=W1)
    assert done.status_code == 200

    fetched = (await client.get(f"/jobs/{job['id']}")).json()
    assert fetched["status"] == "done"
    assert fetched["result"] == "s3://audio/a.mp3"
    assert fetched["params"] == {"voice": "aigerim", "speed": 1.1}
    assert fetched["finished_at"] is not None


async def test_fail_records_the_error(client):
    job = await create(client)
    await client.post("/jobs/claim", headers=W1)

    response = await client.post(f"/jobs/{job['id']}/fail", json={"error": "provider returned 500"}, headers=W1)

    assert response.status_code == 200
    assert response.json()["status"] == "failed"
    assert response.json()["error"] == "provider returned 500"


async def test_claim_on_empty_queue_is_204(client):
    response = await client.post("/jobs/claim", headers=W1)

    assert response.status_code == 204
    assert response.content == b""


@pytest.mark.parametrize(
    "headers",
    [{}, {"Authorization": "Bearer wrong"}, {"Authorization": "token-1"}, {"Authorization": "Basic token-1"}],
)
async def test_worker_endpoints_need_a_valid_token(client, headers):
    job = await create(client)

    for path in ("/jobs/claim", *(f"/jobs/{job['id']}/{action}" for action in ("heartbeat", "complete", "fail"))):
        response = await client.post(path, json={"error": "x", "result": "x"}, headers=headers)
        assert response.status_code == 401, path


async def test_worker_id_comes_from_the_token_not_from_the_request(client):
    await create(client)

    claimed = (await client.post("/jobs/claim", headers=W2)).json()

    assert claimed["worker_id"] == "worker-2"


async def test_another_worker_cannot_finish_or_extend_my_job(client):
    job = await create(client)
    await client.post("/jobs/claim", headers=W1)

    for path, body in (("complete", {"result": "x"}), ("fail", {"error": "x"}), ("heartbeat", None)):
        response = await client.post(f"/jobs/{job['id']}/{path}", json=body, headers=W2)
        assert response.status_code == 409, path
        assert response.json()["detail"] == "Job is held by another worker"

    assert (await client.get(f"/jobs/{job['id']}")).json()["status"] == "running"


@pytest.mark.parametrize("finish", ["complete", "fail"])
async def test_finished_job_cannot_change_again(client, finish):
    job = await create(client)
    await client.post("/jobs/claim", headers=W1)
    body = {"result": "x"} if finish == "complete" else {"error": "x"}
    await client.post(f"/jobs/{job['id']}/{finish}", json=body, headers=W1)

    again = await client.post(f"/jobs/{job['id']}/complete", json={"result": "x"}, headers=W1)

    assert again.status_code == 409
    expected = "done" if finish == "complete" else "failed"
    assert again.json() == {"detail": f"Job is {expected}, expected running", "status": expected}


async def test_queued_job_cannot_be_completed_without_claim(client):
    job = await create(client)

    response = await client.post(f"/jobs/{job['id']}/complete", json={"result": "x"}, headers=W1)

    assert response.status_code == 409


async def test_unknown_job_is_404(client):
    missing = uuid.uuid4()

    assert (await client.get(f"/jobs/{missing}")).status_code == 404
    assert (await client.post(f"/jobs/{missing}/complete", json={"result": "x"}, headers=W1)).status_code == 404
    assert (await client.get("/jobs/not-a-uuid")).status_code == 422


async def test_invalid_job_is_rejected(client):
    assert (await client.post("/jobs", json={"prompt": "x", "max_attempts": 0})).status_code == 422
    assert (await client.post("/jobs", json={"prompt": "x", "params": "not an object"})).status_code == 422
    assert (await client.post("/jobs", json={"prompt": ""})).status_code == 422
    assert (await client.post("/jobs", json={})).status_code == 422


async def test_two_workers_over_http_split_the_queue(client):
    created = {(await create(client, prompt=f"фраза {i}"))["id"] for i in range(40)}

    async def drain(headers) -> list[str]:
        taken = []
        while (response := await client.post("/jobs/claim", headers=headers)).status_code == 200:
            taken.append(response.json()["id"])
        return taken

    first, second = await asyncio.gather(drain(W1), drain(W2))

    assert not set(first) & set(second)
    assert set(first) | set(second) == created
