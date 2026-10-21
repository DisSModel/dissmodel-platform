"""API tests. Those marked with a version fail on v0.1.0 (commit 5239341)."""
from __future__ import annotations


def _submit(client):
    r = client.post("/submit_job", json={"model_name": "toy", "input_dataset": "s3://dissmodel-inputs/x.tif"})
    assert r.status_code == 200, r.text
    return r.json()["experiment_id"]


def test_requires_api_key(api):
    client, _, _ = api
    r = client.get("/models", headers={"X-API-Key": "wrong"})
    assert r.status_code == 403


def test_submit_sets_output_under_experiment_folder(api):
    client, redis, _ = api
    exp = _submit(client)
    r = client.get(f"/experiments/{exp}")
    assert r.status_code == 200
    assert r.json()["output_path"] == f"s3://dissmodel-outputs/experiments/{exp}/output.tif"
    assert redis.lrange("queue:normal", 0, -1) == [exp]


def test_unknown_model_is_404(api):
    client, _, _ = api
    r = client.post("/submit_job", json={"model_name": "nope", "input_dataset": "x"})
    assert r.status_code == 404


# v0.1.0: GET /experiments/{id}, cited in the docs for the full record, did not exist.
def test_get_experiment_returns_full_record(api):
    client, _, _ = api
    exp = _submit(client)
    body = client.get(f"/experiments/{exp}").json()
    for field in ("resolved_spec", "model_commit", "code_version", "artifacts", "metrics", "source"):
        assert field in body


# v0.1.0: the reproduction copied the original code_version.
def test_reproduce_records_installed_version(api):
    client, _, main = api
    exp = _submit(client)
    original = main._load_record(exp)
    original.code_version = "0.0.1"
    original.status = "completed"
    main._store_record(original)

    r = client.post(f"/experiments/{exp}/reproduce")
    assert r.status_code == 200
    new_id = r.json()["experiment_id"]
    new = client.get(f"/experiments/{new_id}").json()

    from worker.runner import _code_version
    assert new["code_version"] == _code_version()
    assert new["metrics"]["reproduces"] == exp
    assert new["resolved_spec"] == original.resolved_spec


# v0.1.0: cancelling left the id in the queue.
def test_cancel_removes_from_queue(api):
    client, redis, _ = api
    exp = _submit(client)
    r = client.delete(f"/job/{exp}")
    assert r.status_code == 200
    assert redis.lrange("queue:normal", 0, -1) == []
    assert client.get(f"/job/{exp}").json()["status"] == "cancelled"


def test_publish_requires_completed(api):
    client, _, _ = api
    exp = _submit(client)
    assert client.post(f"/experiments/{exp}/publish").status_code == 400


def test_publish_includes_artifacts_and_metrics(api):
    client, _, main = api
    exp = _submit(client)
    record = main._load_record(exp)
    record.status = "completed"
    record.artifacts["output"] = "abc"
    main._store_record(record)
    body = client.post(f"/experiments/{exp}/publish").json()
    assert body["output_sha256"] == "abc"
    assert "metrics" in body and "artifacts" in body


def test_admin_sync_reports_head(api):
    client, _, _ = api
    body = client.post("/admin/sync").json()
    assert body["head"] == "local-dev"
