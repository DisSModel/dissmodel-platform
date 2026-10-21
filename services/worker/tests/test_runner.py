"""
Worker tests. Each one fails on the code of v0.1.0 (commit 5239341):
the comment on top of the test says how.
"""
from __future__ import annotations

from pathlib import Path

import fakeredis
import pytest
from dissmodel.executor.schemas import JobRequest

from worker import worker as worker_mod
from worker.api_registry import load_model_spec
from worker.job_runner import artifact_dir
from worker.runner import (
    REPRODUCES_KEY,
    _code_version,
    build_record,
    build_reproduction,
    run_experiment,
)
from worker.storage import env_flag


def _submit(name: str, **kw):
    return build_record(JobRequest(model_name=name, input_dataset="/nowhere", **kw))


# ── Job execution ─────────────────────────────────────────────────────────────

# v0.1.0: ZeroDivisionError in the profiling report after the output was
# written; the job ended as "failed".
def test_fast_job_completes(platform_env):
    record = run_experiment(_submit("toy_bytes"))
    assert record.status == "completed"
    assert record.parameters == {"k": 2}


# v0.1.0: output_path stayed None and the executor wrote to the worker's
# working directory (inside the container).
def test_output_goes_under_output_root(platform_env):
    submitted = _submit("toy_bytes")
    expected = f"{platform_env['outputs']}/{submitted.experiment_id}/output.bin"
    assert submitted.output_path == expected

    record = run_experiment(submitted)
    assert record.output_path == expected
    assert Path(expected).exists()
    assert not list(platform_env["tmp"].glob("output_*"))


# v0.1.0: with a bare file name as output_path, both side artifacts were
# written "inside" the output file and lost with only a warning.
def test_side_artifacts_are_saved_next_to_output(platform_env):
    record = run_experiment(_submit("toy_bytes"))
    folder = Path(record.output_path).parent
    short = record.experiment_id[:8]

    assert {"output", "profiling", "record_json"} <= set(record.artifacts)
    assert (folder / f"profiling_{short}.md").exists()
    assert (folder / f"{short}.record.json").exists()
    assert not any("Warning: Could not save" in line for line in record.logs)


def test_artifact_dir():
    assert artifact_dir("output_ab12.tif") == "."
    assert artifact_dir(None) == "."
    assert artifact_dir("/data/out/x.tif") == "/data/out"
    assert artifact_dir("s3://bucket/exp/123/output.tif") == "s3://bucket/exp/123"


# v0.1.0: only the dissmodel version was recorded.
def test_environment_is_recorded(platform_env):
    record = run_experiment(_submit("toy_bytes"))
    env = record.metrics["environment"]
    assert env["packages"]["dissmodel"] == _code_version()
    assert env["executor"].endswith("ToyBytesExecutor")
    assert env["python"]


# ── Registry ──────────────────────────────────────────────────────────────────

# v0.1.0: lru_cache kept the first version of the spec until the API restarted;
# the sidecar's git pull never cleared it.
def test_registry_reads_current_spec(platform_env):
    path = platform_env["configs"] / "models" / "toy_bytes.toml"
    assert load_model_spec("toy_bytes")["model"]["parameters"]["k"] == 2
    path.write_text(path.read_text().replace("k = 2", "k = 3"))
    assert load_model_spec("toy_bytes")["model"]["parameters"]["k"] == 3


def test_registry_rejects_names_outside_models(platform_env):
    with pytest.raises(FileNotFoundError):
        load_model_spec("../models/toy_bytes")


def test_unpinned_package_is_flagged(platform_env):
    path = platform_env["configs"] / "models" / "toy_bytes.toml"
    path.write_text(path.read_text().replace('output_suffix', 'package = "/opt/x"\noutput_suffix'))
    record = _submit("toy_bytes")
    assert any("not pinned" in line for line in record.logs)


# ── Queue ─────────────────────────────────────────────────────────────────────

@pytest.fixture
def fake_redis(monkeypatch):
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(worker_mod, "redis_client", client)
    return client


# v0.1.0: DELETE /job/{id} marked the record but the worker ran it anyway.
def test_cancelled_job_is_not_run(platform_env, fake_redis):
    record = _submit("toy_bytes")
    record.status = "cancelled"
    worker_mod._save_record(record)

    worker_mod.process_job(record.experiment_id)

    assert worker_mod._load_record(record.experiment_id).status == "cancelled"
    assert not Path(record.output_path).exists()


def test_failed_job_is_recorded(platform_env, fake_redis):
    path = platform_env["configs"] / "models" / "toy_bytes.toml"
    path.write_text(path.read_text().replace('class = "toy_bytes"', 'class = "missing"'))
    record = _submit("toy_bytes")
    worker_mod._save_record(record)

    worker_mod.process_job(record.experiment_id)

    stored = worker_mod._load_record(record.experiment_id)
    assert stored.status == "failed"
    assert any(line.startswith("Failed:") for line in stored.logs)


# ── Reproduction ──────────────────────────────────────────────────────────────

# v0.1.0 (API): the reproduction copied the original code_version even when
# a different dissmodel was installed.
def test_reproduction_records_installed_version(platform_env):
    original = _submit("toy_bytes")
    original.code_version = "0.0.1"
    repro = build_reproduction(original)

    assert repro.code_version == _code_version()
    assert repro.metrics[REPRODUCES_KEY] == original.experiment_id
    assert repro.resolved_spec == original.resolved_spec
    assert repro.output_path != original.output_path
    assert any("original ran with 0.0.1" in line for line in repro.logs)


def _run_and_reproduce(fake_redis, monkeypatch, *, compress=None, offset=None):
    original = _submit("toy_tiff")
    worker_mod._save_record(original)
    worker_mod.process_job(original.experiment_id)
    original = worker_mod._load_record(original.experiment_id)
    assert original.status == "completed"

    if compress:
        monkeypatch.setenv("TOY_COMPRESS", compress)
    if offset:
        monkeypatch.setenv("TOY_OFFSET", offset)

    repro = build_reproduction(original)
    worker_mod._save_record(repro)
    worker_mod.process_job(repro.experiment_id)
    return worker_mod._load_record(repro.experiment_id).metrics["reproduction"]


# v0.1.0: /reproduce never compared the outputs.
def test_reproduction_identical(platform_env, fake_redis, monkeypatch):
    result = _run_and_reproduce(fake_redis, monkeypatch)
    assert result["file_identical"] is True
    assert result["values_identical"] is True


def test_reproduction_same_values_different_file(platform_env, fake_redis, monkeypatch):
    # Same data, different encoding: the case of the GeoTIFF written on macOS ARM.
    result = _run_and_reproduce(fake_redis, monkeypatch, compress="lzw")
    assert result["file_identical"] is False
    assert result["values_identical"] is True


def test_reproduction_detects_different_values(platform_env, fake_redis, monkeypatch):
    result = _run_and_reproduce(fake_redis, monkeypatch, offset="1e-6")
    assert result["values_identical"] is False


# ── Settings ──────────────────────────────────────────────────────────────────

# v0.1.0: bool(os.getenv("MINIO_SECURE")) made MINIO_SECURE=false turn TLS on.
@pytest.mark.parametrize(("value", "expected"), [
    ("false", False), ("0", False), ("", False), ("true", True), ("1", True), ("YES", True),
])
def test_env_flag(monkeypatch, value, expected):
    monkeypatch.setenv("X_FLAG", value)
    assert env_flag("X_FLAG") is expected
