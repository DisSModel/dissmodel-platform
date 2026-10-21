"""
Fixtures for the worker tests.

The tests run the real subprocess path (``python -m worker.job_runner``)
with a temporary configs repository and a local output root, so no
MinIO, Redis or Docker is needed.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

SERVICES = Path(__file__).resolve().parents[2]
if str(SERVICES) not in sys.path:
    sys.path.insert(0, str(SERVICES))

TOMLS = {
    "toy_bytes": """
[model]
name = "toy_bytes"
class = "toy_bytes"
executor_module = "worker.tests.toy_executors"
output_suffix = ".bin"
[model.parameters]
k = 2
""",
    "toy_tiff": """
[model]
name = "toy_tiff"
class = "toy_tiff"
executor_module = "worker.tests.toy_executors"
""",
}


@pytest.fixture
def platform_env(tmp_path, monkeypatch):
    configs = tmp_path / "configs"
    (configs / "models").mkdir(parents=True)
    for name, text in TOMLS.items():
        (configs / "models" / f"{name}.toml").write_text(text)

    outputs = tmp_path / "outputs"
    monkeypatch.setenv("CONFIGS_PATH", str(configs))
    monkeypatch.setenv("DISSMODEL_OUTPUT_ROOT", str(outputs))
    # The job subprocess must import the `worker` package and the toy executors.
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(SERVICES), os.getenv("PYTHONPATH")])))
    # Anything an executor writes with a relative path lands here, not in the repo.
    monkeypatch.chdir(tmp_path)
    return {"configs": configs, "outputs": outputs, "tmp": tmp_path}
