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

TOMLS["toy_lazy"] = """
[model]
name = "toy_lazy"
class = "toy_lazy"
executor_module = "worker.tests.lazy_pkg"
output_suffix = ".bin"
"""

# Same layout as disslucc's examples/dissmodel-configs/lucc_continuous.toml:
# spec fields at the [model] level, run values under [model.parameters].
TOMLS["toy_spec_fields"] = """
[model]
name = "toy_spec_fields"
class = "toy_bytes"
executor_module = "worker.tests.toy_executors"
description = "spec fields at the model level"
dissmodel = ">=0.6.0,<0.7.0"
output_suffix = ".bin"
land_use_types = ["f", "d", "outros"]
resolution = 5000.0

[model.static]
f = -1

[[model.potential]]
lu = "f"
const = 0.7

[model.parameters]
k = 3
resolution = 1000.0
"""
