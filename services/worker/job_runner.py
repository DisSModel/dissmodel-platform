# services/worker/job_runner.py
"""
Subprocess entry point for a single experiment.

Runs in a fresh Python process — packages installed by the parent
process via pip are fully visible here without import caching issues.
This is the correct approach for the plugin pattern where executor
packages are declared in the TOML and installed at runtime.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
import os

# mantendo o stdout "limpo" apenas para o nosso JSON.
sys.stdout = sys.stderr

def _import_executor_package(package: str) -> None:
    """
    Import the package and its executor submodule to trigger
    __init_subclass__ registration in ExecutorRegistry.
    """
    pkg_name = (
        package
        .split("==")[0]
        .split("@")[0]
        .rstrip("/")
        .split("/")[-1]
        .replace("-", "_")
    )

    for module in [pkg_name, f"{pkg_name}.executor"]:
        try:
            importlib.import_module(module)
        except ImportError:
            pass


def main(record_path: str) -> None:
    from dissmodel.executor.registry import ExecutorRegistry
    from dissmodel.executor.schemas  import ExperimentRecord

    record  = ExperimentRecord.model_validate_json(Path(record_path).read_text())
    spec    = record.resolved_spec
    package = spec.get("model", {}).get("package", "")

    # Import external executor package — already installed by parent process
    if package:
        _import_executor_package(package)

    # Always import built-in executors
    import worker.executors  # noqa: F401

    # Resolve and run executor
    model_class  = spec.get("model", {}).get("class")
    executor_cls = ExecutorRegistry.get(model_class)
    executor     = executor_cls()

    try:
        executor.validate(record)
        result = executor.run(record)
        record = executor.save(result, record)
        record.add_log(f"Completed — output={record.output_path}")
    except Exception as exc:
        record.status = "failed"
        record.add_log(f"Failed: {exc}")
        raise

    # Return updated record to parent process via stdout
    #print(record.model_dump_json())
    with os.fdopen(1, "w") as real_stdout:
        real_stdout.write(record.model_dump_json() + "\n")


if __name__ == "__main__":
    main(sys.argv[1])