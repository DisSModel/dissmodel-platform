# services/worker/job_runner.py
"""
Subprocess entry point: run one ExperimentRecord and print it as JSON.

Called by ``worker.runner.run_experiment`` as
``python -m worker.job_runner <record.json>``. Anything the executor
prints goes to stderr; stdout carries only the final record.
"""
from __future__ import annotations

import importlib
import importlib.metadata
import os
import platform
import posixpath
import sys
from pathlib import Path

# Distributions whose versions are recorded with every run, besides the
# one that provides the executor.
RECORDED_DISTRIBUTIONS = ("dissmodel", "disscube", "disslucc", "numpy", "rasterio")


def artifact_dir(output_path: str | None) -> str:
    """
    Folder for the side artifacts (profiling report, record JSON): the
    folder of ``output_path``, or ``.`` when it has none.

    The earlier code took ``output_path.rsplit("/", 1)[0]``, which for a
    bare file name returns the file name itself, so both artifacts were
    written "inside" the output file and silently lost.
    """
    if not output_path:
        return "."
    return posixpath.dirname(output_path) or "."


def _pct(part: float, total: float) -> str:
    # Fast executors finish in less than the 1 ms the timings are rounded
    # to; dividing by that zero used to fail the job after it had already
    # written its output.
    return f"{(part / total) * 100:.1f}%" if total > 0 else "—"


def profiling_report(name: str, record, timings: dict[str, float]) -> str:
    rows = [
        ("Validate", timings["time_validate_sec"]),
        ("Load",     timings["time_load_sec"]),
        ("Run",      timings["time_run_sec"]),
        ("Save",     timings["time_save_sec"]),
    ]
    total = timings["time_total_sec"]
    lines = [
        f"# Profiling Report: {name}\n",
        f"**Experiment ID:** `{record.experiment_id}`",
        f"**Date:** `{record.created_at.isoformat()}`",
        "**Execution Node:** `Cloud Worker`\n",
        "## Execution Times\n",
        "| Phase | Time (seconds) | % of Total |",
        "|---|---|---|",
    ]
    lines += [f"| **{label}** | {t:.3f} | {_pct(t, total)} |" for label, t in rows]
    lines.append(f"| **Total** | **{total:.3f}** | **100%** |")
    return "\n".join(lines) + "\n"


def _distribution_of(executor_cls) -> str | None:
    """Name of the installed distribution that provides the executor's module, if any."""
    top = executor_cls.__module__.split(".", 1)[0]
    dists = importlib.metadata.packages_distributions().get(top, [])
    return dists[0] if dists else None


def environment(executor_cls) -> dict:
    """Versions of Python and of the packages that took part in the run."""
    names = list(RECORDED_DISTRIBUTIONS)
    dist = _distribution_of(executor_cls)
    if dist and dist not in names:
        names.append(dist)

    versions = {}
    for name in names:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            continue
    return {
        "python":            platform.python_version(),
        "executor":          f"{executor_cls.__module__}.{executor_cls.__qualname__}",
        "executor_package":  dist,
        "packages":          versions,
    }


def main(record_path: str) -> None:
    real_stdout = os.fdopen(os.dup(1), "w")
    sys.stdout  = sys.stderr

    from dissmodel.executor.registry import ExecutorRegistry
    from dissmodel.executor.runner import execute_lifecycle
    from dissmodel.executor.schemas import ExperimentRecord
    from dissmodel.io._utils import write_text

    record = ExperimentRecord.model_validate_json(Path(record_path).read_text())
    model  = record.resolved_spec.get("model", {})

    # The package itself is installed by the parent process (runner._ensure_package).
    module = model.get("executor_module", "")
    if module:
        importlib.import_module(module)

    import worker.executors  # noqa: F401  (executors bundled with the platform)

    executor_cls = ExecutorRegistry.get(model.get("class"))
    executor     = executor_cls()

    try:
        record.metrics["environment"] = environment(executor_cls)
        record, timings = execute_lifecycle(executor, record)

        base_dir      = artifact_dir(record.output_path)
        short_id      = record.experiment_id[:8]
        profiling_uri = f"{base_dir}/profiling_{short_id}.md"
        record_uri    = f"{base_dir}/{short_id}.record.json"

        report = profiling_report(getattr(executor_cls, "name", "Model"), record, timings)
        try:
            record.add_artifact("profiling", write_text(report, profiling_uri, content_type="text/markdown"))
            record.add_log(f"Saved profiling artifact → {profiling_uri}")
        except Exception as exc:  # noqa: BLE001 — a side artifact must not fail a finished run
            record.add_log(f"Warning: Could not save profiling artifact: {exc}")

        record.add_log(
            "Completed — "
            f"val={timings['time_validate_sec']:.2f}s | load={timings['time_load_sec']:.2f}s | "
            f"run={timings['time_run_sec']:.2f}s | save={timings['time_save_sec']:.2f}s | "
            f"total={timings['time_total_sec']:.2f}s"
        )

        # The record JSON is written last so that it includes every log line
        # above. It cannot contain its own checksum, so that one is only in
        # the record returned to the worker.
        try:
            json_data = record.model_dump_json(indent=2)
            record.add_artifact("record_json", write_text(json_data, record_uri, content_type="application/json"))
            record.add_log(f"Saved record JSON → {record_uri}")
        except Exception as exc:  # noqa: BLE001 — a side artifact must not fail a finished run
            record.add_log(f"Warning: Could not save record JSON: {exc}")

    except Exception as exc:
        record.status = "failed"
        record.add_log(f"Failed: {exc}")
        raise
    finally:
        sys.stdout = real_stdout

    real_stdout.write(record.model_dump_json() + "\n")
    real_stdout.flush()


if __name__ == "__main__":
    main(sys.argv[1])
