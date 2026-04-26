# services/worker/job_runner.py
from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path


def main(record_path: str) -> None:
    _real_stdout = os.fdopen(os.dup(1), "w")
    sys.stdout   = sys.stderr

    from dissmodel.executor.runner   import execute_lifecycle
    from dissmodel.executor.registry import ExecutorRegistry
    from dissmodel.executor.schemas  import ExperimentRecord
    from dissmodel.io._utils         import write_text

    record  = ExperimentRecord.model_validate_json(Path(record_path).read_text())
    spec    = record.resolved_spec
    package = spec.get("model", {}).get("package", "")
    module  = spec.get("model", {}).get("executor_module", "")  # ← novo

    if package:
        import subprocess
        subprocess.run(
            [sys.executable, "-m", "pip", "install", package],
            check=True, capture_output=True,
        )

    if module:
        importlib.import_module(module)                          # ← novo, sem try/except
    
    import worker.executors  # noqa: F401

    model_class  = spec.get("model", {}).get("class")
    executor_cls = ExecutorRegistry.get(model_class)
    executor     = executor_cls()

    try:
        record, timings = execute_lifecycle(executor, record)

        t_val   = timings["time_validate_sec"]
        t_load  = timings["time_load_sec"]
        t_run   = timings["time_run_sec"]
        t_save  = timings["time_save_sec"]
        t_total = timings["time_total_sec"]

        md_report = (
            f"# Profiling Report: {getattr(executor_cls, 'name', 'Model')}\n\n"
            f"**Experiment ID:** `{record.experiment_id}`\n"
            f"**Date:** `{record.created_at.isoformat()}`\n"
            f"**Execution Node:** `Cloud Worker`\n\n"
            "## Execution Times\n\n"
            "| Phase | Time (seconds) | % of Total |\n"
            "|---|---|---|\n"
            f"| **Validate** | {t_val:.3f}   | {(t_val   / t_total) * 100:.1f}% |\n"
            f"| **Load**     | {t_load:.3f}  | {(t_load  / t_total) * 100:.1f}% |\n"
            f"| **Run**      | {t_run:.3f}   | {(t_run   / t_total) * 100:.1f}% |\n"
            f"| **Save**     | {t_save:.3f}  | {(t_save  / t_total) * 100:.1f}% |\n"
            f"| **Total**    | **{t_total:.3f}** | **100%** |\n"
        )

        if record.output_path:
            base_dir = record.output_path.rsplit("/", 1)[0]
        else:
            base_dir = "."

        profiling_uri = f"{base_dir}/profiling_{record.experiment_id[:8]}.md"
        record_uri    = f"{base_dir}/{record.experiment_id[:8]}.record.json"

        try:
            chk_md = write_text(md_report, profiling_uri, content_type="text/markdown")
            record.add_artifact("profiling", chk_md)
            record.add_log(f"Saved profiling artifact → {profiling_uri}")
        except Exception as e:
            record.add_log(f"Warning: Could not save profiling artifact: {e}")

        try:
            json_data = record.model_dump_json(indent=2)
            chk_json  = write_text(json_data, record_uri, content_type="application/json")
            record.add_artifact("record_json", chk_json)
            record.add_log(f"Saved record JSON → {record_uri}")
        except Exception as e:
            record.add_log(f"Warning: Could not save record JSON: {e}")

        record.add_log(
            f"Completed — val={t_val:.2f}s | load={t_load:.2f}s | "
            f"run={t_run:.2f}s | save={t_save:.2f}s | total={t_total:.2f}s"
        )

    except Exception as exc:
        record.status = "failed"
        record.add_log(f"Failed: {exc}")
        raise
    finally:
        sys.stdout = _real_stdout

    _real_stdout.write(record.model_dump_json() + "\n")
    _real_stdout.flush()


if __name__ == "__main__":
    main(sys.argv[1])