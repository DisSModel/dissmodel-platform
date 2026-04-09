# services/worker/job_runner.py
from __future__ import annotations

import importlib
import os
import sys
import time
from pathlib import Path


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
    # Redirect stdout to stderr before any import that might print
    # — keeps stdout clean for the JSON result returned to parent process
    _real_stdout = os.fdopen(os.dup(1), "w")
    sys.stdout   = sys.stderr

    from dissmodel.executor.registry import ExecutorRegistry
    from dissmodel.executor.schemas  import ExperimentRecord
    from dissmodel.io._utils         import write_text  # <-- Para salvar os artefatos

    record  = ExperimentRecord.model_validate_json(Path(record_path).read_text())
    spec    = record.resolved_spec
    package = spec.get("model", {}).get("package", "")

    if package:
        _import_executor_package(package)

    import worker.executors  # noqa: F401

    model_class  = spec.get("model", {}).get("class")
    executor_cls = ExecutorRegistry.get(model_class)
    executor     = executor_cls()

    try:
        # ── 1. Execução com Cronômetro (Feature Parity com o CLI) ─────────────
        t0 = time.perf_counter()
        executor.validate(record)
        t_val = time.perf_counter() - t0

        t0 = time.perf_counter()
        result = executor.run(record)
        t_run = time.perf_counter() - t0

        t0 = time.perf_counter()
        record = executor.save(result, record)
        t_save = time.perf_counter() - t0

        t_total = t_val + t_run + t_save

        # ── 2. Alimentando o dicionário nativo 'metrics' ──────────────────────
        record.metrics["time_validate_sec"] = round(t_val, 3)
        record.metrics["time_run_sec"]      = round(t_run, 3)
        record.metrics["time_save_sec"]     = round(t_save, 3)
        record.metrics["time_total_sec"]    = round(t_total, 3)

        # ── 3. Criando os Artefatos de Profiling e Record ─────────────────────
        md_report = (
            f"# Profiling Report: {getattr(executor_cls, 'name', 'Model')}\n\n"
            f"**Experiment ID:** `{record.experiment_id}`\n"
            f"**Date:** `{record.created_at.isoformat()}`\n"
            f"**Execution Node:** `Cloud Worker`\n\n"
            "## Execution Times\n\n"
            "| Phase | Time (seconds) | % of Total |\n"
            "|---|---|---|\n"
            f"| **Validate** | {t_val:.3f} | {(t_val/t_total)*100:.1f}% |\n"
            f"| **Run** | {t_run:.3f} | {(t_run/t_total)*100:.1f}% |\n"
            f"| **Save** | {t_save:.3f} | {(t_save/t_total)*100:.1f}% |\n"
            f"| **Total** | **{t_total:.3f}** | **100%** |\n"
        )

        # Resolução de diretório compatível com s3://
        if record.output_path:
            base_dir = record.output_path.rsplit("/", 1)[0]
        else:
            base_dir = "."

        profiling_uri = f"{base_dir}/profiling_{record.experiment_id[:8]}.md"
        record_uri    = f"{base_dir}/{record.experiment_id[:8]}.record.json"
        
        # Grava o Markdown na S3
        try:
            chk_md = write_text(md_report, profiling_uri, content_type="text/markdown")
            record.add_artifact("profiling", chk_md)
            record.add_log(f"Saved profiling artifact → {profiling_uri}")
        except Exception as e:
            record.add_log(f"Warning: Could not save profiling artifact: {e}")

        # Grava o JSON do Record diretamente na S3
        try:
            json_data = record.model_dump_json(indent=2)
            chk_json = write_text(json_data, record_uri, content_type="application/json")
            record.add_artifact("record_json", chk_json)
            record.add_log(f"Saved record JSON → {record_uri}")
        except Exception as e:
            record.add_log(f"Warning: Could not save record JSON: {e}")

        # Log final padronizado
        record.add_log(
            f"Completed — val={t_val:.2f}s | run={t_run:.2f}s | save={t_save:.2f}s | total={t_total:.2f}s"
        )

    except Exception as exc:
        record.status = "failed"
        record.add_log(f"Failed: {exc}")
        raise
    finally:
        # Always restore and write result — even on failure the parent needs the record
        sys.stdout = _real_stdout

    _real_stdout.write(record.model_dump_json() + "\n")
    _real_stdout.flush()


if __name__ == "__main__":
    main(sys.argv[1])