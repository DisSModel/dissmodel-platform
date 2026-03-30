# services/worker/runner.py
from __future__ import annotations

import importlib.metadata
import subprocess
import tomllib
from pathlib import Path

from worker.registry import ExecutorRegistry
from worker.schemas import ExperimentRecord, JobRequest, InlineJobRequest

CONFIGS_PATH = Path("/configs")


# ── Registry helpers ──────────────────────────────────────────────────────────

def _git_head() -> str:
    """Return current HEAD hash of the configs repo, or 'local' if git is missing."""
    try:
        # Se a pasta .git não existe, nem tenta rodar o comando
        if not (CONFIGS_PATH / ".git").exists():
            return "local-dev"

        return subprocess.check_output(
            ["git", "-C", str(CONFIGS_PATH), "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Se o comando 'git' não for encontrado ou falhar, retorna 'unknown'
        return "unknown"

def ____git_head() -> str:
    """Return current HEAD hash of the configs repo."""
    try:
        return subprocess.check_output(
            ["git", "-C", str(CONFIGS_PATH), "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except subprocess.CalledProcessError:
        return "unknown"


def _code_version() -> str:
    """Return installed dissmodel version, or 'dev' if not installed."""
    try:
        return importlib.metadata.version("dissmodel")
    except importlib.metadata.PackageNotFoundError:
        return "dev"


def _load_spec(model_name: str) -> dict:
    """
    Load and return the TOML spec for a registered model.
    Raises FileNotFoundError if the model is not in the registry.
    """
    # Import here to use the cached version from registry.py
    from worker.api_registry import load_model_spec
    return load_model_spec(model_name)


def _merge_parameters(resolved_spec: dict, overrides: dict) -> dict:
    """
    Merge TOML defaults with per-request overrides.
    Request parameters always win.
    """
    defaults = resolved_spec.get("model", {}).get("parameters", {})
    return {**defaults, **overrides}


# ── Record factory ────────────────────────────────────────────────────────────

def build_record(req: JobRequest) -> ExperimentRecord:
    """
    Build an ExperimentRecord from a JobRequest.
    Snapshots the model spec at submission time — immutable from here on.
    """
    spec = _load_spec(req.model_name)

    record = ExperimentRecord(
        model_name    = req.model_name,
        model_commit  = _git_head(),
        code_version  = _code_version(),
        resolved_spec = spec,
        input_format  = req.input_format,
        column_map    = req.column_map,
        band_map      = req.band_map,
        parameters    = _merge_parameters(spec, req.parameters),
    )

    # Resolve input URI into DataSource
    record.source.uri  = req.input_dataset
    record.source.type = _infer_source_type(req.input_dataset)

    record.add_log(f"Record created — model={req.model_name} commit={record.model_commit}")
    return record


def build_record_inline(req: InlineJobRequest) -> ExperimentRecord:
    """
    Build an ExperimentRecord from an inline TOML spec.
    Marks the record as non-reproducible via the registry.
    """
    spec = tomllib.loads(req.model_spec_toml)

    record = ExperimentRecord(
        model_name    = spec.get("model", {}).get("name", "inline"),
        model_commit  = "local-inline",   # not reproducible via registry
        code_version  = _code_version(),
        resolved_spec = spec,
        input_format  = req.input_format,
        column_map    = req.column_map,
        band_map      = req.band_map,
        parameters    = _merge_parameters(spec, req.parameters),
    )

    record.source.uri  = req.input_dataset
    record.source.type = _infer_source_type(req.input_dataset)

    record.add_log("Record created from inline spec — not reproducible via registry")
    return record


def _infer_source_type(uri: str) -> str:
    """Infer DataSource.type from URI scheme."""
    if uri.startswith("s3://"):
        return "s3"
    if uri.startswith("http://") or uri.startswith("https://"):
        return "http"
    return "local"


# ── Main runner ───────────────────────────────────────────────────────────────

def run_experiment(record: ExperimentRecord) -> ExperimentRecord:
    """
    Execute a simulation end-to-end.

    Orchestrates the executor lifecycle:
        validate → run → save

    Updates record.status throughout. On failure, sets status="failed"
    and appends the error to record.logs before re-raising.
    """
    executor_cls = _resolve_executor(record)
    executor     = executor_cls()

    try:
        # Validate before touching any data
        record.status = "running"
        record.add_log("Validating spec and input...")
        executor.validate(record)

        # Execute simulation
        record.add_log(f"Running executor={executor_cls.name}...")
        result = executor.run(record)

        # Persist results
        record.add_log("Saving output...")
        record = executor.save(result, record)

        record.add_log(f"Completed — output={record.output_path}")
        return record

    except Exception as exc:
        record.status = "failed"
        record.add_log(f"Failed: {exc}")
        raise


def reproduce_experiment(original: ExperimentRecord) -> ExperimentRecord:
    """
    Re-run an experiment from its stored snapshot.

    Uses resolved_spec directly — bypasses the current registry state.
    This guarantees the reproduction uses the exact same spec as the
    original, even if the TOML has changed since.
    """
    record = ExperimentRecord(
        model_name    = original.model_name,
        model_commit  = original.model_commit,
        code_version  = _code_version(),
        resolved_spec = original.resolved_spec,   # snapshot — not a fresh load
        input_format  = original.input_format,
        column_map    = original.column_map,
        band_map      = original.band_map,
        parameters    = original.parameters,
        source        = original.source.model_copy(),
    )

    record.add_log(f"Reproducing experiment={original.experiment_id}")
    return run_experiment(record)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _resolve_executor(record: ExperimentRecord):
    """
    Look up the executor class from the model spec.
    Raises KeyError with a clear message if not registered.
    """
    model_class = record.resolved_spec.get("model", {}).get("class")
    if not model_class:
        raise ValueError(
            f"Model spec for '{record.model_name}' is missing 'model.class'. "
            f"Check the TOML in dissmodel-configs."
        )

    # Ensure all executors are imported so __init_subclass__ has fired
    import worker.executors  # noqa: F401

    return ExecutorRegistry.get(model_class)