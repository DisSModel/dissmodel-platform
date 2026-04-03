# services/worker/runner.py
from __future__ import annotations

import importlib
import importlib.metadata
import subprocess
import sys
import tomllib
from pathlib import Path

from dissmodel.executor.registry import ExecutorRegistry
from dissmodel.executor.schemas  import ExperimentRecord, InlineJobRequest, JobRequest

CONFIGS_PATH = Path("/configs")


# ── Registry helpers ──────────────────────────────────────────────────────────

def _git_head() -> str:
    """Return current HEAD hash of the configs repo, or 'local-dev' if git is missing."""
    try:
        if not (CONFIGS_PATH / ".git").exists():
            return "local-dev"
        return subprocess.check_output(
            ["git", "-C", str(CONFIGS_PATH), "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
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
    from worker.api_registry import load_model_spec
    return load_model_spec(model_name)


def _merge_parameters(resolved_spec: dict, overrides: dict) -> dict:
    """
    Merge TOML defaults with per-request overrides.
    Request parameters always win.
    """
    defaults = resolved_spec.get("model", {}).get("parameters", {})
    return {**defaults, **overrides}


def _infer_source_type(uri: str) -> str:
    """Infer DataSource.type from URI scheme."""
    if uri.startswith("s3://"):
        return "s3"
    if uri.startswith("http://") or uri.startswith("https://"):
        return "http"
    return "local"


# ── Package installation ──────────────────────────────────────────────────────

def _ensure_package(spec: dict) -> None:
    """
    Install the model package declared in the spec before resolving executor.

    Supports three URI schemes:
      PyPI:    "coastal-dynamics==0.1.0"
      GitHub:  "git+https://github.com/org/repo@branch"
      Local:   "/opt/coastal-dynamics"  (editable install — development only)

    No-op if 'package' field is absent from the spec.
    """
    package = spec.get("model", {}).get("package")
    if not package:
        return

    if package.startswith("/"):
        # Local volume — editable install for development
        cmd = [sys.executable, "-m", "pip", "install",
               "-e", package, "--quiet"]
    else:
        # PyPI or GitHub
        cmd = [sys.executable, "-m", "pip", "install",
               package, "--quiet", "--no-cache-dir"]

    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Failed to install package '{package}': {exc}\n"
            f"Check the 'package' field in the model TOML."
        ) from exc


def _import_package(spec: dict) -> None:
    """
    Import the model package after installation so __init_subclass__
    fires and registers the executor in ExecutorRegistry.
    """
    package = spec.get("model", {}).get("package", "")
    if not package:
        return

    pkg_name = (
        package
        .split("==")[0]
        .split("@")[0]
        .rstrip("/")
        .split("/")[-1]
        .replace("-", "_")
    )

    # Try importing the package and its executor submodule
    # Registration happens in executor/__init__.py via __init_subclass__
    for module in [pkg_name, f"{pkg_name}.executor"]:
        try:
            importlib.import_module(module)
        except ImportError:
            pass


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
        model_commit  = "local-inline",
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


# ── Main runner ───────────────────────────────────────────────────────────────

def run_experiment(record: ExperimentRecord) -> ExperimentRecord:
    """
    Execute a simulation end-to-end.

    Orchestrates the executor lifecycle:
        install package → validate → run → save

    Updates record.status throughout. On failure, sets status="failed"
    and appends the error to record.logs before re-raising.
    """
    try:
        record.status = "running"

        # Install and import model package before resolving executor
        _ensure_package(record.resolved_spec)
        _import_package(record.resolved_spec)

        executor_cls = _resolve_executor(record)
        executor     = executor_cls()

        record.add_log("Validating spec and input...")
        executor.validate(record)

        record.add_log(f"Running executor={executor_cls.name}...")
        result = executor.run(record)

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
    Guarantees the reproduction uses the exact same spec as the original,
    even if the TOML has changed since.
    """
    record = ExperimentRecord(
        model_name    = original.model_name,
        model_commit  = original.model_commit,
        code_version  = _code_version(),
        resolved_spec = original.resolved_spec,
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

    # Ensure built-in executors are registered
    import worker.executors  # noqa: F401

    return ExecutorRegistry.get(model_class)