# services/worker/runner.py
"""
Build ExperimentRecords from API requests and run them in a subprocess.

Every record gets an ``output_path`` under ``DISSMODEL_OUTPUT_ROOT``
(``s3://dissmodel-outputs/experiments`` by default), one folder per
experiment. Without it, executors fall back to a file name relative to
the worker's working directory, which lives inside the container and is
lost on restart.
"""
from __future__ import annotations

import importlib.metadata
import logging
import os
import subprocess
import sys
import tempfile
import tomllib

from dissmodel.executor.schemas import ExperimentRecord, InlineJobRequest, JobRequest

from worker.api_registry import configs_head, load_model_spec

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_ROOT = "s3://dissmodel-outputs/experiments"

# Key in record.metrics that links a reproduction to the experiment it repeats.
REPRODUCES_KEY = "reproduces"


# ── Provenance helpers ────────────────────────────────────────────────────────

def _code_version() -> str:
    """Installed dissmodel version, or 'dev' if not installed."""
    try:
        return importlib.metadata.version("dissmodel")
    except importlib.metadata.PackageNotFoundError:
        return "dev"


def output_root() -> str:
    return os.getenv("DISSMODEL_OUTPUT_ROOT", DEFAULT_OUTPUT_ROOT).rstrip("/")


def default_output_path(record: ExperimentRecord) -> str:
    """
    ``<root>/<experiment_id>/output<suffix>``.

    The suffix comes from ``model.output_suffix`` in the spec
    (default ``.tif``, which is what the raster executors write).
    """
    suffix = record.resolved_spec.get("model", {}).get("output_suffix", ".tif")
    return f"{output_root()}/{record.experiment_id}/output{suffix}"


# Keys of [model] that describe the registration, not the run. The same set
# as dissmodel.executor.cli._SPEC_METADATA_KEYS (checked by a test), plus
# the platform's own output_suffix.
SPEC_METADATA_KEYS = frozenset({
    "executor_module", "name", "class", "description", "package", "dissmodel",
    "output_suffix",
})


def _merge_parameters(resolved_spec: dict, overrides: dict) -> dict:
    """
    ``record.parameters`` for a run, in increasing precedence:

    1. every key of ``[model]`` that is not registration metadata or
       ``[model.parameters]`` (e.g. ``land_use_types``, ``[[model.potential]]``,
       ``[model.static]``);
    2. ``[model.parameters]``;
    3. the request's ``parameters``.

    This is what ``dissmodel``'s CLI does with ``--toml`` (``_load_toml``)
    and what ``ModelExecutor.run()`` expects. Taking only layer 2, as
    v0.1.0 did, left the DisSLUCC executors without ``land_use_types``
    and the coefficients, and they failed ``validate()``.
    """
    model = dict(resolved_spec.get("model", {}))

    # land_use_types may be a table ([model.land_use_types] types = [...]).
    land_use_types = model.get("land_use_types")
    if isinstance(land_use_types, dict):
        model["land_use_types"] = land_use_types.get("types", [])

    spec_extra = {k: v for k, v in model.items() if k not in SPEC_METADATA_KEYS and k != "parameters"}
    return {**spec_extra, **model.get("parameters", {}), **overrides}


def _dissmodel_requirement_warning(spec: dict) -> str | None:
    """Warning when the installed dissmodel does not satisfy ``model.dissmodel``."""
    required = spec.get("model", {}).get("dissmodel")
    if not required:
        return None
    from packaging.specifiers import InvalidSpecifier, SpecifierSet

    try:
        ok = _code_version() in SpecifierSet(required)
    except InvalidSpecifier:
        return f"Warning: model.dissmodel '{required}' is not a valid version specifier"
    if ok:
        return None
    return f"Warning: the model requires dissmodel {required}; {_code_version()} is installed"


def _infer_source_type(uri: str) -> str:
    """Infer DataSource.type from the URI scheme."""
    if uri.startswith("s3://"):
        return "s3"
    if uri.startswith(("http://", "https://")):
        return "http"
    return "local"


def _package_is_pinned(package: str) -> bool:
    """
    True when the package spec names one exact version.

    ``name==1.2.3`` and ``git+https://...@<40-hex commit>`` are pinned;
    a branch, a tag-less git URL, a range or a local path are not, and
    the record then cannot say which code ran.
    """
    if "==" in package:
        return True
    if package.startswith("git+") and "@" in package:
        ref = package.rsplit("@", 1)[1]
        return len(ref) == 40 and all(c in "0123456789abcdef" for c in ref.lower())
    return False


# ── Package installation ──────────────────────────────────────────────────────

def _ensure_package(spec: dict) -> None:
    """
    Install the model package declared in the spec (``model.package``).

    Supports ``name==1.2.3`` (PyPI), ``git+https://github.com/org/repo@<commit>``
    and a local path (editable install, development only). No-op if the
    field is absent.
    """
    package = spec.get("model", {}).get("package")
    if not package:
        return

    logger.info(f"Installing package: {package}")

    if package.startswith("/"):
        cmd = [sys.executable, "-m", "pip", "install", "-e", package, "--quiet"]
    else:
        cmd = [sys.executable, "-m", "pip", "install", package, "--quiet", "--no-cache-dir"]

    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Failed to install package '{package}': {exc}\n"
            f"Check the 'package' field in the model TOML."
        ) from exc


# ── Record factories ──────────────────────────────────────────────────────────

def _finish_record(record: ExperimentRecord, input_dataset: str) -> ExperimentRecord:
    record.source.uri  = input_dataset
    record.source.type = _infer_source_type(input_dataset)
    record.output_path = default_output_path(record)

    package = record.resolved_spec.get("model", {}).get("package")
    if package and not _package_is_pinned(package):
        record.add_log(
            f"Warning: model.package '{package}' is not pinned to one version; "
            f"the installed version is recorded in metrics['environment']"
        )
    warning = _dissmodel_requirement_warning(record.resolved_spec)
    if warning:
        record.add_log(warning)
    return record


def build_record(req: JobRequest) -> ExperimentRecord:
    """
    Build an ExperimentRecord from a JobRequest.
    Snapshots the model spec at submission time — immutable from here on.
    """
    spec = load_model_spec(req.model_name)

    record = ExperimentRecord(
        model_name    = req.model_name,
        model_commit  = configs_head(),
        code_version  = _code_version(),
        resolved_spec = spec,
        input_format  = req.input_format,
        column_map    = req.column_map,
        band_map      = req.band_map,
        parameters    = _merge_parameters(spec, req.parameters),
    )
    _finish_record(record, req.input_dataset)
    record.add_log(f"Record created — model={req.model_name} commit={record.model_commit}")
    return record


def build_record_inline(req: InlineJobRequest) -> ExperimentRecord:
    """
    Build an ExperimentRecord from an inline TOML spec.
    Marked as not reproducible via the registry (``model_commit='local-inline'``).
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
    _finish_record(record, req.input_dataset)
    record.add_log("Record created from inline spec — not reproducible via registry")
    return record


def build_reproduction(original: ExperimentRecord) -> ExperimentRecord:
    """
    New record that repeats ``original`` from its stored snapshot.

    Uses the original ``resolved_spec``, input, mappings and parameters,
    so it does not depend on the current state of the registry. The
    ``code_version`` is the one installed now, because that is the code
    that will run; a reproduction under a different version is still
    useful, but the record must say so. ``metrics['reproduces']`` links
    back to the original; the worker compares the outputs when the run
    ends (``metrics['reproduction']``).
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
        period        = original.period,
        source        = original.source.model_copy(),
    )
    record.output_path = default_output_path(record)
    record.metrics[REPRODUCES_KEY] = original.experiment_id
    record.add_log(f"Reproducing experiment={original.experiment_id}")
    if record.code_version != original.code_version:
        record.add_log(
            f"Warning: dissmodel {record.code_version} installed; "
            f"the original ran with {original.code_version}"
        )
    return record


# ── Execution ─────────────────────────────────────────────────────────────────

def run_experiment(record: ExperimentRecord) -> ExperimentRecord:
    """
    Execute a simulation end-to-end in a subprocess.

    The package declared in the spec is installed here, in the parent
    process, so the subprocess starts with it importable and registers
    the executor on import.
    """
    record.status = "running"
    record.add_log("Dispatching to subprocess...")

    _ensure_package(record.resolved_spec)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(record.model_dump_json())
        record_path = f.name

    try:
        result = subprocess.run(
            [sys.executable, "-m", "worker.job_runner", record_path],
            capture_output=True, text=True, check=False,
        )

        if result.returncode != 0:
            record.status = "failed"
            record.add_log(f"Subprocess failed:\n{result.stderr}")
            logger.error(f"job_runner stderr:\n{result.stderr}")
            raise RuntimeError(result.stderr)

        return ExperimentRecord.model_validate_json(result.stdout)

    finally:
        os.unlink(record_path)


def reproduce_experiment(original: ExperimentRecord) -> ExperimentRecord:
    """Re-run an experiment from its stored snapshot (synchronously)."""
    return run_experiment(build_reproduction(original))
