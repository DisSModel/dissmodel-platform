# services/worker/schemas.py
from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


# ── Data provenance ───────────────────────────────────────────────────────────

class DataSource(BaseModel):
    """Tracks the origin and integrity of an input dataset."""

    type:       Literal["local", "s3", "http", "bdc_stac", "wcpms"] = "local"
    uri:        str = ""
    collection: str = ""   # reserved for BDC/STAC integration (post-MVP)
    version:    str = ""   # reserved for BDC collection version
    checksum:   str = ""   # sha256 — filled by executor.load()


# ── Core experiment record ────────────────────────────────────────────────────

class ExperimentRecord(BaseModel):
    """
    Immutable provenance object for a single simulation run.

    Captures everything needed to reproduce the result exactly:
    model spec snapshot, input provenance, variable mapping, and
    output checksums. Filled progressively by the runner and executor.
    """

    # Identity
    experiment_id: str      = Field(default_factory=lambda: str(uuid4()))
    created_at:    datetime = Field(default_factory=datetime.utcnow)

    # Model provenance
    model_name:    str  = ""
    model_commit:  str  = ""   # git hash of dissmodel-configs at execution time
    code_version:  str  = ""   # dissmodel PyPI tag (e.g. "0.1.5")
    resolved_spec: dict = {}   # full TOML snapshot — immutable after job starts

    # Input provenance
    source:       DataSource = Field(default_factory=DataSource)
    input_format: Literal["tiff", "vector", "auto"] = "auto"

    # Variable mapping — travels with the request, stored for reproducibility
    column_map: dict = {}   # canonical → real column name (vector input)
    band_map:   dict = {}   # canonical → real band name (raster input)

    # Execution parameters — override TOML defaults per run
    parameters: dict = {}   # resolution, n_steps, start_year, etc.

    # Results
    output_path:   str | None = None
    output_sha256: str | None = None
    metrics:       dict       = {}

    # Lifecycle
    status: Literal["pending", "running", "completed", "failed"] = "pending"
    logs:   list[str] = []

    def add_log(self, message: str) -> None:
        """Append a timestamped log entry."""
        ts = datetime.utcnow().strftime("%H:%M:%S")
        self.logs.append(f"[{ts}] {message}")


# ── API request / response ────────────────────────────────────────────────────

class JobRequest(BaseModel):
    """Payload for POST /submit_job."""

    model_name:    str
    input_dataset: str
    input_format:  Literal["tiff", "vector", "auto"] = "auto"
    parameters:    dict = {}
    column_map:    dict = {}
    band_map:      dict = {}
    priority:      Literal["low", "normal", "high"] = "normal"


class JobResponse(BaseModel):
    """Response for POST /submit_job and GET /job/{id}."""

    job_id:        str
    experiment_id: str
    status:        str
    model_name:    str
    created_at:    datetime
    output_path:   str | None = None
    output_sha256: str | None = None
    logs:          list[str]  = []


class InlineJobRequest(BaseModel):
    """
    Payload for POST /submit_job_inline.

    Accepts a raw TOML string instead of a registered model name.
    Intended for calibration and exploration in Jupyter — results
    are not reproducible via the registry and are marked as such.
    """

    input_dataset:   str
    model_spec_toml: str
    input_format:    Literal["tiff", "vector", "auto"] = "auto"
    parameters:      dict = {}
    column_map:      dict = {}
    band_map:        dict = {}