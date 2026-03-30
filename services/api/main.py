# services/api/main.py
from __future__ import annotations

import io
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import redis
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from minio import Minio

from worker.api_registry import list_models, load_model_spec, start_sync_scheduler, sync_configs
from worker.runner import build_record, build_record_inline, reproduce_experiment, run_experiment
from worker.schemas import ExperimentRecord, InlineJobRequest, JobRequest, JobResponse

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(level=os.getenv("DISSMODEL_LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# ── Infrastructure clients ────────────────────────────────────────────────────

redis_client = redis.Redis(
    host             = os.getenv("REDIS_HOST", "redis"),
    port             = int(os.getenv("REDIS_PORT", 6379)),
    decode_responses = True,
)

minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "minio:9000"),
    access_key = os.getenv("MINIO_ACCESS_KEY", "inpe_admin"),
    secret_key = os.getenv("MINIO_SECRET_KEY", "inpe_secret_2024"),
    secure     = False,
)

# ── Auth ──────────────────────────────────────────────────────────────────────

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=True)
_VALID_KEYS     = set(os.getenv("API_KEYS", "dev-key").split(","))


async def require_api_key(key: str = Depends(_API_KEY_HEADER)) -> str:
    if key not in _VALID_KEYS:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return key


# ── App lifecycle ─────────────────────────────────────────────────────────────

_scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler

    # Ensure MinIO buckets exist before accepting requests
    _init_buckets()

    # Start background sync of dissmodel-configs
    #_scheduler = start_sync_scheduler(
    #    interval_seconds=int(os.getenv("CONFIGS_SYNC_INTERVAL", 900))
    #)
    #logger.info("Config sync scheduler started")

    yield

    if _scheduler:
        _scheduler.shutdown()


def _init_buckets() -> None:
    for bucket in ("dissmodel-inputs", "dissmodel-outputs"):
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            logger.info(f"Bucket created: {bucket}")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "DisSModel Platform API",
    description = "Submission, tracking and reproduction of LUCC simulations",
    version     = "0.1.0",
    lifespan    = lifespan,
)

AUTH = [Depends(require_api_key)]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _store_record(record: ExperimentRecord) -> None:
    redis_client.set(f"experiment:{record.experiment_id}", record.model_dump_json())


def _load_record(experiment_id: str) -> ExperimentRecord:
    raw = redis_client.get(f"experiment:{experiment_id}")
    if not raw:
        raise HTTPException(status_code=404, detail=f"Experiment '{experiment_id}' not found")
    return ExperimentRecord.model_validate_json(raw)


def _to_response(record: ExperimentRecord) -> JobResponse:
    return JobResponse(
        job_id        = record.experiment_id,
        experiment_id = record.experiment_id,
        status        = record.status,
        model_name    = record.model_name,
        created_at    = record.created_at,
        output_path   = record.output_path,
        output_sha256 = record.output_sha256,
        logs          = record.logs,
    )


def _enqueue(record: ExperimentRecord, priority: str = "normal") -> None:
    """Push experiment_id to the Redis priority queue."""
    _store_record(record)
    redis_client.lpush(f"queue:{priority}", record.experiment_id)
    logger.info(f"Enqueued experiment={record.experiment_id} model={record.model_name}")


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    try:
        redis_client.ping()
        minio_client.bucket_exists("dissmodel-outputs")
        return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {exc}")


@app.get("/")
async def root():
    return {"service": "DisSModel Platform API", "version": "0.1.0", "docs": "/docs"}


# ── Models registry ───────────────────────────────────────────────────────────

@app.get("/models", dependencies=AUTH)
async def get_models():
    """List all registered models from dissmodel-configs."""
    return {"models": list_models()}


@app.get("/models/{model_name}", dependencies=AUTH)
async def get_model(model_name: str):
    """Return the full TOML spec for a registered model."""
    try:
        return {"model_name": model_name, "spec": load_model_spec(model_name)}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# ── Job submission ─────────────────────────────────────────────────────────────

@app.post("/submit_job", response_model=JobResponse, dependencies=AUTH)
async def submit_job(req: JobRequest):
    """Submit a job using a registered model."""
    try:
        record = build_record(req)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    _enqueue(record, priority=req.priority)
    return _to_response(record)


@app.post("/submit_job_inline", response_model=JobResponse, dependencies=AUTH)
async def submit_job_inline(req: InlineJobRequest):
    """
    Submit a job with an inline TOML spec.
    Result is not reproducible via the registry — for exploration only.
    """
    record = build_record_inline(req)
    _enqueue(record)
    return _to_response(record)


# ── Job status ────────────────────────────────────────────────────────────────

@app.get("/job/{experiment_id}", response_model=JobResponse, dependencies=AUTH)
async def get_job(experiment_id: str):
    """Return current status and provenance for an experiment."""
    return _to_response(_load_record(experiment_id))


@app.get("/jobs", dependencies=AUTH)
async def list_jobs(limit: int = 100, status: Optional[str] = None):
    """List experiments, optionally filtered by status."""
    records = []
    for key in redis_client.scan_iter("experiment:*"):
        raw = redis_client.get(key)
        if raw:
            record = ExperimentRecord.model_validate_json(raw)
            if status is None or record.status == status:
                records.append(_to_response(record).model_dump())

    records.sort(key=lambda r: r["created_at"], reverse=True)
    return {"experiments": records[:limit], "total": len(records)}


@app.delete("/job/{experiment_id}", dependencies=AUTH)
async def cancel_job(experiment_id: str):
    """Cancel a queued experiment."""
    record = _load_record(experiment_id)
    if record.status in ("completed", "failed"):
        raise HTTPException(status_code=400, detail="Cannot cancel a completed or failed experiment")
    record.status = "cancelled"
    record.add_log("Cancelled via API")
    _store_record(record)
    return {"message": "Experiment cancelled", "experiment_id": experiment_id}


# ── Reproduce & publish ───────────────────────────────────────────────────────

@app.post("/experiments/{experiment_id}/reproduce", response_model=JobResponse, dependencies=AUTH)
async def reproduce(experiment_id: str):
    """
    Re-run an experiment from its stored snapshot.
    Uses the original resolved_spec — independent of current registry state.
    """
    original = _load_record(experiment_id)
    new_record = ExperimentRecord(
        model_name    = original.model_name,
        model_commit  = original.model_commit,
        code_version  = original.code_version,
        resolved_spec = original.resolved_spec,
        source        = original.source.model_copy(),
        input_format  = original.input_format,
        column_map    = original.column_map,
        band_map      = original.band_map,
        parameters    = original.parameters,
    )
    new_record.add_log(f"Reproducing experiment={experiment_id}")
    _enqueue(new_record)
    return _to_response(new_record)


@app.post("/experiments/{experiment_id}/publish", dependencies=AUTH)
async def publish(experiment_id: str):
    """
    Export a reproducibility package (spec + record + output path).
    Full Zenodo deposit is post-MVP — returns the package as JSON for now.
    """
    record = _load_record(experiment_id)
    if record.status != "completed":
        raise HTTPException(status_code=400, detail="Only completed experiments can be published")

    return {
        "experiment_id": record.experiment_id,
        "model_name":    record.model_name,
        "model_commit":  record.model_commit,
        "code_version":  record.code_version,
        "input":         record.source.model_dump(),
        "column_map":    record.column_map,
        "band_map":      record.band_map,
        "parameters":    record.parameters,
        "resolved_spec": record.resolved_spec,
        "output_path":   record.output_path,
        "output_sha256": record.output_sha256,
        "logs":          record.logs,
        "note":          "Zenodo deposit not yet implemented — save this JSON for reproducibility",
    }


# ── Data upload ───────────────────────────────────────────────────────────────

@app.post("/data/upload", dependencies=AUTH)
async def upload_dataset(
    file:  UploadFile = File(...),
    label: str        = Form(...),
):
    """Upload a dataset to MinIO. For large files, use mc directly."""
    import hashlib

    content  = await file.read()
    checksum = hashlib.sha256(content).hexdigest()
    path     = f"inputs/{label}/{file.filename}"

    minio_client.put_object(
        bucket_name  = "dissmodel-inputs",
        object_name  = path,
        data         = io.BytesIO(content),
        length       = len(content),
        content_type = file.content_type or "application/octet-stream",
    )

    logger.info(f"Uploaded {path} ({len(content)/1e6:.1f} MB)")

    return {
        "uri":      f"s3://dissmodel-inputs/{path}",
        "label":    label,
        "checksum": checksum,
        "size_mb":  round(len(content) / 1e6, 2),
    }


# ── Admin ─────────────────────────────────────────────────────────────────────

@app.post("/admin/sync", dependencies=AUTH)
async def admin_sync():
    """Force an immediate git pull of dissmodel-configs."""
    try:
        output = sync_configs()
        return {"message": "Sync completed", "git_output": output}
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Error handlers ────────────────────────────────────────────────────────────

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)