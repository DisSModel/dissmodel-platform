# services/api/main.py
from __future__ import annotations

import hashlib
import hmac
import io
import logging
import os
import tomllib
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated
from urllib.parse import quote, urlencode, urlparse

import redis
from dissmodel.executor.schemas import (
    ExperimentRecord,
    InlineJobRequest,
    JobRequest,
    JobResponse,
)
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from worker.api_registry import list_models, load_model_spec, sync_configs
from worker.runner import build_record, build_record_inline, build_reproduction
from worker.storage import ensure_buckets, minio_client

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(level=os.getenv("DISSMODEL_LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# ── Infrastructure clients ────────────────────────────────────────────────────

redis_client = redis.Redis(
    host             = os.getenv("REDIS_HOST", "redis"),
    port             = int(os.getenv("REDIS_PORT", "6379")),
    password         = os.getenv("REDIS_PASSWORD") or None,
    decode_responses = True,
)

QUEUES = ("queue:high", "queue:normal", "queue:low")

# ── Auth ──────────────────────────────────────────────────────────────────────

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=True)
_VALID_KEYS     = set(os.getenv("API_KEYS", "dev-key").split(","))


async def require_api_key(key: str = Depends(_API_KEY_HEADER)) -> str:
    if key not in _VALID_KEYS:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return key


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure MinIO buckets exist before accepting requests. The configs
    # repository is updated by the configs-sync sidecar, not by the API.
    ensure_buckets()
    yield


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
        input_sha256  = record.source.checksum or None,
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
        return {"status": "healthy", "timestamp": datetime.now(UTC).isoformat()}
    except Exception as exc:  # noqa: BLE001 — any failure means unhealthy
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
    try:
        record = build_record_inline(req)
    except tomllib.TOMLDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid TOML in model_spec_toml: {exc}") from exc
    _enqueue(record)
    return _to_response(record)


# ── Job status ────────────────────────────────────────────────────────────────

@app.get("/job/{experiment_id}", response_model=JobResponse, dependencies=AUTH)
async def get_job(experiment_id: str):
    """Return current status and provenance for an experiment."""
    return _to_response(_load_record(experiment_id))


@app.get("/jobs", dependencies=AUTH)
async def list_jobs(limit: int = 100, status: str | None = None):
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
    for queue in QUEUES:
        redis_client.lrem(queue, 0, experiment_id)
    record.status = "cancelled"
    record.add_log("Cancelled via API")
    _store_record(record)
    return {"message": "Experiment cancelled", "experiment_id": experiment_id}


# ── Reproduce & publish ───────────────────────────────────────────────────────

@app.post("/experiments/{experiment_id}/reproduce", response_model=JobResponse, dependencies=AUTH)
async def reproduce(experiment_id: str):
    """
    Re-run an experiment from its stored snapshot.

    Uses the original resolved_spec, input and parameters — independent
    of the current registry state — and records the dissmodel version
    installed now. When the run ends, the worker compares its output with
    the original and stores the result in ``metrics['reproduction']``
    (see GET /experiments/{id}).
    """
    original   = _load_record(experiment_id)
    new_record = build_reproduction(original)
    _enqueue(new_record)
    return _to_response(new_record)


@app.get("/experiments/{experiment_id}", dependencies=AUTH)
async def get_experiment(experiment_id: str):
    """Full ExperimentRecord: provenance, artifacts, metrics and logs."""
    record = _load_record(experiment_id)
    return {**record.model_dump(mode="json"), "output_sha256": record.output_sha256}


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
        "period":        record.period,
        "output_path":   record.output_path,
        "output_sha256": record.output_sha256,
        "artifacts":     record.artifacts,
        "metrics":       record.metrics,
        "logs":          record.logs,
        "note":          "Zenodo deposit not yet implemented — save this JSON for reproducibility",
    }


# ── Data upload ───────────────────────────────────────────────────────────────

@app.post("/data/upload", dependencies=AUTH)
async def upload_dataset(
    file:  Annotated[UploadFile, File()],
    label: Annotated[str, Form()],
):
    """Upload a dataset to MinIO. For large files, use mc directly."""
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


def _presign_url(bucket: str, key: str, expires_seconds: int = 3600) -> str:
    """Gera presigned URL sem conexão de rede — cálculo local puro."""
    server_url = os.getenv("MINIO_URL", "http://localhost:19000").rstrip("/")
    access_key = os.getenv("MINIO_ACCESS_KEY", "inpe_admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "inpe_secret_2024")

    now        = datetime.now(UTC)
    date_stamp = now.strftime("%Y%m%d")
    amz_date   = now.strftime("%Y%m%dT%H%M%SZ")
    region     = "us-east-1"
    service    = "s3"

    credential = f"{access_key}/{date_stamp}/{region}/{service}/aws4_request"

    params = {
        "X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
        "X-Amz-Credential":    credential,
        "X-Amz-Date":          amz_date,
        "X-Amz-Expires":       str(expires_seconds),
        "X-Amz-SignedHeaders": "host",
    }

    host        = urlparse(server_url).netloc
    canonical   = f"GET\n/{bucket}/{quote(key, safe='/')}\n{urlencode(sorted(params.items()))}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD"
    string2sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{date_stamp}/{region}/{service}/aws4_request\n{hashlib.sha256(canonical.encode()).hexdigest()}"

    def _sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    signing_key = _sign(
        _sign(_sign(_sign(f"AWS4{secret_key}".encode(), date_stamp), region), service),
        "aws4_request",
    )
    signature = hmac.new(signing_key, string2sign.encode(), hashlib.sha256).hexdigest()

    params["X-Amz-Signature"] = signature
    return f"{server_url}/{bucket}/{quote(key, safe='/')  }?{urlencode(sorted(params.items()))}"

@app.get("/download", dependencies=AUTH)
async def get_presigned_url(uri: str, expires_hours: int = 1):
    if not uri.startswith("s3://"):
        raise HTTPException(status_code=400, detail="URI deve começar com s3://")

    clean = uri.removeprefix("s3://")
    if "/" not in clean:
        raise HTTPException(status_code=400, detail="Formato inválido — esperado s3://bucket/objeto")

    bucket, key = clean.split("/", 1)

    try:
        url = _presign_url(bucket, key, expires_seconds=expires_hours * 3600)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {"url": url, "expires_in_hours": expires_hours}

# ── Admin ─────────────────────────────────────────────────────────────────────

@app.post("/admin/sync", dependencies=AUTH)
async def admin_sync():
    """
    Try a ``git pull`` of dissmodel-configs and report the commit new jobs will record.

    In the Docker stack the checkout is read-only here and is updated by
    the configs-sync sidecar; specs are read from disk on every request,
    so a pull by the sidecar takes effect immediately.
    """
    return sync_configs()


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
    uvicorn.run(app, host="0.0.0.0", port=8000)  # nosec B104