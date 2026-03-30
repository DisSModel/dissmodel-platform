# services/worker/storage.py
from __future__ import annotations

import hashlib
import io
import os

from minio import Minio
from minio.error import S3Error

# ── Client ────────────────────────────────────────────────────────────────────

minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "minio:9000"),
    access_key = os.getenv("MINIO_ACCESS_KEY", "inpe_admin"),
    secret_key = os.getenv("MINIO_SECRET_KEY", "inpe_secret_2024"),
    secure     = bool(os.getenv("MINIO_SECURE", "")),
)

BUCKET_INPUTS  = "dissmodel-inputs"
BUCKET_OUTPUTS = "dissmodel-outputs"


# ── Download ──────────────────────────────────────────────────────────────────

def download_to_file(uri: str, dest: str) -> str:
    """
    Download an s3:// or http(s):// URI to a local path.
    Returns dest. Passes through local paths unchanged.
    """
    if uri.startswith("s3://"):
        bucket, key = _parse_s3(uri)
        minio_client.fget_object(bucket, key, dest)
        return dest

    if uri.startswith("http://") or uri.startswith("https://"):
        import urllib.request
        urllib.request.urlretrieve(uri, dest)
        return dest

    return uri   # local path — return as-is


def download_to_bytes(uri: str) -> bytes:
    """Download an s3:// URI and return raw bytes."""
    bucket, key = _parse_s3(uri)
    obj = minio_client.get_object(bucket, key)
    return obj.read()


# ── Upload ────────────────────────────────────────────────────────────────────

def upload_bytes(data: bytes, object_path: str, content_type: str = "application/octet-stream") -> str:
    """
    Upload bytes to BUCKET_OUTPUTS.
    Returns the s3:// URI of the uploaded object.
    """
    minio_client.put_object(
        bucket_name  = BUCKET_OUTPUTS,
        object_name  = object_path,
        data         = io.BytesIO(data),
        length       = len(data),
        content_type = content_type,
    )
    return f"s3://{BUCKET_OUTPUTS}/{object_path}"


def upload_file(local_path: str, object_path: str, content_type: str = "application/octet-stream") -> str:
    """
    Upload a local file to BUCKET_OUTPUTS.
    Returns the s3:// URI of the uploaded object.
    """
    minio_client.fput_object(
        bucket_name  = BUCKET_OUTPUTS,
        object_name  = object_path,
        file_path    = local_path,
        content_type = content_type,
    )
    return f"s3://{BUCKET_OUTPUTS}/{object_path}"


# ── Buckets ───────────────────────────────────────────────────────────────────

def ensure_buckets() -> None:
    """Create input and output buckets if they do not exist."""
    for bucket in (BUCKET_INPUTS, BUCKET_OUTPUTS):
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)


# ── Helpers ───────────────────────────────────────────────────────────────────

def sha256_file(path: str) -> str:
    """Return sha256 hex digest of a local file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    """Return sha256 hex digest of bytes."""
    return hashlib.sha256(data).hexdigest()


def _parse_s3(uri: str) -> tuple[str, str]:
    """Parse s3://bucket/key into (bucket, key)."""
    parts = uri[5:].split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid s3 URI: {uri}")
    return parts