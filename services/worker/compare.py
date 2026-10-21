# services/worker/compare.py
"""
Compare the output of a reproduction with the output of the original run.

Two levels, as in the Open LUCC workflow:

- ``file_identical``: the checksums the executors stored in
  ``artifacts['output']`` are equal. This is the strongest claim, but it
  can fail with identical data, because GeoTIFF compression and headers
  depend on the GDAL build (the case seen on macOS ARM).
- ``values_identical``: for GeoTIFF outputs, a SHA-256 over the decoded
  bands (shape, dtype and raw array bytes) is equal. This is the check
  that holds across machines. ``None`` when it could not be computed
  (not a GeoTIFF, rasterio missing, or an output that cannot be read).

Neither level is a tolerance: values either match bit for bit or not.
"""
from __future__ import annotations

import hashlib
import os
import tempfile

from dissmodel.executor.schemas import ExperimentRecord

GEOTIFF_SUFFIXES = (".tif", ".tiff")


def decoded_values_sha256(path: str) -> str:
    """SHA-256 over every band of a raster: shape, dtype and values."""
    import rasterio

    h = hashlib.sha256()
    with rasterio.open(path) as src:
        h.update(f"{src.count}x{src.height}x{src.width}".encode())
        for band in range(1, src.count + 1):
            data = src.read(band)
            h.update(str(data.dtype).encode())
            h.update(data.tobytes())
    return h.hexdigest()


def _local_copy(uri: str, workdir: str) -> str:
    """Path to a readable local copy of ``uri`` (downloaded when it is s3:// or http)."""
    if not uri.startswith(("s3://", "http://", "https://")):
        return uri
    from worker.storage import download_to_file

    dest = os.path.join(workdir, f"{len(os.listdir(workdir))}_{os.path.basename(uri)}")
    return download_to_file(uri, dest)


def compare_outputs(original: ExperimentRecord, reproduction: ExperimentRecord) -> dict:
    """Result stored in ``reproduction.metrics['reproduction']``."""
    result: dict = {
        "of":               original.experiment_id,
        "original_output":  original.output_path,
        "file_identical":   None,
        "values_identical": None,
    }

    a = original.artifacts.get("output")
    b = reproduction.artifacts.get("output")
    if a and b:
        result["file_identical"] = a == b

    paths = (original.output_path or "", reproduction.output_path or "")
    if all(p.lower().endswith(GEOTIFF_SUFFIXES) for p in paths):
        try:
            with tempfile.TemporaryDirectory() as workdir:
                hashes = [decoded_values_sha256(_local_copy(p, workdir)) for p in paths]
            result["values_identical"] = hashes[0] == hashes[1]
            result["values_sha256"] = hashes[1]
        except Exception as exc:  # noqa: BLE001 — the comparison is reported, never fatal
            result["values_error"] = str(exc)

    return result


def summary(result: dict) -> str:
    def word(v):
        return {True: "identical", False: "DIFFERS", None: "not checked"}[v]

    return (
        f"Reproduction of {result['of']}: file {word(result['file_identical'])}, "
        f"decoded values {word(result['values_identical'])}"
    )
