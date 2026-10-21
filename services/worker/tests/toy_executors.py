"""Small executors used by the worker tests (registered on import)."""
from __future__ import annotations

import os

import numpy as np
from dissmodel.executor import ModelExecutor
from dissmodel.io._utils import write_bytes


class ToyBytesExecutor(ModelExecutor):
    """Finishes in well under a millisecond and writes raw bytes."""

    name = "toy_bytes"

    def load(self, record):
        return np.arange(10, dtype="int32")

    def run(self, data, record):
        return data * int(record.parameters.get("k", 1))

    def save(self, result, record):
        uri = record.output_path or f"output_{record.experiment_id[:8]}.bin"
        record.artifacts["output"] = write_bytes(result.tobytes(), uri)
        record.output_path = uri
        record.status = "completed"
        return record


class ToyTiffExecutor(ModelExecutor):
    """
    Writes a small GeoTIFF. TOY_COMPRESS changes only the file encoding
    (as a different GDAL build would); TOY_OFFSET changes the values.
    """

    name = "toy_tiff"

    def load(self, record):
        return np.arange(64, dtype="float32").reshape(8, 8)

    def run(self, data, record):
        return data + float(os.getenv("TOY_OFFSET", "0"))

    def save(self, result, record):
        import hashlib

        import rasterio
        from rasterio.transform import from_origin

        path = record.output_path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with rasterio.open(
            path, "w", driver="GTiff", height=8, width=8, count=1, dtype="float32",
            crs="EPSG:4326", transform=from_origin(0, 8, 1, 1),
            compress=os.getenv("TOY_COMPRESS", "deflate"),
        ) as dst:
            dst.write(result, 1)
        with open(path, "rb") as f:
            record.artifacts["output"] = hashlib.sha256(f.read()).hexdigest()
        record.status = "completed"
        return record
