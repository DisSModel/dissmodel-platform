# services/worker/executors/flood_vector.py
from __future__ import annotations

import io
import os
import tempfile

import geopandas as gpd

from worker.base    import ModelExecutor
from worker.schemas import ExperimentRecord
from worker.storage import minio_client, sha256_file, upload_file, BUCKET_INPUTS

# ── Land use constants ────────────────────────────────────────────────────────
# Kept here until coastal_dynamics is available as a dependency

MANGUE                    = 1
VEGETACAO_TERRESTRE       = 2
MAR                       = 3
AREA_ANTROPIZADA          = 4
SOLO_DESCOBERTO           = 5
SOLO_INUNDADO             = 6
AREA_ANTROPIZADA_INUNDADA = 7
MANGUE_MIGRADO            = 8
MANGUE_INUNDADO           = 9
VEG_TERRESTRE_INUNDADA    = 10

USOS_INUNDADOS: list[int] = [
    MAR, SOLO_INUNDADO, AREA_ANTROPIZADA_INUNDADA,
    MANGUE_INUNDADO, VEG_TERRESTRE_INUNDADA,
]

REGRAS_INUNDACAO: dict[int, int] = {
    MANGUE:              MANGUE_INUNDADO,
    MANGUE_MIGRADO:      MANGUE_INUNDADO,
    VEGETACAO_TERRESTRE: VEG_TERRESTRE_INUNDADA,
    AREA_ANTROPIZADA:    AREA_ANTROPIZADA_INUNDADA,
    SOLO_DESCOBERTO:     SOLO_INUNDADO,
}


class FloodVectorExecutor(ModelExecutor):
    """
    Executor for the vector-based hydrological flood model.

    Wraps the FloodModel developed and tested in Jupyter,
    without requiring the coastal_dynamics package.
    Input: shapefile / GeoJSON / GPKG / zipped shapefile from MinIO.
    Output: GPKG saved to dissmodel-outputs.
    """

    name = "flood_vector"

    # ── Load ──────────────────────────────────────────────────────────────────

    def load(self, record: ExperimentRecord) -> gpd.GeoDataFrame:
        """
        Fetch input from MinIO into memory and read with GeoPandas.
        Mirrors exactly what the researcher did in Jupyter.
        """
        uri = record.source.uri

        if uri.startswith("s3://"):
            # Parse s3://bucket/key and stream into BytesIO — no temp file needed
            parts  = uri[5:].split("/", 1)
            bucket, key = parts[0], parts[1]
            obj    = minio_client.get_object(bucket, key)
            data   = io.BytesIO(obj.read())
            record.source.checksum = sha256_file_bytes(data.getvalue())
            data.seek(0)
            gdf = gpd.read_file(data)

        else:
            # Local path — development / testing outside Docker
            local_path = uri
            record.source.checksum = sha256_file(local_path)
            gdf = gpd.read_file(local_path)

        # Apply column_map if dataset uses non-canonical names
        if record.column_map:
            gdf = gdf.rename(columns={v: k for k, v in record.column_map.items()})

        record.add_log(f"Loaded GDF: {len(gdf)} features")
        return gdf

    # ── Validate ──────────────────────────────────────────────────────────────

    def validate(self, record: ExperimentRecord) -> None:
        """Check that required columns exist after applying column_map."""
        params   = record.parameters
        attr_uso = params.get("attr_uso", "uso")
        attr_alt = params.get("attr_alt", "alt")

        gdf     = self.load(record)
        missing = {attr_uso, attr_alt} - set(gdf.columns)

        if missing:
            raise ValueError(
                f"Required columns missing after column_map: {missing}\n"
                f"Dataset columns: {list(gdf.columns)}\n"
                f"Current column_map: {record.column_map}"
            )

        # Check that uso column contains known land use values
        unknown = set(gdf[attr_uso].unique()) - set(REGRAS_INUNDACAO) - set(USOS_INUNDADOS)
        if unknown:
            record.add_log(f"Warning: unknown land use values in '{attr_uso}': {unknown}")

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self, record: ExperimentRecord) -> gpd.GeoDataFrame:
        from dissmodel.core import Environment
        from flood_vector_model import FloodModel   # local to the executor package

        params   = record.parameters
        end_time = params.get("end_time", 10)

        gdf = self.load(record)

        env = Environment(
            start_time = params.get("start_time", 1),
            end_time   = end_time,
        )

        FloodModel(
            gdf           = gdf,
            taxa_elevacao = params.get("taxa_elevacao", 0.5),
            attr_uso      = params.get("attr_uso", "uso"),
            attr_alt      = params.get("attr_alt", "alt"),
        )

        record.add_log(f"Running {end_time} steps...")
        env.run()
        record.add_log("Simulation complete")

        return gdf

    # ── Save ──────────────────────────────────────────────────────────────────

    def save(self, result: gpd.GeoDataFrame, record: ExperimentRecord) -> ExperimentRecord:
        """
        Save result to MinIO as GPKG — mirrors what the researcher
        did manually in Jupyter with minio.put_object().
        """
        buffer = io.BytesIO()
        result.to_file(buffer, driver="GPKG", layer="result")
        buffer.seek(0)
        content = buffer.getvalue()

        object_path = f"experiments/{record.experiment_id}/output.gpkg"

        minio_client.put_object(
            bucket_name  = "dissmodel-outputs",
            object_name  = object_path,
            data         = io.BytesIO(content),
            length       = len(content),
            content_type = "application/geopackage+sqlite3",
        )

        record.output_path   = f"s3://dissmodel-outputs/{object_path}"
        record.output_sha256 = sha256_file_bytes(content)
        record.status        = "completed"
        record.add_log(f"Saved to {record.output_path}")

        return record


# ── Helper ────────────────────────────────────────────────────────────────────

def sha256_file_bytes(data: bytes) -> str:
    """sha256 of in-memory bytes — avoids writing to disk."""
    import hashlib
    return hashlib.sha256(data).hexdigest()