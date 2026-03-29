# services/worker/executors/coastal_vector.py
from __future__ import annotations

import os
import tempfile

from worker.base    import ModelExecutor
from worker.schemas import ExperimentRecord
from worker.storage import download_to_file, sha256_file, upload_file


class CoastalVectorExecutor(ModelExecutor):
    """
    Executor for coastal dynamics simulations from a vector input
    (shapefile, GeoJSON, GPKG, or zipped shapefile).
    Rasterizes the input before running the simulation.
    """

    name = "coastal_vector"

    # ── Load ──────────────────────────────────────────────────────────────────

    def load(self, record: ExperimentRecord):
        """
        Rasterize vector input into a RasterBackend.
        Applies column_map before rasterization.
        Returns (backend, meta, start_time=1).
        """
        import geopandas as gpd
        from dissmodel.geo.raster.io import shapefile_to_raster_backend
        from coastal_dynamics.common.constants import CRS

        params     = record.parameters
        spec       = record.resolved_spec["model"]
        local_path = self._resolve_uri(record.source.uri)
        record.source.checksum = sha256_file(local_path)

        # Apply column_map before rasterization
        if record.column_map:
            gdf = gpd.read_file(local_path)
            gdf = gdf.rename(columns={v: k for k, v in record.column_map.items()})
            # Write renamed GDF to a temp file so shapefile_to_raster_backend can read it
            with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as f:
                tmp_vector = f.name
            gdf.to_file(tmp_vector, driver="GPKG")
            source_path = tmp_vector
        else:
            source_path = local_path
            tmp_vector  = None

        try:
            defaults = spec.get("shapefile_defaults", {})
            attrs    = {k: defaults.get(k, 0.0)
                        for k in spec.get("bands", {}).keys()}

            backend = shapefile_to_raster_backend(
                path       = source_path,
                resolution = params.get("resolution", 100.0),
                attrs      = attrs,
                crs        = params.get("crs", CRS),
                nodata     = 0,
            )
        finally:
            if tmp_vector:
                os.unlink(tmp_vector)

        meta = {"crs": params.get("crs", CRS), "tags": {}}

        record.add_log(
            f"Rasterized vector: shape={backend.shape} "
            f"resolution={params.get('resolution', 100.0)}m"
        )
        return backend, meta, 1   # vector input always starts at step 1

    # ── Validate ──────────────────────────────────────────────────────────────

    def validate(self, record: ExperimentRecord) -> None:
        """Check canonical bands exist after rasterization and column_map."""
        backend, *_ = self.load(record)
        expected    = set(record.resolved_spec.get("model", {}).get("bands", {}).keys())
        actual      = set(backend.band_names())
        missing     = expected - actual

        if missing:
            raise ValueError(
                f"Bands missing after rasterization: {missing}\n"
                f"Current column_map: {record.column_map}\n"
                f"Available bands: {list(actual)}"
            )

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self, record: ExperimentRecord):
        from dissmodel.core import Environment
        from coastal_dynamics.raster.flood_model    import FloodModel
        from coastal_dynamics.raster.mangrove_model import MangroveModel

        params               = record.parameters
        backend, meta, start = self.load(record)

        env = Environment(
            start_time = start,
            end_time   = params.get("end_time", 88),
        )

        self._build_models(backend, params)
        record.add_log(f"Running steps {start} → {params.get('end_time', 88)}...")
        env.run()

        return backend, meta

    def _build_models(self, backend, params: dict) -> None:
        from coastal_dynamics.raster.flood_model    import FloodModel
        from coastal_dynamics.raster.mangrove_model import MangroveModel

        FloodModel(
            backend       = backend,
            taxa_elevacao = params.get("sea_level_rise_rate", 0.5),
        )
        MangroveModel(
            backend       = backend,
            taxa_elevacao = params.get("sea_level_rise_rate", 0.5),
            altura_mare   = params.get("tide_height", 6.0),
            acrecao_ativa = params.get("acrecao_ativa", False),
        )

    # ── Save ──────────────────────────────────────────────────────────────────

    def save(self, result, record: ExperimentRecord) -> ExperimentRecord:
        from dissmodel.geo.raster.io import save_geotiff
        from coastal_dynamics.common.constants import TIFF_BANDS, CRS

        backend, meta = result

        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
            tmp_path = f.name

        try:
            save_geotiff(
                backend,
                tmp_path,
                band_spec = TIFF_BANDS,
                crs       = meta.get("crs") or CRS,
            )

            object_path = f"experiments/{record.experiment_id}/output.tif"
            uri = upload_file(tmp_path, object_path, content_type="image/tiff")

            record.output_path   = uri
            record.output_sha256 = sha256_file(tmp_path)
            record.status        = "completed"

        finally:
            os.unlink(tmp_path)

        return record
