# services/worker/executors/coastal_tiff.py
from __future__ import annotations

import os
import tempfile

from worker.base    import ModelExecutor
from worker.schemas import ExperimentRecord
from worker.storage import download_to_file, sha256_file, upload_file


class CoastalTiffExecutor(ModelExecutor):
    """
    Executor for coastal dynamics simulations from a GeoTIFF input.
    Supports resuming a simulation from a previously saved state.
    """

    name = "coastal_tiff"

    # ── Load ──────────────────────────────────────────────────────────────────

    def load(self, record: ExperimentRecord):
        """
        Download GeoTIFF, apply band_map, return (backend, meta, start_time).
        start_time is read from TIFF tags if present.
        """
        from dissmodel.geo.raster.io import load_geotiff
        from coastal_dynamics.common.constants import TIFF_BANDS

        local_path = self._resolve_uri(record.source.uri)
        record.source.checksum = sha256_file(local_path)

        backend, meta = load_geotiff(local_path, band_spec=TIFF_BANDS)

        # Rename bands to canonical vocabulary
        for canonical, real in record.band_map.items():
            backend.rename_band(real, canonical)

        tags       = meta.get("tags", {})
        start_time = int(tags.get("passo", 0)) + 1

        record.add_log(
            f"Loaded GeoTIFF: shape={backend.shape} "
            f"start={start_time} crs={meta.get('crs')}"
        )
        return backend, meta, start_time

    # ── Validate ──────────────────────────────────────────────────────────────

    def validate(self, record: ExperimentRecord) -> None:
        """
        Check canonical bands exist and elevation values are plausible.
        """
        backend, *_ = self.load(record)
        expected    = set(record.resolved_spec.get("model", {}).get("bands", {}).keys())
        actual      = set(backend.band_names())
        missing     = expected - actual

        if missing:
            raise ValueError(
                f"Bands missing after applying band_map: {missing}\n"
                f"Current band_map: {record.band_map}\n"
                f"Available bands: {list(actual)}"
            )

        # Sanity check on elevation range
        if "alt" in actual:
            alt = backend.get("alt")
            if alt.min() < -500 or alt.max() > 9000:
                raise ValueError(
                    f"Band 'alt' has implausible values: "
                    f"[{alt.min():.1f}, {alt.max():.1f}]. "
                    f"Check band_map — 'alt' should be elevation in meters."
                )

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self, record: ExperimentRecord):
        from dissmodel.core import Environment
        from coastal_dynamics.raster.flood_model    import FloodModel
        from coastal_dynamics.raster.mangrove_model import MangroveModel

        params             = record.parameters
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
                transform = meta.get("transform"),
            )

            object_path = f"experiments/{record.experiment_id}/output.tif"
            uri = upload_file(tmp_path, object_path, content_type="image/tiff")

            record.output_path   = uri
            record.output_sha256 = sha256_file(tmp_path)
            record.status        = "completed"

        finally:
            os.unlink(tmp_path)

        return record