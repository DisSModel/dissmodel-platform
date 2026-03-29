# services/worker/executors/lucc.py
from __future__ import annotations

import os
import tempfile
from typing import TYPE_CHECKING

import geopandas as gpd

from worker.base    import ModelExecutor
from worker.schemas import ExperimentRecord
from worker.storage import download_to_file, sha256_file, upload_file

if TYPE_CHECKING:
    pass


class LUCCExecutor(ModelExecutor):
    """
    Executor for LUCC simulations using PotentialCLinearRegression
    + AllocationCClueLike (vector / GeoDataFrame backend).
    """

    name = "potential_c_linear"

    # ── Load ──────────────────────────────────────────────────────────────────

    def load(self, record: ExperimentRecord) -> gpd.GeoDataFrame:
        """
        Resolve input URI, apply column_map, fill source provenance.
        Returns a GeoDataFrame with canonical column names.
        """
        local_path = self._resolve_uri(record.source.uri)
        record.source.checksum = sha256_file(local_path)

        gdf = gpd.read_file(local_path)

        # Rename dataset columns to canonical model vocabulary
        if record.column_map:
            gdf = gdf.rename(columns={v: k for k, v in record.column_map.items()})

        return gdf

    # ── Validate ──────────────────────────────────────────────────────────────

    def validate(self, record: ExperimentRecord) -> None:
        """
        Check that all columns referenced in the spec betas exist
        in the dataset after applying column_map.
        """
        gdf      = self.load(record)
        expected = self._expected_columns(record.resolved_spec)
        missing  = expected - set(gdf.columns)

        if missing:
            hint = (
                f"Columns missing after applying column_map: {missing}\n"
                f"Current column_map: {record.column_map}\n"
                f"Dataset columns: {list(gdf.columns)}"
            )
            raise ValueError(hint)

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self, record: ExperimentRecord) -> gpd.GeoDataFrame:
        from dissmodel.core import Environment
        from disslucc import DemandPreComputedValues, load_demand_csv
        from disslucc.potential.continuous.linear import PotentialCLinearRegression
        from disslucc.allocation.continuous.clue  import AllocationCClueLike
        from disslucc.schemas import RegressionSpec, AllocationSpec

        spec     = record.resolved_spec["model"]
        params   = record.parameters
        lu_types = spec["land_use_types"]

        gdf = self.load(record)
        record.add_log(f"Loaded GDF: {len(gdf)} features, columns={list(gdf.columns)}")

        env = Environment(end_time=params.get("n_steps", 7) - 1)

        demand = DemandPreComputedValues(
            annual_demand  = load_demand_csv(params["demand_csv"], lu_types),
            land_use_types = lu_types,
        )

        potential_data = [[
            RegressionSpec(
                const  = p["const"],
                betas  = p.get("betas", {}),
                is_log = p.get("is_log", False),
            )
            for p in spec["potential"]
        ]]

        allocation_data = [[
            AllocationSpec(**{k: v for k, v in a.items() if k != "lu"})
            for a in spec["allocation"]
        ]]

        potential = PotentialCLinearRegression(
            gdf              = gdf,
            potential_data   = potential_data,
            demand           = demand,
            land_use_types   = lu_types,
            land_use_no_data = spec.get("land_use_no_data"),
        )

        AllocationCClueLike(
            gdf             = gdf,
            demand          = demand,
            potential       = potential,
            land_use_types  = lu_types,
            static          = spec.get("static", {}),
            complementar_lu = spec.get("complementar_lu", lu_types[0]),
            cell_area       = spec.get("cell_area", 1.0),
            allocation_data = allocation_data,
        )

        record.add_log(f"Running {params.get('n_steps', 7)} steps...")
        env.run()

        return gdf

    # ── Save ──────────────────────────────────────────────────────────────────

    def save(self, result: gpd.GeoDataFrame, record: ExperimentRecord) -> ExperimentRecord:
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as f:
            tmp_path = f.name

        try:
            result.to_file(tmp_path, driver="GPKG")

            object_path = f"experiments/{record.experiment_id}/output.gpkg"
            uri = upload_file(tmp_path, object_path, content_type="application/geopackage+sqlite3")

            record.output_path   = uri
            record.output_sha256 = sha256_file(tmp_path)
            record.status        = "completed"

        finally:
            os.unlink(tmp_path)

        return record

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _expected_columns(resolved_spec: dict) -> set[str]:
        """Collect all column names referenced in potential betas."""
        cols = set()
        for p in resolved_spec.get("model", {}).get("potential", []):
            cols.update(p.get("betas", {}).keys())
        return cols