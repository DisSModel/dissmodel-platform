# services/worker/executors/flood_vector.py
from __future__ import annotations

import io
import os
import tempfile

import geopandas as gpd

from worker.base    import ModelExecutor
from worker.schemas import ExperimentRecord
from worker.storage import minio_client, sha256_file, upload_file, BUCKET_INPUTS


import geopandas as gpd
from libpysal.weights import Queen

from dissmodel.geo import SpatialModel
from dissmodel.visualization import track_plot


@track_plot("flooded_cells", "blue")
class FloodModel(SpatialModel):
    """
    Hydrological model implemented with DisSModel + GeoDataFrame.

    Equivalence with the raster version
    -----------------------------------
    RasterBackend.shift2d()          →  neighs_id(idx) / neighbor_values()
    np.isin(uso, USOS_INUNDADOS)     →  uso_past.isin(USOS_INUNDADOS)
    loop over DIRS_MOORE             →  loop over real GDF neighbors
    vectorized over full grid        →  cell-by-cell loop (slower,
                                        but faithful to real geometry)

    Parameters
    ----------
    gdf           : GeoDataFrame with columns attr_uso and attr_alt
    taxa_elevacao : meters/year — IPCC RCP8.5 ≈ 0.011
    attr_uso      : land-use column. Default: "uso"
    attr_alt      : elevation column. Default: "alt"
    """


    

    def setup(
        self,
        taxa_elevacao: float = 0.011,
        attr_uso:      str   = "uso",
        attr_alt:      str   = "alt",
    ) -> None:
        self.taxa_elevacao = taxa_elevacao
        self.attr_uso      = attr_uso
        self.attr_alt      = attr_alt

        # metrics exposed for @track_plot / Chart
        self.flooded_cells = 0
        self.novas_inundadas   = 0
        self.nivel_mar_atual   = 0.0

        # Queen = Moore neighborhood (8 directions) for regular grids
        # silence_warnings suppresses island warnings (cells without neighbors)
        self.create_neighborhood(strategy=Queen, silence_warnings=True)

    def execute(self) -> None:
        nivel_mar = self.env.now() * self.taxa_elevacao

        # Snapshots — equivalent to cell.past[] in TerraME
        uso_past = self.gdf[self.attr_uso].copy()
        alt_past = self.gdf[self.attr_alt].copy()

        # ── sources: isSeaOrFlooded(uso) and alt >= 0 ─────────────────────────
        fontes = set(
            uso_past.index[
                uso_past.isin(USOS_INUNDADOS) & (alt_past >= 0)
            ]
        )

        # ── A. Elevation — flow diffusion (relative condition) ────────────────
        # Lua: if neighbor.past[alt] <= currentAlt: neigh[alt] += flow
        alt_nova = alt_past.copy()

        for idx in fontes:
            alt_atual = alt_past[idx]
            vizinhos  = self.neighs_id(idx)

            viz_baixos = 1 + sum(
                1 for n in vizinhos if alt_past[n] <= alt_atual
            )
            fluxo = self.taxa_elevacao / viz_baixos

            alt_nova[idx] += fluxo
            for n in vizinhos:
                if alt_past[n] <= alt_atual:
                    alt_nova[n] += fluxo

        self.gdf[self.attr_alt] = alt_nova

        # ── B. Flooding — absolute elevation threshold ───────────────────────
        # Lua: if neighbor.past[alt] <= seaLevel and not isSeaOrFlooded(neigh):
        #          applyFlooding(neighbor)
        # Uses alt_past — faithful to TerraME .past semantics
        uso_novo = uso_past.copy()

        for idx in self.gdf.index:
            uso_atual = uso_past[idx]
            if uso_atual not in REGRAS_INUNDACAO:
                continue
            if alt_past[idx] > nivel_mar:
                continue
            if any(n in fontes for n in self.neighs_id(idx)):
                uso_novo[idx] = REGRAS_INUNDACAO[uso_atual]

        self.gdf[self.attr_uso] = uso_novo

        # ── metrics ──────────────────────────────────────────────────────────
        inund = uso_novo.isin(USOS_INUNDADOS) & (uso_novo != MAR)
        novas = uso_novo.isin(USOS_INUNDADOS) & ~uso_past.isin(USOS_INUNDADOS)

        self.flooded_cells = int(inund.sum())
        self.novas_inundadas   = int(novas.sum())
        self.nivel_mar_atual   = round(nivel_mar, 4)

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

    # services/worker/executors/flood_vector.py

    def load(self, record: ExperimentRecord) -> gpd.GeoDataFrame:
        uri = record.source.uri

        if uri.startswith("s3://"):
            parts        = uri[5:].split("/", 1)
            bucket, key  = parts[0], parts[1]

            # Mirror exactly what worked in Jupyter
            obj  = minio_client.get_object(bucket, key)
            data = io.BytesIO(obj.read())

            record.source.checksum = sha256_file_bytes(data.getvalue())
            data.seek(0)   # rewind after checksum read

            gdf = gpd.read_file(data)

        else:
            record.source.checksum = sha256_file(uri)
            gdf = gpd.read_file(uri)

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