# ExperimentRecord

The `ExperimentRecord` is the foundational object for reproducibility in the DisSModel Platform. It is an immutable Pydantic model (once stored) that captures the complete provenance of a simulation, including the model version, input data integrity, and the exact configuration used.

## Purpose

*   **Auditability:** Every execution is logged with a unique `experiment_id`.
*   **Immutability:** Once a job is submitted, its `resolved_spec` is snapshotted, ensuring that subsequent changes to the Model Registry do not affect historical results.
*   **Reproducibility:** A record contains enough information to re-run the exact same simulation and verify the `output_sha256`.

## Schema Definition

| Field | Type | Description |
|---|---|---|
| `experiment_id` | `str` | Unique UUID/hash for the execution. |
| `model_name` | `str` | Name of the model in the registry (e.g., `coastal-dynamics-v1`). |
| `model_commit` | `str` | Git hash of the `dissmodel-configs` repo at submission time. |
| `code_version` | `str` | Version of the `dissmodel` library used. |
| `resolved_spec` | `dict` | Full snapshot of the TOML configuration (including defaults). |
| `source` | `DataSource` | Object containing `uri`, `type` (s3/http/local), and `checksum`. |
| `input_format` | `str` | Format of the input data (`geotiff`, `vector`, `zarr`). |
| `column_map` | `dict` | (Vector only) Mapping from canonical names to dataset columns. |
| `band_map` | `dict` | (Raster only) Mapping from canonical names to TIFF bands. |
| `parameters` | `dict` | Final resolved parameters (TOML defaults + request overrides). |
| `status` | `str` | `queued`, `running`, `completed`, `failed`, or `cancelled`. |
| `created_at` | `datetime` | UTC timestamp of job submission. |
| `output_path` | `str` | S3 URI where the result is stored. |
| `output_sha256` | `str` | SHA256 checksum of the generated output file. |
| `logs` | `list[str]` | Execution logs and lifecycle events. |
| `artifacts` | `dict` | Key-value store of additional outputs (e.g., profiling reports). |

## Registry vs. Execution

The following table distinguishes between what is fixed in the Model Registry (TOML) and what can vary per execution via the API request:

| Feature | Registry (TOML) | Execution (API Request) |
|---|---|---|
| Model Class | **Fixed** | N/A |
| Package/Module | **Fixed** | N/A |
| Canonical Vocab | **Fixed** | N/A |
| Parameters | Defaults | **Overrides** |
| Input Dataset | N/A | **Required** |
| Variable Mapping| N/A | **Context-dependent** |

## Example JSON Response

`GET /job/{experiment_id}`

```json
{
  "job_id": "exp_8f2d1c9e",
  "experiment_id": "exp_8f2d1c9e",
  "status": "completed",
  "model_name": "coastal-v1",
  "created_at": "2024-05-12T14:30:00Z",
  "output_path": "s3://dissmodel-outputs/results/exp_8f2d1c9e.tif",
  "output_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "input_sha256": "d5a8c9b2...",
  "logs": [
    "2024-05-12T14:30:00Z - Record created — model=coastal-v1 commit=a1b2c3d",
    "2024-05-12T14:30:05Z - Dispatching to subprocess...",
    "2024-05-12T14:30:45Z - Completed — val=0.5s | load=10s | run=25s | save=4s | total=39.5s"
  ]
}
```

## Methodological Citation

When publishing results, use the following template for the methodology section:

> "Simulations were performed on the DisSModel Platform (v0.1.0) using the `coastal-v1` model (registry commit `a1b2c3d`). Input data integrity was verified via SHA256 (`d5a8c9b2...`). The complete execution provenance is preserved in ExperimentRecord `exp_8f2d1c9e`."

!!! warning "Non-reproducible records"
    Jobs submitted via `POST /submit_job_inline` will have `model_commit: 'local-inline'`. These records contain the full spec but are **not reproducible** via the registry and should not be used for final academic publication.
