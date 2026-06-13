# REST API Reference

The DisSModel Platform API is built with FastAPI. All endpoints (except `/health` and `/`) require the `X-API-Key` header for authentication.

**Base URL:** `http://localhost:8000`

## Job Submission

### `POST /submit_job`
Submit a simulation job using a model registered in the [Registry](registry.md).

**Body Schema:**
| Field | Type | Required | Description |
|---|---|---|---|
| `model_name` | `str` | Yes | Name of the registered model. |
| `input_dataset`| `str` | Yes | URI (`s3://`, `http://`, or local path). |
| `input_format` | `str` | Yes | `geotiff`, `vector`, or `zarr`. |
| `column_map` | `dict` | No | Mapping for vector columns. |
| `band_map` | `dict` | No | Mapping for raster bands. |
| `parameters` | `dict` | No | Overrides for TOML defaults. |
| `priority` | `str` | No | `high`, `normal` (default), or `low`. |

**Example:**
```bash
curl -X POST http://localhost:8000/submit_job \
  -H "X-API-Key: dev-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "coastal-v1",
    "input_dataset": "s3://dissmodel-inputs/data.tif",
    "input_format": "geotiff",
    "parameters": {"threshold": 0.8}
  }'
```

---

### `POST /submit_job_inline`
Submit a job with an inline TOML spec. Used for development and exploration.

**Body Schema:**
| Field | Type | Required | Description |
|---|---|---|---|
| `model_spec_toml` | `str` | Yes | Full content of the TOML spec. |
| `input_dataset` | `str` | Yes | URI of the dataset. |
| `input_format` | `str` | Yes | `geotiff`, `vector`, or `zarr`. |
| `column_map` | `dict` | No | Mapping for vector columns. |
| `band_map` | `dict` | No | Mapping for raster bands. |
| `parameters` | `dict` | No | Overrides. |

---

## Status & Tracking

### `GET /job/{experiment_id}`
Return the current status and provenance record for a specific experiment.

**Response:** `JobResponse` object (see [ExperimentRecord](experiment-record.md)).

---

### `GET /jobs`
List simulations, optionally filtered by status.

**Query Parameters:**
| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | `int` | `100` | Max number of records to return. |
| `status` | `str` | `null` | Filter by status (`completed`, `failed`, etc.). |

---

## Reproduce & Publish

### `POST /experiments/{experiment_id}/reproduce`
Re-run an experiment using its original `resolved_spec` snapshot, independent of the current registry state.

---

### `POST /experiments/{experiment_id}/publish`
Export a reproducibility package. Returns the full provenance as JSON.

---

## Model Registry

### `GET /models`
List all registered models and their basic metadata.

---

### `GET /models/{model_name}`
Return the full TOML spec for a specific model.

---

### `POST /admin/sync`
Force an immediate `git pull` of the `dissmodel-configs` repository.

---

## Data Management

### `POST /data/upload`
Upload a dataset to the `dissmodel-inputs` bucket.

**Body (Multipart Form):**
*   `file`: The file to upload.
*   `label`: A tag for the input directory (e.g., `baseline`).

**Response:**
```json
{
  "uri": "s3://dissmodel-inputs/inputs/baseline/data.tif",
  "checksum": "sha256...",
  "size_mb": 45.2
}
```

---

### `GET /download`
Generate a presigned URL for downloading a file from S3.

**Query Parameters:**
*   `uri`: The `s3://` URI.
*   `expires_hours`: Validity period (default: `1`).
