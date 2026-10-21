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
| `input_format` | `str` | No | `tiff`, `vector`, or `auto` (default). |
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
    "input_format": "tiff",
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
| `input_format` | `str` | No | `tiff`, `vector`, or `auto` (default). |
| `column_map` | `dict` | No | Mapping for vector columns. |
| `band_map` | `dict` | No | Mapping for raster bands. |
| `parameters` | `dict` | No | Overrides. |

---

## Status & Tracking

### `GET /job/{experiment_id}`
Return the current status and provenance record for a specific experiment.

**Response:** `JobResponse` object (see [ExperimentRecord](experiment-record.md)).

---

### `GET /experiments/{experiment_id}`
Return the full `ExperimentRecord`: `resolved_spec`, `model_commit`, `code_version`, input `source` (with checksum), `parameters`, `output_path`, `artifacts` (checksums of the output, the profiling report and the record JSON), `metrics` and `logs`. This is the response to cite.

`metrics['environment']` lists Python, the executor class and the versions of the packages used in the run.

---

### `GET /jobs`
List simulations, optionally filtered by status.

**Query Parameters:**
| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | `int` | `100` | Max number of records to return. |
| `status` | `str` | `null` | Filter by status (`completed`, `failed`, etc.). |

---

### `DELETE /job/{experiment_id}`
Cancel a queued experiment: it is marked `cancelled`, removed from the queue and skipped by the worker.

---

## Reproduce & Publish

### `POST /experiments/{experiment_id}/reproduce`
Re-run an experiment using its original `resolved_spec` snapshot, input and parameters, independent of the current registry state. The new record carries the `dissmodel` version installed now (a warning is logged when it differs from the original) and `metrics['reproduces']` with the original id.

When the run ends, the worker compares the two outputs and stores the result in `metrics['reproduction']`:

| Field | Meaning |
|---|---|
| `file_identical` | The output files have the same SHA-256. |
| `values_identical` | For GeoTIFF outputs: the decoded bands (shape, dtype, values) have the same SHA-256. Holds across machines where compression or headers differ. |
| `values_sha256` | Hash of the decoded bands of the reproduction. |

Both are exact comparisons, not tolerances. `null` means the check could not be made (for example, a non-GeoTIFF output).

---

### `POST /experiments/{experiment_id}/publish`
Export a reproducibility package. Returns the full provenance as JSON, including `artifacts` and `metrics`. Zenodo deposit is not implemented.

---

## Model Registry

### `GET /models`
List all registered models and their basic metadata.

---

### `GET /models/{model_name}`
Return the full TOML spec for a specific model.

---

### `POST /admin/sync`
Try a `git pull` of `dissmodel-configs` and report the commit (`head`) that new jobs will record. In the Docker stack the checkout is read-only in the API and is updated by the `configs-sync` sidecar; specs are read from disk on every request, so its pulls take effect immediately.

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
