# API reference

Base URL: `http://localhost:8000` in the development stack, `http://<host>/dissmodel/api` behind the production nginx. The interactive OpenAPI page is at `/docs`.

Every route except `/` and `/health` needs the header `X-API-Key: <key>`, with a key listed in `API_KEYS`. A missing key gives 403 (`Not authenticated`), a wrong one 403 (`Invalid API key`).

| Method | Path | Purpose |
|---|---|---|
| `POST` | [`/submit_job`](#post-submit_job) | run a registered model |
| `POST` | [`/submit_job_inline`](#post-submit_job_inline) | run a spec sent in the request |
| `GET` | [`/job/{id}`](#get-jobid) | status summary |
| `GET` | [`/experiments/{id}`](#get-experimentsid) | full record |
| `GET` | [`/jobs`](#get-jobs) | list runs |
| `DELETE` | [`/job/{id}`](#delete-jobid) | cancel a queued run |
| `POST` | [`/experiments/{id}/reproduce`](#post-experimentsidreproduce) | run again from the record |
| `POST` | [`/experiments/{id}/publish`](#post-experimentsidpublish) | provenance package |
| `GET` | [`/models`](#get-models), `/models/{name}` | registry |
| `POST` | [`/admin/sync`](#post-adminsync) | registry commit in use |
| `POST` | [`/data/upload`](#post-dataupload) | upload an input |
| `GET` | [`/download`](#get-download) | temporary download link |
| `GET` | `/health` | Redis and MinIO reachable |

## Jobs

### `POST /submit_job`

Run a model from the registry ([Model registry](registry.md)).

| Field | Type | Required | Description |
|---|---|---|---|
| `model_name` | string | yes | file name in `models/`, without `.toml` |
| `input_dataset` | string | yes | `/local/path`, `s3://bucket/key` or `http(s)://...` |
| `parameters` | object | no | overrides; see [Parameters](registry.md#parameters) |
| `column_map`, `band_map` | object | no | model name → dataset name; see [Variable mapping](executors.md#variable-mapping) |
| `input_format` | string | no | `auto` (default), `vector` or `tiff`; stored in the record for the executor |
| `priority` | string | no | `high`, `normal` (default) or `low` |

```bash
curl -X POST http://localhost:8000/submit_job \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"model_name": "lucc_continuous",
       "input_dataset": "/data/inputs/csAC.zip",
       "parameters": {"demand_csv": "/data/inputs/examples_demand_lab1.csv"}}'
```

Response (`JobResponse`):

```json
{
  "job_id": "1833555e45074258ad41fbbf7d397d25",
  "experiment_id": "1833555e45074258ad41fbbf7d397d25",
  "status": "pending",
  "model_name": "lucc_continuous",
  "created_at": "2026-10-21T16:27:58.191313Z",
  "output_path": "s3://dissmodel-outputs/experiments/1833555e45074258ad41fbbf7d397d25/output.tif",
  "output_sha256": null,
  "input_sha256": null,
  "logs": ["Record created — model=lucc_continuous commit=a1b2c3d"]
}
```

`job_id` and `experiment_id` are the same value. 404 if the model is not in the registry (the message lists the registered ones).

### `POST /submit_job_inline`

Same as `/submit_job`, with the spec in the request instead of a registry name. Fields: `model_spec_toml` (the TOML text, required), `input_dataset`, `parameters`, `column_map`, `band_map`, `input_format`; always normal priority. The record gets `model_commit: "local-inline"`. 400 if the TOML does not parse.

### `GET /job/{id}`

The `JobResponse` above, with the current `status` (`pending`, `running`, `completed`, `failed`, `cancelled`), `output_sha256`, `input_sha256` and `logs`. 404 if the id does not exist.

### `GET /experiments/{id}`

The full [`ExperimentRecord`](experiment-record.md): spec, parameters, input, output, artifacts, metrics (including `environment` and, for reproductions, `reproduction`) and logs.

### `GET /jobs`

`{"experiments": [JobResponse, ...], "total": N}`, newest first. Query: `limit` (default 100), `status`. It reads every record from Redis, so it slows down as records accumulate.

### `DELETE /job/{id}`

Marks a `pending` run as `cancelled` and removes it from the queue. A run that is already `running` is not interrupted. 400 for a run that is `completed` or `failed`.

## Reproduction and provenance

### `POST /experiments/{id}/reproduce`

Queues a new run with the original record's spec, input, maps and parameters, independent of the current registry. Returns the new run's `JobResponse`. When it finishes, its record has `metrics['reproduces']` (the original id) and `metrics['reproduction']`:

| Field | Meaning |
|---|---|
| `file_identical` | the output files have the same SHA-256 |
| `values_identical` | GeoTIFF outputs only: the decoded bands have the same SHA-256 |
| `values_sha256` | that hash for the new output |

Exact comparisons; `null` when a check could not be made. See [ExperimentRecord](experiment-record.md#reproducing-a-run).

### `POST /experiments/{id}/publish`

For a `completed` run, returns its provenance as one JSON object: `model_name`, `model_commit`, `code_version`, `input`, `column_map`, `band_map`, `parameters`, `period`, `resolved_spec`, `output_path`, `output_sha256`, `artifacts`, `metrics`, `logs`. Nothing is deposited anywhere. 400 for a run that is not completed.

## Registry

### `GET /models`

`{"models": [{"name": ..., "class": ..., "description": ...}, ...]}`. A spec that does not parse is listed with an `error` field.

### `GET /models/{name}`

`{"model_name": ..., "spec": {...}}`, the TOML as a JSON object. 404 if absent.

### `POST /admin/sync`

`{"pulled": bool, "head": "<commit>", "detail": "..."}`. Tries `git pull` in the registry checkout, which fails in the Docker stack because the API sees it read-only (the `configs-sync` container pulls it); `head` is the commit new jobs will record either way.

## Data

### `POST /data/upload`

Multipart form with `file` and `label`. Stores the file in MinIO at `s3://dissmodel-inputs/inputs/<label>/<file name>` and returns `{"uri", "label", "checksum", "size_mb"}`. The whole file is read into the API's memory: use it for files up to about 100 MB, and `mc` for larger ones ([Input and output data](data.md#uploading)).

### `GET /download`

Query: `uri` (`s3://bucket/key`, required), `expires_hours` (default 1). Returns `{"url", "expires_in_hours"}`, a presigned link to `MINIO_URL`. The link is computed without checking that the object exists. 400 if the URI is not `s3://bucket/key`.

## Service

### `GET /health`

`{"status": "healthy", "timestamp": ...}` when Redis answers and the `dissmodel-outputs` bucket exists; 503 otherwise. No key needed.
