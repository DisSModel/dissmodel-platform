# ExperimentRecord

Every job has one `ExperimentRecord` (defined in `dissmodel.executor.schemas`). The API creates it when the job is submitted; the worker completes it. It answers: which spec ran, on which input, with which code, and what came out.

## Fields

| Field | Set by | Content |
|---|---|---|
| `experiment_id` | API | 32 hexadecimal characters |
| `created_at` | API | submission time (UTC) |
| `model_name` | API | name in the registry, or `[model].name` of an inline spec |
| `model_commit` | API | short commit of the registry checkout; `local-inline` for inline specs; `local-dev` when the registry folder is not a git checkout |
| `code_version` | API | installed `dissmodel` version |
| `resolved_spec` | API | the whole TOML spec, as it was at submission |
| `parameters` | API | what the executor receives: spec fields + `[model.parameters]` + the request's `parameters` ([how](registry.md#parameters)) |
| `source` | API, executor | input: `uri`, `type` (`local`, `s3`, `http`) and `checksum` (SHA-256, filled by the executor's `load`) |
| `input_format`, `column_map`, `band_map` | API | from the request; see [Executors](executors.md#variable-mapping) |
| `period` | executor | time window read from a data cube, when the executor uses one |
| `output_path` | API, executor | `<DISSMODEL_OUTPUT_ROOT>/<experiment_id>/output.tif`, unless the executor writes elsewhere |
| `artifacts` | executor, worker | SHA-256 of each file written: `output`, `profiling`, `record_json` |
| `output_sha256` | — | read-only shortcut for `artifacts['output']` |
| `metrics` | executor, worker | the executor's own metrics, phase timings (`time_*_sec`), `environment`, and for reproductions `reproduces` and `reproduction` |
| `status` | API, worker | `pending` → `running` → `completed` or `failed`; `cancelled` |
| `logs` | everyone | one line per event; a failure keeps the traceback |

`metrics['environment']` lists the Python version, the executor class, the package that provides it, and the versions of `dissmodel`, `disscube`, `disslucc`, `numpy` and `rasterio` installed in the worker.

The record is stored in Redis (`experiment:<id>`) and, at the end of a run, as `<id8>.record.json` next to the output. That copy is written last, so it contains every log line except the one that announces it, and it cannot contain its own checksum (`artifacts['record_json']` is only in the Redis copy).

## Example

`GET /experiments/{id}` after LuccME Lab 1 (`resolved_spec` and the coefficients shortened):

```json
{
  "experiment_id": "1833555e45074258ad41fbbf7d397d25",
  "created_at": "2026-10-21T16:27:58.191313Z",
  "model_name": "lucc_continuous",
  "model_commit": "local-dev",
  "code_version": "0.6.6",
  "resolved_spec": "…",
  "source": {
    "type": "local",
    "uri": "/data/inputs/csAC.zip",
    "checksum": "dc659e611cb082bc40a2acbac8ec816cbaff66bf5c97f4940abe8c7c3c91a6eb"
  },
  "parameters": {
    "land_use_types": ["f", "d", "outros"],
    "resolution": 5000.0,
    "n_steps": 7,
    "demand_csv": "/data/inputs/examples_demand_lab1.csv",
    "potential_data": "…",
    "allocation_data": "…"
  },
  "output_path": "/data/outputs/experiments/1833555e45074258ad41fbbf7d397d25/output.tif",
  "artifacts": {
    "output": "d987b6c2872d02ba8b8300624514afadfd1d060db87a0790caecb228766f9f32",
    "profiling": "61dbf889941a63ac3c3b2f7c797ead56002dc8c03389c512aeb3b5f0bce7cc05",
    "record_json": "2f71ca343c8c591b3f293d8c0bbe85d46ae798a370f9b06560ebc1868a928945"
  },
  "metrics": {
    "environment": {
      "python": "3.13.16",
      "executor": "disslucc.executors.continuous.LuccContinuousExecutor",
      "executor_package": "disslucc",
      "packages": {"dissmodel": "0.6.6", "disscube": "0.6.0", "disslucc": "0.5.0",
                   "numpy": "2.5.4", "rasterio": "1.5.2"}
    },
    "final_f_area": 134724.37744140625,
    "final_d_area": 23136.416625976562,
    "time_load_sec": 1.271,
    "time_run_sec": 0.152,
    "time_total_sec": 1.481
  },
  "status": "completed",
  "logs": [
    "Record created — model=lucc_continuous commit=local-dev",
    "Dispatching to subprocess...",
    "Loaded: 6,574 features",
    "Ran 7 steps (21.7 ms/step)",
    "Saved to /data/outputs/experiments/1833555e45074258ad41fbbf7d397d25/output.tif",
    "Completed — val=0.00s | load=1.27s | run=0.15s | save=0.06s | total=1.48s"
  ],
  "output_sha256": "d987b6c2872d02ba8b8300624514afadfd1d060db87a0790caecb228766f9f32"
}
```

## Reproducing a run

`POST /experiments/{id}/reproduce` creates a new record from the stored one: the same `resolved_spec`, input, mappings and parameters, so later changes to the registry do not matter. `code_version` is the `dissmodel` installed now; when it differs from the original, a warning is logged. `metrics['reproduces']` holds the original id.

When the reproduction finishes, the worker compares the two outputs and stores `metrics['reproduction']`:

```json
{
  "of": "1833555e45074258ad41fbbf7d397d25",
  "original_output": "/data/outputs/experiments/1833555e45074258ad41fbbf7d397d25/output.tif",
  "file_identical": true,
  "values_identical": true,
  "values_sha256": "5927e542515090cb26ca4095d644f4d8dcac514313428d52216f3c534087a3a8"
}
```

| Field | Meaning |
|---|---|
| `file_identical` | the output files have the same SHA-256 |
| `values_identical` | for GeoTIFF outputs, the decoded bands (shape, data type and every value) have the same SHA-256 |

Both are exact; there is no tolerance. `null` means the check could not be made (an output that is not a GeoTIFF, or a file that could not be read; the reason is in `values_error`).

The two can disagree. A GeoTIFF written with the same values but a different compression library has `file_identical: false` and `values_identical: true`; this happens across operating systems and GDAL builds. For identity across machines, compare the values.

The reproduction runs on the same platform, usually the same worker image. It shows that the record is complete and the run deterministic; it is not an independent replication.

## Citing a run

`POST /experiments/{id}/publish` returns the provenance of a completed run as JSON (spec, input, parameters, output, artifacts, metrics and logs). Archive that JSON together with the output, for example on Zenodo; the platform does not deposit it for you.

In the text, name what a reader needs to repeat the run:

> Simulations were run with the DisSLUCC 0.5.0 continuous executor (DisSModel 0.6.6) on the DisSModel Platform, model specification `lucc_continuous` at commit `<model_commit>` of `<registry repository>`. Input SHA-256 `<source.checksum>`; output SHA-256 `<output_sha256>`. The experiment record is archived at `<DOI>`.

Records with `model_commit: "local-inline"` contain the full spec, so they can be reproduced on the platform, but the spec is not tied to a versioned file. Register the model before producing results you will cite.
