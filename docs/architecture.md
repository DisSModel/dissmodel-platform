# Architecture

## Services

Development stack (`docker-compose.yml`):

| Service | Image | Role |
|---|---|---|
| `api` | `services/api` (FastAPI) | receives jobs, builds the `ExperimentRecord`, queues it; serves records, uploads and download links |
| `worker` | `services/worker` | takes jobs from the queue and runs them; one job at a time per worker |
| `redis` | `redis:7-alpine` | the job queues and the records (`experiment:<id>`), with append-only persistence |
| `minio` | `minio/minio` | S3-compatible storage: buckets `dissmodel-inputs` and `dissmodel-outputs` |
| `configs-sync` | `alpine/git` | clones `CONFIGS_REPO` and pulls it on a timer into a volume shared read-only with `api` and `worker` |
| `jupyter` | `services/jupyter` | JupyterLab with DisSModel, DisSCube and DisSLUCC, for exploring and submitting jobs |

The production stack (`docker-compose.prod.yml`) adds `nginx` in front of everything (paths under `/dissmodel/`) and two Streamlit explorers from `dissmodel-ca` and `dissmodel-sysdyn`; see [Deployment](deployment.md).

## The path of a job

```text
 POST /submit_job ──► api ──────────────────────────────┐
                       │ 1. load models/<name>.toml      │
                       │ 2. build ExperimentRecord       │ Redis
                       │ 3. store experiment:<id> ───────┤  experiment:<id>
                       │ 4. push <id> to queue:<prio> ───┤  queue:high|normal|low
                                                         │
 worker ◄── 5. pop <id> (high before normal before low) ─┘
   │ 6. install model.package, if any
   │ 7. python -m worker.job_runner <record.json>   (new process)
   │       import model.executor_module → executors register themselves
   │       ExecutorRegistry.get(model.class)
   │       validate → load → run → save            (dissmodel.execute_lifecycle)
   │       write record JSON and profiling next to the output
   │ 8. if it is a reproduction: compare with the original output
   └ 9. store the final record in Redis
```

Step by step:

1. **Spec.** `/submit_job` reads `models/<model_name>.toml` from the registry checkout; `/submit_job_inline` takes the TOML in the request.
2. **Record.** The API snapshots the spec in `resolved_spec`, records the registry commit (`model_commit`) and the installed `dissmodel` version (`code_version`), merges the parameters ([Model registry](registry.md#parameters)) and sets `output_path` to `<DISSMODEL_OUTPUT_ROOT>/<experiment_id>/output.tif`. From here on the record, not the registry, decides what runs.
3. **Queue.** The record goes to Redis and its id to one of three queues, by `priority`.
4. **Subprocess.** The worker runs each job in a new Python process. A package installed for one job (`model.package`) is then importable without restarting the worker, and a crash in a model does not take the worker down.
5. **Lifecycle.** The executor's `validate`, `load`, `run` and `save` run in that order, each timed. The executor writes the output; the job runner adds the environment (`metrics['environment']`), the profiling report and the record JSON.
6. **Result.** The worker stores the final record. Failures are stored too, with `status: "failed"` and the traceback in `logs`.

A cancelled job (`DELETE /job/{id}`) is removed from the queue; if the worker had already taken it, it sees the status and skips it. A job that is already running is not interrupted.

## Code

| Path | Content |
|---|---|
| `services/api/main.py` | FastAPI app: routes, API-key check, Redis access |
| `services/worker/worker.py` | queue loop, cancellation, reproduction check |
| `services/worker/runner.py` | record builders (`build_record`, `build_record_inline`, `build_reproduction`), parameter merge, package install, subprocess launch |
| `services/worker/job_runner.py` | the subprocess: imports the executor, runs the lifecycle, writes the side artifacts |
| `services/worker/compare.py` | output comparison for reproductions |
| `services/worker/api_registry.py` | reading specs from the registry checkout |
| `services/worker/storage.py` | MinIO client and helpers |
| `services/worker/executors/` | place for executors bundled with the platform (empty) |

The API imports the `worker` package to build records. In the Compose files `services/worker` is mounted into the API container for that; the API image alone does not contain it.

The `ModelExecutor` contract, `ExperimentRecord` and `execute_lifecycle` come from the `dissmodel` package; the platform does not redefine them.

## Scaling

`docker compose up -d --scale worker=N` starts N workers on the same queue; each takes one job at a time. There is no distribution of a single job across machines and no scheduler beyond the three priority queues. Every worker needs the same inputs, so with several hosts use `s3://` or `http(s)://` inputs rather than local paths.
