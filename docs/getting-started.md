# Getting started

This page takes you from a clean machine to a finished, reproduced run of LuccME Lab 1 with DisSLUCC.

## 1. Requirements

- Docker with Compose v2 (Docker Desktop on Windows and macOS).
- About 4 GB of memory free for the build. With less, build one image at a time: `docker compose --parallel 1 build`.
- About 10 GB of disk for the images.

## 2. Configure

```bash
git clone https://github.com/DisSModel/dissmodel-platform.git
cd dissmodel-platform
cp .env.example .env
```

Edit `.env` and replace the two `change-me` values:

- `MINIO_ROOT_PASSWORD` (at least 8 characters): password of the storage server, used internally by the API and the worker.
- `API_KEYS`: the keys accepted by the API, separated by commas. Give each person their own.

Generate them with `openssl rand -hex 32`. The other values in the file are the defaults and work as they are. `.env` is ignored by git; never commit it.

`CONFIGS_REPO` can stay empty for now ([step 7](#7-register-the-model)).

## 3. Start

```bash
docker compose up -d --build
docker compose ps
curl http://localhost:8000/health
```

The first build takes several minutes (GDAL and the Python packages). When `/health` answers `"status": "healthy"`, the API reaches Redis and MinIO.

| Service | Address | Notes |
|---|---|---|
| API | http://localhost:8000/docs | interactive OpenAPI page; click *Authorize* and paste a key |
| JupyterLab | http://localhost:8888 | no password in this setup ([Security](security.md)) |
| MinIO console | http://localhost:19001 | log in with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |

## 4. Put the input where the worker can read it

The host folder `./data/inputs` is mounted read-only at `/data/inputs` in the worker and in JupyterLab. Download the Lab 1 inputs from the DisSLUCC repository:

```bash
base=https://raw.githubusercontent.com/DisSModel/disslucc/main/data/input
curl -sLo data/inputs/csAC.zip "$base/csAC.zip"
curl -sLo data/inputs/examples_demand_lab1.csv "$base/examples_demand_lab1.csv"
```

Inputs can also live in MinIO (`s3://dissmodel-inputs/...`) or at a public URL; see [Input and output data](data.md).

## 5. Run the first job

The quickest way is the example script, which uses only the Python standard library:

```bash
DISSMODEL_API_KEY=<your key> python3 examples/submit_lab1.py
```

```text
submitted  a0fb58583622415684d27bbe2ede09c2
status     completed
output     s3://dissmodel-outputs/experiments/a0fb58583622415684d27bbe2ede09c2/output.tif
sha256     d987b6c2872d02ba8b8300624514afadfd1d060db87a0790caecb228766f9f32
reproduce  f51f49e142ea4782996a0a57853fde53
file identical:   True
values identical: True
```

The output checksum should be the one above: it is the same GeoTIFF that `python -m disslucc.executors.continuous run` writes from the same TOML.

What the script does, with `curl` (needs `jq` to put the TOML file into JSON):

```bash
KEY=<your key>

# Submit with the spec inline (no registry needed)
jq -n --rawfile toml examples/lucc_continuous_lab1.toml '{
  model_spec_toml: $toml,
  input_dataset:   "/data/inputs/csAC.zip",
  parameters:      {demand_csv: "/data/inputs/examples_demand_lab1.csv"}
}' | curl -s -X POST http://localhost:8000/submit_job_inline \
       -H "X-API-Key: $KEY" -H "Content-Type: application/json" -d @-

# Follow it (status: pending → running → completed | failed)
curl -s -H "X-API-Key: $KEY" http://localhost:8000/job/<experiment_id>

# Full record
curl -s -H "X-API-Key: $KEY" http://localhost:8000/experiments/<experiment_id>

# Repeat it; the new record says whether the output is identical
curl -s -X POST -H "X-API-Key: $KEY" http://localhost:8000/experiments/<experiment_id>/reproduce
```

## 6. Where the results are

Each run writes to its own folder, `s3://dissmodel-outputs/experiments/<experiment_id>/`:

| File | Content |
|---|---|
| `output.tif` | the model output |
| `<id8>.record.json` | the `ExperimentRecord` at the end of the run |
| `profiling_<id8>.md` | time spent in validate, load, run and save |

Browse them in the MinIO console, or get a temporary download link:

```bash
curl -s -H "X-API-Key: $KEY" \
  "http://localhost:8000/download?uri=s3://dissmodel-outputs/experiments/<id>/output.tif"
```

To write outputs to the host instead, set `DISSMODEL_OUTPUT_ROOT=/data/outputs/experiments` in `.env`; they then appear under `./data/outputs/experiments/`.

## 7. Register the model

An inline job records `model_commit: "local-inline"`: the spec is stored in the record, but it is not tied to a versioned file. For results you will cite, register the model in a git repository of specs:

1. Create a repository with a `models/` folder and copy `examples/lucc_continuous_lab1.toml` to `models/lucc_continuous.toml`.
2. Set `CONFIGS_REPO` (and `CONFIGS_BRANCH`) in `.env` and run `docker compose up -d`.
3. The `configs-sync` container clones it and pulls every `CONFIGS_SYNC_INTERVAL` seconds. Check with `curl -H "X-API-Key: $KEY" http://localhost:8000/models`.
4. Submit by name; the record now carries the commit of the spec:

```bash
curl -s -X POST http://localhost:8000/submit_job \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"model_name": "lucc_continuous",
       "input_dataset": "/data/inputs/csAC.zip",
       "parameters": {"demand_csv": "/data/inputs/examples_demand_lab1.csv"}}'
```

!!! warning "DisSModel/dissmodel-configs"
    The specs currently in [DisSModel/dissmodel-configs](https://github.com/DisSModel/dissmodel-configs) point to packages that predate DisSLUCC 0.5.0 (`disslucc-continuous`, `disslucc-discrete`) and install them from branches. Use the example above until that repository is updated.

See [Model registry](registry.md) for the spec format.

## 8. Notebooks

`./workspace` is your JupyterLab home (`/home/jovyan/work`). It has examples that run DisSModel directly in the notebook (`ca_game_of_life.ipynb`, `sysdyn_sir.ipynb`) and one that reads results from MinIO (`carregando_resultado.ipynb`). `floodmodel.ipynb` expects `synthetic_grid_60x60_shp.zip` in the `dissmodel-inputs` bucket, which is not distributed with the platform.

## 9. Stop

```bash
docker compose down        # keeps MinIO and Redis data
docker compose down -v     # also deletes them: all inputs, outputs and records
```

Problems: see [Troubleshooting](deployment.md#troubleshooting).
