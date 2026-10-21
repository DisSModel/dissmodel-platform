# Deployment

The platform is distributed as two Docker Compose files:

| File | For | Exposes |
|---|---|---|
| `docker-compose.yml` | a workstation or a development server | API on 8000, JupyterLab on 8888, MinIO on 19000 (API) and 19001 (console), Redis on 6379 |
| `docker-compose.prod.yml` | a server behind one port | nginx on port 80 only, with everything under `/dissmodel/` |

There are no Kubernetes manifests.

## Production stack

```bash
cp .env.example .env     # replace every "change-me"
docker compose -f docker-compose.prod.yml up -d --build
```

| Path | Service |
|---|---|
| `/dissmodel/api/` | API (OpenAPI page at `/dissmodel/api/docs`) |
| `/dissmodel/jupyter/` | JupyterLab |
| `/dissmodel/minio/`, `/dissmodel/minio/ui/` | MinIO API and console |
| `/dissmodel/ca/`, `/dissmodel/sysdyn/` | Streamlit explorers for `dissmodel-ca` and `dissmodel-sysdyn` |

Before exposing it:

- Put TLS in front of nginx (a reverse proxy or a certificate in `services/nginx/nginx.conf`); the stack itself serves plain HTTP.
- JupyterLab starts **without a token** in both Compose files. Anyone who reaches the port can run code on the server. Set a password or token, or keep JupyterLab off the public network. See [Security](security.md).
- Set `MINIO_URL` to the public address of the MinIO API (for example `https://host/dissmodel/minio`), so download links work from outside.

## Environment variables

Read from `.env` by Compose. `.env.example` lists them all with their defaults; only `MINIO_ROOT_PASSWORD` and `API_KEYS` must be set.

| Variable | Used by | Default |
|---|---|---|
| `MINIO_ROOT_PASSWORD` | MinIO; passed to API and worker as `MINIO_SECRET_KEY` | `inpe_secret_2024` — public, replace |
| `MINIO_ROOT_USER` | MinIO; passed as `MINIO_ACCESS_KEY` | `inpe_admin` |
| `MINIO_URL` | API, presigned links | `http://localhost:19000` |
| `API_KEYS` | API | `dev-key` — public, replace |
| `MINIO_ENDPOINT`, `REDIS_HOST`, `REDIS_PORT` | API, worker, JupyterLab (development stack; fixed in the production file) | `minio:9000`, `redis`, `6379` |
| `API_PORT`, `JUPYTER_PORT` | host ports (development stack) | 8000, 8888 |
| `CONFIGS_REPO`, `CONFIGS_BRANCH`, `CONFIGS_SYNC_INTERVAL` | `configs-sync` | empty, `main`, 900 s |
| `DISSMODEL_OUTPUT_ROOT` | API (sets each record's `output_path`) | `s3://dissmodel-outputs/experiments` |
| `DISSMODEL_LOG_LEVEL` | API, worker | `INFO` |

Set inside the Compose files: `CONFIGS_PATH` (`/configs`). The code also reads `REDIS_PASSWORD`, `WORKER_ID` and `MINIO_SECURE`. Leave `MINIO_SECURE` unset unless MinIO is served over TLS: `dissmodel` 0.6.6 treats any non-empty value, including `false`, as true.

## Resources

The development stack limits each worker to 2 CPUs and 4 GB of memory (`deploy.resources`). A job that needs more is usually killed by the kernel and recorded as failed; raise the limit in the Compose file. One worker runs one job at a time; add workers with `--scale worker=N`.

## Updating

```bash
git pull
docker compose up -d --build
```

The versions of `dissmodel`, `disscube` and `disslucc` are pinned in `services/*/requirements.txt`. Change them together, rebuild, and note the change in `CHANGELOG.md`: records made before and after carry different `code_version` and `metrics['environment']`, and a reproduction across versions is logged as such.

## Backups

What matters is in two Docker volumes and one file:

| Data | Where |
|---|---|
| inputs, outputs, record JSONs | volume `dissmodel-minio-data` (`dissmodel-prod-minio-data` in production) |
| records and queues | volume `dissmodel-redis-data` (`dissmodel-prod-redis-data`) |
| secrets | `.env` |

The record JSON written next to each output in MinIO is a copy of the final record, so the MinIO volume alone is enough to keep the provenance of finished runs.

`scripts/backup.sh` stops the stack, copies both volumes and `.env` to `backups/<date>/` and starts the stack again:

```bash
scripts/backup.sh                                       # development stack
COMPOSE_FILE=docker-compose.prod.yml scripts/backup.sh  # production stack
```

To restore, with the stack stopped, unpack each archive into its volume:

```bash
docker compose down
for v in dissmodel-minio-data dissmodel-redis-data; do
  docker volume create "$v"
  docker run --rm -v "$v":/v -v "$PWD/backups/<date>":/b alpine \
    sh -c "rm -rf /v/* && tar xzf /b/$v.tar.gz -C /v"
done
docker compose up -d
```

## Troubleshooting

**`entrypoint.sh: no such file or directory` (Windows).** The shell scripts were checked out with CRLF line endings. The repository now forces LF (`.gitattributes`); in an older clone, run `git rm --cached -r . && git reset --hard`, or convert with `sed -i 's/\r$//' services/*/entrypoint.sh`, then rebuild.

**The build stops responding.** Not enough memory to build the images in parallel. Use `docker compose --parallel 1 build`.

**The MinIO image cannot be pulled from Docker Hub.** The same image is published on Quay: set `image: quay.io/minio/minio:RELEASE.2024-01-01T16-36-33Z` in the Compose file.

**`GET /models` returns an empty list.** `CONFIGS_REPO` is empty, or the clone failed: `docker compose logs configs-sync`.

**A job stays `pending`.** No worker is consuming the queue: `docker compose ps worker` and `docker compose logs worker`.

**A job fails with `Executor '<name>' not registered`.** `model.class` does not match the `name` of any executor, or `model.executor_module` is missing or wrong. The error lists the executors that are registered.

**A job fails with `Missing parameters`.** The executor's required parameters are not in the spec or the request; the record's `parameters` field shows what it received.

**Health:** `scripts/health-check.sh` checks the containers and the HTTP endpoints of the development stack.
