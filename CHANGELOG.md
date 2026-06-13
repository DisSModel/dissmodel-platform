# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-06-13

First public release of the DisSModel Platform MVP.

### Added

- **REST API** (`services/api`) — FastAPI gateway with `X-API-Key` authentication applied to all routes; endpoints for job submission (async and inline), status polling, reproduction, publishing, model listing, file upload/download, and admin sync.
- **Worker** (`services/worker`) — Redis-queue consumer that delegates execution to `dissmodel.executor.runner.execute_lifecycle`; publishes profiling metrics and `ExperimentRecord` JSON to MinIO.
- **JupyterLab** (`services/jupyter`) — Containerised notebook environment (port 8888) with `dissmodel`, `ipyleaflet`, `ipywidgets`, and `folium` pre-installed.
- **Streamlit CA Explorer** (`services/streamlit-ca`) — Interactive explorer for Cellular Automata models.
- **Streamlit SysDyn Explorer** (`services/streamlit-sysdyn`) — Interactive explorer for System Dynamics models.
- **Nginx reverse proxy** (`services/nginx`) — Routes `/dissmodel/jupyter`, `/dissmodel/api`, `/dissmodel/minio` in production.
- **Docker Compose** — Development (`docker-compose.yml`) and production (`docker-compose.prod.yml`) stacks with Redis, MinIO, config-sync sidecar, and all services.
- **CI pipeline** (`.github/workflows/ci.yml`) — Lint (ruff), type-check (mypy), security scan (bandit), API tests, worker executor validation, and Docker build jobs; all gates are enforced (no `|| true` bypasses).
- **Dependabot** — Automated dependency updates for `services/api`, `services/worker`, and `services/jupyter`.
- **Presigned URL generation** — Local HMAC signing for MinIO download links without extra network round-trips.
- **Config-sync sidecar** — Git-backed model registry auto-pulled into all services at runtime.
- **Executor contract validation** (`scripts/validate_executors.py`) — CI step that checks registered executors comply with the dissmodel interface before tests run.

### Fixed

- Moved mid-file imports (`hmac`, `hashlib`, `urllib.parse`, `datetime.timezone`) to module top in `services/api/main.py`.
- Removed unused imports (`json`, `timedelta`, `S3Error`, `start_sync_scheduler`, `reproduce_experiment`, `run_experiment`) from `services/api/main.py` and `services/worker/storage.py`.
- Added `# type: ignore[misc]` for redis-py sync/async stub ambiguity in `services/worker/worker.py`.
- Added `# nosec B104` and `# nosec B310` for intentional false positives in bandit scan.

### Changed

- Renamed `services/frontend/` → `services/jupyter/` to reflect that the service is JupyterLab, not a generic web frontend.
- Updated all repository URLs from `LambdaGeo/dissmodel-platform` → `DisSModel/dissmodel-platform` in `README.md` and `docs/deployment.md`.
- Updated core library link from `LambdaGeo/dissmodel` → `DisSModel/dissmodel`.
- Updated organisation name from `LambdaGeo / INPE` → `DisSModel / INPE` in `README.md` contact section.
- `typecheck` CI job now sets `PYTHONPATH=$PWD/services` so worker imports resolve correctly without stubs.

[Unreleased]: https://github.com/DisSModel/dissmodel-platform/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/DisSModel/dissmodel-platform/releases/tag/v0.1.0
