# Local Installation Checklist

This document records the local installation test of DisSModel Platform on Windows using Docker Desktop and Git Bash.

## Test environment

- Operating system: Windows
- Terminal: Git Bash
- Container runtime: Docker Desktop with WSL 2
- Docker Engine: 29.7.2
- Docker Compose: v5.5.1
- CPU allocated to Docker: 8 CPUs
- Memory allocated to Docker: approximately 3.7 GiB

## Repository setup

- [x] Forked `DisSModel/dissmodel-platform`
- [x] Cloned the fork locally
- [x] Created the `installation-check` branch
- [x] Copied `.env.example` to `.env`
- [x] Confirmed that `.env` is ignored by Git

Commands used:

    git clone https://github.com/hissa02/dissmodel-platform.git
    cd dissmodel-platform
    git switch -c installation-check origin/main
    cp .env.example .env
    git check-ignore -v .env

## Environment configuration

New local values were generated for the MinIO password and API key:

    openssl rand -hex 32
    openssl rand -hex 32

The real values are stored only in `.env` and are not included in this document.

The following variables were configured:

    MINIO_ROOT_USER=<local-user>
    MINIO_ROOT_PASSWORD=<redacted>
    API_KEYS=<redacted>
    CONFIGS_REPO=https://github.com/DisSModel/dissmodel-configs.git
    CONFIGS_BRANCH=main

The values from `.env.example` were not reused.

## Starting the platform

Because Docker Desktop had approximately 3.7 GiB of available memory, building all images in parallel caused the Docker Engine to stop responding. Building sequentially worked:

    docker compose --parallel 1 build
    docker compose up -d
    docker compose ps

The successful sequential build took approximately 7 minutes. An earlier parallel attempt was interrupted because of Docker Engine and memory limitations.

## Windows-specific observations

The API and Worker initially failed with:

    entrypoint.sh: no such file or directory

The scripts had Windows CRLF line endings. Converting them locally to LF allowed the containers to start:

    sed -i 's/\r$//' services/api/entrypoint.sh services/worker/entrypoint.sh
    docker compose build api worker
    docker compose up -d

The repository files were restored after testing because this task does not include source-code changes.

The image `minio/minio:RELEASE.2024-01-01T16-36-33Z` could not be pulled from Docker Hub. The equivalent image used temporarily for local execution was:

    quay.io/minio/minio:RELEASE.2024-01-01T16-36-33Z

No Docker Compose change is included in this documentation-only contribution.

## Service verification

| Service | Local address | Result |
| --- | --- | --- |
| JupyterLab | http://localhost:8888 | Opened successfully |
| API documentation | http://localhost:8000/docs | Opened successfully |
| MinIO console | http://localhost:19001 | Opened successfully |
| Redis | Port 6379 | Healthy |
| Worker | Internal service | Running |

The README lists the MinIO console at `http://localhost:9001`. In the tested configuration, container port 9001 was published as host port 19001.

The MinIO endpoint responded successfully:

    curl -s -o /dev/null -w "MinIO HTTP: %{http_code}\n" http://localhost:19000/minio/health/live

Response:

    MinIO HTTP: 200

Docker displayed MinIO as `unhealthy` because its health check calls `curl` inside the container, but `curl` is not included in the image:

    exec: "curl": executable file not found in $PATH

## API authentication and model catalog

The API key was read from `.env` without printing it:

    API_KEY=$(sed -n 's/^API_KEYS=//p' .env)

The catalog was requested with:

    curl -s -H "X-API-Key: $API_KEY" http://localhost:8000/models

The API returned:

- `brmangue_raster`
- `lucc_continuous_raster`
- `lucc_continuous_vector`
- `lucc_discrete_vector`

## Test job

- [ ] Upload a compatible input dataset
- [ ] Submit a job using `POST /submit_job`
- [ ] Save the returned experiment ID
- [ ] Check it using `GET /job/{experiment_id}`
- [ ] Confirm successful completion
- [ ] Confirm that the output was stored in MinIO

The test is pending because no input dataset is included in the repository.

The local input directory contains only:

    data/inputs/.gitkeep

The MinIO input and output buckets were created, but both were empty.

The notebook `workspace/floodmodel.ipynb` expects:

    dissmodel-inputs/synthetic_grid_60x60_shp.zip

That file is not included in the platform repository. It is mentioned in the documentation of `DisSModel/brmangue-dissmodel`, but its corresponding GitHub download path returns HTTP 404.

A compatible dataset and its expected model parameters were requested from the project maintainer. This section will be completed after they are provided.

## Differences from the current README

1. The MinIO console used port 19001 instead of 9001.
2. API requests require `X-API-Key`, which is absent from the README example.
3. `FloodModel` was not returned by the current model catalog.
4. `CONFIGS_REPO` had to be configured before models were listed.
5. No test dataset is included or linked.
6. Windows CRLF line endings prevented API and Worker startup.
7. The configured MinIO image could not be pulled from Docker Hub.

## Stopping the platform

To stop without deleting stored volumes:

    docker compose down

The following command was not used because it deletes the volumes:

    docker compose down -v