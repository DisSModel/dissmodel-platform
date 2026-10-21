# DisSModel Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11](https://img.shields.io/badge/Python-3.11-green.svg)](https://www.python.org/)

A small server for running [DisSModel](https://github.com/DisSModel/dissmodel) executors as queued jobs. You submit a job to a REST API, a worker runs it, and every run leaves an `ExperimentRecord`: the model spec, the input checksum, the package versions, the output and its checksum. A run can be repeated from its record, and the worker reports whether the new output is identical to the original.

> **Status: optional and experimental (MVP).** Reproducing DisSModel, DisSCube and DisSLUCC results does not depend on this platform: the packages run on their own from the command line, with pinned versions and the records they write. The platform is a convenience for running jobs on a shared server. It has not been hardened for exposure to the internet; see [Security](docs/security.md).

## What it runs

Any class that implements DisSModel's `ModelExecutor` and is installed in the worker. The worker image ships with:

| Package | Version | Executors |
|---|---|---|
| `dissmodel` | 0.6.6 | core (`ModelExecutor`, `ExperimentRecord`) |
| `disslucc` | 0.5.0 | `lucc_continuous`, `lucc_discrete`, `lucc_continuous_saturation` |

Other executors are installed from the `package` field of a model spec (see [Model registry](docs/registry.md)).

## Quick start

Requires Docker with Compose v2 and about 4 GB of free memory for the build.

```bash
git clone https://github.com/DisSModel/dissmodel-platform.git
cd dissmodel-platform
cp .env.example .env        # then replace every "change-me"
docker compose up -d --build
```

| Service | Address |
|---|---|
| API (OpenAPI docs) | http://localhost:8000/docs |
| JupyterLab | http://localhost:8888 |
| MinIO console | http://localhost:19001 |

First job: LuccME Lab 1 with DisSLUCC, a few seconds of computing.

```bash
base=https://raw.githubusercontent.com/DisSModel/disslucc/main/data/input
curl -sLo data/inputs/csAC.zip "$base/csAC.zip"
curl -sLo data/inputs/examples_demand_lab1.csv "$base/examples_demand_lab1.csv"

DISSMODEL_API_KEY=<one of your API_KEYS> python3 examples/submit_lab1.py
```

The script submits the job, waits for it, asks for a reproduction and prints whether the two outputs are identical. [Getting started](docs/getting-started.md) explains each step.

## Documentation

- [Getting started](docs/getting-started.md): install, first job, where the results go
- [Architecture](docs/architecture.md): services and the path of a job
- [ExperimentRecord](docs/experiment-record.md): what is recorded; reproducing and citing a run
- [Model registry](docs/registry.md): model specs in TOML and how parameters are merged
- [Executors](docs/executors.md): writing and testing a `ModelExecutor`
- [Input and output data](docs/data.md)
- [API reference](docs/api-reference.md)
- [Security](docs/security.md)
- [Deployment](docs/deployment.md): server setup, backups, troubleshooting

The same pages build as a site with MkDocs (`pip install mkdocs-material && mkdocs serve`).

## Development

```bash
pip install -r services/worker/requirements.txt -r services/api/requirements.txt -r requirements-test.txt
PYTHONPATH=services pytest services/worker/tests services/api/tests
```

The tests need neither Docker, Redis nor MinIO. Contribution workflow: [CONTRIBUTING.md](CONTRIBUTING.md); changes: [CHANGELOG.md](CHANGELOG.md).

## Team

- **Sérgio Souza Costa** (UFMA, LambdaGeo) — maintainer
- **Hissa Barbara Oliveira** — PIBIT 2026, Plano 3: Prototipagem de Plataforma Cloud-Native para Simulação Espacial Distribuída com DisSModel

## License

MIT — see [LICENSE](LICENSE).
