# DisSModel Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://docs.docker.com/compose/)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11+-green.svg)](https://www.python.org/)

Scalable execution platform for **DisSModel** (Distributed Spatial Simulation Model).

An integrated environment for developing and running geospatial models, featuring JupyterLab, a REST API, and distributed workers.

## 🌍 Where It Runs

- ✅ Local server (desktop/laptop)
- ✅ On-premise cluster (INPE, universities)
- ✅ Private cloud (OpenStack, etc.)
- ✅ Public cloud (AWS, GCP, Azure) — optional

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- 8 GB+ RAM recommended
- 20 GB+ free disk space

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/LambdaGeo/dissmodel-platform.git
cd dissmodel-platform

# 2. Configure environment variables
cp .env.example .env

# 3. Start the platform
docker compose up --build

# 4. Access services
# JupyterLab: http://localhost:8888
# API Docs:   http://localhost:8000/docs
# MinIO:      http://localhost:9001
```

### Stop the Platform

```bash
docker compose down
```

### Stop and Remove Data

```bash
docker compose down -v  # Removes volumes (use with caution!)
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DISSMODEL PLATFORM                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [Researcher] → Browser → JupyterLab (Container)               │
│                              │                                  │
│                              ├── Direct Python (imports)        │
│                              └── REST API (heavy jobs)          │
│                              │                                  │
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              SERVER / DOCKER HOST                        │   │
│  │                                                          │   │
│  │  🟦 Jupyter    🟩 API      🟥 Worker    🗄️ MinIO  🔄 Redis │   │
│  │  (Frontend)  (Gateway)   (Processing) (Storage) (Queue)  │   │
│  │                                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Service | Port | Description |
|---------|------|-------------|
| **JupyterLab** | 8888 | Python development environment |
| **API Gateway** | 8000 | FastAPI for job submission |
| **Worker** | — | Background processing (scalable) |
| **MinIO** | 9000/9001 | S3-compatible object storage |
| **Redis** | 6379 | Message queue and cache |

## 📚 Documentation

- [Architecture](docs/architecture.md) — Technical details and design decisions
- [Deployment](docs/deployment.md) — How to deploy in different environments
- [User Guide](docs/user-guide.md) — For researchers

## 📋 Usage Examples

### Local Development (Direct Python)

```python
from dissmodel.core import Environment
from dissmodel.geo.raster import RasterBackend

# Load data
backend = RasterBackend.from_file('/data/inputs/dem.tif')

# Configure and run model
env = Environment(end_time=100)
model = FloodModel(backend=backend, sea_level=0.05)
results = env.run()

# Visualise
model.display()
```

### API Execution (Heavy Jobs)

```python
import requests

payload = {
    "model_name": "FloodModel",
    "input_dataset": "dem.tif",
    "parameters": {"sea_level": 0.05, "steps": 1000}
}

response = requests.post(
    "http://api:8000/submit_job",
    json=payload
)

job_id = response.json()['job_id']
```

## 🔧 Configuration

### Environment Variables

Copy `.env.example` to `.env` and adjust as needed:

```bash
# MinIO Credentials
MINIO_ROOT_USER=user
MINIO_ROOT_PASSWORD=user_password

# DisSModel Config
DISSMODEL_LOG_LEVEL=INFO
DISSMODEL_CLOUD=false
```

### Scaling Workers

```bash
# Scale up to 5 workers
docker compose up --scale worker=5
```

## 🤝 Contributing

1. Fork the repository
2. Create a branch (`git checkout -b feature/new-feature`)
3. Commit your changes (`git commit -m 'Add new feature'`)
4. Push to the branch (`git push origin feature/new-feature`)
5. Open a Pull Request

See [docs/developer-guide.md](docs/developer-guide.md) for more details.

## 📄 License

MIT License — see [LICENSE](LICENSE)

## 🙏 Acknowledgements

- [DisSModel](https://github.com/LambdaGeo/dissmodel) — Core modelling library
- [Jupyter Project](https://jupyter.org/) — Development environment
- [MinIO](https://min.io/) — S3-compatible object storage
- [Pangeo](https://pangeo.io/) — Inspiration for cloud-native architecture

## 📞 Contact

- **Organisation:** LambdaGeo / INPE
- **Issues:** https://github.com/LambdaGeo/dissmodel-platform/issues
- **Discussions:** https://github.com/LambdaGeo/dissmodel-platform/discussions
