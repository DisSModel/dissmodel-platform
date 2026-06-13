# DisSModel Platform: Overview

The **DisSModel Platform** is a FastAPI-powered service designed for the execution, tracking, and reproduction of Land Use and Cover Change (LUCC) simulations. Built upon the `dissmodel` core framework and integrated with the Pangeo ecosystem, it provides a robust infrastructure for scientific reproducibility, enabling researchers to transition seamlessly from local exploration in Jupyter notebooks to large-scale production runs.

## Architecture Layers

```text
┌───────────────────────────────────────────────────────────┐
│                  Researcher (Jupyter/CLI)                 │
└──────────────┬───────────────────────────┬────────────────┘
               │ (HTTPS + X-API-Key)       │ (S3/MinIO)
               ▼                           ▼
┌───────────────────────────┐     ┌─────────────────────────┐
│        FastAPI API        │     │      MinIO Storage      │
│  (Job submission, Status) ◄─────►   (Inputs, Outputs)     │
└──────────────┬────────────┘     └────────────▲────────────┘
               │ (Redis Queue)                 │
               ▼                               │
┌───────────────────────────┐                  │
│       Dask Worker         │──────────────────┘
│  (Generic Job Runner)     │
└──────────────┬────────────┘
               │ (Subprocess)
               ▼
┌───────────────────────────┐
│       Model Executor      │
│ (LUCC, Coastal, etc.)     │
└───────────────────────────┘
```

## Repository Ecosystem

| Repository | Artifact | Lifecycle |
|---|---|---|
| `dissmodel` | Core Library | Fundamental logic, ABCs, and Geo-primitives. |
| `dissmodel-platform` | Web Service | This repo. Handles API, Queue, and Workers. |
| `dissmodel-configs` | Registry | Git-versioned TOML specs and calibrated coefficients. |
| `dissmodel-executors` | Plugins | Collection of concrete `ModelExecutor` implementations. |

## Comparison

| Feature | DisSModel Platform | LuccME / TerraME | Pure Pangeo |
|---|---|---|---|
| **Reproducibility** | Native (ExperimentRecord) | File-based | Manual / Scripted |
| **Scaling** | Dask-ready Workers | Shared Memory / Cluster | Highly Scalable |
| **Interface** | REST API / Python | Lua / CLI | Python |
| **Versioning** | Git-based TOML | Manual | Manual |

## Documentation Sections

*   [ExperimentRecord](experiment-record.md): The immutable proof of provenance.
*   [Model Registry](registry.md): Decoupling configuration from code via TOML.
*   [ModelExecutor](executor.md): The core interface for simulation plugins.
*   [Variable Mapping](variable-mapping.md): Canonical vocabulary for data decoupling.
*   [API Reference](api-reference.md): Complete REST endpoint documentation.
*   [Data Management](data-management.md): Handling S3, HTTP, and local datasets.
*   [Executor Test Harness](executor-harness.md): Contract testing for developers.
*   [Security](security.md): Authentication and internal credentials.
*   [Researcher Guide](researcher-guide.md): Step-by-step from zero to publication.

## Out of Scope (v0.1)

| Feature | Status |
|---|---|
| Docker Sandbox | MVP runs in shared worker env (PR review required) |
| BDC / STAC Integration | Planned for Phase 2 |
| Automatic Zenodo Deposit | Manual JSON export for now |
| JWT / OAuth2 Auth | API Key only in MVP |
| Web Dashboard | API only; Jupyter used as frontend |
