# DisSModel Platform Documentation

Welcome to the official documentation for the **DisSModel Platform**.

The DisSModel Platform is a robust service designed for the execution, tracking, and reproduction of Land Use and Cover Change (LUCC) simulations. It provides a standardized environment for scientific modeling, ensuring that experiments are audit-ready and fully reproducible.

> **Status: optional and experimental.** The platform runs DisSModel executors behind a REST API and keeps an `ExperimentRecord` for each run. Reproducing DisSModel, DisSCube and DisSLUCC results does not depend on it: the packages run on their own from the command line, with pinned versions and the records they write (see the Open LUCC workflow). The platform is a convenience for running jobs on a shared server.

## Key Features

*   **Immutable Provenance:** Every simulation is recorded with a complete snapshot of its configuration and data integrity.
*   **Model Registry:** Decouples model parameters from implementation via version-controlled TOML files.
*   **Isolated Execution:** A Redis queue feeds one or more workers; each job runs in its own Python subprocess.
*   **Researcher-Centric:** Designed to facilitate the transition from local Jupyter exploration to large-scale production.

## Getting Started

To understand how the platform works, we recommend starting with the following sections:

1.  [**Platform Overview**](platform/index.md) - Learn about the architecture and core components.
2.  [**Researcher Guide**](platform/researcher-guide.md) - A step-by-step guide to developing and running models.
3.  [**API Reference**](platform/api-reference.md) - Complete documentation of the available REST endpoints.

---
*This documentation was automatically generated from the source code.*
