# DisSModel Platform

The DisSModel Platform runs [DisSModel](https://github.com/DisSModel/dissmodel) executors as queued jobs on a server and keeps a record of each run.

- You submit a job to a REST API: a model (registered in a git repository of TOML specs, or sent inline), an input dataset and parameters.
- A worker runs the model's `ModelExecutor` in a separate Python process and stores the output in MinIO.
- Each run leaves an [`ExperimentRecord`](experiment-record.md): the spec it used, the input checksum, the versions of Python and of the packages involved, the output checksum and the logs.
- `POST /experiments/{id}/reproduce` runs it again from that record, and the worker reports whether the output is identical, file for file and value for value.

!!! note "Status: optional and experimental"
    Reproducing DisSModel, DisSCube and DisSLUCC results does not depend on this platform. The packages run from the command line, with pinned versions and the records they write; that is the path used in the published work. The platform is a convenience for running jobs on a shared server. It has not been hardened for exposure to the internet ([Security](security.md)).

## Where to start

| If you want to | Read |
|---|---|
| install it and run a first job | [Getting started](getting-started.md) |
| understand how a job moves through the services | [Architecture](architecture.md) |
| know what a run records and how to repeat or cite it | [ExperimentRecord](experiment-record.md) |
| register a model | [Model registry](registry.md) |
| bring your own model | [Executors](executors.md) |
| call the API | [API reference](api-reference.md) |
| set it up on a server | [Deployment](deployment.md) |

## What it is not

- It does not distribute one simulation across machines. Each job runs on one worker; more workers run more jobs at once.
- It does not integrate with Pangeo (Dask, Intake, Zarr) or the Brazil Data Cube.
- It does not deposit results in Zenodo; `/publish` returns the provenance as JSON.
