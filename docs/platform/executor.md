# ModelExecutor

The `ModelExecutor` is the abstract base class (ABC) that every simulation plugin must implement. It defines the lifecycle of a simulation on the platform, from data loading to result persistence.

## Interface Definition

```python
from abc import ABC, abstractmethod
from dissmodel.executor.schemas import ExperimentRecord

class ModelExecutor(ABC):
    """
    Base interface for all DisSModel platform executors.
    Concrete subclasses must implement the load/validate/run/save cycle.
    """

    @abstractmethod
    def load(self, record: ExperimentRecord) -> Any:
        """
        Resolve URIs and load data into memory (e.g., Xarray, GeoDataFrame).
        Should use record.source.uri and record.resolved_spec.
        """
        pass

    @abstractmethod
    def validate(self, data: Any, record: ExperimentRecord) -> None:
        """
        Verify if the loaded data matches the model requirements.
        Check CRS, dimensions, required bands/columns, and mapping.
        Raise ValueError on failure.
        """
        pass

    @abstractmethod
    def run(self, data: Any, record: ExperimentRecord) -> Any:
        """
        Execute the core simulation logic.
        This is where the actual modeling happens.
        """
        pass

    @abstractmethod
    def save(self, result: Any, record: ExperimentRecord) -> str:
        """
        Persist the result to the output storage (MinIO/S3).
        Must return the final S3 URI and update record.output_sha256.
        """
        pass
```

## Execution Lifecycle

The platform's generic runner executes the following sequence:

| Phase | Responsibility | Error Handling |
|---|---|---|
| **`load()`** | Fetch data from S3/HTTP; parse formats. | Retried by worker on network failure. |
| **`validate()`** | Contract verification (mapping, CRS). | Fails immediately (Invalid Request). |
| **`run()`** | Mathematical execution; temporal loops. | Logs stack trace to `ExperimentRecord`. |
| **`save()`** | Write artifacts; generate checksums. | Ensures output path is deterministic. |

## Auto-Registration Mechanism

Executors use Python's `__init_subclass__` to register themselves automatically in the `ExecutorRegistry`. The `class` field in the TOML spec connects the configuration to the implementation.

```python
# In dissmodel-executors repo:
class CoastalTiffExecutor(ModelExecutor):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        ExecutorRegistry.register(cls.__name__, cls)
```

## Executor Hierarchy

The following hierarchy represents the standard executors available:

*   `ModelExecutor` (ABC)
    *   `RasterModelExecutor` (Base for GeoTIFF/NetCDF)
        *   `CoastalTiffExecutor` — Sea Level Rise logic for rasters.
        *   `LUCCExecutor` — Potential/Allocation flow for land-use change.
    *   `VectorModelExecutor` (Base for Shapefile/GPKG)
        *   `CoastalVectorExecutor` — High-resolution coastal flooding.

### Key Differences: Raster vs. Vector

| Feature | `CoastalTiffExecutor` | `CoastalVectorExecutor` |
|---|---|---|
| **Data Structure** | Xarray / Dask | GeoDataFrame (Pandas-like) |
| **Mapping** | `band_map` | `column_map` |
| **Connectivity** | 4/8-way Adjacency | Topology-based (libpysal) |
| **Scaling** | Pixel-based (Chunked) | Entity-based (Vectorized) |

## The Generic Runner

When a worker picks up a job, it calls `run_experiment(record)`, which spawns a subprocess running `worker.job_runner`. This subprocess:
1.  Installs the required `package` from the spec.
2.  Imports the `executor_module`.
3.  Fetches the class from `ExecutorRegistry.get(class_key)`.
4.  Triggers the `execute_lifecycle(executor, record)`.
