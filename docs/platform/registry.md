# Model Registry (TOML)

The Model Registry is a centralized repository of git-versioned TOML files that define the configuration, parameters, and canonical vocabulary for simulations. This pattern decouples the model's configuration from its Python implementation.

## Design Principle: Configuration as Code

By storing coefficients and model specifications in a separate `dissmodel-configs` repository, the platform ensures:
1.  **Version Control:** Every change to a model parameter is tracked via git.
2.  **Consistency:** All researchers use the same calibrated coefficients for a given model version.
3.  **Transparency:** The exact spec used for an experiment is snapshotted in the [ExperimentRecord](experiment-record.md).

## TOML Structure

A typical model specification (`models/coastal-v1.toml`) looks like this:

```toml
[model]
name        = "coastal-v1"
description = "Coastal dynamics model with Sea Level Rise"
class       = "CoastalTiffExecutor"          # Registry key for the Python class
package     = "dissmodel-executors==0.1.0"   # PyPI/Git/Local package to install

[model.parameters]
taxa_elevacao = 0.011                        # Default value (can be overridden)
threshold     = 0.5

[model.bands]                                # Canonical Vocabulary (Raster)
elevation = "alt"
land_use  = "uso"

[model.columns]                              # Canonical Vocabulary (Vector)
id_cell   = "id"
soil_type = "solo"

[[model.potential]]                          # Complex nested structures
name = "deforestation"
[model.potential.betas]
distance_to_roads = -0.45
slope             = 1.2
```

## Canonical Vocabulary

The `[model.bands]` and `[model.columns]` sections define the **Canonical Vocabulary**. The `ModelExecutor` code uses these generic names (e.g., `elevation`), while the `band_map` / `column_map` provided in the API request maps them to the actual names in the dataset (e.g., `SRTM_B1`).

This allows the same model code to run on different datasets without modification. See [Variable Mapping](variable-mapping.md) for details.

## Synchronization Flow

The platform keeps its local cache in sync with the `dissmodel-configs` repository:

1.  **Git Push:** A researcher pushes a new TOML or updates an existing one to the `main` branch of `dissmodel-configs`.
2.  **Background Sync:** Every 15 minutes (configurable), an `APScheduler` job runs `git pull` on the worker/API nodes.
3.  **Cache Invalidation:** If changes are detected, the `lru_cache` of `load_model_spec()` is cleared.
4.  **Instant Availability:** New models or parameters are immediately visible via `GET /models`.

### Manual Synchronization

To force an immediate sync without waiting for the scheduler:

```bash
curl -X POST http://localhost:8000/admin/sync -H "X-API-Key: your-token"
```

## Inline Specs (Jupyter)

For rapid exploration, you can bypass the registry using `POST /submit_job_inline`.

!!! tip "Exploration vs. Production"
    Use inline specs to test new model structures. Once the logic is stable, move the spec to the `dissmodel-configs` repo to gain git-versioning and full reproducibility.

**Limitations of Inline Specs:**
*   `model_commit` is marked as `local-inline`.
*   Not reproducible via the standard registry flow.
*   Required for development in Jupyter before opening a PR to the configs repo.
