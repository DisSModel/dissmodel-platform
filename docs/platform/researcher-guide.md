# Researcher Guide

This guide takes you through the complete lifecycle of a simulation, from local development in a Jupyter notebook to large-scale execution and academic publication.

## Phase 1: Local Development (Jupyter)

The goal of this phase is to implement and test your simulation logic.

1.  **Installation:**
    ```bash
    pip install dissmodel dissmodel-platform
    ```
2.  **Implementation:**
    Create a new subclass of `ModelExecutor` in your notebook. Implement the `load`, `validate`, `run`, and `save` methods.
3.  **Validation:**
    Use the `ExecutorTestHarness` to verify your implementation:
    ```python
    from dissmodel.executor.testing import ExecutorTestHarness
    harness = ExecutorTestHarness(MyExecutor)
    harness.run_contract_tests()
    ```
4.  **Integration:**
    Move your class to a file (e.g., `my_model.py`) in the `dissmodel-executors` repository and open a Pull Request.

## Phase 2: Model Registration

Once your code is in the repository, you need to register the calibrated coefficients.

1.  **Calibration:** Calibrate your model parameters using your preferred tools (R, Python, etc.).
2.  **Create TOML:** Add a new `.toml` file to `dissmodel-configs/models/`. Define the `class`, `package`, `parameters`, and canonical vocabulary.
3.  **Sync:** `git push` your changes. The platform will sync automatically in 15 minutes, or you can force it:
    ```bash
    curl -X POST http://localhost:8000/admin/sync -H "X-API-Key: your-token"
    ```

## Phase 3: Platform Execution

Now you can run the simulation on the platform's production workers.

1.  **Upload Data:**
    ```bash
    curl -X POST http://localhost:8000/data/upload \
      -F "file=@large_dataset.tif" -F "label=baseline"
    ```
2.  **Submit Job:**
    ```bash
    curl -X POST http://localhost:8000/submit_job \
      -d '{
        "model_name": "my-model-v1",
        "input_dataset": "s3://dissmodel-inputs/inputs/baseline/large_dataset.tif",
        "input_format": "geotiff",
        "parameters": {"taxa": 0.05}
      }'
    ```
3.  **Track Status:**
    Use `GET /job/{id}` to follow the progress.

## Phase 4: Reproduction & Publication

To ensure your results are valid for a paper:

1.  **Verify Reproduction:**
    ```bash
    curl -X POST http://localhost:8000/experiments/{id}/reproduce
    ```
    Compare the `output_sha256` of the original and the reproduced job. They must be identical.
2.  **Publish Package:**
    Use `POST /experiments/{id}/publish` to export the JSON provenance package.
3.  **Methodological Citation:**
    Include the `experiment_id`, `model_commit`, and `input_sha256` in your paper.

## Mapping: Original Script → Platform

| Script Concept | DisSModel Platform Equivalent |
|---|---|
| Hardcoded Paths | `DataSource` URIs (`s3://`) |
| Global Variables | `model.parameters` in TOML |
| Column/Band Names | [Variable Mapping](variable-mapping.md) |
| Local Loops | `ModelExecutor.run()` method |
| `plt.show()` | `Map` and `Chart` integration |
| Result saving | `ModelExecutor.save()` (auto-upload) |
| Log files | `ExperimentRecord.logs` |

!!! tip "Full Reproducibility"
    Always prefer submitting jobs via the Registry (`/submit_job`) for final results. Inline jobs (`/submit_job_inline`) are excellent for debugging but lack the git-versioned proof of provenance required for high-impact publications.
