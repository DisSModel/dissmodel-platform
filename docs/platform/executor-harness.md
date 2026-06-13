# ExecutorTestHarness

The `ExecutorTestHarness` is a utility designed to ensure that a `ModelExecutor` implementation correctly follows the platform's contract. It is used both locally in Jupyter notebooks and automatically in CI/CD pipelines.

## Purpose

The harness bridge the gap between development and production:
1.  **Contract Verification:** Checks if the class has the required methods and type hints.
2.  **Structural Integrity:** Verifies if the executor correctly auto-registers in the `ExecutorRegistry`.
3.  **Sanity Check:** Runs the `load/validate/run/save` cycle with sample data to catch runtime errors early.

## Automated Checks

The harness performs the following internal checks:

| Method | Check Description |
|---|---|
| `_check_name` | Ensures the class name matches the registry key. |
| `_check_methods` | Verifies presence of `load`, `validate`, `run`, and `save`. |
| `_check_annotations` | Ensures type hints match the `ModelExecutor` ABC. |
| `run_contract_tests()` | Executes all structural checks above. |
| `run_with_sample_data()` | Executes the full lifecycle with a dummy `ExperimentRecord`. |

## Usage Examples

### 1. In a Jupyter Notebook

Researchers should use the harness before opening a Pull Request to the `dissmodel-executors` repository.

```python
from dissmodel.executor.testing import ExecutorTestHarness
from my_new_executor import CoastalTiffExecutor

# Step 1: Structural check
harness = ExecutorTestHarness(CoastalTiffExecutor)
if harness.run_contract_tests():
    print("✅ Contract is valid!")

# Step 2: Runtime check with sample record
record = ExperimentRecord(
    model_name="test",
    resolved_spec={"model": {"parameters": {"taxa": 0.01}}},
    source={"uri": "local_test.tif", "type": "local"}
)
harness.run_with_sample_data(record)
```

### 2. In CI/CD (Pytest)

The `dissmodel-platform` includes a script (`scripts/validate_executors.py`) that uses the harness to automatically validate all executors in the `executors/` folder.

```python
# scripts/validate_executors.py logic
for name, cls in ExecutorRegistry.items():
    harness = ExecutorTestHarness(cls)
    assert harness.run_contract_tests(), f"Executor {name} failed contract"
```

## Adding Custom Unit Tests

While the harness covers the platform's requirements, you should add model-specific tests to verify mathematical correctness:

```python
def test_coastal_logic():
    executor = CoastalTiffExecutor()
    # Mocking data...
    result = executor.run(mock_data, mock_record)
    assert result.mean() > 0  # Custom assertion
```

!!! tip "Plugin Integration"
    A `ModelExecutor` that passes the harness tests is guaranteed to be compatible with the platform's worker and API, significantly reducing integration bugs.
