# Executors

An executor is a subclass of DisSModel's `ModelExecutor` (`dissmodel.executor`). The platform runs any executor it can import; the contract is defined in `dissmodel`, not here, so an executor written for the command line runs on the platform unchanged.

## The contract

```python
from dissmodel.executor import ModelExecutor
from dissmodel.executor.schemas import ExperimentRecord
from dissmodel.io import load_dataset, save_dataset   # s3://, http(s):// and local paths


class MyExecutor(ModelExecutor):
    name = "my_model"            # registry key; a model spec selects it with class = "my_model"

    def validate(self, record: ExperimentRecord) -> None:
        # Optional. Checks on the record only, before any data is read.
        if "n_steps" not in record.parameters:
            raise ValueError("n_steps is required")

    def load(self, record: ExperimentRecord):
        # Read the input; fill the input checksum.
        (backend, meta), checksum = load_dataset(record.source.uri, fmt="raster")
        record.source.checksum = checksum
        return backend

    def run(self, data, record: ExperimentRecord):
        # Simulate. No reading or writing of files here.
        ...
        return data

    def save(self, result, record: ExperimentRecord) -> ExperimentRecord:
        # Write the output to record.output_path and record its checksum.
        uri = record.output_path
        record.artifacts["output"] = save_dataset(
            (result, {"crs": result.crs, "transform": result.transform}), uri)
        record.status = "completed"
        return record
```

Defining the class registers it (`ExecutorRegistry`, through `__init_subclass__`); nothing else is needed.

The worker calls `validate`, `load`, `run` and `save` in that order (`dissmodel.executor.runner.execute_lifecycle`) and times each one. Rules that keep a run reproducible:

- Read everything the run depends on from `record.parameters` and `record.source`; nothing from environment variables or files outside the record.
- Set `record.source.checksum` in `load`, and `record.artifacts["output"]` in `save`.
- Write the output to `record.output_path` when it is set. The platform always sets it (one folder per experiment); without it, the output ends up in the worker's working directory, inside the container.
- Put any numbers worth keeping in `record.metrics` (for example final areas); they are stored with the record.

The same class runs from the command line with `run_cli`:

```python
if __name__ == "__main__":
    from dissmodel.executor.cli import run_cli
    run_cli(MyExecutor)
```

```bash
python -m my_package.my_executor run --toml model.toml --input data.zip --output out.tif
```

## Available executors

| Package | `class` | Input |
|---|---|---|
| `disslucc` 0.5.0 (in the worker image) | `lucc_continuous` | vector (shapefile, GeoPackage, zipped shapefile) or GeoTIFF; continuous land use (CLUE-like allocation) |
| | `lucc_discrete` | the same; discrete land use (CLUE-S-like allocation) |
| | `lucc_continuous_saturation` | GeoTIFF with named bands; allocation with saturation, by region |
| [`brmangue-dissmodel`](https://github.com/DisSModel/brmangue-dissmodel) | see its documentation | installed through `package` |

For DisSLUCC, use `executor_module = "disslucc.executors"`.

## Getting an executor onto the platform

Pick one:

1. **A package (recommended).** Publish the executor in a package (PyPI or a git repository) and name it in the spec: `package = "my-package==1.0.0"` and `executor_module = "my_package.executors"`. The worker installs it before the job. Nothing changes in the platform.
2. **In the worker image.** Add the package to `services/worker/requirements.txt` with an exact version and rebuild. Use this for executors that most jobs need, as with DisSLUCC.
3. **Bundled.** Put a module in `services/worker/executors/`; the job runner imports that package on every job. Then also import your module in `services/worker/executors/__init__.py`.

## Testing

`ExecutorTestHarness` (from `dissmodel.executor.testing`) checks the contract without data, and runs the whole cycle when given a record:

```python
from dissmodel.executor.schemas import ExperimentRecord
from dissmodel.executor.testing import ExecutorTestHarness

harness = ExecutorTestHarness(MyExecutor)
harness.run_contract_tests()          # structure only

record = ExperimentRecord(parameters={"n_steps": 3}, output_path="out.tif")
record.source.uri = "tests/data/small.tif"
harness.run_with_sample_data(record)  # validate → load → run → save
```

`run_contract_tests` checks that `name` is a non-empty string without spaces, that `load`, `run` and `save` are implemented, and that `run` and `save` take two arguments besides `self`. `run_with_sample_data` runs the cycle and warns when `save` does not set `status = "completed"` or the output checksum.

The CI of this repository runs the contract checks on the executors in `services/worker/executors/` (`scripts/validate_executors.py`). Tests of what a model computes belong in the executor's own package.

## Variable mapping

A model expects its input columns or bands under fixed names. When a dataset uses other names, the request can map them:

```json
{
  "model_name": "lucc_continuous",
  "input_dataset": "s3://dissmodel-inputs/acre_2008.gpkg",
  "column_map": {"assentamen": "assentamento_rural", "uc_us": "UC_Uso_Sustentavel"}
}
```

Keys are the names the model uses; values are the names in the dataset. `column_map` is for vector inputs and `band_map` for GeoTIFF bands. Both are stored in the record and passed to the executor; **applying them is up to the executor**. DisSLUCC 0.5.0 applies `column_map` to vector inputs and ignores `band_map`. When the dataset already uses the model's names, leave the maps out.
