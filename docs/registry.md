# Model registry

A model spec is a TOML file that says which executor to run and with which coefficients. Specs live in a git repository (the registry), one file per model in `models/`. Keeping them in git means each record can point to the commit that defined the model, and calibrated coefficients are data under version control rather than constants in code.

## Spec format

The [Lab 1 example](https://github.com/DisSModel/dissmodel-platform/blob/main/examples/lucc_continuous_lab1.toml), shortened:

```toml
[model]
# Registration
name             = "lucc_continuous"
class            = "lucc_continuous"       # ExecutorRegistry key = the executor's `name`
executor_module  = "disslucc.executors"    # imported so that the executor registers itself
description      = "LUCC continuous simulation (CLUE-like, linear regression potential)"
dissmodel        = ">=0.6.0,<0.7.0"        # dissmodel versions the model was written for
# package        = "disslucc==0.5.0"       # installed before the run, if not in the image

# Model fields: passed to the executor
land_use_types   = ["f", "d", "outros"]
complementar_lu  = "f"
cell_area        = 25.0
resolution       = 5000.0

[model.static]
f      = -1
d      = -1
outros = 1

[[model.potential_data]]
lu    = "f"
const = 0.7392
  [model.potential_data.betas]
  assentamen = -0.2193
  uc_us      =  0.1754

# Run values: defaults that a request may override
[model.parameters]
n_steps = 7
```

### Registration keys

| Key | Required | Meaning |
|---|---|---|
| `class` | yes | the `name` of the `ModelExecutor` to run |
| `executor_module` | when the executor is not bundled with the platform | module or package to import; for a package, its direct submodules are imported too |
| `package` | no | pip requirement installed in the worker before the run: `name==1.2.3`, `git+https://...@<commit>`, or a local path (development) |
| `dissmodel` | no | version specifier; a mismatch with the installed `dissmodel` is logged |
| `name`, `description` | no | shown by `GET /models` |
| `output_suffix` | no | extension of `output_path` (default `.tif`) |

Pin `package` to one version (`==` or a full commit hash). A branch name or a range makes the record unable to say which code ran; the platform logs a warning, and the installed version still appears in `metrics['environment']`.

## Parameters

The executor reads its configuration from `record.parameters`, built in three layers, each overriding the previous one:

1. every key of `[model]` that is not a registration key or `[model.parameters]` (above: `land_use_types`, `static`, `potential_data`, ...);
2. `[model.parameters]`;
3. the `parameters` of the request.

This is the same merge that `dissmodel`'s command line does with `--toml`, so a spec behaves the same on the platform and in `python -m <executor module> run --toml ...`.

Values that change per run without changing the model (number of steps, an input-specific path such as `demand_csv`) go in the request. Calibrated coefficients go in the spec: a run that overrides them in the request is recorded, but the commit no longer describes the model that ran.

## Synchronisation

The `configs-sync` container clones `CONFIGS_REPO` (branch `CONFIGS_BRANCH`) into a volume and runs `git pull` every `CONFIGS_SYNC_INTERVAL` seconds (default 900). The API and the worker see the volume read-only and read the spec from disk at every submission, so a pulled change applies to the next job.

To publish a change:

1. Commit and push the spec to the registry repository.
2. Wait for the next pull, or restart the sidecar to pull now: `docker compose restart configs-sync`.
3. `POST /admin/sync` reports the commit (`head`) that new jobs will record.

A record keeps the commit and the full spec it was created with; a later change to the file does not affect it, nor its reproductions.

## Inline specs

`POST /submit_job_inline` takes the TOML text in the request instead of a registry name. It is meant for trying out a spec. The record stores the spec and can be reproduced, but `model_commit` is `local-inline`: the spec does not exist in any versioned file. Move it to the registry before producing results to publish.

## Commands

```bash
curl -H "X-API-Key: $KEY" http://localhost:8000/models                    # list
curl -H "X-API-Key: $KEY" http://localhost:8000/models/lucc_continuous    # one spec
curl -X POST -H "X-API-Key: $KEY" http://localhost:8000/admin/sync        # commit in use
```

A spec that does not parse appears in `GET /models` with an `error` field instead of disappearing from the list.
