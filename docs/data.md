# Input and output data

## Inputs

A job names its input in `input_dataset`. The executor reads it (in DisSModel, with `dissmodel.io.load_dataset`), so the formats are those of the executor; DisSModel reads shapefiles (also zipped), GeoPackage, GeoJSON and GeoTIFF.

| URI | Where the file is | Use it for |
|---|---|---|
| `/data/inputs/...` | `./data/inputs` on the host, mounted read-only in the worker and in JupyterLab | one server, files you copy there |
| `s3://dissmodel-inputs/...` | the platform's MinIO | files shared by several workers or uploaded through the API |
| `https://...` | a public server | published datasets; downloaded at each run |

The record keeps the URI and the input's SHA-256 (`source.checksum`, filled by the executor). The checksum is what identifies the input: a URI can point to different content later.

Parameters that name files (for example DisSLUCC's `demand_csv`) are read by the executor as given; check in the executor's documentation which kinds of path it accepts. DisSLUCC 0.5.0 reads `demand_csv` from a local path only.

## Uploading

Small files (the whole file passes through the API):

```bash
curl -X POST http://localhost:8000/data/upload \
  -H "X-API-Key: $KEY" \
  -F "label=acre" -F "file=@csAC.zip"
```

```json
{"uri": "s3://dissmodel-inputs/inputs/acre/csAC.zip", "label": "acre",
 "checksum": "dc659e61…", "size_mb": 0.45}
```

Large files: copy them straight to MinIO with the MinIO client (`mc`), using the root credentials from `.env`:

```bash
mc alias set dissmodel http://localhost:19000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc cp brasil_5km.tif dissmodel/dissmodel-inputs/inputs/brasil/
```

The MinIO console (http://localhost:19001) also accepts uploads.

## Outputs

Each run writes to `<DISSMODEL_OUTPUT_ROOT>/<experiment_id>/` (default `s3://dissmodel-outputs/experiments/<experiment_id>/`): the output (`output.tif`), the record (`<id8>.record.json`) and the profiling report (`profiling_<id8>.md`). The record's `artifacts` holds the SHA-256 of each.

Set `DISSMODEL_OUTPUT_ROOT=/data/outputs/experiments` to write them to `./data/outputs` on the host instead.

To download from MinIO without its credentials, ask the API for a link valid for one hour (or `expires_hours`):

```bash
curl -H "X-API-Key: $KEY" \
  "http://localhost:8000/download?uri=s3://dissmodel-outputs/experiments/<id>/output.tif"
```

The link points to `MINIO_URL`, which must be reachable from where you open it.
