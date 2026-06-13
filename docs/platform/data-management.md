# Data Management

The DisSModel Platform handles input and output data using a uniform URI scheme. This allows simulations to run on local development machines or cloud workers without changing the model code.

## URI Resolution

The `ModelExecutor` uses the internal `_resolve()` logic to fetch data before the simulation starts.

| URI Scheme | Resolution Logic | Typical Use Case |
|---|---|---|
| `s3://bucket/key` | Downloaded from MinIO to worker's local scratch. | **Production** (MinIO/AWS S3). |
| `http://...` | Downloaded via `urllib` to local scratch. | Public datasets (e.g., NASA, ESA). |
| `/abs/path/...` | Used directly as a local file path. | **Development** (Local machine). |

### Implementation Detail

```python
# Extracted from storage.py
def download_to_file(uri: str, dest: str) -> str:
    if uri.startswith("s3://"):
        bucket, key = _parse_s3(uri)
        minio_client.fget_object(bucket, key, dest)
        return dest
    if uri.startswith("http://"):
        urllib.request.urlretrieve(uri, dest)
        return dest
    return uri  # Local path unchanged
```

## Storage Strategy

| Situation | Data Location | Access Method |
|---|---|---|
| **Large Datasets** | MinIO `dissmodel-inputs` | Use `mc cp` (MinIO Client). |
| **Small Files (<100MB)** | MinIO `dissmodel-inputs` | Use `POST /data/upload` API. |
| **Simulation Results** | MinIO `dissmodel-outputs` | Auto-uploaded by the Platform. |
| **Local Testing** | Local Disk | Direct path in JobRequest. |

## Uploading Data

### Via MinIO Client (`mc`)

For datasets larger than 100 MB, use the official MinIO client:

```bash
# Configure the client
mc alias set inpe http://minio:9000 inpe_admin inpe_secret_2024

# Upload a GeoTIFF
mc cp my_dataset.tif inpe/dissmodel-inputs/baseline/maranhao_2024.tif
```

### Via API

For small datasets:

```bash
curl -X POST http://localhost:8000/data/upload \
  -H "X-API-Key: dev-key" \
  -F "file=@my_dataset.tif" \
  -F "label=baseline"
```

## Intake Catalogs

The platform supports `Intake` for structured data access. If a `catalog.yaml` exists in the `dissmodel-configs` repository, executors can use it to load datasets by name instead of URI.

**Example `catalog.yaml`:**
```yaml
sources:
  maranhao_elevation:
    driver: rasterio
    args:
      urlpath: s3://dissmodel-inputs/baseline/altimetry.tif
      storage_options:
        endpoint_url: http://minio:9000
```

## Note on Local Paths

During the MVP phase, local file paths continue to work if the worker has access to the same filesystem (e.g., in a Docker Compose development environment). However, for multi-node production clusters, **S3 URIs are mandatory**.
