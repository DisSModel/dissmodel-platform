# Security & Authentication

The DisSModel Platform implements a two-layered security architecture to protect simulation data and computational resources.

## Architecture Overview

```text
Researcher  ──(1)──►  FastAPI API  ──(2)──►  MinIO Storage
(External)           (X-API-Key)            (Internal Creds)
```

1.  **Researcher → API:** Authenticated via the `X-API-Key` header.
2.  **API/Worker → MinIO:** Uses internal service-to-service credentials that are never exposed to the researcher.

## Authentication (X-API-Key)

Every request to the platform (except health checks) must include a valid API key.

```python
# Extracted from main.py
_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=True)
_VALID_KEYS     = set(os.getenv("API_KEYS", "dev-key").split(","))

async def require_api_key(key: str = Depends(_API_KEY_HEADER)) -> str:
    if key not in _VALID_KEYS:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return key
```

### Configuration via Environment

API keys and MinIO credentials are configured using environment variables (or a `.env` file):

```bash
# .env example
API_KEYS=researcher-1-token,researcher-2-token
MINIO_ACCESS_KEY=inpe_admin
MINIO_SECRET_KEY=inpe_secret_2024
MINIO_ENDPOINT=minio:9000
```

## Security Roadmap

| Phase | Authentication | Execution Security |
|---|---|---|
| **MVP** | API Key (Static) | Manual PR Review of Executors |
| **Phase 2** | JWT / OpenID Connect | PyPI-only Plugins (Signed) |
| **Phase 3** | OAuth2 (Inpe Accounts) | Docker/Wasm Sandboxing |

## Pipeline Security (CI/CD)

All Pull Requests to the `dissmodel-executors` repository undergo automated security scanning:

*   **Bandit:** Scans for common security issues in Python code (e.g., `subprocess` usage, hardcoded secrets).
*   **Mypy:** Ensures type safety to prevent memory-related bugs.
*   **Contract Testing:** Using `ExecutorTestHarness` to ensure the executor does not attempt to access restricted internal APIs.

## Execution Guardrails

Since workers run simulations as subprocesses, they have limited access to the system:
*   **Read-only Root:** The worker filesystem is mostly read-only.
*   **Scratch Space:** Each job is assigned a unique temporary directory for intermediate files.
*   **Resource Limits:** CPU and Memory limits are enforced via Docker/Dask.

!!! danger "Credential Protection"
    Never commit API keys or `.env` files to git. Use the `X-API-Key` header exclusively for authentication. If a key is compromised, rotate it immediately in the platform's environment configuration.
