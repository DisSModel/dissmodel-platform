# Security

The platform is an MVP for a research group's server. Read this page before making it reachable by anyone else.

## What is protected

- **API.** Every route except `/` and `/health` requires an `X-API-Key` header whose value is in `API_KEYS`. Keys are static and compared as plain strings; there are no users, roles or expiry. Give each person their own key so one can be removed (edit `.env` and `docker compose up -d api`).
- **MinIO and Redis credentials** are only in `.env` and in the containers' environment. Researchers use the API and presigned links; they do not need them.
- **Network.** The services talk on a private Docker network. In the production stack only nginx publishes a port.

## What is not

- **JupyterLab has no token or password** in either Compose file (`--NotebookApp.token=''`, `--ServerApp.token=`). Whoever reaches it can run code with access to the shared inputs and outputs. Keep it off any network you do not trust, or set a token (`--ServerApp.token=<value>`) or password in the Compose file.
- **Development ports.** `docker-compose.yml` publishes Redis (6379, no password) and MinIO (19000, 19001) on all interfaces. Use it on a workstation or behind a firewall.
- **No TLS.** Both stacks serve plain HTTP; API keys travel in clear. Put a TLS-terminating proxy in front.
- **Model code runs with the worker's rights.** A spec's `package` is installed with pip and its executor runs inside the worker, which holds the MinIO credentials. There is no sandbox. Only let people you trust register specs or submit inline specs, and review the packages specs point to.
- **Defaults.** Without `.env`, the Compose files fall back to the key `dev-key` and the MinIO credentials `inpe_admin` / `inpe_secret_2024`, which are public. Always create `.env`.

## Checklist for a shared server

1. `.env` created from `.env.example`, every `change-me` replaced with `openssl rand -hex 32`; `.env` not in git.
2. One API key per person.
3. Production stack (`docker-compose.prod.yml`), with TLS in front of nginx.
4. JupyterLab with a token, or not exposed.
5. Registry repository writable only by people you trust; specs pin `package` to exact versions.
6. Backups of the MinIO and Redis volumes ([Deployment](deployment.md#backups)).

## Reporting a problem

Open an issue at https://github.com/DisSModel/dissmodel-platform/issues, or write to the maintainer for anything that should not be public.
