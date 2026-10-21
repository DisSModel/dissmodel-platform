"""
API tests: FastAPI TestClient with an in-memory Redis.

The client is created without entering the lifespan, so MinIO is never
contacted. Imports resolve as in the container: the API code at the top
level and the worker package next to it.
"""
from __future__ import annotations

import sys
from pathlib import Path

import fakeredis
import pytest

API = Path(__file__).resolve().parents[1]
SERVICES = API.parent
for p in (str(API), str(SERVICES)):
    if p not in sys.path:
        sys.path.insert(0, p)

KEY = "test-key"
TOML = """
[model]
name = "toy"
class = "toy"
[model.parameters]
k = 2
"""


@pytest.fixture
def api(tmp_path, monkeypatch):
    configs = tmp_path / "configs"
    (configs / "models").mkdir(parents=True)
    (configs / "models" / "toy.toml").write_text(TOML)
    monkeypatch.setenv("CONFIGS_PATH", str(configs))
    monkeypatch.setenv("DISSMODEL_OUTPUT_ROOT", "s3://dissmodel-outputs/experiments")

    import main
    from fastapi.testclient import TestClient

    redis = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(main, "redis_client", redis)
    monkeypatch.setattr(main, "_VALID_KEYS", {KEY})

    client = TestClient(main.app)  # no `with`: the lifespan (MinIO) does not run
    client.headers["X-API-Key"] = KEY
    return client, redis, main
