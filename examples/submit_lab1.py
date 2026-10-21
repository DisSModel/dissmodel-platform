"""
Submit LuccME Lab 1 (DisSLUCC continuous executor) to a running platform,
wait for it, reproduce it and print the comparison.

Before running, put the two input files where the worker can read them
(see docs/getting-started.md):

    data/inputs/csAC.zip
    data/inputs/examples_demand_lab1.csv

Usage:
    DISSMODEL_API=http://localhost:8000 DISSMODEL_API_KEY=<key> python examples/submit_lab1.py

Uses only the standard library.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

API = os.getenv("DISSMODEL_API", "http://localhost:8000").rstrip("/")
KEY = os.getenv("DISSMODEL_API_KEY", "dev-key")
TOML = Path(__file__).with_name("lucc_continuous_lab1.toml")


def call(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{API}{path}", data=data, method=method,
        headers={"X-API-Key": KEY, "Content-Type": "application/json"},
    )
    # The URL comes from the user's own DISSMODEL_API setting.
    with urllib.request.urlopen(req) as resp:  # nosec B310
        return json.load(resp)


def wait(experiment_id: str, timeout: float = 600) -> dict:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        record = call("GET", f"/experiments/{experiment_id}")
        if record["status"] in ("completed", "failed", "cancelled"):
            return record
        time.sleep(2)
    sys.exit(f"Timed out waiting for {experiment_id}")


def main() -> None:
    job = call("POST", "/submit_job_inline", {
        "model_spec_toml": TOML.read_text(),
        "input_dataset":   "/data/inputs/csAC.zip",
        "parameters":      {"demand_csv": "/data/inputs/examples_demand_lab1.csv"},
    })
    print("submitted ", job["experiment_id"])

    record = wait(job["experiment_id"])
    print("status    ", record["status"])
    if record["status"] != "completed":
        print("\n".join(record["logs"]))
        sys.exit(1)
    print("output    ", record["output_path"])
    print("sha256    ", record["output_sha256"])

    repro = call("POST", f"/experiments/{record['experiment_id']}/reproduce")
    print("reproduce ", repro["experiment_id"])
    result = wait(repro["experiment_id"])["metrics"].get("reproduction", {})
    print("file identical:  ", result.get("file_identical"))
    print("values identical:", result.get("values_identical"))


if __name__ == "__main__":
    main()
