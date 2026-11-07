# services/worker/compare.py
"""
Output comparison for reproductions.

The comparison itself (bytes, decoded values, declared tolerance) lives in
``dissmodel.executor.compare`` since dissmodel 0.7.0, so the platform, the
``dissmodel compare`` command and the notebooks give the same verdict. The
only platform-specific part kept here is turning an ``s3://`` URI into a
local file.
"""
from __future__ import annotations

import os

from dissmodel.executor.compare import (  # noqa: F401  (re-exported)
    OK_VERDICTS,
    decoded_values_sha256,
    summary,
)
from dissmodel.executor.compare import compare_outputs as _compare_outputs
from dissmodel.executor.schemas import ExperimentRecord


def _local_copy(uri: str, workdir: str) -> str:
    """Path to a readable local copy of ``uri`` (downloaded when it is s3:// or http)."""
    if not uri.startswith(("s3://", "http://", "https://")):
        return uri
    from worker.storage import download_to_file

    dest = os.path.join(workdir, f"{len(os.listdir(workdir))}_{os.path.basename(uri)}")
    return download_to_file(uri, dest)


def compare_outputs(
    original: ExperimentRecord,
    reproduction: ExperimentRecord,
    atol: float | None = None,
) -> dict:
    """Result stored in ``reproduction.metrics['reproduction']``."""
    return _compare_outputs(original, reproduction, atol, local_copy=_local_copy)
