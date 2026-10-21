"""Executor defined in a submodule of a lazily re-exporting package."""
from __future__ import annotations

import numpy as np
from dissmodel.executor import ModelExecutor
from dissmodel.io._utils import write_bytes


class LazyExecutor(ModelExecutor):
    name = "toy_lazy"

    def load(self, record):
        return np.arange(4, dtype="int32")

    def run(self, data, record):
        return data

    def save(self, result, record):
        record.artifacts["output"] = write_bytes(result.tobytes(), record.output_path)
        record.status = "completed"
        return record
