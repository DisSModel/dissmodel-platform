# services/worker/worker.py
"""Redis queue consumer: takes experiment ids and runs them."""
from __future__ import annotations

import logging
import os
import time

import redis
from dissmodel.executor.schemas import ExperimentRecord

from worker.compare import compare_outputs, summary
from worker.runner import REPRODUCES_KEY, run_experiment
from worker.storage import ensure_buckets

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level  = os.getenv("DISSMODEL_LOG_LEVEL", "INFO"),
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

WORKER_ID = os.getenv("WORKER_ID", "worker-1")
QUEUES    = ["queue:high", "queue:normal", "queue:low"]

# ── Redis ─────────────────────────────────────────────────────────────────────

redis_client = redis.Redis(
    host             = os.getenv("REDIS_HOST", "redis"),
    port             = int(os.getenv("REDIS_PORT", "6379")),
    password         = os.getenv("REDIS_PASSWORD") or None,
    decode_responses = True,
)

# ── Record persistence ────────────────────────────────────────────────────────

def _load_record(experiment_id: str) -> ExperimentRecord:
    raw = redis_client.get(f"experiment:{experiment_id}")
    if not raw:
        raise ValueError(f"Experiment '{experiment_id}' not found in Redis")
    return ExperimentRecord.model_validate_json(raw)


def _save_record(record: ExperimentRecord) -> None:
    redis_client.set(f"experiment:{record.experiment_id}", record.model_dump_json())


# ── Job processing ────────────────────────────────────────────────────────────

def _compare_with_original(record: ExperimentRecord) -> None:
    """For a reproduction, store the comparison with the original run on the record."""
    original_id = record.metrics.get(REPRODUCES_KEY)
    if not original_id or record.status != "completed":
        return
    try:
        original = _load_record(original_id)
    except ValueError as exc:
        record.add_log(f"Warning: cannot compare with the original: {exc}")
        return
    result = compare_outputs(original, record)
    record.metrics["reproduction"] = result
    record.add_log(summary(result))


def process_job(experiment_id: str) -> None:
    """Run one queued experiment and store the updated record."""
    logger.info(f"[{WORKER_ID}] Processing experiment={experiment_id}")

    try:
        record = _load_record(experiment_id)
    except ValueError as exc:
        logger.error(f"[{WORKER_ID}] {exc}")
        return

    # DELETE /job/{id} only marks the record; the id is still in the queue.
    if record.status == "cancelled":
        logger.info(f"[{WORKER_ID}] Skipping cancelled experiment={experiment_id}")
        return

    try:
        completed = run_experiment(record)
    except Exception as exc:  # noqa: BLE001 — any failure is recorded on the job
        record.status = "failed"
        # The full stderr is already in the logs (run_experiment); keep the last line here.
        lines = str(exc).strip().splitlines()
        record.add_log(f"Failed: {lines[-1] if lines else repr(exc)}")
        _save_record(record)
        logger.error(f"[{WORKER_ID}] Failed experiment={experiment_id}: {exc}")
        return

    _compare_with_original(completed)
    _save_record(completed)
    logger.info(
        f"[{WORKER_ID}] Completed experiment={experiment_id} output={completed.output_path}"
    )


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info(f"Worker {WORKER_ID} starting")
    logger.info(f"Redis: {os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', '6379')}")
    logger.info(f"Queues: {QUEUES}")

    ensure_buckets()

    while True:
        try:
            # brpop blocks up to 5 s and respects queue priority order
            result = redis_client.brpop(QUEUES, timeout=5)  # type: ignore[misc]
            if result:
                _, experiment_id = result  # type: ignore[misc]
                process_job(str(experiment_id))

        except KeyboardInterrupt:
            logger.info(f"Worker {WORKER_ID} shutting down")
            break

        except Exception as exc:  # noqa: BLE001 — keep the consumer alive
            logger.error(f"[{WORKER_ID}] Unexpected error: {exc}")
            time.sleep(5)


if __name__ == "__main__":
    main()
