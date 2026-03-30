# services/worker/worker.py
from __future__ import annotations

import logging
import os
import time

import redis

from dissmodel.executor.schemas import ExperimentRecord
from worker.runner import run_experiment
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
    port             = int(os.getenv("REDIS_PORT", 6379)),
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

def process_job(experiment_id: str) -> None:
    """Dequeue and execute a single experiment."""
    logger.info(f"[{WORKER_ID}] Processing experiment={experiment_id}")

    try:
        record = _load_record(experiment_id)
    except ValueError as exc:
        logger.error(f"[{WORKER_ID}] {exc}")
        return

    try:
        completed = run_experiment(record)
        _save_record(completed)
        logger.info(
            f"[{WORKER_ID}] Completed experiment={experiment_id} "
            f"output={completed.output_path}"
        )

    except Exception as exc:
        # run_experiment already sets status="failed" and logs the error
        # on the record — just persist and move on
        record.status = "failed"
        _save_record(record)
        logger.error(f"[{WORKER_ID}] Failed experiment={experiment_id}: {exc}")


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info(f"Worker {WORKER_ID} starting")
    logger.info(f"Redis: {os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', 6379)}")
    logger.info(f"Queues: {QUEUES}")

    # Ensure MinIO buckets exist before processing any job
    ensure_buckets()

    while True:
        try:
            # brpop blocks up to 5s and respects queue priority order
            result = redis_client.brpop(QUEUES, timeout=5)

            if result:
                _, experiment_id = result
                process_job(experiment_id)

        except KeyboardInterrupt:
            logger.info(f"Worker {WORKER_ID} shutting down")
            break

        except Exception as exc:
            logger.error(f"[{WORKER_ID}] Unexpected error: {exc}")
            time.sleep(5)   # back off before retrying


if __name__ == "__main__":
    main()