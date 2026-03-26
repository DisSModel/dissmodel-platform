# ═══════════════════════════════════════════════════════════════
# DISSMODEL PLATFORM - Worker (DisSModel Execution)
# ═══════════════════════════════════════════════════════════════

import os
import json
import time
import logging
from datetime import datetime

import redis
from minio import Minio
from minio.error import S3Error

# ───────────────────────────────────────────────────────────────
# Configuration
# ───────────────────────────────────────────────────────────────

logging.basicConfig(
    level=os.getenv('DISSMODEL_LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
WORKER_QUEUE = os.getenv('WORKER_QUEUE', 'dissmodel_jobs')
WORKER_ID = os.getenv('WORKER_ID', 'worker-1')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'inpe_admin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'inpe_secret_2024')

# ───────────────────────────────────────────────────────────────
# Connections
# ───────────────────────────────────────────────────────────────

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# ───────────────────────────────────────────────────────────────
# Helper Functions
# ───────────────────────────────────────────────────────────────

def update_job_status(job_id: str, status: str, **kwargs):
    """Atualiza o status do job no Redis."""
    job_data = redis_client.get(f"job:{job_id}")

    if not job_data:
        logger.warning(f"Job {job_id} not found")
        return

    job = json.loads(job_data)
    job["status"] = status
    job["updated_at"] = datetime.utcnow().isoformat()

    for key, value in kwargs.items():
        job[key] = value

    redis_client.set(f"job:{job_id}", json.dumps(job))
    logger.info(f"Job {job_id} status updated to: {status}")


def execute_model(job_data: dict) -> str:
    """
    Executa o modelo DisSModel.
    Retorna o path do resultado no MinIO.
    """
    model_name = job_data["model_name"]
    input_dataset = job_data["input_dataset"]
    parameters = job_data.get("parameters", {})
    job_id = job_data["job_id"]

    logger.info(f"Executing model: {model_name}")
    logger.info(f"Input: {input_dataset}")
    logger.info(f"Parameters: {parameters}")

    # ───────────────────────────────────────────────────────────
    # TODO: Implementar execução real do DisSModel aqui
    # ───────────────────────────────────────────────────────────
    # from dissmodel.core import Environment
    # from dissmodel.models import get_model
    #
    # model = get_model(model_name)
    # env = Environment(**parameters)
    # results = env.run(model, input_dataset)
    #
    # output_path = f"results/{job_id}/output.zarr"
    # results.to_zarr(f"s3://dissmodel-outputs/{output_path}")
    # return output_path
    # ───────────────────────────────────────────────────────────

    # Simulação para teste
    time.sleep(5)

    output_path = f"results/{job_id}/result.txt"
    result_content = f"Model: {model_name}\nJob: {job_id}\nStatus: completed"

    try:
        bucket_name = "dissmodel-outputs"
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        minio_client.put_object(
            bucket_name,
            output_path,
            data=result_content.encode('utf-8'),
            length=len(result_content.encode('utf-8'))
        )

        logger.info(f"Result saved to: {bucket_name}/{output_path}")
        return f"{bucket_name}/{output_path}"

    except S3Error as e:
        logger.error(f"Failed to save result: {str(e)}")
        raise


# ───────────────────────────────────────────────────────────────
# Main Worker Loop
# ───────────────────────────────────────────────────────────────

def process_job(job_id: str):
    """Processa um único job."""
    logger.info(f"Worker {WORKER_ID} processing job: {job_id}")

    try:
        update_job_status(job_id, "running")

        job_data = redis_client.get(f"job:{job_id}")
        if not job_data:
            raise ValueError(f"Job data not found for {job_id}")

        job = json.loads(job_data)
        result_path = execute_model(job)

        update_job_status(
            job_id,
            "completed",
            completed_at=datetime.utcnow().isoformat(),
            result_path=result_path
        )

        logger.info(f"Job {job_id} completed successfully")

    except Exception as e:
        logger.error(f"Job {job_id} failed: {str(e)}")
        update_job_status(
            job_id,
            "failed",
            error=str(e),
            completed_at=datetime.utcnow().isoformat()
        )


def main():
    """Main worker loop."""
    logger.info(f"Worker {WORKER_ID} started")
    logger.info(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    logger.info(f"Queue: {WORKER_QUEUE}")
    logger.info(f"MinIO: {MINIO_ENDPOINT}")

    for bucket in ["dissmodel-inputs", "dissmodel-outputs"]:
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            logger.info(f"Bucket created: {bucket}")

    while True:
        try:
            # FIX: brpop com lista respeita prioridade em um único blocking call
            result = redis_client.brpop(
                ["queue:high", "queue:normal", "queue:low"],
                timeout=5
            )

            if result:
                _, job_id = result
                process_job(job_id)

        except KeyboardInterrupt:
            logger.info("Worker shutting down...")
            break
        except Exception as e:
            logger.error(f"Worker error: {str(e)}")
            time.sleep(5)


if __name__ == "__main__":
    main()
