#!/bin/bash
# DisSModel Platform — backup of the MinIO and Redis volumes and of .env.
#
#   scripts/backup.sh                         # development stack
#   COMPOSE_FILE=docker-compose.prod.yml scripts/backup.sh
#
# Stops the stack while copying, so the copy is consistent, then starts it again.
# Restore: see docs/deployment.md#backups.

set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
if [ "$COMPOSE_FILE" = "docker-compose.prod.yml" ]; then
    VOLUMES="dissmodel-prod-minio-data dissmodel-prod-redis-data"
else
    VOLUMES="dissmodel-minio-data dissmodel-redis-data"
fi

BACKUP_DIR="$(pwd)/backups/$(date +%Y-%m-%d_%H-%M-%S)"
mkdir -p "$BACKUP_DIR"

echo "Stopping services ($COMPOSE_FILE)..."
docker compose -f "$COMPOSE_FILE" down

for volume in $VOLUMES; do
    echo "Copying volume $volume..."
    docker run --rm -v "$volume":/v:ro -v "$BACKUP_DIR":/b alpine \
        tar czf "/b/$volume.tar.gz" -C /v .
done
cp .env "$BACKUP_DIR/env"

echo "Starting services..."
docker compose -f "$COMPOSE_FILE" up -d

echo "Backup written to $BACKUP_DIR"
