#!/bin/bash
set -e

echo "🔧 DisSModel Worker Starting..."
echo "✅ All dependencies and models loaded from Docker image."

echo "🚀 Starting worker process..."
# O "$@" repassa os comandos finais do Dockerfile (CMD) para o script
exec "$@"