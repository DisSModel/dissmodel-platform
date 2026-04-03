#!/bin/bash
set -e

echo "🔧 DisSModel API Starting..."
echo "✅ All dependencies and models loaded from Docker image."

echo "🚀 Starting API process..."
# O "$@" repassa o comando final (uvicorn main:app ...) configurado no Dockerfile
exec "$@"