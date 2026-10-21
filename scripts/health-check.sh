#!/bin/bash

# ═══════════════════════════════════════════════════════════════
# DISSMODEL PLATFORM - Health Check Script
# ═══════════════════════════════════════════════════════════════

echo "🏥 DisSModel Platform - Health Check"
echo "═══════════════════════════════════════════════════════════"

# Check containers
echo "📦 Containers:"
docker compose ps

echo ""
echo "🔗 Service Health:"

# Jupyter
if curl -s http://localhost:8888/api > /dev/null; then
    echo "✅ JupyterLab: OK"
else
    echo "❌ JupyterLab: FAILED"
fi

# API
if curl -s http://localhost:8000/health > /dev/null; then
    echo "✅ API: OK"
else
    echo "❌ API: FAILED"
fi

# MinIO
if curl -s http://localhost:19000/minio/health/live > /dev/null; then
    echo "✅ MinIO: OK"
else
    echo "❌ MinIO: FAILED"
fi

# Redis
if docker compose exec redis redis-cli ping > /dev/null 2>&1; then
    echo "✅ Redis: OK"
else
    echo "❌ Redis: FAILED"
fi

echo ""
echo "═══════════════════════════════════════════════════════════"