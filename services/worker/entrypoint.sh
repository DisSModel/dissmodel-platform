# services/worker/entrypoint.sh
#!/bin/bash

echo "🔧 DisSModel Worker Starting..."

if [ -d "/opt/dissmodel" ]; then
    echo "📦 Installing dissmodel from development volume..."
    pip install -e /opt/dissmodel --quiet
    echo "✅ dissmodel installed from /opt/dissmodel"
else
    echo "ℹ️  No development volume found, using installed dissmodel"
fi

echo "🚀 Starting worker..."

# Run as module so `from worker.schemas import ...` resolves correctly
exec python -m worker.worker "$@"