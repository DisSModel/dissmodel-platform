#!/bin/bash

# ═══════════════════════════════════════════════════════════════
# DISSMODEL PLATFORM - Worker Entrypoint
# ═══════════════════════════════════════════════════════════════

echo "🔧 DisSModel Worker Starting..."

# Instalar dissmodel em modo editable se volume estiver montado
if [ -d "/opt/dissmodel" ]; then
    echo "📦 Installing dissmodel from development volume..."
    pip install -e /opt/dissmodel --quiet
    echo "✅ dissmodel installed from /opt/dissmodel"
else
    echo "ℹ️  No development volume found, using installed dissmodel"
fi

echo "🚀 Starting worker..."

# Executar o worker (passa todos os argumentos recebidos)
exec python worker.py "$@"