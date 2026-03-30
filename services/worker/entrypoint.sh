#!/bin/bash
set -e

echo "🔧 DisSModel Worker Starting..."

# 1. Verifica se o volume da biblioteca está montado
if [ -d "/opt/dissmodel" ]; then
    # 2. Testa se o Python já consegue importar a dissmodel. 
    # Se sim, o link simbólico (-e) já está configurado no site-packages.
    if ! python3 -c "import dissmodel" &> /dev/null; then
        echo "📦 Installing dissmodel from development volume (first time)..."
        pip install -e /opt/dissmodel --quiet
        echo "✅ dissmodel linked successfully."
    else
        echo "✅ dissmodel already linked. Skipping pip install."
    fi
else
    echo "ℹ️  No development volume found at /opt/dissmodel, using pre-installed version."
fi

echo "🚀 Starting worker..."

# 3. Executa como módulo para garantir que os imports relativos de worker.schemas funcionem
exec python -m worker.worker "$@"