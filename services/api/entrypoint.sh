#!/bin/bash
set -e

echo "🔧 DisSModel API Starting..."

# 1. Verifica se a biblioteca está montada no volume
if [ -d "/opt/dissmodel" ]; then
    # 2. Só executa o pip install se o Python ainda não 'conhecer' o pacote dissmodel
    # Isso evita perder tempo processando dependências se o link -e já existir no volume/cache
    if ! python3 -c "import dissmodel" &> /dev/null; then
        echo "📦 Installing dissmodel in editable mode (first time or link broken)..."
        pip install -e /opt/dissmodel --quiet
    else
        echo "✅ dissmodel is already linked. Skipping installation."
    fi
else
    echo "⚠️  Warning: /opt/dissmodel not found. Using pre-installed library if available."
fi

# 3. Executa o comando final (uvicorn ou worker.worker)
exec "$@"