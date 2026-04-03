#!/bin/bash
set -e

# Caminho para o arquivo que indica que a instalação foi concluída
# Como usamos o mesmo volume de cache no docker-compose, o flag é compartilhado
INSTALL_FLAG="/home/appuser/.cache/pip/.dissmodel_installed"

echo "🔧 DisSModel API Starting..."

# 1. Verifica se a biblioteca está montada no volume
if [ -d "/opt/dissmodel" ]; then
    # 2. Só instala se o flag de instalação não existir
    if [ ! -f "$INSTALL_FLAG" ]; then
        echo "📦 Installing dissmodel and ALL dependencies (API mode)..."
        
        # Removemos o --quiet para você conseguir ver o progresso no primeiro boot
        pip install -e /opt/dissmodel
        
        # Cria o flag (se o worker já criou, ele apenas atualiza o timestamp)
        touch "$INSTALL_FLAG"
        echo "✅ dissmodel and dependencies linked successfully."
    else
        echo "✅ Dependencies already cached. Skipping installation."
    fi
else
    echo "⚠️  Warning: /opt/dissmodel not found. Using pre-installed library if available."
fi

# 3. Executa o comando final (uvicorn)
echo "🚀 Starting API..."
exec "$@"