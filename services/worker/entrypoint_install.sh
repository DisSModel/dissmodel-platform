#!/bin/bash
set -e

INSTALL_FLAG="/home/appuser/.cache/pip/.dissmodel_installed"
# A URL exata da sua branch usando a sintaxe do PIP
GIT_URL="git+https://github.com/LambdaGeo/dissmodel.git@update-platform"

echo "🔧 DisSModel Worker Starting..."

# CENÁRIO 1: Desenvolvimento Local (Mapeamento de Volume no Docker Compose)
if [ -d "/opt/dissmodel" ]; then
    if [ ! -f "$INSTALL_FLAG" ]; then
        echo "📦 Installing LOCAL dissmodel and dependencies..."
        pip install -e /opt/dissmodel
        touch "$INSTALL_FLAG"
        echo "✅ Local installation complete."
    else
        echo "✅ Local dependencies already cached."
    fi

# CENÁRIO 2: Nuvem/Produção (Sempre baixa da branch update-platform)
else
    echo "☁️ Installing dissmodel from GitHub (Branch: update-platform)..."
    
    # O --upgrade garante que, se você der restart no container, 
    # ele vai puxar o commit mais recente dessa branch.
    pip install "$GIT_URL" --upgrade --no-cache-dir
    
    echo "✅ GitHub installation complete."
fi

echo "🚀 Starting worker..."
exec python -m worker.worker "$@"