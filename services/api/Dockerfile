FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

# 1. Instala dependências de sistema (IGUAL ao Worker para suportar GeoPandas/GDAL)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential git \
    libgdal-dev gdal-bin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --upgrade pip

# 2. Instala dependências da API
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copia o código da API
COPY . .

# 4. Configura o entrypoint para instalar a dissmodel no boot
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# 5. Permissões
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]