FROM jupyter/base-notebook:python-3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libspatialindex-dev \
    gdal-bin git \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Diretórios
RUN mkdir -p /data/inputs /data/outputs /examples \
    && chown -R jovyan:users /data /examples

COPY jupyter_config.py /etc/jupyter/

USER jovyan

# Dependências gerais (SEM dissmodel)
COPY requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# 🔥 instala dissmodel local (via volume)
#RUN pip install -e /opt/dissmodel

# 🔥 remove numba se aparecer
RUN pip uninstall -y numba llvmlite || true

RUN mkdir -p /home/jovyan/work

WORKDIR /home/jovyan/work

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8888/api || exit 1


CMD ["start-notebook.sh", "--NotebookApp.token=''"]