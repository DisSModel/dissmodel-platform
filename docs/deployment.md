# DisSModel Platform - Guia de Deploy

## Pré-requisitos

- Docker 20.10+
- Docker Compose 2.0+
- 8GB+ RAM
- 20GB+ disco

## Deploy Local (Desenvolvimento)

```bash
# Clonar
git clone https://github.com/DisSModel/dissmodel-platform.git
cd dissmodel-platform

# Configurar
cp .env.example .env

# Subir
docker compose up --build

# Acessar
# JupyterLab: http://localhost:8888
# API: http://localhost:8000/docs
# MinIO: http://localhost:9001
```

## Deploy em Servidor (Produção)

```bash
# 1. Configurar .env com credenciais seguras
# 2. Configurar firewall (portas 8888, 8000, 9001)
# 3. Usar HTTPS via reverse proxy (nginx/traefik)

# Subir em background
docker compose up -d

# Ver logs
docker compose logs -f

# Escalar workers
docker compose up -d --scale worker=5
```

## Deploy com Kubernetes (Fase 2)

```bash
# Usar Helm charts do Pangeo
helm repo add pangeo https://pangeo-data.github.io/helm-charts/
helm install dissmodel pangeo/pangeo
```

## Backup

```bash
# Backup de dados
./scripts/backup.sh

# Restore
docker compose down -v
./scripts/restore.sh backup-2024-01-01.tar.gz
docker compose up -d
```

## Troubleshooting

### Containers não iniciam
```bash
docker compose logs
docker compose down -v
docker compose up --build
```

### Sem espaço em disco
```bash
docker system prune -a
docker compose down -v
```

### Workers não processam jobs
```bash
docker compose logs worker
docker compose ps
docker compose up --scale worker=3