# DisSModel Platform - Arquitetura

## Visão Geral

A plataforma DisSModel é uma arquitetura baseada em microsserviços containerizados, projetada para execução escalável de modelos geoespaciais.

## Diagrama de Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                    DISSMODEL PLATFORM                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [Pesquisador] → Navegador → JupyterLab (Container)             │
│                              │                                  │
│                              ├── Python Direto (imports)        │
│                              └── API REST (jobs pesados)        │
│                              │                                  │
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              SERVIDOR / DOCKER HOST                      │   │
│  │                                                          │   │
│  │  🟦 Jupyter    🟩 API      🟥 Worker    🗄️ MinIO  🔄 Redis │   │
│  │  (Frontend)  (Gateway)   (Process.)   (Dados)   (Fila)   │   │
│  │                                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Componentes

### Frontend (JupyterLab)
- Ambiente de desenvolvimento interativo
- Execução local de modelos leves
- Submissão de jobs pesados via API

### API Gateway (FastAPI)
- Recebe requisições de jobs
- Valida parâmetros
- Enfileira tarefas no Redis

### Workers
- Consomem fila Redis
- Executam modelos DisSModel
- Salvam resultados no MinIO

### Storage (MinIO)
- S3-compatible
- Armazena inputs e outputs
- Persistente via volumes Docker

### Queue (Redis)
- Fila de mensagens
- Suporte a prioridades (high, normal, low)
- Cache de metadados de jobs

## Fluxo de Dados

1. Pesquisador sobe dados para `data/inputs/`
2. Dados são acessíveis via MinIO (`s3://dissmodel-inputs/`)
3. Modelo é desenvolvido no JupyterLab
4. Jobs pesados são submetidos via API
5. Workers processam e salvam em `data/outputs/`

## Escalabilidade

- Workers podem ser escalados horizontalmente
- `docker compose up --scale worker=5`
- Em produção (Fase 2, planejado, não implementado): Kubernetes + Dask

## Segurança

- Credenciais via variáveis de ambiente
- Rede Docker isolada
- Volumes com permissões restritas

## Monitoramento

- Health checks em todos os serviços
- Logs centralizados (stdout/stderr)
- Métricas via Prometheus (Fase 2)