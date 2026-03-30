# services/worker/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar, TYPE_CHECKING

if TYPE_CHECKING:
    from worker.schemas import ExperimentRecord


class ModelExecutor(ABC):
    """
    Interface base para executores de modelos DisSModel.

    Subclasses são registradas automaticamente no ExecutorRegistry
    via __init_subclass__ apenas por existirem — sem boilerplate.

    Exemplo mínimo
    --------------
    class MyExecutor(ModelExecutor):
        name = "my_model"

        def load(self, record: ExperimentRecord):
            return gpd.read_file(record.source.uri)

        def run(self, record: ExperimentRecord):
            data = self.load(record)
            # ... executa simulação ...
            return data

        def save(self, result, record: ExperimentRecord) -> ExperimentRecord:
            # ... persiste resultado ...
            record.status = "completed"
            return record
    """

    # Atributo de classe — define a chave no registry.
    # Deve ser uma string estática, nunca uma property.
    name: ClassVar[str]

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Importa aqui para evitar circular import entre base e registry
        from worker.registry import ExecutorRegistry
        if hasattr(cls, "name"):
            ExecutorRegistry.register(cls)

    # ── Ciclo de vida obrigatório ─────────────────────────────────────────

    @abstractmethod
    def load(self, record: ExperimentRecord):
        """
        Carrega e resolve o input (GDF, RasterBackend, etc.).

        Responsabilidades:
        - Resolver a URI (s3://, http://, path local)
        - Aplicar column_map (vector) ou band_map (raster)
        - Preencher record.source.checksum com sha256 do dado baixado
        - Retornar o dado carregado no formato esperado por run()
        """

    @abstractmethod
    def run(self, record: ExperimentRecord):
        """
        Executa a simulação.

        Recebe record com resolved_spec e parameters já mesclados.
        Retorna o resultado bruto — formato definido pela subclasse
        e consumido por save().
        """

    @abstractmethod
    def save(self, result, record: ExperimentRecord) -> ExperimentRecord:
        """
        Persiste o resultado e retorna o record atualizado.

        Responsabilidades:
        - Salvar output no MinIO
        - Preencher record.output_path e record.output_sha256
        - Definir record.status = "completed"
        - Retornar record completo
        """

    # ── Hook opcional ─────────────────────────────────────────────────────

    def validate(self, record: ExperimentRecord) -> None:
        """
        Valida spec e dado antes de rodar.

        Chamado pelo runner antes de run(). Subclasses sobrescrevem
        para verificar colunas/bandas canônicas, ranges de valores, etc.
        Lança ValueError com mensagem acionável se inválido.

        Implementação padrão: no-op.
        """

    # ── Utilitários disponíveis para subclasses ───────────────────────────

    def _resolve_uri(self, uri: str) -> str:
        """
        Resolve uma URI para path local acessível pelo worker.

        s3://bucket/key  → baixa para /tmp/<key>, retorna path
        http(s)://...    → baixa para /tmp/<filename>, retorna path
        /path/local      → retorna como está
        """
        import os, hashlib, urllib.request
        from worker.storage import minio_client

        if uri.startswith("s3://"):
            # s3://bucket/path/to/file.tif
            parts      = uri[5:].split("/", 1)
            bucket     = parts[0]
            object_key = parts[1]
            local_path = f"/tmp/{os.path.basename(object_key)}"
            minio_client.fget_object(bucket, object_key, local_path)
            return local_path

        if uri.startswith("http://") or uri.startswith("https://"):
            filename   = uri.split("/")[-1]
            local_path = f"/tmp/{filename}"
            urllib.request.urlretrieve(uri, local_path)
            return local_path

        return uri   # path local — funciona igual ao script original

    @staticmethod
    def _sha256(path_or_bytes) -> str:
        """Calcula sha256 de um arquivo ou bytes."""
        import hashlib
        if isinstance(path_or_bytes, (str, bytes.__class__)) and \
           not isinstance(path_or_bytes, bytes):
            with open(path_or_bytes, "rb") as f:
                data = f.read()
        else:
            data = path_or_bytes
        return hashlib.sha256(data).hexdigest()