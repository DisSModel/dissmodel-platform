# scripts/validate_executors.py
#
# Valida automaticamente todos os executors na pasta services/worker/executors/
# Não requer imports manuais em __init__.py

import sys
import os
import importlib.util
from pathlib import Path

# Configura o path para encontrar o pacote 'worker'
BASE_DIR = Path(__file__).parent.parent
SERVICES_DIR = BASE_DIR / "services"
sys.path.insert(0, str(SERVICES_DIR))

from dissmodel.executor.testing import ExecutorTestHarness
from dissmodel.executor.registry import ExecutorRegistry

def discover_and_import_executors():
    """Varre a pasta de executors e importa todos os arquivos .py"""
    executors_path = SERVICES_DIR / "worker" / "executors"
    if not executors_path.exists():
        return

    for path in executors_path.glob("*.py"):
        if path.name in ("__init__.py", "schemas.py", "testing.py"):
            continue
        
        # Importa o módulo dinamicamente para disparar o __init_subclass__
        module_name = f"worker.executors.{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            print(f"📦 Carregado: {module_name}")

if __name__ == "__main__":
    print("🔍 Iniciando descoberta de executors...")
    discover_and_import_executors()

    executors = ExecutorRegistry._executors

    if not executors:
        print("⚠️  Nenhum executor encontrado em services/worker/executors/")
        sys.exit(0)

    failed = []
    print(f"🧪 Validando {len(executors)} executor(es)...\n")

    for name, cls in executors.items():
        try:
            harness = ExecutorTestHarness(cls)
            if harness.run_contract_tests():
                print(f"✅ {name} passou nos testes de contrato")
            else:
                print(f"❌ {name} falhou nos testes de contrato")
                failed.append(name)
        except Exception as e:
            print(f"💥 Erro ao testar {name}: {e}")
            failed.append(name)

    if failed:
        print(f"\n❌ Falha na validação de {len(failed)} executor(es).")
        sys.exit(1)

    print("\n✨ Todos os executors validados com sucesso!")
