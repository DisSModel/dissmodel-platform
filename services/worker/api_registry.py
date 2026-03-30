# services/worker/api_registry.py
from __future__ import annotations

import subprocess
import tomllib
from functools import lru_cache
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler

CONFIGS_PATH = Path("/configs")
MODELS_DIR   = CONFIGS_PATH / "models"


# ── Git sync ──────────────────────────────────────────────────────────────────

def sync_configs() -> str:
    if not CONFIGS_PATH.exists():
        return "Path de configs não encontrado."

    # Se não houver .git ou o comando git não existir, apenas limpa o cache
    if not (CONFIGS_PATH / ".git").exists():
        load_model_spec.cache_clear()
        return "Modo local: cache limpo."

    try:
        result = subprocess.run(
            ["git", "-C", str(CONFIGS_PATH), "pull", "--ff-only"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            load_model_spec.cache_clear()
        return result.stdout.strip()
    except FileNotFoundError:
        load_model_spec.cache_clear()
        return "Git não instalado: usando arquivos locais."

def sync_configs____() -> str:
    """
    Tenta sincronizar via git. Se falhar ou não houver git, 
    apenas limpa o cache para uso de arquivos locais.
    """
    if not CONFIGS_PATH.exists():
        return f"Configs path '{CONFIGS_PATH}' não encontrado."

    # Se NÃO for um repositório git, apenas limpa o cache e sai silenciosamente
    if not (CONFIGS_PATH / ".git").exists():
        load_model_spec.cache_clear()
        return "Modo local: cache de modelos limpo."

    try:
        # Tenta rodar o git pull, mas não trava se o comando 'git' não existir
        result = subprocess.run(
            ["git", "-C", str(CONFIGS_PATH), "pull", "--ff-only"],
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode == 0 and "Already up to date." not in result.stdout:
            load_model_spec.cache_clear()
        
        return result.stdout.strip() if result.returncode == 0 else "Git pull falhou."

    except FileNotFoundError:
        # SE O BINÁRIO 'GIT' NÃO EXISTIR, CAI AQUI:
        load_model_spec.cache_clear()
        return "Comando 'git' não encontrado: usando arquivos locais."

def sync_configs__() -> str:
    """
    Tenta sincronizar via git se o repositório existir.
    Caso contrário, apenas limpa o cache para desenvolvimento local.
    """
    if not CONFIGS_PATH.exists():
        return f"Configs path '{CONFIGS_PATH}' not found. Skipping sync."

    # 1. Verifica se a pasta montada é um repositório Git
    if not (CONFIGS_PATH / ".git").exists():
        # Em modo desenvolvimento local (sem git), apenas limpamos o cache
        # para que novos arquivos .toml sejam lidos corretamente.
        load_model_spec.cache_clear()
        return "Local development mode: cache cleared (no git repo found)"

    # 2. Tenta rodar o git pull (dentro de um try/except caso o binário 'git' não exista)
    try:
        result = subprocess.run(
            ["git", "-C", str(CONFIGS_PATH), "pull", "--ff-only"],
            capture_output=True,
            text=True,
            check=False # Não levanta exceção automaticamente se o git retornar erro
        )

        if result.returncode != 0:
            # Se o git falhar por outro motivo (ex: conflito), logamos mas não travamos a API
            return f"Git pull failed (is it a valid repo?): {result.stderr.strip()}"

        # Invalida o cache se houve mudança real no repositório
        if "Already up to date." not in result.stdout:
            load_model_spec.cache_clear()

        return result.stdout.strip()

    except FileNotFoundError:
        # Caso o binário 'git' não esteja instalado no container
        load_model_spec.cache_clear()
        return "Git command not found in container: using local files and clearing cache."


def start_sync_scheduler(interval_seconds: int = 900) -> BackgroundScheduler:
    """
    Start the background scheduler for periodic config sync.
    Called once at API startup.
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        sync_configs,
        trigger   = "interval",
        seconds   = interval_seconds,
        id        = "config_sync",
        misfire_grace_time = 60,
    )
    scheduler.start()
    return scheduler


# ── Spec loading ──────────────────────────────────────────────────────────────

@lru_cache(maxsize=64)
def load_model_spec(model_name: str) -> dict:
    """
    Load and cache a model spec from TOML.

    Cache is invalidated by sync_configs() after a successful git pull.
    Raises FileNotFoundError if the model is not registered.
    """
    path = MODELS_DIR / f"{model_name}.toml"

    if not path.exists():
        available = [p.stem for p in MODELS_DIR.glob("*.toml")]
        raise FileNotFoundError(
            f"Model '{model_name}' not found in registry. "
            f"Available: {available or 'none — is dissmodel-configs mounted?'}"
        )

    with open(path, "rb") as f:
        return tomllib.load(f)


def list_models() -> list[dict]:
    """
    Return metadata for all registered models.
    Used by GET /models.
    """
    if not MODELS_DIR.exists():
        return []

    models = []
    for path in sorted(MODELS_DIR.glob("*.toml")):
        try:
            spec = load_model_spec(path.stem)
            models.append({
                "name":        path.stem,
                "class":       spec.get("model", {}).get("class", ""),
                "description": spec.get("model", {}).get("description", ""),
            })
        except Exception:
            pass   # skip malformed TOMLs silently

    return models


# ── Catalog ───────────────────────────────────────────────────────────────────

def load_catalog():
    """
    Open the Intake catalog from the configs repo.
    Returns None if catalog.yaml is not present (optional in MVP).
    """
    import intake

    catalog_path = CONFIGS_PATH / "catalog.yaml"
    if not catalog_path.exists():
        return None

    return intake.open_catalog(str(catalog_path))