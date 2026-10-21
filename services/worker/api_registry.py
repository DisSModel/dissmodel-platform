# services/worker/api_registry.py
"""
Model registry: TOML specs in the dissmodel-configs repository.

The repository is kept up to date by the ``configs-sync`` sidecar
(see docker-compose.yml), which runs ``git pull`` on a timer and shares
the checkout with the API and the worker as a read-only volume.

Specs are read from disk on every call. An earlier version cached them
with ``lru_cache``; since the sidecar pulls in a different container,
nothing ever cleared that cache and the API kept serving the spec it
first read until it was restarted. Parsing a TOML file is cheap enough
that a cache is not worth that risk.
"""
from __future__ import annotations

import os
import subprocess
import tomllib
from pathlib import Path


def configs_path() -> Path:
    """Root of the dissmodel-configs checkout (``CONFIGS_PATH``, default ``/configs``)."""
    return Path(os.getenv("CONFIGS_PATH", "/configs"))


def models_dir() -> Path:
    return configs_path() / "models"


# ── Git state ─────────────────────────────────────────────────────────────────

def configs_head(short: bool = True) -> str:
    """
    Commit of the configs checkout, recorded as ``model_commit``.

    Returns ``"local-dev"`` when the folder is not a git repository and
    ``"unknown"`` when git is missing or fails.
    """
    root = configs_path()
    if not (root / ".git").exists():
        return "local-dev"
    cmd = ["git", "-C", str(root), "rev-parse"]
    if short:
        cmd.append("--short")
    cmd.append("HEAD")
    try:
        return subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode().strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def sync_configs() -> dict:
    """
    Try a fast-forward ``git pull`` and report the resulting commit.

    In the Docker stack the API sees the checkout read-only, so the pull
    fails there and the sidecar remains responsible for updating it; the
    endpoint still reports which commit new jobs will record.
    """
    root = configs_path()
    if not root.exists():
        return {"pulled": False, "head": None, "detail": f"Configs path '{root}' not found"}

    if not (root / ".git").exists():
        return {"pulled": False, "head": "local-dev", "detail": "Not a git checkout: using local files"}

    try:
        result = subprocess.run(
            ["git", "-C", str(root), "pull", "--ff-only"],
            capture_output=True, text=True, check=False,
        )
    except FileNotFoundError:
        return {"pulled": False, "head": "unknown", "detail": "git not installed"}

    detail = (result.stdout or result.stderr).strip()
    return {"pulled": result.returncode == 0, "head": configs_head(), "detail": detail}


# ── Spec loading ──────────────────────────────────────────────────────────────

def load_model_spec(model_name: str) -> dict:
    """
    Load a model spec from ``models/<model_name>.toml``.

    Raises FileNotFoundError if the model is not registered (including
    names that would point outside the models folder).
    """
    directory = models_dir()
    path = directory / f"{model_name}.toml"

    if path.parent != directory or not path.exists():
        available = sorted(p.stem for p in directory.glob("*.toml")) if directory.exists() else []
        raise FileNotFoundError(
            f"Model '{model_name}' not found in registry. "
            f"Available: {available or 'none — is dissmodel-configs mounted?'}"
        )

    with open(path, "rb") as f:
        return tomllib.load(f)


def list_models() -> list[dict]:
    """
    Return metadata for all registered models (used by GET /models).

    A spec that does not parse is listed with its error instead of being
    skipped, so a broken TOML shows up in the API instead of vanishing.
    """
    directory = models_dir()
    if not directory.exists():
        return []

    models = []
    for path in sorted(directory.glob("*.toml")):
        try:
            spec = load_model_spec(path.stem)
        except (OSError, tomllib.TOMLDecodeError) as exc:
            models.append({"name": path.stem, "error": str(exc)})
            continue
        model = spec.get("model", {})
        models.append({
            "name":        path.stem,
            "class":       model.get("class", ""),
            "description": model.get("description", ""),
        })
    return models
