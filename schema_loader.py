"""
schema_loader.py - Chargement et cache des fichiers de schéma.
Google Style Docstrings.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import streamlit as st

logger = logging.getLogger(__name__)

_SCHEMAS_DIR = Path("schemas")


def list_schemas() -> list[str]:
    """Retourne la liste des noms de domaine disponibles.

    Returns:
        Liste de noms sans extension, triée alphabétiquement.
    """
    if not _SCHEMAS_DIR.exists():
        _SCHEMAS_DIR.mkdir(parents=True)
    patterns = ("*.json", "*.yaml", "*.yml")
    files: list[Path] = []
    for pat in patterns:
        files.extend(_SCHEMAS_DIR.glob(pat))
    names = sorted({f.stem for f in files})
    return names


@st.cache_data(ttl=60)
def load_schema(domain: str) -> dict[str, Any]:
    """Charge et met en cache le schéma correspondant au domaine.

    Args:
        domain: Nom du domaine sans extension.

    Returns:
        Dictionnaire Python représentant le schéma.

    Raises:
        FileNotFoundError: Si aucun fichier de schéma n'est trouvé.
    """
    for ext, loader in [(".json", _load_json), (".yaml", _load_yaml), (".yml", _load_yaml)]:
        path = _SCHEMAS_DIR / f"{domain}{ext}"
        if path.exists():
            logger.info("Chargement du schéma : %s", path)
            return loader(path)
    raise FileNotFoundError(f"Aucun schéma trouvé pour le domaine '{domain}'")


def _load_json(path: Path) -> dict[str, Any]:
    """Charge un fichier JSON.

    Args:
        path: Chemin vers le fichier JSON.

    Returns:
        Dictionnaire Python.
    """
    try:
        with path.open(encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON invalide dans {path}: {exc}") from exc


def _load_yaml(path: Path) -> dict[str, Any]:
    """Charge un fichier YAML.

    Args:
        path: Chemin vers le fichier YAML.

    Returns:
        Dictionnaire Python.
    """
    try:
        import yaml
    except ImportError as exc:
        raise ImportError("PyYAML requis : pip install pyyaml") from exc
    try:
        with path.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
            if not isinstance(data, dict):
                raise ValueError("Le fichier YAML doit être un dictionnaire racine.")
            return data
    except Exception as exc:
        raise ValueError(f"YAML invalide dans {path}: {exc}") from exc
