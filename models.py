"""
models.py - Génération dynamique de modèles Pydantic.
Google Style Docstrings.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from pydantic import BaseModel, ValidationError, create_model, field_validator

logger = logging.getLogger(__name__)

_PYTHON_TYPE_MAP: dict[str, type] = {
    "str": str,
    "int": int,
    "float": float,
    "date": date,
}


def build_model(schema_fields: list[dict]) -> type[BaseModel]:
    """Crée dynamiquement un modèle Pydantic à partir d'une liste de champs.

    Args:
        schema_fields: Liste de dicts décrivant les champs du formulaire.

    Returns:
        Classe Pydantic prête à l'emploi.
    """
    field_definitions: dict[str, Any] = {}
    validators: dict[str, Any] = {}

    for field in schema_fields:
        name: str = field["name"]
        raw_type: str = field.get("type", "str")
        python_type: type = _PYTHON_TYPE_MAP.get(raw_type, str)
        required: bool = field.get("required", True)
        options: list | None = field.get("options")
        min_val = field.get("min_value")
        max_val = field.get("max_value")

        if required:
            annotated_type = python_type
            default = ...
        else:
            annotated_type = Optional[python_type]
            default = None

        field_definitions[name] = (annotated_type, default)

        if options:
            str_options = [str(o) for o in options]

            def make_option_validator(opts: list[str], field_name: str):
                @field_validator(field_name, mode="before")
                @classmethod
                def _check_option(cls, v):
                    if v is not None and str(v) not in opts:
                        raise ValueError(
                            f"Valeur '{v}' invalide. Choix autorisés : {', '.join(opts)}"
                        )
                    return v
                return _check_option

            validators[f"validate_{name}_options"] = make_option_validator(str_options, name)

        if (min_val is not None or max_val is not None) and python_type in (int, float):
            def make_range_validator(lo, hi, field_name: str, pt: type):
                @field_validator(field_name, mode="before")
                @classmethod
                def _check_range(cls, v):
                    if v is None:
                        return v
                    try:
                        v = pt(v)
                    except (ValueError, TypeError):
                        raise ValueError(f"Doit être un nombre ({pt.__name__})")
                    if lo is not None and v < lo:
                        raise ValueError(f"Valeur trop basse (min = {lo})")
                    if hi is not None and v > hi:
                        raise ValueError(f"Valeur trop haute (max = {hi})")
                    return v
                return _check_range

            validators[f"validate_{name}_range"] = make_range_validator(
                min_val, max_val, name, python_type
            )

    model: type[BaseModel] = create_model(
        "DynamicForm",
        **field_definitions,
        __validators__=validators,
    )
    return model


def validate_data(
    model_cls: type[BaseModel], raw_data: dict[str, Any]
) -> tuple[BaseModel | None, list[str]]:
    """Valide raw_data contre model_cls.

    Args:
        model_cls: Modèle Pydantic généré par build_model.
        raw_data: Dictionnaire brut provenant du formulaire Streamlit.

    Returns:
        Tuple (instance, erreurs).
    """
    try:
        instance = model_cls(**raw_data)
        return instance, []
    except ValidationError as exc:
        messages = []
        for error in exc.errors():
            loc = " → ".join(str(l) for l in error["loc"])
            messages.append(f"**{loc}** : {error['msg']}")
        return None, messages
