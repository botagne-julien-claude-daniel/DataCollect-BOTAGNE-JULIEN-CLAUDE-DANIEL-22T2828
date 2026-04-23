"""
database.py - Gestion de la persistance PostgreSQL (Supabase).
Google Style Docstrings.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st

logger = logging.getLogger(__name__)


@st.cache_resource
def get_connection():
    """Retourne une connexion PostgreSQL persistante mise en cache.

    Returns:
        Connexion psycopg2 active.
    """
    url = st.secrets["DATABASE_URL"]
    conn = psycopg2.connect(url)
    conn.autocommit = False
    logger.info("Connexion PostgreSQL ouverte.")
    return conn


@contextmanager
def transaction(conn):
    """Gestionnaire de contexte avec commit/rollback automatique.

    Args:
        conn: Connexion psycopg2 active.

    Yields:
        Curseur de la transaction.
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


_PG_TYPE_MAP: dict[str, str] = {
    "str": "TEXT",
    "int": "INTEGER",
    "float": "REAL",
    "date": "DATE",
}


def ensure_schemas_table(conn) -> None:
    """Crée la table _schemas si elle n'existe pas.

    Cette table stocke les schémas créés via l'onglet Admin.

    Args:
        conn: Connexion psycopg2 active.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS _schemas (
        id SERIAL PRIMARY KEY,
        domain TEXT UNIQUE NOT NULL,
        schema_json TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    with transaction(conn) as cur:
        cur.execute(ddl)
    logger.debug("Table _schemas assurée.")


def save_schema_db(conn, domain: str, schema: dict) -> None:
    """Sauvegarde un schéma dans la table _schemas de Supabase.

    Args:
        conn: Connexion psycopg2 active.
        domain: Nom du domaine (identifiant unique).
        schema: Dictionnaire du schéma à sauvegarder.
    """
    import json
    schema_json = json.dumps(schema, ensure_ascii=False)
    sql = """
    INSERT INTO _schemas (domain, schema_json)
    VALUES (%s, %s)
    ON CONFLICT (domain) DO UPDATE SET schema_json = EXCLUDED.schema_json;
    """
    with transaction(conn) as cur:
        cur.execute(sql, (domain, schema_json))
    logger.info("Schéma sauvegardé en DB : %s", domain)


def load_schemas_db(conn) -> dict[str, dict]:
    """Charge tous les schémas depuis la table _schemas.

    Args:
        conn: Connexion psycopg2 active.

    Returns:
        Dictionnaire {domain: schema_dict}.
    """
    import json
    try:
        with transaction(conn) as cur:
            cur.execute("SELECT domain, schema_json FROM _schemas ORDER BY domain;")
            rows = cur.fetchall()
            return {row["domain"]: json.loads(row["schema_json"]) for row in rows}
    except Exception as exc:
        logger.warning("load_schemas_db : %s", exc)
        return {}


def delete_schema_db(conn, domain: str) -> None:
    """Supprime un schéma de la table _schemas.

    Args:
        conn: Connexion psycopg2 active.
        domain: Nom du domaine à supprimer.
    """
    with transaction(conn) as cur:
        cur.execute("DELETE FROM _schemas WHERE domain = %s;", (domain,))
    logger.info("Schéma supprimé : %s", domain)


def ensure_table(conn, table_name: str, fields: list[dict]) -> None:
    """Crée la table PostgreSQL si elle n'existe pas.

    Args:
        conn: Connexion psycopg2 active.
        table_name: Nom de la table.
        fields: Liste de champs du schéma.
    """
    col_defs = [
        "id SERIAL PRIMARY KEY",
        "created_at TIMESTAMP DEFAULT NOW()",
    ]
    for f in fields:
        pg_type = _PG_TYPE_MAP.get(f.get("type", "str"), "TEXT")
        col_defs.append(f'"{f["name"]}" {pg_type}')

    ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)});'
    with transaction(conn) as cur:
        cur.execute(ddl)
    logger.debug("Table assurée : %s", table_name)


def insert_row(conn, table_name: str, data: dict[str, Any]) -> int:
    """Insère un enregistrement et retourne son id.

    Args:
        conn: Connexion psycopg2 active.
        table_name: Nom de la table cible.
        data: Dictionnaire colonne → valeur.

    Returns:
        L'id de la ligne insérée.
    """
    cols = ", ".join(f'"{c}"' for c in data)
    placeholders = ", ".join("%s" for _ in data)
    sql = f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders}) RETURNING id'
    with transaction(conn) as cur:
        cur.execute(sql, list(data.values()))
        row = cur.fetchone()
        return row["id"]


def fetch_all(conn, table_name: str) -> pd.DataFrame:
    """Charge toutes les lignes d'une table dans un DataFrame.

    Args:
        conn: Connexion psycopg2 active.
        table_name: Nom de la table.

    Returns:
        DataFrame pandas, vide si la table n'existe pas encore.
    """
    try:
        with transaction(conn) as cur:
            cur.execute(f'SELECT * FROM "{table_name}" ORDER BY id DESC')
            rows = cur.fetchall()
            if not rows:
                return pd.DataFrame()
            return pd.DataFrame([dict(r) for r in rows])
    except Exception as exc:
        logger.warning("fetch_all(%s) : %s", table_name, exc)
        return pd.DataFrame()
