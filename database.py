"""
database.py - Gestion de la persistance PostgreSQL (Supabase).
Google Style Docstrings.
"""

from __future__ import annotations

import json
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
    """Retourne une connexion PostgreSQL persistante.

    Returns:
        Connexion psycopg2 active.
    """
    url = st.secrets["DATABASE_URL"]
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


@contextmanager
def transaction(conn):
    """Gestionnaire de contexte avec commit/rollback.

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
    """Crée la table _schemas avec gestion des rôles.

    Args:
        conn: Connexion psycopg2 active.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS _schemas (
        id SERIAL PRIMARY KEY,
        domain TEXT UNIQUE NOT NULL,
        schema_json TEXT NOT NULL,
        creator_id TEXT NOT NULL DEFAULT 'anonymous',
        creator_password TEXT,
        is_public BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    with transaction(conn) as cur:
        cur.execute(ddl)
        # Ajouter les colonnes si elles n'existent pas (migration)
        for col, definition in [
            ("creator_id", "TEXT NOT NULL DEFAULT 'anonymous'"),
            ("creator_password", "TEXT"),
            ("is_public", "BOOLEAN DEFAULT FALSE"),
        ]:
            try:
                cur.execute(f"ALTER TABLE _schemas ADD COLUMN IF NOT EXISTS {col} {definition};")
            except Exception:
                pass


def save_schema_db(
    conn,
    domain: str,
    schema: dict,
    creator_id: str = "anonymous",
    creator_password: str = "",
    is_public: bool = False,
) -> None:
    """Sauvegarde un schéma dans Supabase.

    Args:
        conn: Connexion psycopg2 active.
        domain: Identifiant unique du formulaire.
        schema: Dictionnaire du schéma.
        creator_id: Identifiant du créateur.
        creator_password: Mot de passe du créateur.
        is_public: Si True, visible par tous via lien.
    """
    schema_json = json.dumps(schema, ensure_ascii=False)
    sql = """
    INSERT INTO _schemas (domain, schema_json, creator_id, creator_password, is_public)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (domain) DO UPDATE SET
        schema_json = EXCLUDED.schema_json,
        is_public = EXCLUDED.is_public;
    """
    with transaction(conn) as cur:
        cur.execute(sql, (domain, schema_json, creator_id, creator_password, is_public))


def load_schemas_db(conn) -> dict[str, dict]:
    """Charge tous les schémas (vue admin complète).

    Args:
        conn: Connexion psycopg2 active.

    Returns:
        Dictionnaire domain: schema avec métadonnées.
    """
    try:
        with transaction(conn) as cur:
            cur.execute("""
                SELECT domain, schema_json, creator_id, is_public, created_at
                FROM _schemas ORDER BY created_at DESC;
            """)
            rows = cur.fetchall()
            result = {}
            for row in rows:
                data = json.loads(row["schema_json"])
                data["_creator_id"] = row["creator_id"]
                data["_is_public"] = row["is_public"]
                data["_created_at"] = str(row["created_at"])
                result[row["domain"]] = data
            return result
    except Exception as exc:
        logger.warning("load_schemas_db : %s", exc)
        return {}


def load_schemas_for_user(conn, creator_id: str) -> dict[str, dict]:
    """Charge les schémas visibles par un utilisateur donné.

    Args:
        conn: Connexion psycopg2 active.
        creator_id: Identifiant de l'utilisateur connecté.

    Returns:
        Dictionnaire domain: schema.
    """
    try:
        with transaction(conn) as cur:
            cur.execute("""
                SELECT domain, schema_json, creator_id, is_public, created_at
                FROM _schemas
                WHERE creator_id = %s
                ORDER BY created_at DESC;
            """, (creator_id,))
            rows = cur.fetchall()
            result = {}
            for row in rows:
                data = json.loads(row["schema_json"])
                data["_creator_id"] = row["creator_id"]
                data["_is_public"] = row["is_public"]
                result[row["domain"]] = data
            return result
    except Exception as exc:
        logger.warning("load_schemas_for_user : %s", exc)
        return {}


def load_schema_by_domain(conn, domain: str) -> dict | None:
    """Charge un schéma spécifique par son domain.

    Args:
        conn: Connexion psycopg2 active.
        domain: Identifiant du formulaire.

    Returns:
        Dictionnaire du schéma ou None.
    """
    try:
        with transaction(conn) as cur:
            cur.execute("""
                SELECT domain, schema_json, creator_id, creator_password, is_public
                FROM _schemas WHERE domain = %s;
            """, (domain,))
            row = cur.fetchone()
            if not row:
                return None
            data = json.loads(row["schema_json"])
            data["_creator_id"] = row["creator_id"]
            data["_creator_password"] = row["creator_password"]
            data["_is_public"] = row["is_public"]
            return data
    except Exception as exc:
        logger.warning("load_schema_by_domain : %s", exc)
        return None


def verify_creator_password(conn, domain: str, password: str) -> bool:
    """Vérifie le mot de passe du créateur d'un formulaire.

    Args:
        conn: Connexion psycopg2 active.
        domain: Identifiant du formulaire.
        password: Mot de passe à vérifier.

    Returns:
        True si le mot de passe est correct.
    """
    try:
        with transaction(conn) as cur:
            cur.execute(
                "SELECT creator_password FROM _schemas WHERE domain = %s;",
                (domain,)
            )
            row = cur.fetchone()
            if not row:
                return False
            return row["creator_password"] == password
    except Exception:
        return False


def delete_schema_db(conn, domain: str) -> None:
    """Supprime un schéma et toutes ses données.

    Args:
        conn: Connexion psycopg2 active.
        domain: Identifiant du formulaire à supprimer.
    """
    with transaction(conn) as cur:
        cur.execute("DELETE FROM _schemas WHERE domain = %s;", (domain,))
        try:
            cur.execute(f'DROP TABLE IF EXISTS "{domain}";')
        except Exception:
            pass


def ensure_table(conn, table_name: str, fields: list[dict]) -> None:
    """Crée la table de données si elle n'existe pas.

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


def insert_row(conn, table_name: str, data: dict[str, Any]) -> int:
    """Insère un enregistrement et retourne son id.

    Args:
        conn: Connexion psycopg2 active.
        table_name: Nom de la table cible.
        data: Dictionnaire colonne valeur.

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
    """Charge toutes les lignes d'une table.

    Args:
        conn: Connexion psycopg2 active.
        table_name: Nom de la table.

    Returns:
        DataFrame pandas.
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
def track_session(conn) -> None:
    """Enregistre une session active.

    Args:
        conn: Connexion psycopg2 active.
    """
    try:
        with transaction(conn) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS _sessions (
                    id SERIAL PRIMARY KEY,
                    last_seen TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("INSERT INTO _sessions (last_seen) VALUES (NOW());")
            cur.execute("""
                DELETE FROM _sessions
                WHERE last_seen < NOW() - INTERVAL '5 minutes';
            """)
    except Exception as exc:
        logger.warning("track_session : %s", exc)


def count_active_sessions(conn) -> int:
    """Compte les sessions actives des 5 dernières minutes.

    Args:
        conn: Connexion psycopg2 active.

    Returns:
        Nombre d'utilisateurs actifs.
    """
    try:
        with transaction(conn) as cur:
            cur.execute("""
                SELECT COUNT(*) as total FROM _sessions
                WHERE last_seen > NOW() - INTERVAL '5 minutes';
            """)
            row = cur.fetchone()
            return row["total"] if row else 0
    except Exception:
        return 0
