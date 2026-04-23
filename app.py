"""
app.py - Point d'entrée de l'application Streamlit.

Moteur universel de collecte de données piloté par des fichiers de schéma
JSON/YAML. L'interface est entièrement générée à la volée.

Google Style Docstrings.
Run: ``streamlit run app.py``
"""

from __future__ import annotations

import io
import logging
import traceback
from datetime import date, datetime
from typing import Any

import pandas as pd
import streamlit as st

from database import ensure_table, fetch_all, get_connection, insert_row
from models import build_model, validate_data
from schema_loader import list_schemas, load_schema

# ---------------------------------------------------------------------------
# Configuration globale
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="DataCollect Universal",
    page_icon="🗂️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS personnalisé — palette sombre industrielle / typographie éditoriale
# ---------------------------------------------------------------------------

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Serif+Display&display=swap');

    html, body, [class*="css"] {
        font-family: 'Space Mono', monospace;
        background-color: #0f0f0f;
        color: #e8e4dc;
    }
    h1, h2, h3 {
        font-family: 'DM Serif Display', serif;
        letter-spacing: -0.5px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
        background: #1a1a1a;
        border-radius: 4px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        color: #888;
        border-radius: 3px;
        font-size: 0.75rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }
    .stTabs [aria-selected="true"] {
        background: #2a2a2a !important;
        color: #f0c040 !important;
    }
    .stButton > button {
        background: #f0c040;
        color: #0f0f0f;
        border: none;
        border-radius: 3px;
        font-family: 'Space Mono', monospace;
        font-weight: 700;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        padding: 0.5rem 1.2rem;
    }
    .stButton > button:hover {
        background: #ffd966;
        color: #0f0f0f;
    }
    .stSelectbox > div > div, .stTextInput > div > div {
        background: #1a1a1a;
        border-color: #333;
        color: #e8e4dc;
    }
    .stForm {
        background: #141414;
        border: 1px solid #2a2a2a;
        border-radius: 6px;
        padding: 1.5rem;
    }
    .metric-box {
        background: #1a1a1a;
        border: 1px solid #2a2a2a;
        border-radius: 4px;
        padding: 1rem 1.2rem;
        text-align: center;
    }
    .metric-box .value {
        font-size: 2rem;
        font-weight: 700;
        color: #f0c040;
    }
    .metric-box .label {
        font-size: 0.7rem;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: #888;
    }
    .badge {
        display: inline-block;
        background: #f0c040;
        color: #0f0f0f;
        font-size: 0.65rem;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 2px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-left: 8px;
        vertical-align: middle;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Fonctions utilitaires UI
# ---------------------------------------------------------------------------
def render_field(field: dict) -> Any:
    label: str = field.get("label", field["name"])
    field_type: str = field.get("type", "str")
    help_text: str | None = field.get("help")
    required: bool = field.get("required", True)
    suffix = " *" if required else " (optionnel)"
    full_label = f"{label}{suffix}"

    options: list | None = field.get("options")

    if options:
        display_options = options if not required else ["— Sélectionner —"] + list(options)
        value = st.selectbox(full_label, display_options, help=help_text)
        return None if value == "— Sélectionner —" else value

    if field_type == "int":
        min_val = field.get("min_value")
        default_val = field.get("default", min_val if min_val is not None else 0)
        return st.number_input(
            full_label,
            step=1,
            value=default_val,
            min_value=min_val,
            max_value=field.get("max_value"),
            help=help_text,
        )

    if field_type == "float":
        min_v = field.get("min_value")
        max_v = field.get("max_value")
        default_val = float(field.get("default", min_v if min_v is not None else 0.0))
        return st.number_input(
            full_label,
            step=field.get("step", 0.1),
            value=default_val,
            min_value=float(min_v) if min_v is not None else None,
            max_value=float(max_v) if max_v is not None else None,
            format="%.2f",
            help=help_text,
        )

    if field_type == "date":
        default_date = date.today()
        return st.date_input(full_label, value=default_date, help=help_text)

    if field.get("multiline"):
        return st.text_area(full_label, value=field.get("default", ""), help=help_text)
    return st.text_input(full_label, value=field.get("default", ""), help=help_text)


def render_statistics(df: pd.DataFrame, schema_fields: list[dict]) -> None:
    """Affiche un dashboard statistique pour les données collectées.

    Args:
        df: DataFrame pandas chargé depuis SQLite.
        schema_fields: Liste des champs du schéma (pour typer les colonnes).
    """
    if df.empty:
        st.info("📭 Aucune donnée collectée pour l'instant.")
        return

    # KPIs généraux
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(
            f'<div class="metric-box"><div class="value">{len(df)}</div>'
            f'<div class="label">Enregistrements</div></div>',
            unsafe_allow_html=True,
        )
    with col2:
        if "created_at" in df.columns:
            last = pd.to_datetime(df["created_at"]).max()
            st.markdown(
                f'<div class="metric-box"><div class="value">{last.strftime("%d/%m")}</div>'
                f'<div class="label">Dernière saisie</div></div>',
                unsafe_allow_html=True,
            )
    with col3:
        numeric_cols = [
            f["name"] for f in schema_fields if f.get("type") in ("int", "float")
        ]
        count_text = f"{len(numeric_cols)} num."
        st.markdown(
            f'<div class="metric-box"><div class="value">{count_text}</div>'
            f'<div class="label">Colonnes numériques</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # Graphiques par type de champ
    for field in schema_fields:
        name = field["name"]
        label = field.get("label", name)
        ftype = field.get("type", "str")

        if name not in df.columns:
            continue

        if field.get("options") or ftype == "str":
            counts = df[name].value_counts().reset_index()
            counts.columns = [label, "Nombre"]
            st.markdown(f"**Répartition — {label}**")
            st.bar_chart(counts.set_index(label))

        elif ftype in ("int", "float"):
            col_a, col_b = st.columns(2)
            series = pd.to_numeric(df[name], errors="coerce").dropna()
            with col_a:
                st.markdown(f"**Distribution — {label}**")
                hist_data = series.value_counts().sort_index()
                st.bar_chart(hist_data)
            with col_b:
                st.markdown(f"**Statistiques — {label}**")
                st.dataframe(
                    series.describe().rename("valeur").to_frame(),
                    use_container_width=True,
                )


def export_dataframe(df: pd.DataFrame, domain: str) -> None:
    """Affiche les boutons d'export CSV et Excel.

    Args:
        df: DataFrame à exporter.
        domain: Nom du domaine (utilisé dans le nom du fichier horodaté).
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"{domain}_{timestamp}"

    col1, col2 = st.columns(2)
    with col1:
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")  # BOM pour Excel FR
        st.download_button(
            label="⬇ Exporter CSV",
            data=csv_bytes,
            file_name=f"{base_name}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with col2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=domain[:31])
        st.download_button(
            label="⬇ Exporter Excel",
            data=buffer.getvalue(),
            file_name=f"{base_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


# ---------------------------------------------------------------------------
# Application principale
# ---------------------------------------------------------------------------

def main() -> None:
    """Point d'entrée principal de l'application Streamlit."""

    # ---- En-tête ----
    st.markdown(
        "<h1>🗂️ DataCollect <span class='badge'>Universal</span></h1>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<p style='color:#888;font-size:0.8rem;letter-spacing:0.06em;'>"
        "MOTEUR DE COLLECTE DE DONNÉES PILOTÉ PAR SCHÉMA — ZÉRO QUESTION CODÉE EN DUR"
        "</p>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # ---- Sélecteur de domaine (sidebar) ----
    with st.sidebar:
        st.markdown("### ⚙️ Configuration")
        available = list_schemas()

        if not available:
            st.error(
                "Aucun schéma trouvé dans `schemas/`. "
                "Placez-y au moins un fichier `.json` ou `.yaml`."
            )
            st.stop()

        domain = st.selectbox(
            "Formulaire actif",
            available,
            help="Chaque formulaire correspond à un fichier de schéma dans schemas/",
        )
        st.markdown("---")
        st.markdown(
            "<small style='color:#555;'>"
            "Les schémas sont rechargés automatiquement toutes les 60 s.</small>",
            unsafe_allow_html=True,
        )

    # ---- Chargement du schéma (avec gestion d'erreur globale) ----
    try:
        schema = load_schema(domain)
    except Exception as exc:
        st.error(f"❌ Impossible de charger le schéma `{domain}` : {exc}")
        logger.exception("Erreur de chargement du schéma")
        st.stop()

    schema_fields: list[dict] = schema.get("fields", [])
    form_title: str = schema.get("title", domain.replace("_", " ").title())
    form_description: str = schema.get("description", "")

    # ---- Connexion DB & table ----
    try:
        conn = get_connection()
        ensure_table(conn, domain, schema_fields)
    except Exception as exc:
        st.error(f"❌ Erreur de base de données : {exc}")
        logger.exception("Erreur DB")
        st.stop()

    # ---- Modèle Pydantic dynamique ----
    try:
        model_cls = build_model(schema_fields)
    except Exception as exc:
        st.error(f"❌ Erreur de construction du modèle de validation : {exc}")
        logger.exception("Erreur build_model")
        st.stop()

    # ====================================================================
    # ONGLETS
    # ====================================================================
    tab_form, tab_data, tab_stats = st.tabs(["✏️  Saisie", "📋  Données", "📊  Statistiques"])

    # ------------------------------------------------------------------
    # ONGLET 1 : FORMULAIRE
    # ------------------------------------------------------------------
    with tab_form:
        st.markdown(f"## {form_title}")
        if form_description:
            st.caption(form_description)

        with st.form(key=f"form_{domain}", clear_on_submit=True):
            raw_values: dict[str, Any] = {}

            # Génération dynamique des widgets
            for field in schema_fields:
                try:
                    raw_values[field["name"]] = render_field(field)
                except Exception as exc:
                    st.warning(f"Widget `{field.get('name')}` : {exc}")
                    logger.warning("Erreur rendu widget : %s", exc)

            submitted = st.form_submit_button("✅ Soumettre", use_container_width=True)

        if submitted:
            try:
                # Validation Pydantic
                instance, errors = validate_data(model_cls, raw_values)

                if errors:
                    st.error("❌ **Erreurs de validation — corrigez les champs suivants :**")
                    for err in errors:
                        st.markdown(f"  • {err}")
                else:
                    # Persistance SQLite
                    data_to_save = instance.model_dump()
                    row_id = insert_row(conn, domain, data_to_save)
                    st.success(f"✅ Enregistrement #{row_id} sauvegardé avec succès !")
                    logger.info("Insertion réussie — table=%s id=%s", domain, row_id)

            except Exception as exc:
                st.error(f"❌ Erreur inattendue lors de la soumission : {exc}")
                with st.expander("Détails techniques"):
                    st.code(traceback.format_exc())
                logger.exception("Erreur soumission formulaire")

    # ------------------------------------------------------------------
    # ONGLET 2 : DONNÉES BRUTES
    # ------------------------------------------------------------------
    with tab_data:
        st.markdown(f"## Données — *{form_title}*")
        try:
            df = fetch_all(conn, domain)
        except Exception as exc:
            st.error(f"Erreur de lecture : {exc}")
            df = pd.DataFrame()

        if df.empty:
            st.info("📭 Aucune donnée encore collectée.")
        else:
            st.markdown(f"`{len(df)}` enregistrement(s) trouvé(s).")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.markdown("#### Export")
            export_dataframe(df, domain)

    # ------------------------------------------------------------------
    # ONGLET 3 : STATISTIQUES
    # ------------------------------------------------------------------
    with tab_stats:
        st.markdown(f"## Statistiques — *{form_title}*")
        try:
            df_stats = fetch_all(conn, domain)
        except Exception as exc:
            st.error(f"Erreur de lecture : {exc}")
            df_stats = pd.DataFrame()

        try:
            render_statistics(df_stats, schema_fields)
        except Exception as exc:
            st.error(f"❌ Erreur d'affichage des statistiques : {exc}")
            logger.exception("Erreur render_statistics")


# ---------------------------------------------------------------------------
# Lancement
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
