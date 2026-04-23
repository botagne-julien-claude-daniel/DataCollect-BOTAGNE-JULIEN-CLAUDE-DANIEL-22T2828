"""
app.py - Point d'entrée de l'application Streamlit.
Moteur universel de collecte de données piloté par schéma.
Google Style Docstrings.
Run: ``streamlit run app.py``
"""

from __future__ import annotations

import io
import json
import logging
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from database import ensure_table, fetch_all, get_connection, insert_row
from models import build_model, validate_data
from schema_loader import list_schemas, load_schema

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

SCHEMAS_DIR = Path("schemas")


def save_schema_file(domain: str, schema: dict) -> None:
    """Sauvegarde un schéma JSON dans le dossier schemas/.

    Args:
        domain: Nom du domaine (nom du fichier sans extension).
        schema: Dictionnaire du schéma à sauvegarder.
    """
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    path = SCHEMAS_DIR / f"{domain}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(schema, f, ensure_ascii=False, indent=2)
    load_schema.clear()


def render_field(field: dict) -> Any:
    """Génère le widget Streamlit approprié pour un champ du schéma.

    Args:
        field: Dictionnaire décrivant un champ.

    Returns:
        La valeur saisie par l'utilisateur.
    """
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
        return st.date_input(full_label, value=date.today(), help=help_text)

    if field.get("multiline"):
        return st.text_area(full_label, value=field.get("default", ""), help=help_text)
    return st.text_input(full_label, value=field.get("default", ""), help=help_text)


def render_statistics(df: pd.DataFrame, schema_fields: list[dict]) -> None:
    """Affiche un dashboard statistique.

    Args:
        df: DataFrame pandas chargé depuis la base de données.
        schema_fields: Liste des champs du schéma.
    """
    if df.empty:
        st.info("📭 Aucune donnée collectée pour l'instant.")
        return

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
        numeric_cols = [f["name"] for f in schema_fields if f.get("type") in ("int", "float")]
        st.markdown(
            f'<div class="metric-box"><div class="value">{len(numeric_cols)} num.</div>'
            f'<div class="label">Colonnes numériques</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")

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
                st.bar_chart(series.value_counts().sort_index())
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
        domain: Nom du domaine pour le nom du fichier.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"{domain}_{timestamp}"

    col1, col2 = st.columns(2)
    with col1:
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
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


def render_admin_tab() -> None:
    """Affiche l'onglet Admin pour créer un nouveau formulaire sans code."""

    st.markdown("## ⚙️ Créer un nouveau formulaire")
    st.caption("Remplis ce formulaire pour créer un nouveau questionnaire. Aucune connaissance technique requise.")

    st.markdown("---")

    # --- Infos générales du formulaire ---
    st.markdown("### 1️⃣ Informations générales")
    form_title = st.text_input("Titre du formulaire *", placeholder="Ex : Enquête de satisfaction")
    form_description = st.text_area("Description (optionnel)", placeholder="Ex : Collecte des avis étudiants")
    domain_name = st.text_input(
        "Nom interne (sans espaces ni accents) *",
        placeholder="Ex : enquete_etudiants",
        help="Ce nom sera utilisé comme identifiant. Utilisez uniquement des lettres, chiffres et underscores."
    )

    st.markdown("---")
    st.markdown("### 2️⃣ Champs du formulaire")
    st.caption("Ajoute autant de champs que tu veux.")

    # Nombre de champs
    nb_fields = st.number_input("Combien de champs veux-tu ?", min_value=1, max_value=20, value=3, step=1)

    fields = []
    for i in range(int(nb_fields)):
        st.markdown(f"**Champ {i + 1}**")
        col1, col2 = st.columns(2)

        with col1:
            field_label = st.text_input(
                f"Libellé du champ {i + 1} *",
                placeholder="Ex : Nom de l'étudiant",
                key=f"label_{i}"
            )
            field_name = st.text_input(
                f"Nom interne {i + 1} *",
                placeholder="Ex : nom_etudiant",
                key=f"name_{i}"
            )
            field_type = st.selectbox(
                f"Type de donnée {i + 1}",
                ["Texte", "Nombre entier", "Nombre décimal", "Date", "Liste déroulante"],
                key=f"type_{i}"
            )

        with col2:
            field_required = st.checkbox(f"Obligatoire", value=True, key=f"required_{i}")
            field_help = st.text_input(
                f"Texte d'aide (optionnel)",
                placeholder="Ex : Entrez votre prénom",
                key=f"help_{i}"
            )

            field_options = ""
            field_min = None
            field_max = None

            if field_type == "Liste déroulante":
                field_options = st.text_input(
                    f"Options (séparées par des virgules) *",
                    placeholder="Ex : Oui, Non, Peut-être",
                    key=f"options_{i}"
                )
            elif field_type in ("Nombre entier", "Nombre décimal"):
                field_min = st.number_input(f"Valeur minimum", value=0, key=f"min_{i}")
                field_max = st.number_input(f"Valeur maximum", value=100, key=f"max_{i}")

        # Construction du champ
        type_map = {
            "Texte": "str",
            "Nombre entier": "int",
            "Nombre décimal": "float",
            "Date": "date",
            "Liste déroulante": "str",
        }

        field_dict: dict[str, Any] = {
            "name": field_name.strip().replace(" ", "_"),
            "label": field_label.strip(),
            "type": type_map[field_type],
            "required": field_required,
        }

        if field_help:
            field_dict["help"] = field_help

        if field_type == "Liste déroulante" and field_options:
            field_dict["options"] = [o.strip() for o in field_options.split(",") if o.strip()]

        if field_type in ("Nombre entier", "Nombre décimal") and field_min is not None:
            field_dict["min_value"] = field_min
            field_dict["max_value"] = field_max

        fields.append(field_dict)
        st.markdown("---")

    # --- Bouton de création ---
    if st.button("✅ Créer le formulaire", use_container_width=True):
        errors = []

        if not form_title.strip():
            errors.append("Le titre du formulaire est obligatoire.")
        if not domain_name.strip():
            errors.append("Le nom interne est obligatoire.")
        if not domain_name.strip().replace("_", "").isalnum():
            errors.append("Le nom interne ne doit contenir que des lettres, chiffres et underscores.")
        for i, f in enumerate(fields):
            if not f.get("label"):
                errors.append(f"Le libellé du champ {i + 1} est obligatoire.")
            if not f.get("name"):
                errors.append(f"Le nom interne du champ {i + 1} est obligatoire.")

        if errors:
            for err in errors:
                st.error(f"❌ {err}")
        else:
            schema = {
                "title": form_title.strip(),
                "description": form_description.strip(),
                "fields": [f for f in fields if f.get("name") and f.get("label")],
            }
            try:
                save_schema_file(domain_name.strip(), schema)
                st.success(f"✅ Formulaire **{form_title}** créé avec succès ! Sélectionne-le dans la sidebar.")
                st.balloons()
            except Exception as exc:
                st.error(f"❌ Erreur lors de la création : {exc}")


def main() -> None:
    """Point d'entrée principal de l'application Streamlit."""

    st.markdown(
        "<h1>🗂️ DataCollect <span class='badge'>Universal</span></h1>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<p style='color:#888;font-size:0.8rem;letter-spacing:0.06em;'>"
        "MOTEUR DE COLLECTE DE DONNÉES PILOTÉ PAR SCHÉMA — RÉALISÉ PAR BOTAGNE JULIEN CLAUDE DANIEL"
        "</p>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    with st.sidebar:
        st.markdown("### ⚙️ Configuration")
        available = list_schemas()

        if not available:
            st.warning("Aucun formulaire disponible. Crées-en un dans l'onglet **Admin**.")
            domain = None
        else:
            domain = st.selectbox(
                "Formulaire actif",
                available,
                help="Choisissez le formulaire à utiliser",
            )
        st.markdown("---")

    # ====================================================================
    # ONGLETS
    # ====================================================================
    tab_form, tab_data, tab_stats, tab_admin = st.tabs([
        "✏️  Saisie",
        "📋  Données",
        "📊  Statistiques",
        "⚙️  Admin"
    ])

    # ------------------------------------------------------------------
    # ONGLET ADMIN
    # ------------------------------------------------------------------
    with tab_admin:
        render_admin_tab()

    # Si aucun formulaire disponible, on s'arrête ici
    if not domain:
        with tab_form:
            st.info("👆 Crée d'abord un formulaire dans l'onglet **Admin**.")
        with tab_data:
            st.info("👆 Crée d'abord un formulaire dans l'onglet **Admin**.")
        with tab_stats:
            st.info("👆 Crée d'abord un formulaire dans l'onglet **Admin**.")
        return

    # Chargement du schéma
    try:
        schema = load_schema(domain)
    except Exception as exc:
        st.error(f"❌ Impossible de charger le schéma `{domain}` : {exc}")
        st.stop()

    schema_fields: list[dict] = schema.get("fields", [])
    form_title: str = schema.get("title", domain.replace("_", " ").title())
    form_description: str = schema.get("description", "")

    # Connexion DB
    try:
        conn = get_connection()
        ensure_table(conn, domain, schema_fields)
    except Exception as exc:
        st.error(f"❌ Erreur de base de données : {exc}")
        st.stop()

    # Modèle Pydantic
    try:
        model_cls = build_model(schema_fields)
    except Exception as exc:
        st.error(f"❌ Erreur de validation : {exc}")
        st.stop()

    # ------------------------------------------------------------------
    # ONGLET SAISIE
    # ------------------------------------------------------------------
    with tab_form:
        st.markdown(f"## {form_title}")
        if form_description:
            st.caption(form_description)

        with st.form(key=f"form_{domain}", clear_on_submit=True):
            raw_values: dict[str, Any] = {}
            for field in schema_fields:
                try:
                    raw_values[field["name"]] = render_field(field)
                except Exception as exc:
                    st.warning(f"Widget `{field.get('name')}` : {exc}")

            submitted = st.form_submit_button("✅ Soumettre", use_container_width=True)

        if submitted:
            try:
                instance, errors = validate_data(model_cls, raw_values)
                if errors:
                    st.error("❌ Corrigez les erreurs suivantes :")
                    for err in errors:
                        st.markdown(f"• {err}")
                else:
                    row_id = insert_row(conn, domain, instance.model_dump())
                    st.success(f"✅ Enregistrement #{row_id} sauvegardé !")
            except Exception as exc:
                st.error(f"❌ Erreur : {exc}")
                with st.expander("Détails"):
                    st.code(traceback.format_exc())

    # ------------------------------------------------------------------
    # ONGLET DONNÉES
    # ------------------------------------------------------------------
    with tab_data:
        st.markdown(f"## Données — *{form_title}*")
        try:
            df = fetch_all(conn, domain)
        except Exception as exc:
            st.error(f"Erreur : {exc}")
            df = pd.DataFrame()

        if df.empty:
            st.info("📭 Aucune donnée encore collectée.")
        else:
            st.markdown(f"`{len(df)}` enregistrement(s).")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.markdown("#### Export")
            export_dataframe(df, domain)

    # ------------------------------------------------------------------
    # ONGLET STATISTIQUES
    # ------------------------------------------------------------------
    with tab_stats:
        st.markdown(f"## Statistiques — *{form_title}*")
        try:
            df_stats = fetch_all(conn, domain)
            render_statistics(df_stats, schema_fields)
        except Exception as exc:
            st.error(f"❌ Erreur : {exc}")


if __name__ == "__main__":
    main()
