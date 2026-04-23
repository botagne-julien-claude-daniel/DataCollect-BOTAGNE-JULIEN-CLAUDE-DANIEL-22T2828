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
        domain: Nom du domaine.
        schema: Dictionnaire du schéma.
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
    """Affiche un dashboard statistique enrichi.

    Args:
        df: DataFrame pandas chargé depuis la base de données.
        schema_fields: Liste des champs du schéma.
    """
    if df.empty:
        st.info("📭 Aucune donnée collectée pour l'instant.")
        return

    numeric_fields = [f for f in schema_fields if f.get("type") in ("int", "float")]
    categoric_fields = [f for f in schema_fields if f.get("options") or f.get("type") == "str"]
    numeric_cols = [f["name"] for f in numeric_fields]

    # ----------------------------------------------------------------
    # KPIs généraux
    # ----------------------------------------------------------------
    st.markdown("### 📊 Vue générale")
    col1, col2, col3, col4 = st.columns(4)

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
        st.markdown(
            f'<div class="metric-box"><div class="value">{len(numeric_cols)}</div>'
            f'<div class="label">Champs numériques</div></div>',
            unsafe_allow_html=True,
        )
    with col4:
        completeness = round((df.notna().sum().sum() / (len(df) * len(df.columns))) * 100, 1)
        st.markdown(
            f'<div class="metric-box"><div class="value">{completeness}%</div>'
            f'<div class="label">Complétude</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ----------------------------------------------------------------
    # FILTRES
    # ----------------------------------------------------------------
    st.markdown("### 🔍 Filtres")
    df_filtered = df.copy()

    filter_cols = st.columns(min(len(categoric_fields), 3)) if categoric_fields else []
    for i, field in enumerate(categoric_fields[:3]):
        name = field["name"]
        label = field.get("label", name)
        if name in df.columns:
            unique_vals = df[name].dropna().unique().tolist()
            if unique_vals:
                with filter_cols[i]:
                    selected = st.multiselect(
                        f"Filtrer par {label}",
                        options=unique_vals,
                        default=unique_vals,
                        key=f"filter_{name}"
                    )
                    if selected:
                        df_filtered = df_filtered[df_filtered[name].isin(selected)]

    st.caption(f"**{len(df_filtered)}** enregistrements après filtrage sur **{len(df)}** total.")

    st.markdown("---")

    # ----------------------------------------------------------------
    # ANALYSE NUMERIQUE
    # ----------------------------------------------------------------
    if numeric_cols:
        st.markdown("### 🔢 Analyse numérique")

        # Tableau des statistiques descriptives
        num_df = df_filtered[numeric_cols].apply(pd.to_numeric, errors="coerce")
        stats = num_df.describe().T
        stats.index = [
            next((f.get("label", f["name"]) for f in numeric_fields if f["name"] == col), col)
            for col in stats.index
        ]
        stats.columns = ["Nb", "Moyenne", "Écart-type", "Min", "Q25%", "Médiane", "Q75%", "Max"]
        st.dataframe(stats.round(2), use_container_width=True)

        st.markdown("---")

        # Distributions individuelles
        st.markdown("### 📈 Distributions")
        dist_cols = st.columns(min(len(numeric_cols), 2))
        for i, field in enumerate(numeric_fields):
            name = field["name"]
            label = field.get("label", name)
            if name in df_filtered.columns:
                series = pd.to_numeric(df_filtered[name], errors="coerce").dropna()
                if not series.empty:
                    with dist_cols[i % 2]:
                        st.markdown(f"**{label}**")
                        hist = series.value_counts(bins=min(10, len(series.unique()))).sort_index()
                        st.bar_chart(hist)
                        col_a, col_b, col_c = st.columns(3)
                        col_a.metric("Moyenne", f"{series.mean():.2f}")
                        col_b.metric("Médiane", f"{series.median():.2f}")
                        col_c.metric("Écart-type", f"{series.std():.2f}")

        st.markdown("---")

        # Tableau croisé
        if len(numeric_cols) >= 1 and categoric_fields:
            st.markdown("### 🔀 Tableau croisé")
            st.caption("Moyenne d'une variable numérique selon une catégorie.")

            col_x, col_y = st.columns(2)
            with col_x:
                selected_num = st.selectbox(
                    "Variable numérique",
                    options=[f["name"] for f in numeric_fields],
                    format_func=lambda x: next(
                        (f.get("label", f["name"]) for f in numeric_fields if f["name"] == x), x
                    ),
                    key="crosstab_num"
                )
            with col_y:
                selected_cat = st.selectbox(
                    "Variable catégorielle",
                    options=[f["name"] for f in categoric_fields if f["name"] in df_filtered.columns],
                    format_func=lambda x: next(
                        (f.get("label", f["name"]) for f in categoric_fields if f["name"] == x), x
                    ),
                    key="crosstab_cat"
                )

            if selected_num and selected_cat:
                cross = (
                    df_filtered.groupby(selected_cat)[selected_num]
                    .apply(lambda s: pd.to_numeric(s, errors="coerce").mean())
                    .round(2)
                    .reset_index()
                )
                num_label = next(
                    (f.get("label", f["name"]) for f in numeric_fields if f["name"] == selected_num),
                    selected_num
                )
                cat_label = next(
                    (f.get("label", f["name"]) for f in categoric_fields if f["name"] == selected_cat),
                    selected_cat
                )
                cross.columns = [cat_label, f"Moyenne {num_label}"]
                st.dataframe(cross, use_container_width=True, hide_index=True)
                st.bar_chart(cross.set_index(cat_label))

        st.markdown("---")

        # Corrélations
        if len(numeric_cols) >= 2:
            st.markdown("### 🔗 Corrélations entre variables numériques")
            st.caption("Valeur proche de 1 = forte relation positive, proche de -1 = forte relation inverse.")
            corr_df = num_df.rename(columns={
                f["name"]: f.get("label", f["name"]) for f in numeric_fields
            })
            corr = corr_df.corr().round(2)
            st.dataframe(corr.style.background_gradient(cmap="RdYlGn", vmin=-1, vmax=1),
                        use_container_width=True)

    st.markdown("---")

    # ----------------------------------------------------------------
    # ANALYSE CATEGORIELLE
    # ----------------------------------------------------------------
    if categoric_fields:
        st.markdown("### 🏷️ Répartitions catégorielles")
        for field in categoric_fields:
            name = field["name"]
            label = field.get("label", name)
            if name in df_filtered.columns:
                counts = df_filtered[name].value_counts().reset_index()
                counts.columns = [label, "Nombre"]
                counts["Pourcentage"] = (counts["Nombre"] / counts["Nombre"].sum() * 100).round(1)
                col_a, col_b = st.columns([1, 1])
                with col_a:
                    st.markdown(f"**{label}**")
                    st.dataframe(counts, use_container_width=True, hide_index=True)
                with col_b:
                    st.markdown(f"&nbsp;")
                    st.bar_chart(counts.set_index(label)["Nombre"])

    st.markdown("---")

    # ----------------------------------------------------------------
    # EVOLUTION TEMPORELLE
    # ----------------------------------------------------------------
    if "created_at" in df_filtered.columns:
        st.markdown("### 📅 Évolution des saisies dans le temps")
        df_filtered["created_at"] = pd.to_datetime(df_filtered["created_at"])
        daily = df_filtered.groupby(df_filtered["created_at"].dt.date).size().reset_index()
        daily.columns = ["Date", "Saisies"]
        st.line_chart(daily.set_index("Date"))


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

    st.markdown("### 1️⃣ Informations générales")
    form_title = st.text_input("Titre du formulaire *", placeholder="Ex : Enquête de satisfaction")
    form_description = st.text_area("Description (optionnel)", placeholder="Ex : Collecte des avis étudiants")
    domain_name = st.text_input(
        "Nom interne (sans espaces ni accents) *",
        placeholder="Ex : enquete_etudiants",
        help="Utilisez uniquement des lettres, chiffres et underscores."
    )

    st.markdown("---")
    st.markdown("### 2️⃣ Champs du formulaire")
    st.caption("Ajoute autant de champs que tu veux.")

    nb_fields = st.number_input("Combien de champs veux-tu ?", min_value=1, max_value=20, value=3, step=1)

    fields = []
    for i in range(int(nb_fields)):
        st.markdown(f"**Champ {i + 1}**")
        col1, col2 = st.columns(2)

        with col1:
            field_label = st.text_input(f"Libellé *", placeholder="Ex : Nom", key=f"label_{i}")
            field_name = st.text_input(f"Nom interne *", placeholder="Ex : nom", key=f"name_{i}")
            field_type = st.selectbox(
                f"Type",
                ["Texte", "Nombre entier", "Nombre décimal", "Date", "Liste déroulante"],
                key=f"type_{i}"
            )

        with col2:
            field_required = st.checkbox("Obligatoire", value=True, key=f"required_{i}")
            field_help = st.text_input("Texte d'aide (optionnel)", placeholder="Ex : Entrez votre prénom", key=f"help_{i}")

            field_options = ""
            field_min = None
            field_max = None

            if field_type == "Liste déroulante":
                field_options = st.text_input(
                    "Options (séparées par des virgules) *",
                    placeholder="Ex : Oui, Non, Peut-être",
                    key=f"options_{i}"
                )
            elif field_type in ("Nombre entier", "Nombre décimal"):
                field_min = st.number_input("Valeur minimum", value=0, key=f"min_{i}")
                field_max = st.number_input("Valeur maximum", value=100, key=f"max_{i}")

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

    if st.button("✅ Créer le formulaire", use_container_width=True):
        errors = []
        if not form_title.strip():
            errors.append("Le titre est obligatoire.")
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
                save_schema_file(domain_name.s
