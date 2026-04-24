"""
app.py - Point d'entrée DataCollect Universal.
Interface premium, thème clair, partage de lien par étude.
Google Style Docstrings.
"""

from __future__ import annotations

import io
import logging
import traceback
from datetime import date, datetime
from typing import Any

import pandas as pd
import streamlit as st

from database import (
    delete_schema_db,
    ensure_schemas_table,
    ensure_table,
    fetch_all,
    get_connection,
    insert_row,
    load_schemas_db,
    save_schema_db,
)
from models import build_model, validate_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="DataCollect Universal",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS PREMIUM — thème clair professionnel
# ---------------------------------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Playfair+Display:wght@600;700&display=swap');

/* BASE */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background-color: #F8F9FC;
    color: #1A1D23;
}

/* SIDEBAR */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1E3A5F 0%, #16304F 100%);
    border-right: none;
}
[data-testid="stSidebar"] * {
    color: #E8EDF5 !important;
}
[data-testid="stSidebar"] .stSelectbox label {
    color: #A8B8CC !important;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}

/* HEADER */
.dc-header {
    background: linear-gradient(135deg, #1E3A5F 0%, #2563EB 50%, #3B82F6 100%);
    border-radius: 16px;
    padding: 2.5rem 3rem;
    margin-bottom: 2rem;
    position: relative;
    overflow: hidden;
}
.dc-header::before {
    content: '';
    position: absolute;
    top: -50%;
    right: -10%;
    width: 400px;
    height: 400px;
    background: rgba(255,255,255,0.05);
    border-radius: 50%;
}
.dc-header::after {
    content: '';
    position: absolute;
    bottom: -30%;
    right: 15%;
    width: 200px;
    height: 200px;
    background: rgba(255,255,255,0.05);
    border-radius: 50%;
}
.dc-header h1 {
    font-family: 'Playfair Display', serif;
    color: white;
    font-size: 2.4rem;
    margin: 0;
    font-weight: 700;
}
.dc-header p {
    color: rgba(255,255,255,0.75);
    margin: 0.5rem 0 0 0;
    font-size: 0.9rem;
    letter-spacing: 0.04em;
}
.dc-badge {
    display: inline-block;
    background: rgba(255,255,255,0.2);
    color: white;
    font-size: 0.65rem;
    font-weight: 600;
    padding: 3px 10px;
    border-radius: 20px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-left: 10px;
    vertical-align: middle;
    border: 1px solid rgba(255,255,255,0.3);
}

/* TABS */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    background: white;
    border-radius: 12px;
    padding: 6px;
    border: 1px solid #E5E9F0;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.stTabs [data-baseweb="tab"] {
    background: transparent;
    color: #64748B;
    border-radius: 8px;
    font-size: 0.8rem;
    font-weight: 500;
    letter-spacing: 0.02em;
    padding: 0.5rem 1.2rem;
}
.stTabs [aria-selected="true"] {
    background: #2563EB !important;
    color: white !important;
    box-shadow: 0 2px 8px rgba(37,99,235,0.3);
}

/* CARDS */
.dc-card {
    background: white;
    border-radius: 14px;
    padding: 1.8rem;
    border: 1px solid #E5E9F0;
    box-shadow: 0 2px 12px rgba(0,0,0,0.04);
    margin-bottom: 1.2rem;
}
.dc-card-blue {
    background: linear-gradient(135deg, #EFF6FF, #DBEAFE);
    border: 1px solid #BFDBFE;
    border-radius: 14px;
    padding: 1.8rem;
    margin-bottom: 1.2rem;
}

/* METRIC BOXES */
.metric-row {
    display: flex;
    gap: 1rem;
    margin-bottom: 1.5rem;
}
.metric-box {
    flex: 1;
    background: white;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    border: 1px solid #E5E9F0;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    text-align: center;
}
.metric-box .value {
    font-size: 2rem;
    font-weight: 700;
    color: #2563EB;
    line-height: 1.1;
}
.metric-box .label {
    font-size: 0.7rem;
    font-weight: 500;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #94A3B8;
    margin-top: 4px;
}

/* BUTTONS */
.stButton > button {
    background: linear-gradient(135deg, #2563EB, #3B82F6);
    color: white;
    border: none;
    border-radius: 10px;
    font-family: 'Inter', sans-serif;
    font-weight: 600;
    font-size: 0.85rem;
    letter-spacing: 0.02em;
    padding: 0.65rem 1.5rem;
    box-shadow: 0 4px 12px rgba(37,99,235,0.25);
    transition: all 0.2s;
}
.stButton > button:hover {
    background: linear-gradient(135deg, #1D4ED8, #2563EB);
    box-shadow: 0 6px 16px rgba(37,99,235,0.35);
    transform: translateY(-1px);
}

/* FORM */
.stForm {
    background: white;
    border-radius: 14px;
    padding: 2rem;
    border: 1px solid #E5E9F0;
    box-shadow: 0 2px 12px rgba(0,0,0,0.04);
}

/* INPUTS */
.stTextInput > div > div, .stTextArea > div > div,
.stSelectbox > div > div, .stNumberInput > div > div {
    background: #F8F9FC;
    border: 1.5px solid #E2E8F0;
    border-radius: 8px;
    color: #1A1D23;
}
.stTextInput > div > div:focus-within,
.stTextArea > div > div:focus-within {
    border-color: #2563EB;
    box-shadow: 0 0 0 3px rgba(37,99,235,0.1);
}

/* STUDY CARD */
.study-card {
    background: white;
    border-radius: 16px;
    padding: 2rem;
    border: 1px solid #E5E9F0;
    box-shadow: 0 4px 20px rgba(0,0,0,0.06);
    margin-bottom: 1rem;
    position: relative;
    overflow: hidden;
}
.study-card-accent {
    position: absolute;
    top: 0;
    left: 0;
    width: 5px;
    height: 100%;
    background: linear-gradient(180deg, #2563EB, #3B82F6);
    border-radius: 16px 0 0 16px;
}
.study-card h3 {
    font-family: 'Playfair Display', serif;
    color: #1E3A5F;
    font-size: 1.3rem;
    margin: 0 0 0.5rem 0;
}
.study-card p {
    color: #64748B;
    font-size: 0.85rem;
    margin: 0;
}

/* SHARE BOX */
.share-box {
    background: linear-gradient(135deg, #F0F7FF, #E8F0FE);
    border: 1.5px solid #BFDBFE;
    border-radius: 12px;
    padding: 1.5rem;
    margin-top: 1rem;
}
.share-box h4 {
    color: #1E40AF;
    font-size: 0.9rem;
    font-weight: 600;
    margin: 0 0 0.5rem 0;
}
.share-url {
    background: white;
    border: 1px solid #BFDBFE;
    border-radius: 8px;
    padding: 0.8rem 1rem;
    font-family: 'Inter', monospace;
    font-size: 0.82rem;
    color: #1D4ED8;
    word-break: break-all;
    font-weight: 500;
}

/* STUDY PREVIEW */
.preview-header {
    border-radius: 14px;
    padding: 2rem;
    margin-bottom: 1.5rem;
    color: white;
    position: relative;
    overflow: hidden;
}
.preview-header h2 {
    font-family: 'Playfair Display', serif;
    font-size: 1.8rem;
    margin: 0 0 0.5rem 0;
    color: white;
}
.preview-header p {
    color: rgba(255,255,255,0.85);
    margin: 0;
    font-size: 0.9rem;
}

/* SECTION TITLES */
.section-title {
    font-size: 1.1rem;
    font-weight: 700;
    color: #1E3A5F;
    margin: 1.5rem 0 1rem 0;
    padding-bottom: 0.5rem;
    border-bottom: 2px solid #EFF6FF;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

/* ADMIN STEP */
.step-indicator {
    display: flex;
    align-items: center;
    gap: 0.8rem;
    margin-bottom: 1.2rem;
}
.step-number {
    width: 32px;
    height: 32px;
    background: #2563EB;
    color: white;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: 700;
    font-size: 0.85rem;
    flex-shrink: 0;
}
.step-label {
    font-weight: 600;
    color: #1E3A5F;
    font-size: 1rem;
}

/* DIVIDER */
.dc-divider {
    border: none;
    border-top: 1.5px solid #E5E9F0;
    margin: 1.5rem 0;
}

/* SUCCESS / ERROR */
.stSuccess {
    background: #F0FDF4;
    border: 1px solid #BBF7D0;
    border-radius: 10px;
}
.stError {
    background: #FFF5F5;
    border: 1px solid #FED7D7;
    border-radius: 10px;
}

/* DATAFRAME */
[data-testid="stDataFrame"] {
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid #E5E9F0;
}

/* FIELD CARD */
.field-card {
    background: #F8F9FC;
    border: 1px solid #E5E9F0;
    border-radius: 10px;
    padding: 1.2rem;
    margin-bottom: 0.8rem;
}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# COULEURS THÉMATIQUES par domaine
# ---------------------------------------------------------------------------

THEME_COLORS = [
    ("linear-gradient(135deg, #1E3A5F, #2563EB)", "#EFF6FF"),
    ("linear-gradient(135deg, #065F46, #059669)", "#ECFDF5"),
    ("linear-gradient(135deg, #7C2D12, #DC2626)", "#FFF5F5"),
    ("linear-gradient(135deg, #4C1D95, #7C3AED)", "#F5F3FF"),
    ("linear-gradient(135deg, #713F12, #D97706)", "#FFFBEB"),
    ("linear-gradient(135deg, #0C4A6E, #0284C7)", "#F0F9FF"),
    ("linear-gradient(135deg, #1F2937, #4B5563)", "#F9FAFB"),
]


def get_theme(domain: str) -> tuple[str, str]:
    """Retourne une couleur de thème basée sur le nom du domaine.

    Args:
        domain: Nom du domaine/formulaire.

    Returns:
        Tuple (gradient CSS, couleur de fond claire).
    """
    idx = sum(ord(c) for c in domain) % len(THEME_COLORS)
    return THEME_COLORS[idx]


def get_app_url() -> str:
    """Retourne l'URL de base de l'application.

    Returns:
        URL de base sous forme de chaîne.
    """
    try:
        url = st.get_option("browser.serverAddress") or "votre-app.streamlit.app"
        port = st.get_option("browser.serverPort")
        if port and port not in (80, 443):
            return f"http://{url}:{port}"
        return f"https://{url}"
    except Exception:
        return "https://votre-app.streamlit.app"


# ---------------------------------------------------------------------------
# WIDGETS
# ---------------------------------------------------------------------------

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
    suffix = " *" if required else ""
    full_label = f"{label}{suffix}"
    options: list | None = field.get("options")

    if options:
        display_options = options if not required else ["— Sélectionner —"] + list(options)
        value = st.selectbox(full_label, display_options, help=help_text)
        return None if value == "— Sélectionner —" else value

    if field_type == "int":
        min_val = field.get("min_value")
        default_val = field.get("default", min_val if min_val is not None else 0)
        return st.number_input(full_label, step=1, value=default_val,
                               min_value=min_val, max_value=field.get("max_value"), help=help_text)

    if field_type == "float":
        min_v = field.get("min_value")
        max_v = field.get("max_value")
        default_val = float(field.get("default", min_v if min_v is not None else 0.0))
        return st.number_input(full_label, step=field.get("step", 0.1), value=default_val,
                               min_value=float(min_v) if min_v is not None else None,
                               max_value=float(max_v) if max_v is not None else None,
                               format="%.2f", help=help_text)

    if field_type == "date":
        return st.date_input(full_label, value=date.today(), help=help_text)

    if field.get("multiline"):
        return st.text_area(full_label, value=field.get("default", ""), help=help_text)
    return st.text_input(full_label, value=field.get("default", ""), help=help_text)


# ---------------------------------------------------------------------------
# STATISTIQUES
# ---------------------------------------------------------------------------

def render_statistics(df: pd.DataFrame, schema_fields: list[dict]) -> None:
    """Affiche un dashboard statistique enrichi.

    Args:
        df: DataFrame pandas.
        schema_fields: Liste des champs du schéma.
    """
    if df.empty:
        st.markdown("""
        <div class="dc-card" style="text-align:center; padding: 3rem;">
            <div style="font-size:3rem;">📭</div>
            <div style="font-size:1.1rem; font-weight:600; color:#64748B; margin-top:1rem;">
                Aucune donnée collectée pour l'instant
            </div>
            <div style="color:#94A3B8; font-size:0.85rem; margin-top:0.5rem;">
                Les statistiques apparaîtront dès la première soumission
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    numeric_fields = [f for f in schema_fields if f.get("type") in ("int", "float")]
    categoric_fields = [f for f in schema_fields if f.get("options") or f.get("type") == "str"]
    numeric_cols = [f["name"] for f in numeric_fields]

    completeness = round((df.notna().sum().sum() / (len(df) * len(df.columns))) * 100, 1)
    last_date = ""
    if "created_at" in df.columns:
        last_date = pd.to_datetime(df["created_at"]).max().strftime("%d/%m/%Y")

    st.markdown(f"""
    <div class="metric-row">
        <div class="metric-box">
            <div class="value">{len(df)}</div>
            <div class="label">Réponses collectées</div>
        </div>
        <div class="metric-box">
            <div class="value">{last_date or "—"}</div>
            <div class="label">Dernière saisie</div>
        </div>
        <div class="metric-box">
            <div class="value">{len(numeric_cols)}</div>
            <div class="label">Variables numériques</div>
        </div>
        <div class="metric-box">
            <div class="value">{completeness}%</div>
            <div class="label">Complétude</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # FILTRES
    if categoric_fields:
        st.markdown('<div class="section-title">🔍 Filtres</div>', unsafe_allow_html=True)
        df_filtered = df.copy()
        filter_cols = st.columns(min(len(categoric_fields), 3))
        for i, field in enumerate(categoric_fields[:3]):
            name = field["name"]
            label = field.get("label", name)
            if name in df.columns:
                unique_vals = df[name].dropna().unique().tolist()
                if unique_vals:
                    with filter_cols[i]:
                        selected = st.multiselect(f"Filtrer — {label}", unique_vals, default=unique_vals, key=f"filter_{name}")
                        if selected:
                            df_filtered = df_filtered[df_filtered[name].isin(selected)]
        st.caption(f"**{len(df_filtered)}** réponses affichées sur **{len(df)}** total")
    else:
        df_filtered = df.copy()

    # ANALYSE NUMÉRIQUE
    if numeric_cols:
        st.markdown('<div class="section-title">🔢 Analyse numérique</div>', unsafe_allow_html=True)
        num_df = df_filtered[numeric_cols].apply(pd.to_numeric, errors="coerce")
        stats = num_df.describe().T
        stats.index = [next((f.get("label", f["name"]) for f in numeric_fields if f["name"] == col), col) for col in stats.index]
        stats.columns = ["Nb", "Moyenne", "Écart-type", "Min", "Q25%", "Médiane", "Q75%", "Max"]
        st.dataframe(stats.round(2), use_container_width=True)

        st.markdown('<div class="section-title">📈 Distributions</div>', unsafe_allow_html=True)
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
                        ca, cb, cc = st.columns(3)
                        ca.metric("Moyenne", f"{series.mean():.2f}")
                        cb.metric("Médiane", f"{series.median():.2f}")
                        cc.metric("Écart-type", f"{series.std():.2f}")

        if len(numeric_cols) >= 1 and categoric_fields:
            st.markdown('<div class="section-title">🔀 Tableau croisé</div>', unsafe_allow_html=True)
            col_x, col_y = st.columns(2)
            with col_x:
                selected_num = st.selectbox("Variable numérique",
                    options=[f["name"] for f in numeric_fields],
                    format_func=lambda x: next((f.get("label", f["name"]) for f in numeric_fields if f["name"] == x), x),
                    key="crosstab_num")
            with col_y:
                selected_cat = st.selectbox("Variable catégorielle",
                    options=[f["name"] for f in categoric_fields if f["name"] in df_filtered.columns],
                    format_func=lambda x: next((f.get("label", f["name"]) for f in categoric_fields if f["name"] == x), x),
                    key="crosstab_cat")
            if selected_num and selected_cat:
                cross = (df_filtered.groupby(selected_cat)[selected_num]
                    .apply(lambda s: pd.to_numeric(s, errors="coerce").mean())
                    .round(2).reset_index())
                num_label = next((f.get("label", f["name"]) for f in numeric_fields if f["name"] == selected_num), selected_num)
                cat_label = next((f.get("label", f["name"]) for f in categoric_fields if f["name"] == selected_cat), selected_cat)
                cross.columns = [cat_label, f"Moyenne — {num_label}"]
                st.dataframe(cross, use_container_width=True, hide_index=True)
                st.bar_chart(cross.set_index(cat_label))

        if len(numeric_cols) >= 2:
            st.markdown('<div class="section-title">🔗 Corrélations</div>', unsafe_allow_html=True)
            st.caption("Valeur proche de 1 = forte relation positive, -1 = forte relation inverse")
            corr_df = num_df.rename(columns={f["name"]: f.get("label", f["name"]) for f in numeric_fields})
            corr = corr_df.corr().round(2)
            st.dataframe(corr.style.background_gradient(cmap="RdYlGn", vmin=-1, vmax=1), use_container_width=True)

    # CATÉGORIELLES
    if categoric_fields:
        st.markdown('<div class="section-title">🏷️ Répartitions catégorielles</div>', unsafe_allow_html=True)
        for field in categoric_fields:
            name = field["name"]
            label = field.get("label", name)
            if name in df_filtered.columns:
                counts = df_filtered[name].value_counts().reset_index()
                counts.columns = [label, "Nombre"]
                counts["Pourcentage"] = (counts["Nombre"] / counts["Nombre"].sum() * 100).round(1)
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown(f"**{label}**")
                    st.dataframe(counts, use_container_width=True, hide_index=True)
                with col_b:
                    st.markdown("&nbsp;")
                    st.bar_chart(counts.set_index(label)["Nombre"])

    # ÉVOLUTION TEMPORELLE
    if "created_at" in df_filtered.columns:
        st.markdown('<div class="section-title">📅 Évolution des saisies</div>', unsafe_allow_html=True)
        df_filtered = df_filtered.copy()
        df_filtered["created_at"] = pd.to_datetime(df_filtered["created_at"])
        daily = df_filtered.groupby(df_filtered["created_at"].dt.date).size().reset_index()
        daily.columns = ["Date", "Saisies"]
        st.line_chart(daily.set_index("Date"))


# ---------------------------------------------------------------------------
# EXPORT
# ---------------------------------------------------------------------------

def export_dataframe(df: pd.DataFrame, domain: str) -> None:
    """Affiche les boutons d'export.

    Args:
        df: DataFrame à exporter.
        domain: Nom du domaine.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"{domain}_{timestamp}"
    st.markdown('<div class="section-title">⬇️ Exporter les données</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇ Exporter CSV", data=csv_bytes, file_name=f"{base_name}.csv",
                           mime="text/csv", use_container_width=True)
    with col2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=domain[:31])
        st.download_button("⬇ Exporter Excel", data=buffer.getvalue(),
                           file_name=f"{base_name}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)


# ---------------------------------------------------------------------------
# ONGLET ADMIN
# ---------------------------------------------------------------------------

def render_admin_tab(conn) -> None:
    """Affiche l'onglet Admin pour créer et gérer les formulaires.

    Args:
        conn: Connexion psycopg2 active.
    """
    st.markdown("""
    <div class="dc-card-blue">
        <h3 style="font-family:'Playfair Display',serif; color:#1E3A5F; margin:0 0 0.5rem 0;">
            ⚙️ Gestionnaire de formulaires
        </h3>
        <p style="color:#3B82F6; margin:0; font-size:0.85rem;">
            Crée, gère et partage tes formulaires d'étude. Aucune connaissance technique requise.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # STEP 1
    st.markdown("""
    <div class="step-indicator">
        <div class="step-number">1</div>
        <div class="step-label">Informations générales de l'étude</div>
    </div>
    """, unsafe_allow_html=True)

    with st.container():
        col1, col2 = st.columns(2)
        with col1:
            form_title = st.text_input("📌 Titre de l'étude *", placeholder="Ex : Bien-être étudiant 2025")
            domain_name = st.text_input("🔑 Identifiant unique *", placeholder="Ex : bienetre_2025",
                                        help="Lettres, chiffres et underscores uniquement.")
        with col2:
            form_description = st.text_area("📝 Description", placeholder="Décrivez l'objectif de votre étude...", height=100)

    st.markdown('<hr class="dc-divider">', unsafe_allow_html=True)

    # STEP 2
    st.markdown("""
    <div class="step-indicator">
        <div class="step-number">2</div>
        <div class="step-label">Champs du formulaire</div>
    </div>
    """, unsafe_allow_html=True)

    nb_fields = st.number_input("Nombre de champs", min_value=1, max_value=20, value=3, step=1)

    fields = []
    for i in range(int(nb_fields)):
        with st.container():
            st.markdown(f'<div class="field-card">', unsafe_allow_html=True)
            st.markdown(f"**Champ {i + 1}**")
            col1, col2, col3 = st.columns(3)
            with col1:
                field_label = st.text_input("Libellé *", placeholder="Ex : Âge", key=f"label_{i}")
                field_name = st.text_input("Nom interne *", placeholder="Ex : age", key=f"name_{i}")
            with col2:
                field_type = st.selectbox("Type de donnée",
                    ["Texte", "Nombre entier", "Nombre décimal", "Date", "Liste déroulante"],
                    key=f"type_{i}")
                field_required = st.checkbox("Obligatoire", value=True, key=f"required_{i}")
            with col3:
                field_help = st.text_input("Texte d'aide", placeholder="Optionnel", key=f"help_{i}")
                field_options = ""
                field_min = None
                field_max = None
                if field_type == "Liste déroulante":
                    field_options = st.text_input("Options (séparées par virgules) *",
                        placeholder="Oui, Non, Peut-être", key=f"options_{i}")
                elif field_type in ("Nombre entier", "Nombre décimal"):
                    field_min = st.number_input("Min", value=0, key=f"min_{i}")
                    field_max = st.number_input("Max", value=100, key=f"max_{i}")
            st.markdown('</div>', unsafe_allow_html=True)

        type_map = {"Texte": "str", "Nombre entier": "int", "Nombre décimal": "float",
                    "Date": "date", "Liste déroulante": "str"}
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

    st.markdown('<hr class="dc-divider">', unsafe_allow_html=True)

    if st.button("🚀 Créer et publier le formulaire", use_container_width=True):
        errors = []
        if not form_title.strip():
            errors.append("Le titre est obligatoire.")
        if not domain_name.strip():
            errors.append("L'identifiant est obligatoire.")
        if not domain_name.strip().replace("_", "").isalnum():
            errors.append("L'identifiant : lettres, chiffres et underscores uniquement.")
        for i, f in enumerate(fields):
            if not f.get("label"):
                errors.append(f"Libellé du champ {i + 1} manquant.")
            if not f.get("name"):
                errors.append(f"Nom interne du champ {i + 1} manquant.")
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
                save_schema_db(conn, domain_name.strip(), schema)
                gradient, _ = get_theme(domain_name.strip())
                share_url = f"{get_app_url()}/?study={domain_name.strip()}"
                st.success(f"✅ Formulaire **{form_title}** créé et sauvegardé de façon permanente !")
                st.markdown(f"""
                <div class="share-box">
                    <h4>🔗 Lien de partage de cette étude</h4>
                    <p style="color:#3B82F6; font-size:0.8rem; margin-bottom:0.8rem;">
                        Partagez ce lien à vos participants. Ils accéderont directement à ce formulaire.
                    </p>
                    <div class="share-url">{share_url}</div>
                </div>
                """, unsafe_allow_html=True)
                st.balloons()
                st.rerun()
            except Exception as exc:
                st.error(f"❌ Erreur : {exc}")

    # GESTION DES FORMULAIRES EXISTANTS
    st.markdown('<hr class="dc-divider">', unsafe_allow_html=True)
    st.markdown("""
    <div class="step-indicator">
        <div class="step-number">3</div>
        <div class="step-label">Gérer les formulaires existants</div>
    </div>
    """, unsafe_allow_html=True)

    schemas = load_schemas_db(conn)
    if schemas:
        for domain, schema in schemas.items():
            gradient, bg = get_theme(domain)
            share_url = f"{get_app_url()}/?study={domain}"
            col_info, col_share, col_del = st.columns([3, 2, 1])
            with col_info:
                st.markdown(f"""
                <div class="study-card">
                    <div class="study-card-accent"></div>
                    <h3 style="padding-left:0.5rem;">{schema.get('title', domain)}</h3>
                    <p style="padding-left:0.5rem;">{schema.get('description', '') or 'Aucune description'}</p>
                    <p style="padding-left:0.5rem; margin-top:0.5rem;">
                        <span style="background:#EFF6FF; color:#2563EB; padding:2px 8px; border-radius:6px; font-size:0.75rem; font-weight:600;">
                            {len(schema.get('fields', []))} champs
                        </span>
                    </p>
                </div>
                """, unsafe_allow_html=True)
            with col_share:
                st.markdown(f"""
                <div class="share-box" style="margin-top:0;">
                    <h4>🔗 Lien de partage</h4>
                    <div class="share-url" style="font-size:0.72rem;">{share_url}</div>
                </div>
                """, unsafe_allow_html=True)
            with col_del:
                st.markdown("<div style='margin-top:1.5rem;'></div>", unsafe_allow_html=True)
                if st.button("🗑️", key=f"del_{domain}", help=f"Supprimer {domain}"):
                    try:
                        delete_schema_db(conn, domain)
                        st.success(f"Formulaire **{domain}** supprimé.")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"❌ {exc}")
    else:
        st.info("Aucun formulaire créé pour l'instant.")


# ---------------------------------------------------------------------------
# APPLICATION PRINCIPALE
# ---------------------------------------------------------------------------

def main() -> None:
    """Point d'entrée principal de l'application Streamlit."""

    # Connexion DB
    try:
        conn = get_connection()
        ensure_schemas_table(conn)
    except Exception as exc:
        st.error(f"❌ Erreur de connexion à la base de données : {exc}")
        st.stop()

    # Schémas depuis Supabase
    schemas_db = load_schemas_db(conn)

    # Lecture du paramètre URL ?study=xxx
    query_params = st.query_params
    url_study = query_params.get("study", None)

    # HEADER
    st.markdown("""
    <div class="dc-header">
        <h1>📋 DataCollect <span class="dc-badge">Universal</span></h1>
        <p>Moteur de collecte de données piloté par schéma · Réalisé par Botagne Julien Claude Daniel</p>
    </div>
    """, unsafe_allow_html=True)

    # SIDEBAR
    with st.sidebar:
        st.markdown("""
        <div style="padding: 1rem 0 0.5rem 0;">
            <div style="font-size:1.1rem; font-weight:700; color:white; margin-bottom:0.3rem;">
                📋 DataCollect
            </div>
            <div style="font-size:0.7rem; color:#A8B8CC; letter-spacing:0.08em; text-transform:uppercase;">
                Universal Platform
            </div>
        </div>
        """, unsafe_allow_html=True)

        available = list(schemas_db.keys())

        if url_study and url_study in schemas_db:
            default_idx = available.index(url_study)
        else:
            default_idx = 0

        if not available:
            st.warning("Aucun formulaire disponible.")
            domain = None
        else:
            domain = st.selectbox(
                "Formulaire actif",
                available,
                index=default_idx,
                format_func=lambda x: schemas_db[x].get("title", x),
            )

        if domain:
            gradient, bg = get_theme(domain)
            schema_preview = schemas_db[domain]
            st.markdown(f"""
            <div style="background:rgba(255,255,255,0.08); border-radius:10px; padding:1rem; margin-top:1rem;">
                <div style="font-size:0.7rem; color:#A8B8CC; text-transform:uppercase; letter-spacing:0.08em; margin-bottom:0.5rem;">
                    Étude active
                </div>
                <div style="color:white; font-weight:600; font-size:0.9rem; margin-bottom:0.3rem;">
                    {schema_preview.get('title', domain)}
                </div>
                <div style="color:#A8B8CC; font-size:0.75rem;">
                    {len(schema_preview.get('fields', []))} champs · Données permanentes ✓
                </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<div style='height:2rem'></div>", unsafe_allow_html=True)
        st.markdown("""
        <div style="font-size:0.7rem; color:#A8B8CC; text-align:center; padding-bottom:1rem;">
            🔒 Données sauvegardées en permanence<br>via Supabase PostgreSQL
        </div>
        """, unsafe_allow_html=True)

    # ONGLETS
    tab_form, tab_data, tab_stats, tab_admin = st.tabs([
        "✏️  Saisie", "📋  Données", "📊  Statistiques", "⚙️  Admin"
    ])

    with tab_admin:
        render_admin_tab(conn)

    if not domain:
        with tab_form:
            st.markdown("""
            <div class="dc-card" style="text-align:center; padding:3rem;">
                <div style="font-size:3rem;">👆</div>
                <div style="font-size:1.1rem; font-weight:600; color:#64748B; margin-top:1rem;">
                    Crée d'abord un formulaire dans l'onglet Admin
                </div>
            </div>
            """, unsafe_allow_html=True)
        return

    schema = schemas_db[domain]
    schema_fields: list[dict] = schema.get("fields", [])
    form_title: str = schema.get("title", domain.replace("_", " ").title())
    form_description: str = schema.get("description", "")
    gradient, bg_light = get_theme(domain)
    share_url = f"{get_app_url()}/?study={domain}"

    try:
        ensure_table(conn, domain, schema_fields)
    except Exception as exc:
        st.error(f"❌ Erreur de base de données : {exc}")
        st.stop()

    try:
        model_cls = build_model(schema_fields)
    except Exception as exc:
        st.error(f"❌ Erreur de validation : {exc}")
        st.stop()

    # ----------------------------------------------------------------
    # ONGLET SAISIE
    # ----------------------------------------------------------------
    with tab_form:
        # Aperçu visuel de l'étude
        st.markdown(f"""
        <div class="preview-header" style="background:{gradient};">
            <div style="position:absolute; top:-30px; right:-30px; width:180px; height:180px;
                background:rgba(255,255,255,0.07); border-radius:50%;"></div>
            <div style="position:absolute; bottom:-40px; right:80px; width:120px; height:120px;
                background:rgba(255,255,255,0.05); border-radius:50%;"></div>
            <div style="position:relative;">
                <div style="font-size:0.7rem; letter-spacing:0.12em; text-transform:uppercase;
                    color:rgba(255,255,255,0.65); margin-bottom:0.5rem;">📋 Formulaire d'étude</div>
                <h2>{form_title}</h2>
                <p>{form_description or 'Remplissez le formulaire ci-dessous.'}</p>
                <div style="margin-top:1rem; display:flex; gap:0.8rem; flex-wrap:wrap;">
                    <span style="background:rgba(255,255,255,0.2); color:white; padding:3px 12px;
                        border-radius:20px; font-size:0.75rem; font-weight:500;">
                        {len(schema_fields)} champs
                    </span>
                    <span style="background:rgba(255,255,255,0.2); color:white; padding:3px 12px;
                        border-radius:20px; font-size:0.75rem; font-weight:500;">
                        🔒 Données permanentes
                    </span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Lien de partage
        st.markdown(f"""
        <div class="share-box">
            <h4>🔗 Partager ce formulaire</h4>
            <p style="color:#3B82F6; font-size:0.8rem; margin-bottom:0.8rem;">
                Copiez ce lien et envoyez-le à vos participants. Ils accéderont directement à cette étude.
            </p>
            <div class="share-url">{share_url}</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

        # Formulaire
        with st.form(key=f"form_{domain}", clear_on_submit=True):
            raw_values: dict[str, Any] = {}
            cols_per_row = 2
            field_chunks = [schema_fields[i:i+cols_per_row] for i in range(0, len(schema_fields), cols_per_row)]
            for chunk in field_chunks:
                cols = st.columns(len(chunk))
                for j, field in enumerate(chunk):
                    with cols[j]:
                        try:
                            raw_values[field["name"]] = render_field(field)
                        except Exception as exc:
                            st.warning(f"Widget `{field.get('name')}` : {exc}")
            submitted = st.form_submit_button("✅ Soumettre ma réponse", use_container_width=True)

        if submitted:
            try:
                instance, errors = validate_data(model_cls, raw_values)
                if errors:
                    st.error("❌ Corrigez les erreurs suivantes :")
                    for err in errors:
                        st.markdown(f"• {err}")
                else:
                    row_id = insert_row(conn, domain, instance.model_dump())
                    st.success(f"✅ Réponse #{row_id} enregistrée avec succès ! Merci pour votre participation.")
                    st.balloons()
            except Exception as exc:
                st.error(f"❌ Erreur : {exc}")
                with st.expander("Détails techniques"):
                    st.code(traceback.format_exc())

    # ----------------------------------------------------------------
    # ONGLET DONNÉES
    # ----------------------------------------------------------------
    with tab_data:
        st.markdown(f"""
        <div class="dc-card-blue">
            <h3 style="font-family:'Playfair Display',serif; color:#1E3A5F; margin:0 0 0.3rem 0;">
                📋 {form_title}
            </h3>
            <p style="color:#3B82F6; margin:0; font-size:0.85rem;">{form_description or ''}</p>
        </div>
        """, unsafe_allow_html=True)

        try:
            df = fetch_all(conn, domain)
        except Exception as exc:
            st.error(f"Erreur : {exc}")
            df = pd.DataFrame()

        if df.empty:
            st.markdown("""
            <div class="dc-card" style="text-align:center; padding:3rem;">
                <div style="font-size:3rem;">📭</div>
                <div style="font-size:1.1rem; font-weight:600; color:#64748B; margin-top:1rem;">
                    Aucune donnée collectée pour l'instant
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="display:flex; align-items:center; gap:0.8rem; margin-bottom:1rem;">
                <span style="background:#EFF6FF; color:#2563EB; padding:4px 14px; border-radius:20px;
                    font-weight:600; font-size:0.85rem;">
                    {len(df)} réponse(s) collectée(s)
                </span>
            </div>
            """, unsafe_allow_html=True)
            st.dataframe(df, use_container_width=True, hide_index=True)
            export_dataframe(df, domain)
            # ----------------------------------------------------------------
# ONGLET STATISTIQUES
# ----------------------------------------------------------------
with tab_stats:
    st.markdown(
        f"""
        <div class="preview-header" style="background:{gradient}; padding:1.5rem 2rem;">
            <div style="position:relative;">
                <div style="font-size:0.7rem; letter-spacing:0.12em; text-transform:uppercase;
                    color:rgba(255,255,255,0.65); margin-bottom:0.3rem;">
                    📊 Tableau de bord analytique
                </div>
                <h2 style="font-size:1.4rem; margin:0;">{form_title}</h2>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    try:
        if conn is None:
            raise ValueError("Connexion à la base de données invalide")

        if not domain:
            raise ValueError("Domain non défini")

        df_stats = fetch_all(conn, domain)

        if df_stats is None or df_stats.empty:
            st.warning("⚠️ Aucune donnée disponible pour les statistiques.")
        else:
            render_statistics(df_stats, schema_fields)

    except Exception as exc:
        st.error(f"❌ Erreur : {str(exc)}")


if __name__ == "__main__":
    main()

