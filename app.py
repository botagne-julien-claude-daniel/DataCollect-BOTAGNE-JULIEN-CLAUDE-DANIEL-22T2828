"""
app.py - DataCollect Universal.
Système de rôles, interface envoûtante, thèmes multiples.
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
    load_schema_by_domain,
    load_schemas_db,
    load_schemas_for_user,
    save_schema_db,
    verify_creator_password,
)
from models import build_model, validate_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="DataCollect Universal",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# THÈMES
# ---------------------------------------------------------------------------

THEMES = {
    "☀️ Clair": {
        "bg": "#F8F9FC",
        "card": "#FFFFFF",
        "text": "#1A1D23",
        "text_secondary": "#64748B",
        "accent": "#2563EB",
        "accent_hover": "#1D4ED8",
        "border": "#E2E8F0",
        "input_bg": "#F1F5F9",
        "input_text": "#1A1D23",
        "sidebar_bg": "linear-gradient(160deg,#1E3A5F,#16304F)",
        "sidebar_text": "#E8EDF5",
        "sidebar_muted": "#A8B8CC",
        "secondary_bg": "#EFF6FF",
        "secondary_text": "#2563EB",
        "tag_bg": "#DBEAFE",
        "tag_text": "#1D4ED8",
        "success_bg": "#F0FDF4",
        "success_border": "#86EFAC",
        "error_bg": "#FFF5F5",
        "error_border": "#FCA5A5",
        "metric_value": "#2563EB",
        "metric_label": "#94A3B8",
        "hero": "linear-gradient(135deg,#1E3A5F 0%,#2563EB 60%,#60A5FA 100%)",
        "tab_bg": "#FFFFFF",
        "tab_selected": "#2563EB",
        "divider": "#E2E8F0",
    },
    "🌙 Sombre": {
        "bg": "#0D1117",
        "card": "#161B22",
        "text": "#E6EDF3",
        "text_secondary": "#8B949E",
        "accent": "#7C3AED",
        "accent_hover": "#6D28D9",
        "border": "#30363D",
        "input_bg": "#21262D",
        "input_text": "#E6EDF3",
        "sidebar_bg": "linear-gradient(160deg,#0D1117,#161B22)",
        "sidebar_text": "#E6EDF3",
        "sidebar_muted": "#8B949E",
        "secondary_bg": "#1C1033",
        "secondary_text": "#A78BFA",
        "tag_bg": "#2D1B69",
        "tag_text": "#C4B5FD",
        "success_bg": "#0D2818",
        "success_border": "#238636",
        "error_bg": "#2D0F0F",
        "error_border": "#DA3633",
        "metric_value": "#A78BFA",
        "metric_label": "#8B949E",
        "hero": "linear-gradient(135deg,#1A0533 0%,#4C1D95 60%,#7C3AED 100%)",
        "tab_bg": "#161B22",
        "tab_selected": "#7C3AED",
        "divider": "#30363D",
    },
    "🌊 Océan": {
        "bg": "#F0F9FF",
        "card": "#FFFFFF",
        "text": "#0C4A6E",
        "text_secondary": "#0369A1",
        "accent": "#0284C7",
        "accent_hover": "#0369A1",
        "border": "#BAE6FD",
        "input_bg": "#E0F2FE",
        "input_text": "#0C4A6E",
        "sidebar_bg": "linear-gradient(160deg,#0C4A6E,#075985)",
        "sidebar_text": "#E0F2FE",
        "sidebar_muted": "#7DD3FC",
        "secondary_bg": "#E0F2FE",
        "secondary_text": "#0284C7",
        "tag_bg": "#BAE6FD",
        "tag_text": "#0369A1",
        "success_bg": "#F0FDF4",
        "success_border": "#86EFAC",
        "error_bg": "#FFF5F5",
        "error_border": "#FCA5A5",
        "metric_value": "#0284C7",
        "metric_label": "#7DD3FC",
        "hero": "linear-gradient(135deg,#0C4A6E 0%,#0284C7 60%,#38BDF8 100%)",
        "tab_bg": "#FFFFFF",
        "tab_selected": "#0284C7",
        "divider": "#BAE6FD",
    },
    "🌿 Nature": {
        "bg": "#F0FDF4",
        "card": "#FFFFFF",
        "text": "#052E16",
        "text_secondary": "#166534",
        "accent": "#16A34A",
        "accent_hover": "#15803D",
        "border": "#BBF7D0",
        "input_bg": "#DCFCE7",
        "input_text": "#052E16",
        "sidebar_bg": "linear-gradient(160deg,#052E16,#065F46)",
        "sidebar_text": "#DCFCE7",
        "sidebar_muted": "#86EFAC",
        "secondary_bg": "#DCFCE7",
        "secondary_text": "#16A34A",
        "tag_bg": "#BBF7D0",
        "tag_text": "#15803D",
        "success_bg": "#F0FDF4",
        "success_border": "#86EFAC",
        "error_bg": "#FFF5F5",
        "error_border": "#FCA5A5",
        "metric_value": "#16A34A",
        "metric_label": "#86EFAC",
        "hero": "linear-gradient(135deg,#052E16 0%,#16A34A 60%,#4ADE80 100%)",
        "tab_bg": "#FFFFFF",
        "tab_selected": "#16A34A",
        "divider": "#BBF7D0",
    },
    "🔥 Sunset": {
        "bg": "#FFF7ED",
        "card": "#FFFFFF",
        "text": "#431407",
        "text_secondary": "#9A3412",
        "accent": "#EA580C",
        "accent_hover": "#C2410C",
        "border": "#FED7AA",
        "input_bg": "#FFEDD5",
        "input_text": "#431407",
        "sidebar_bg": "linear-gradient(160deg,#431407,#7C2D12)",
        "sidebar_text": "#FFEDD5",
        "sidebar_muted": "#FDBA74",
        "secondary_bg": "#FFEDD5",
        "secondary_text": "#EA580C",
        "tag_bg": "#FED7AA",
        "tag_text": "#C2410C",
        "success_bg": "#F0FDF4",
        "success_border": "#86EFAC",
        "error_bg": "#FFF5F5",
        "error_border": "#FCA5A5",
        "metric_value": "#EA580C",
        "metric_label": "#FDBA74",
        "hero": "linear-gradient(135deg,#431407 0%,#EA580C 60%,#FB923C 100%)",
        "tab_bg": "#FFFFFF",
        "tab_selected": "#EA580C",
        "divider": "#FED7AA",
    },
}

THEME_COLORS = [
    "linear-gradient(135deg,#1E3A5F,#2563EB)",
    "linear-gradient(135deg,#065F46,#059669)",
    "linear-gradient(135deg,#7C2D12,#DC2626)",
    "linear-gradient(135deg,#4C1D95,#7C3AED)",
    "linear-gradient(135deg,#713F12,#D97706)",
    "linear-gradient(135deg,#0C4A6E,#0284C7)",
]


def get_domain_gradient(domain: str) -> str:
    """Retourne un dégradé basé sur le nom du domaine.

    Args:
        domain: Nom du domaine.

    Returns:
        String CSS gradient.
    """
    idx = sum(ord(c) for c in domain) % len(THEME_COLORS)
    return THEME_COLORS[idx]


def apply_theme(t: dict) -> None:
    """Applique le thème via CSS dynamique.

    Args:
        t: Dictionnaire de couleurs du thème.
    """
    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Playfair+Display:wght@600;700;800&display=swap');

    /* RESET & BASE */
    html, body, [class*="css"] {{
        font-family: 'Inter', sans-serif !important;
        background-color: {t['bg']} !important;
        color: {t['text']} !important;
    }}

    /* CACHER ICÔNE GITHUB ET MENU STREAMLIT */
    #MainMenu, footer, header,
    [data-testid="stToolbar"],
    .viewerBadge_container__1QSob,
    .viewerBadge_link__1S137,
    a[href*="github"],
    button[title*="GitHub"],
    [data-testid="stDecoration"] {{
        display: none !important;
        visibility: hidden !important;
    }}

    /* SIDEBAR */
    [data-testid="stSidebar"] {{
        background: {t['sidebar_bg']} !important;
        border-right: none !important;
    }}
    [data-testid="stSidebar"] * {{
        color: {t['sidebar_text']} !important;
    }}
    [data-testid="stSidebar"] .stSelectbox > div > div {{
        background: rgba(255,255,255,0.1) !important;
        border: 1px solid rgba(255,255,255,0.2) !important;
        color: {t['sidebar_text']} !important;
    }}

    /* TABS */
    .stTabs [data-baseweb="tab-list"] {{
        background: {t['tab_bg']} !important;
        border: 1px solid {t['border']} !important;
        border-radius: 14px !important;
        padding: 5px !important;
        gap: 3px !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
    }}
    .stTabs [data-baseweb="tab"] {{
        background: transparent !important;
        color: {t['text_secondary']} !important;
        border-radius: 10px !important;
        font-size: 0.78rem !important;
        font-weight: 500 !important;
        padding: 0.5rem 1rem !important;
    }}
    .stTabs [aria-selected="true"] {{
        background: {t['tab_selected']} !important;
        color: white !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.2) !important;
    }}

    /* INPUTS - CORRECTION TEXTE INVISIBLE */
    input, textarea, select {{
        color: {t['input_text']} !important;
        background-color: {t['input_bg']} !important;
    }}
    .stTextInput > div > div > input {{
        color: {t['input_text']} !important;
        background-color: {t['input_bg']} !important;
        border: 1.5px solid {t['border']} !important;
        border-radius: 8px !important;
    }}
    .stTextInput > div > div > input:focus {{
        border-color: {t['accent']} !important;
        box-shadow: 0 0 0 3px {t['accent']}22 !important;
    }}
    .stTextArea > div > div > textarea {{
        color: {t['input_text']} !important;
        background-color: {t['input_bg']} !important;
        border: 1.5px solid {t['border']} !important;
        border-radius: 8px !important;
    }}
    .stSelectbox > div > div {{
        color: {t['input_text']} !important;
        background-color: {t['input_bg']} !important;
        border: 1.5px solid {t['border']} !important;
        border-radius: 8px !important;
    }}
    .stNumberInput > div > div > input {{
        color: {t['input_text']} !important;
        background-color: {t['input_bg']} !important;
        border: 1.5px solid {t['border']} !important;
        border-radius: 8px !important;
    }}

    /* BOUTONS */
    .stButton > button {{
        background: {t['accent']} !important;
        color: white !important;
        border: none !important;
        border-radius: 10px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
        padding: 0.65rem 1.5rem !important;
        box-shadow: 0 4px 14px {t['accent']}44 !important;
        transition: all 0.2s !important;
        width: 100% !important;
    }}
    .stButton > button:hover {{
        background: {t['accent_hover']} !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 20px {t['accent']}55 !important;
    }}

    /* FORM */
    [data-testid="stForm"] {{
        background: {t['card']} !important;
        border-radius: 16px !important;
        border: 1px solid {t['border']} !important;
        padding: 1.5rem !important;
        box-shadow: 0 4px 20px rgba(0,0,0,0.06) !important;
    }}

    /* CHECKBOXES & LABELS */
    .stCheckbox label, .stRadio label {{
        color: {t['text']} !important;
    }}
    label {{
        color: {t['text']} !important;
        font-weight: 500 !important;
    }}

    /* DATAFRAME */
    [data-testid="stDataFrame"] {{
        border-radius: 12px !important;
        overflow: hidden !important;
        border: 1px solid {t['border']} !important;
    }}

    /* MÉTRIQUES */
    [data-testid="stMetricValue"] {{
        color: {t['metric_value']} !important;
    }}

    /* SCROLLBAR */
    ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
    ::-webkit-scrollbar-track {{ background: {t['bg']}; }}
    ::-webkit-scrollbar-thumb {{ background: {t['border']}; border-radius: 3px; }}
    </style>
    """, unsafe_allow_html=True)


def get_app_url() -> str:
    """Retourne l'URL de base de l'application.

    Returns:
        URL sous forme de chaîne.
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
# SESSION STATE - AUTHENTIFICATION
# ---------------------------------------------------------------------------

def init_session() -> None:
    """Initialise les variables de session."""
    if "role" not in st.session_state:
        st.session_state.role = "participant"
    if "user_id" not in st.session_state:
        st.session_state.user_id = None
    if "theme" not in st.session_state:
        st.session_state.theme = "☀️ Clair"


def is_admin() -> bool:
    """Vérifie si l'utilisateur est l'administrateur.

    Returns:
        True si admin.
    """
    return st.session_state.role == "admin"


def is_creator(domain_creator_id: str) -> bool:
    """Vérifie si l'utilisateur est le créateur d'un formulaire.

    Args:
        domain_creator_id: ID du créateur du formulaire.

    Returns:
        True si créateur ou admin.
    """
    return is_admin() or st.session_state.user_id == domain_creator_id


# ---------------------------------------------------------------------------
# PANNEAU DE CONNEXION SIDEBAR
# ---------------------------------------------------------------------------

def render_auth_sidebar() -> None:
    """Affiche le panneau d'authentification dans la sidebar."""
    t = THEMES[st.session_state.theme]

    if st.session_state.role == "participant":
        with st.sidebar.expander("🔐 Se connecter", expanded=False):
            login_type = st.radio("Type de connexion",
                ["👑 Administrateur", "👤 Créateur de formulaire"],
                key="login_type", label_visibility="collapsed")

            if login_type == "👑 Administrateur":
                pwd = st.text_input("Mot de passe admin", type="password", key="admin_pwd_input")
                if st.button("Connexion Admin", key="btn_admin_login"):
                    if pwd == st.secrets.get("ADMIN_PASSWORD", ""):
                        st.session_state.role = "admin"
                        st.session_state.user_id = st.secrets.get("ADMIN_ID", "admin")
                        st.rerun()
                    else:
                        st.error("❌ Mot de passe incorrect")
            else:
                user_id = st.text_input("Votre identifiant", placeholder="Ex: prof_dupont", key="creator_id_input")
                if st.button("Connexion Créateur", key="btn_creator_login"):
                    if user_id.strip():
                        st.session_state.role = "creator"
                        st.session_state.user_id = user_id.strip()
                        st.rerun()
                    else:
                        st.error("❌ Identifiant requis")

    else:
        role_label = "👑 Admin" if is_admin() else f"👤 {st.session_state.user_id}"
        st.sidebar.markdown(f"""
        <div style="background:rgba(255,255,255,0.1); border-radius:10px;
            padding:0.8rem 1rem; margin-bottom:0.5rem;">
            <div style="font-size:0.7rem; color:{t['sidebar_muted']}; text-transform:uppercase;
                letter-spacing:0.08em;">Connecté en tant que</div>
            <div style="color:white; font-weight:600; margin-top:3px;">{role_label}</div>
        </div>
        """, unsafe_allow_html=True)
        if st.sidebar.button("🚪 Se déconnecter", key="btn_logout"):
            st.session_state.role = "participant"
            st.session_state.user_id = None
            st.rerun()


# ---------------------------------------------------------------------------
# WIDGETS FORMULAIRE
# ---------------------------------------------------------------------------

def render_field(field: dict) -> Any:
    """Génère le widget Streamlit pour un champ du schéma.

    Args:
        field: Dictionnaire décrivant un champ.

    Returns:
        La valeur saisie.
    """
    label: str = field.get("label", field["name"])
    field_type: str = field.get("type", "str")
    help_text: str | None = field.get("help")
    required: bool = field.get("required", True)
    full_label = f"{label} {'*' if required else ''}"
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

def render_statistics(df: pd.DataFrame, schema_fields: list[dict], t: dict) -> None:
    """Affiche le dashboard statistique enrichi.

    Args:
        df: DataFrame pandas.
        schema_fields: Liste des champs du schéma.
        t: Dictionnaire du thème actif.
    """
    if df.empty:
        st.markdown(f"""
        <div style="background:{t['card']}; border:1px solid {t['border']}; border-radius:16px;
            padding:3rem; text-align:center; margin:1rem 0;">
            <div style="font-size:3rem;">📭</div>
            <div style="font-size:1.1rem; font-weight:600; color:{t['text_secondary']}; margin-top:1rem;">
                Aucune donnée collectée pour l'instant
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    numeric_fields = [f for f in schema_fields if f.get("type") in ("int", "float")]
    categoric_fields = [f for f in schema_fields if f.get("options") or f.get("type") == "str"]
    numeric_cols = [f["name"] for f in numeric_fields]
    completeness = round((df.notna().sum().sum() / (len(df) * len(df.columns))) * 100, 1)
    last_date = pd.to_datetime(df["created_at"]).max().strftime("%d/%m/%Y") if "created_at" in df.columns else "—"

    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    for col, val, label in [
        (col1, str(len(df)), "Réponses"),
        (col2, last_date, "Dernière saisie"),
        (col3, str(len(numeric_cols)), "Variables num."),
        (col4, f"{completeness}%", "Complétude"),
    ]:
        with col:
            st.markdown(f"""
            <div style="background:{t['card']}; border:1px solid {t['border']}; border-radius:14px;
                padding:1.2rem; text-align:center; box-shadow:0 2px 8px rgba(0,0,0,0.04);">
                <div style="font-size:1.8rem; font-weight:800; color:{t['metric_value']};">{val}</div>
                <div style="font-size:0.7rem; text-transform:uppercase; letter-spacing:0.08em;
                    color:{t['metric_label']}; margin-top:4px;">{label}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)

    # FILTRES
    df_filtered = df.copy()
    if categoric_fields:
        st.markdown(f"<div style='font-weight:700; color:{t['text']}; margin-bottom:0.8rem;'>🔍 Filtres</div>",
                    unsafe_allow_html=True)
        filter_cols = st.columns(min(len(categoric_fields), 3))
        for i, field in enumerate(categoric_fields[:3]):
            name = field["name"]
            label = field.get("label", name)
            if name in df.columns:
                unique_vals = df[name].dropna().unique().tolist()
                if unique_vals:
                    with filter_cols[i]:
                        selected = st.multiselect(f"{label}", unique_vals, default=unique_vals, key=f"filter_{name}")
                        if selected:
                            df_filtered = df_filtered[df_filtered[name].isin(selected)]
        st.caption(f"**{len(df_filtered)}** / **{len(df)}** réponses affichées")

    # NUMÉRIQUE
    if numeric_cols:
        st.markdown(f"<div style='font-weight:700; color:{t['text']}; margin:1.5rem 0 0.8rem;'>🔢 Analyse numérique</div>",
                    unsafe_allow_html=True)
        num_df = df_filtered[numeric_cols].apply(pd.to_numeric, errors="coerce")
        stats = num_df.describe().T
        stats.index = [next((f.get("label", f["name"]) for f in numeric_fields if f["name"] == col), col) for col in stats.index]
        stats.columns = ["Nb", "Moyenne", "Écart-type", "Min", "Q25%", "Médiane", "Q75%", "Max"]
        st.dataframe(stats.round(2), use_container_width=True)

        dist_cols = st.columns(min(len(numeric_cols), 2))
        for i, field in enumerate(numeric_fields):
            name = field["name"]
            label = field.get("label", name)
            if name in df_filtered.columns:
                series = pd.to_numeric(df_filtered[name], errors="coerce").dropna()
                if not series.empty:
                    with dist_cols[i % 2]:
                        st.markdown(f"**{label}**")
                        st.bar_chart(series.value_counts(bins=min(10, len(series.unique()))).sort_index())
                        ca, cb, cc = st.columns(3)
                        ca.metric("Moyenne", f"{series.mean():.2f}")
                        cb.metric("Médiane", f"{series.median():.2f}")
                        cc.metric("Écart-type", f"{series.std():.2f}")

        if len(numeric_cols) >= 1 and categoric_fields:
            st.markdown(f"<div style='font-weight:700; color:{t['text']}; margin:1.5rem 0 0.8rem;'>🔀 Tableau croisé</div>",
                        unsafe_allow_html=True)
            col_x, col_y = st.columns(2)
            with col_x:
                selected_num = st.selectbox("Variable numérique",
                    [f["name"] for f in numeric_fields],
                    format_func=lambda x: next((f.get("label", f["name"]) for f in numeric_fields if f["name"] == x), x),
                    key="crosstab_num")
            with col_y:
                selected_cat = st.selectbox("Variable catégorielle",
                    [f["name"] for f in categoric_fields if f["name"] in df_filtered.columns],
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
            st.markdown(f"<div style='font-weight:700; color:{t['text']}; margin:1.5rem 0 0.8rem;'>🔗 Corrélations</div>",
                        unsafe_allow_html=True)
            corr = num_df.rename(columns={f["name"]: f.get("label", f["name"]) for f in numeric_fields}).corr().round(2)
            st.dataframe(corr.style.background_gradient(cmap="RdYlGn", vmin=-1, vmax=1), use_container_width=True)

    # CATÉGORIELLES
    if categoric_fields:
        st.markdown(f"<div style='font-weight:700; color:{t['text']}; margin:1.5rem 0 0.8rem;'>🏷️ Répartitions</div>",
                    unsafe_allow_html=True)
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
                    st.bar_chart(counts.set_index(label)["Nombre"])

    # TEMPOREL
    if "created_at" in df_filtered.columns:
        st.markdown(f"<div style='font-weight:700; color:{t['text']}; margin:1.5rem 0 0.8rem;'>📅 Évolution</div>",
                    unsafe_allow_html=True)
        df_filtered = df_filtered.copy()
        df_filtered["created_at"] = pd.to_datetime(df_filtered["created_at"])
        daily = df_filtered.groupby(df_filtered["created_at"].dt.date).size().reset_index()
        daily.columns = ["Date", "Saisies"]
        st.line_chart(daily.set_index("Date"))


# ---------------------------------------------------------------------------
# EXPORT
# ---------------------------------------------------------------------------

def export_dataframe(df: pd.DataFrame, domain: str, t: dict) -> None:
    """Affiche les boutons d'export.

    Args:
        df: DataFrame à exporter.
        domain: Nom du domaine.
        t: Thème actif.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"{domain}_{timestamp}"
    col1, col2 = st.columns(2)
    with col1:
        st.download_button("⬇ CSV", data=df.to_csv(index=False).encode("utf-8-sig"),
                           file_name=f"{base_name}.csv", mime="text/csv", use_container_width=True)
    with col2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=domain[:31])
        st.download_button("⬇ Excel", data=buffer.getvalue(),
                           file_name=f"{base_name}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)


# ---------------------------------------------------------------------------
# ONGLET ADMIN
# ---------------------------------------------------------------------------

def render_admin_tab(conn, t: dict) -> None:
    """Affiche l'onglet de création et gestion des formulaires.

    Args:
        conn: Connexion psycopg2 active.
        t: Thème actif.
    """
    can_create = st.session_state.role in ("admin", "creator")

    if not can_create:
        st.markdown(f"""
        <div style="background:{t['secondary_bg']}; border:1px solid {t['border']};
            border-radius:16px; padding:2.5rem; text-align:center; margin:1rem 0;">
            <div style="font-size:2.5rem;">🔐</div>
            <div style="font-size:1.1rem; font-weight:700; color:{t['text']}; margin-top:1rem;">
                Connexion requise
            </div>
            <div style="color:{t['text_secondary']}; font-size:0.85rem; margin-top:0.5rem;">
                Connectez-vous via la sidebar pour créer ou gérer des formulaires.
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    st.markdown(f"""
    <div style="background:{t['secondary_bg']}; border:1px solid {t['border']};
        border-radius:16px; padding:1.8rem; margin-bottom:1.5rem;">
        <div style="font-size:1.2rem; font-weight:700; color:{t['text']}; margin-bottom:0.3rem;">
            🚀 Créer un nouveau formulaire
        </div>
        <div style="color:{t['text_secondary']}; font-size:0.85rem;">
            Votre formulaire sera sauvegardé définitivement dans Supabase.
        </div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        form_title = st.text_input("📌 Titre de l'étude *", placeholder="Ex : Bien-être étudiant 2025")
        domain_name = st.text_input("🔑 Identifiant unique *", placeholder="Ex : bienetre_2025")
    with col2:
        form_description = st.text_area("📝 Description", placeholder="Objectif de l'étude...", height=100)
        creator_password = st.text_input("🔒 Mot de passe gestionnaire *",
            type="password", placeholder="Pour gérer votre formulaire",
            help="Retenez ce mot de passe, il vous permettra de gérer votre formulaire.")

    nb_fields = st.number_input("Nombre de champs", min_value=1, max_value=20, value=3, step=1)
    fields = []

    for i in range(int(nb_fields)):
        st.markdown(f"""
        <div style="background:{t['card']}; border:1px solid {t['border']}; border-radius:12px;
            padding:1.2rem; margin-bottom:0.8rem;">
            <div style="font-weight:600; color:{t['accent']}; margin-bottom:0.8rem;">Champ {i+1}</div>
        """, unsafe_allow_html=True)

        col1, col2, col3 = st.columns(3)
        with col1:
            field_label = st.text_input("Libellé *", placeholder="Ex: Âge", key=f"label_{i}")
            field_name = st.text_input("Nom interne *", placeholder="Ex: age", key=f"name_{i}")
        with col2:
            field_type = st.selectbox("Type", ["Texte", "Nombre entier", "Nombre décimal", "Date", "Liste déroulante"], key=f"type_{i}")
            field_required = st.checkbox("Obligatoire", value=True, key=f"required_{i}")
        with col3:
            field_help = st.text_input("Aide", placeholder="Optionnel", key=f"help_{i}")
            field_options, field_min, field_max = "", None, None
            if field_type == "Liste déroulante":
                field_options = st.text_input("Options (virgules)", placeholder="Oui, Non", key=f"options_{i}")
            elif field_type in ("Nombre entier", "Nombre décimal"):
                field_min = st.number_input("Min", value=0, key=f"min_{i}")
                field_max = st.number_input("Max", value=100, key=f"max_{i}")

        st.markdown("</div>", unsafe_allow_html=True)

        type_map = {"Texte": "str", "Nombre entier": "int", "Nombre décimal": "float",
                    "Date": "date", "Liste déroulante": "str"}
        fd: dict[str, Any] = {"name": field_name.strip().replace(" ", "_"),
                               "label": field_label.strip(),
                               "type": type_map[field_type], "required": field_required}
        if field_help: fd["help"] = field_help
        if field_type == "Liste déroulante" and field_options:
            fd["options"] = [o.strip() for o in field_options.split(",") if o.strip()]
        if field_type in ("Nombre entier", "Nombre décimal") and field_min is not None:
            fd["min_value"] = field_min
            fd["max_value"] = field_max
        fields.append(fd)

    if st.button("🚀 Créer et publier", use_container_width=True):
        errors = []
        if not form_title.strip(): errors.append("Titre obligatoire.")
        if not domain_name.strip(): errors.append("Identifiant obligatoire.")
        if not domain_name.strip().replace("_", "").isalnum(): errors.append("Identifiant : lettres, chiffres, underscores.")
        if not creator_password.strip(): errors.append("Mot de passe gestionnaire obligatoire.")
        for i, f in enumerate(fields):
            if not f.get("label"): errors.append(f"Libellé champ {i+1} manquant.")
            if not f.get("name"): errors.append(f"Nom interne champ {i+1} manquant.")

        if errors:
            for err in errors: st.error(f"❌ {err}")
        else:
            schema = {"title": form_title.strip(), "description": form_description.strip(),
                      "fields": [f for f in fields if f.get("name") and f.get("label")]}
            try:
                save_schema_db(conn, domain_name.strip(), schema,
                               creator_id=st.session_state.user_id,
                               creator_password=creator_password.strip(),
                               is_public=False)
                share_url = f"{get_app_url()}/?study={domain_name.strip()}"
                st.success(f"✅ Formulaire **{form_title}** créé et sauvegardé définitivement !")
                st.markdown(f"""
                <div style="background:{t['secondary_bg']}; border:1.5px solid {t['border']};
                    border-radius:12px; padding:1.5rem; margin-top:1rem;">
                    <div style="font-weight:700; color:{t['accent']}; margin-bottom:0.5rem;">
                        🔗 Lien de partage de votre étude
                    </div>
                    <div style="background:{t['card']}; border:1px solid {t['border']};
                        border-radius:8px; padding:0.8rem; font-family:monospace;
                        font-size:0.82rem; color:{t['accent']}; word-break:break-all;">
                        {share_url}
                    </div>
                    <div style="color:{t['text_secondary']}; font-size:0.78rem; margin-top:0.8rem;">
                        ⚠️ Partagez uniquement ce lien aux participants de votre étude.
                    </div>
                </div>
                """, unsafe_allow_html=True)
                st.balloons()
                st.rerun()
            except Exception as exc:
                st.error(f"❌ Erreur : {exc}")

    st.markdown(f"<div style='height:1.5rem'></div>", unsafe_allow_html=True)
    st.markdown(f"""
    <div style="font-weight:700; color:{t['text']}; font-size:1.05rem; margin-bottom:1rem;
        padding-bottom:0.5rem; border-bottom:2px solid {t['divider']};">
        📁 Mes formulaires
    </div>
    """, unsafe_allow_html=True)

    if is_admin():
        schemas = load_schemas_db(conn)
    else:
        schemas = load_schemas_for_user(conn, st.session_state.user_id)

    if not schemas:
        st.info("Aucun formulaire créé pour l'instant.")
    else:
        for domain, schema in schemas.items():
            gradient = get_domain_gradient(domain)
            share_url = f"{get_app_url()}/?study={domain}"
            creator = schema.get("_creator_id", "inconnu")

            col_info, col_actions = st.columns([3, 1])
            with col_info:
                st.markdown(f"""
                <div style="background:{t['card']}; border:1px solid {t['border']}; border-radius:14px;
                    padding:1.5rem; position:relative; overflow:hidden; margin-bottom:0.8rem;">
                    <div style="position:absolute; top:0; left:0; width:4px; height:100%;
                        background:{gradient}; border-radius:14px 0 0 14px;"></div>
                    <div style="padding-left:0.8rem;">
                        <div style="font-family:'Playfair Display',serif; font-size:1.1rem;
                            font-weight:700; color:{t['text']};">{schema.get('title', domain)}</div>
                        <div style="color:{t['text_secondary']}; font-size:0.8rem; margin-top:0.3rem;">
                            {schema.get('description', '') or 'Aucune description'}
                        </div>
                        <div style="margin-top:0.8rem; display:flex; gap:0.5rem; flex-wrap:wrap;">
                            <span style="background:{t['tag_bg']}; color:{t['tag_text']};
                                padding:2px 10px; border-radius:20px; font-size:0.72rem; font-weight:600;">
                                {len(schema.get('fields', []))} champs
                            </span>
                            <span style="background:{t['tag_bg']}; color:{t['tag_text']};
                                padding:2px 10px; border-radius:20px; font-size:0.72rem; font-weight:600;">
                                👤 {creator}
                            </span>
                        </div>
                        <div style="background:{t['secondary_bg']}; border:1px solid {t['border']};
                            border-radius:8px; padding:0.6rem 0.8rem; margin-top:0.8rem;
                            font-family:monospace; font-size:0.75rem; color:{t['accent']};
                            word-break:break-all;">
                            🔗 {share_url}
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            with col_actions:
                st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
                if is_admin() or st.session_state.user_id == schema.get("_creator_id"):
                    if st.button("🗑️ Supprimer", key=f"del_{domain}", use_container_width=True):
                        try:
                            delete_schema_db(conn, domain)
                            st.success(f"Supprimé !")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"❌ {exc}")


# ---------------------------------------------------------------------------
# APPLICATION PRINCIPALE
# ---------------------------------------------------------------------------

def main() -> None:
    """Point d'entrée principal."""
    init_session()

    try:
        conn = get_connection()
        ensure_schemas_table(conn)
    except Exception as exc:
        st.error(f"❌ Erreur connexion : {exc}")
        st.stop()

    t = THEMES[st.session_state.theme]
    apply_theme(t)

    query_params = st.query_params
    url_study = query_params.get("study", None)

    st.markdown(f"""
    <div style="background:{t['hero']}; border-radius:20px; padding:2.5rem 3rem;
        margin-bottom:2rem; position:relative; overflow:hidden;">
        <div style="position:absolute; top:-40px; right:-40px; width:250px; height:250px;
            background:rgba(255,255,255,0.06); border-radius:50%;"></div>
        <div style="position:absolute; bottom:-60px; right:100px; width:180px; height:180px;
            background:rgba(255,255,255,0.04); border-radius:50%;"></div>
        <div style="position:absolute; top:20px; right:200px; width:80px; height:80px;
            background:rgba(255,255,255,0.05); border-radius:50%;"></div>
        <div style="position:relative;">
            <div style="font-size:0.7rem; letter-spacing:0.15em; text-transform:uppercase;
                color:rgba(255,255,255,0.6); margin-bottom:0.5rem;">
                Plateforme de collecte de données
            </div>
            <div style="font-family:'Playfair Display',serif; font-size:2.5rem; font-weight:800;
                color:white; line-height:1.1; margin-bottom:0.5rem;">
                DataCollect
                <span style="display:inline-block; background:rgba(255,255,255,0.2);
                    color:white; font-size:0.6rem; font-weight:600; padding:4px 12px;
                    border-radius:20px; letter-spacing:0.1em; text-transform:uppercase;
                    vertical-align:middle; margin-left:10px; border:1px solid rgba(255,255,255,0.3);">
                    Universal
                </span>
            </div>
            <div style="color:rgba(255,255,255,0.7); font-size:0.85rem;">
                Réalisé par Botagne Julien Claude Daniel · Moteur piloté par schéma
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown(f"""
        <div style="padding:1.2rem 0 0.8rem;">
            <div style="font-family:'Playfair Display',serif; font-size:1.4rem;
                        font-weight:700; color:white;">{form_title}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            try:
                df_stats = fetch_all(conn, domain)
                render_statistics(df_stats, schema_fields, t)
            except Exception as exc:
                st.error(f"❌ Erreur : {exc}")


if __name__ == "__main__":
    main()
