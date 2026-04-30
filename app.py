"""
app.py - DataCollect Universal.
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
    initial_sidebar_state="collapsed",
)

THEMES = {
    "☀️ Clair": {
        "bg": "#F8F9FC", "card": "#FFFFFF", "text": "#1A1D23",
        "text_secondary": "#64748B", "accent": "#2563EB", "accent_hover": "#1D4ED8",
        "border": "#E2E8F0", "input_bg": "#F1F5F9", "input_text": "#1A1D23",
        "sidebar_bg": "linear-gradient(160deg,#1E3A5F,#16304F)",
        "sidebar_text": "#E8EDF5", "sidebar_muted": "#A8B8CC",
        "secondary_bg": "#EFF6FF", "secondary_text": "#2563EB",
        "tag_bg": "#DBEAFE", "tag_text": "#1D4ED8",
        "metric_value": "#2563EB", "metric_label": "#94A3B8",
        "hero": "linear-gradient(135deg,#1E3A5F 0%,#2563EB 60%,#60A5FA 100%)",
        "tab_bg": "#FFFFFF", "tab_selected": "#2563EB", "divider": "#E2E8F0",
    },
    "🌙 Sombre": {
        "bg": "#0D1117", "card": "#161B22", "text": "#E6EDF3",
        "text_secondary": "#8B949E", "accent": "#7C3AED", "accent_hover": "#6D28D9",
        "border": "#30363D", "input_bg": "#21262D", "input_text": "#E6EDF3",
        "sidebar_bg": "linear-gradient(160deg,#0D1117,#161B22)",
        "sidebar_text": "#E6EDF3", "sidebar_muted": "#8B949E",
        "secondary_bg": "#1C1033", "secondary_text": "#A78BFA",
        "tag_bg": "#2D1B69", "tag_text": "#C4B5FD",
        "metric_value": "#A78BFA", "metric_label": "#8B949E",
        "hero": "linear-gradient(135deg,#1A0533 0%,#4C1D95 60%,#7C3AED 100%)",
        "tab_bg": "#161B22", "tab_selected": "#7C3AED", "divider": "#30363D",
    },
    "🌊 Océan": {
        "bg": "#F0F9FF", "card": "#FFFFFF", "text": "#0C4A6E",
        "text_secondary": "#0369A1", "accent": "#0284C7", "accent_hover": "#0369A1",
        "border": "#BAE6FD", "input_bg": "#E0F2FE", "input_text": "#0C4A6E",
        "sidebar_bg": "linear-gradient(160deg,#0C4A6E,#075985)",
        "sidebar_text": "#E0F2FE", "sidebar_muted": "#7DD3FC",
        "secondary_bg": "#E0F2FE", "secondary_text": "#0284C7",
        "tag_bg": "#BAE6FD", "tag_text": "#0369A1",
        "metric_value": "#0284C7", "metric_label": "#7DD3FC",
        "hero": "linear-gradient(135deg,#0C4A6E 0%,#0284C7 60%,#38BDF8 100%)",
        "tab_bg": "#FFFFFF", "tab_selected": "#0284C7", "divider": "#BAE6FD",
    },
    "🌿 Nature": {
        "bg": "#F0FDF4", "card": "#FFFFFF", "text": "#052E16",
        "text_secondary": "#166534", "accent": "#16A34A", "accent_hover": "#15803D",
        "border": "#BBF7D0", "input_bg": "#DCFCE7", "input_text": "#052E16",
        "sidebar_bg": "linear-gradient(160deg,#052E16,#065F46)",
        "sidebar_text": "#DCFCE7", "sidebar_muted": "#86EFAC",
        "secondary_bg": "#DCFCE7", "secondary_text": "#16A34A",
        "tag_bg": "#BBF7D0", "tag_text": "#15803D",
        "metric_value": "#16A34A", "metric_label": "#86EFAC",
        "hero": "linear-gradient(135deg,#052E16 0%,#16A34A 60%,#4ADE80 100%)",
        "tab_bg": "#FFFFFF", "tab_selected": "#16A34A", "divider": "#BBF7D0",
    },
    "🔥 Sunset": {
        "bg": "#FFF7ED", "card": "#FFFFFF", "text": "#431407",
        "text_secondary": "#9A3412", "accent": "#EA580C", "accent_hover": "#C2410C",
        "border": "#FED7AA", "input_bg": "#FFEDD5", "input_text": "#431407",
        "sidebar_bg": "linear-gradient(160deg,#431407,#7C2D12)",
        "sidebar_text": "#FFEDD5", "sidebar_muted": "#FDBA74",
        "secondary_bg": "#FFEDD5", "secondary_text": "#EA580C",
        "tag_bg": "#FED7AA", "tag_text": "#C2410C",
        "metric_value": "#EA580C", "metric_label": "#FDBA74",
        "hero": "linear-gradient(135deg,#431407 0%,#EA580C 60%,#FB923C 100%)",
        "tab_bg": "#FFFFFF", "tab_selected": "#EA580C", "divider": "#FED7AA",
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
    """Retourne un dégradé CSS basé sur le domaine.

    Args:
        domain: Nom du domaine.

    Returns:
        String CSS gradient.
    """
    return THEME_COLORS[sum(ord(c) for c in domain) % len(THEME_COLORS)]


def apply_theme(t: dict) -> None:
    """Applique le thème via CSS.

    Args:
        t: Dictionnaire de couleurs.
    """
    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Playfair+Display:wght@600;700;800&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Inter', sans-serif !important;
        background-color: {t['bg']} !important;
        color: {t['text']} !important;
    }}

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

    [data-testid="stSidebar"] {{
        background: {t['sidebar_bg']} !important;
        border-right: none !important;
    }}
    [data-testid="stSidebar"] * {{
        color: {t['sidebar_text']} !important;
    }}

    .stTabs [data-baseweb="tab-list"] {{
        background: {t['tab_bg']} !important;
        border: 1px solid {t['border']} !important;
        border-radius: 14px !important;
        padding: 5px !important;
        gap: 3px !important;
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
    }}

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

    .stButton > button {{
        background: {t['accent']} !important;
        color: white !important;
        border: none !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
        padding: 0.65rem 1.5rem !important;
        box-shadow: 0 4px 14px {t['accent']}44 !important;
        width: 100% !important;
    }}
    .stButton > button:hover {{
        background: {t['accent_hover']} !important;
        transform: translateY(-2px) !important;
    }}

    [data-testid="stForm"] {{
        background: {t['card']} !important;
        border-radius: 16px !important;
        border: 1px solid {t['border']} !important;
        padding: 1.5rem !important;
        box-shadow: 0 4px 20px rgba(0 0 0 / 0.06) !important;
    }}

    label {{ color: {t['text']} !important; font-weight: 500 !important; }}
    .stCheckbox label {{ color: {t['text']} !important; }}

    [data-testid="stDataFrame"] {{
        border-radius: 12px !important;
        border: 1px solid {t['border']} !important;
    }}
    </style>
    """, unsafe_allow_html=True)

def get_app_url() -> str:
    """
    Retourne l'URL de base de l'application.

    - En production (Streamlit Cloud) : utilise BASE_URL défini dans les secrets
    - En local : fallback sur localhost
    """
    return st.secrets.get("BASE_URL", "http://localhost:8501")
def get_active_users(conn) -> int:
    """Compte les utilisateurs actifs des 5 dernières minutes.

    Args:
        conn: Connexion psycopg2 active.

    Returns:
        Nombre d'utilisateurs actifs.
    """
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS _sessions (
                    id SERIAL PRIMARY KEY,
                    last_seen TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
            cur.execute("INSERT INTO _sessions (last_seen) VALUES (NOW());")
            conn.commit()
            cur.execute("""
                DELETE FROM _sessions
                WHERE last_seen < NOW() - INTERVAL '5 minutes';
            """)
            conn.commit()
            cur.execute("""
                SELECT COUNT(*) as total FROM _sessions
                WHERE last_seen > NOW() - INTERVAL '5 minutes';
            """)
            conn.commit()
            row = cur.fetchone()
            return row["total"] if row else 0
    except Exception:
        return 0

def init_session() -> None:
    """Initialise les variables de session."""
    if "role" not in st.session_state:
        st.session_state.role = "participant"
    if "user_id" not in st.session_state:
        st.session_state.user_id = None
    if "theme" not in st.session_state:
        st.session_state.theme = "☀️ Clair"


def is_admin() -> bool:
    """Vérifie si admin.

    Returns:
        True si admin.
    """
    return st.session_state.role == "admin"


def is_logged_in() -> bool:
    """Vérifie si connecté.

    Returns:
        True si connecté.
    """
    return st.session_state.role in ("admin", "creator")


def render_field(field: dict) -> Any:
    """Génère le widget pour un champ du schéma.

    Args:
        field: Dictionnaire décrivant un champ.

    Returns:
        Valeur saisie.
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
                               min_value=min_val, max_value=field.get("max_value"),
                               help=help_text)

    if field_type == "float":
        min_v = field.get("min_value")
        max_v = field.get("max_value")
        default_val = float(field.get("default", min_v if min_v is not None else 0.0))
        return st.number_input(full_label, step=field.get("step", 0.1),
                               value=default_val,
                               min_value=float(min_v) if min_v is not None else None,
                               max_value=float(max_v) if max_v is not None else None,
                               format="%.2f", help=help_text)

    if field_type == "date":
        return st.date_input(full_label, value=date.today(), help=help_text)

    if field.get("multiline"):
        return st.text_area(full_label, value=field.get("default", ""), help=help_text)
    return st.text_input(full_label, value=field.get("default", ""), help=help_text)
def render_statistics(df: pd.DataFrame, schema_fields: list[dict], t: dict) -> None:
    """Affiche le dashboard statistique.

    Args:
        df: DataFrame pandas.
        schema_fields: Champs du schéma.
        t: Thème actif.
    """
    if df.empty:
        st.info("📭 Aucune donnée collectée pour l'instant.")
        return

    numeric_fields = [f for f in schema_fields if f.get("type") in ("int", "float")]
    categoric_fields = [f for f in schema_fields if f.get("options") or f.get("type") == "str"]
    numeric_cols = [f["name"] for f in numeric_fields]
    completeness = round((df.notna().sum().sum() / (len(df) * len(df.columns))) * 100, 1)
    last_date = pd.to_datetime(df["created_at"]).max().strftime("%d/%m/%Y") if "created_at" in df.columns else "—"

    col1, col2, col3, col4 = st.columns(4)
    for col, val, label in [
        (col1, str(len(df)), "Réponses"),
        (col2, last_date, "Dernière saisie"),
        (col3, str(len(numeric_cols)), "Variables num."),
        (col4, f"{completeness}%", "Complétude"),
    ]:
        with col:
            st.markdown(f"""
            <div style="background:{t['card']}; border:1px solid {t['border']};
                border-radius:14px; padding:1.2rem; text-align:center;">
                <div style="font-size:1.8rem; font-weight:800;
                    color:{t['metric_value']};">{val}</div>
                <div style="font-size:0.7rem; text-transform:uppercase;
                    color:{t['metric_label']}; margin-top:4px;">{label}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
    df_filtered = df.copy()

    if categoric_fields:
        st.markdown(f"<div style='font-weight:700; color:{t['text']}; margin-bottom:0.8rem;'>🔍 Filtres</div>",
                    unsafe_allow_html=True)
        fcols = st.columns(min(len(categoric_fields), 3))
        for i, field in enumerate(categoric_fields[:3]):
            name = field["name"]
            label = field.get("label", name)
            if name in df.columns:
                unique_vals = df[name].dropna().unique().tolist()
                if unique_vals:
                    with fcols[i]:
                        sel = st.multiselect(label, unique_vals, default=unique_vals,
                                             key=f"filter_{name}")
                        if sel:
                            df_filtered = df_filtered[df_filtered[name].isin(sel)]
        st.caption(f"**{len(df_filtered)}** / **{len(df)}** réponses")

    if numeric_cols:
        st.markdown(f"<div style='font-weight:700; color:{t['text']}; margin:1.5rem 0 0.8rem;'>🔢 Analyse numérique</div>",
                    unsafe_allow_html=True)
        num_df = df_filtered[numeric_cols].apply(pd.to_numeric, errors="coerce")
        stats = num_df.describe().T
        stats.index = [next((f.get("label", f["name"]) for f in numeric_fields if f["name"] == c), c)
                       for c in stats.index]
        stats.columns = ["Nb", "Moyenne", "Écart-type", "Min", "Q25%", "Médiane", "Q75%", "Max"]
        st.dataframe(stats.round(2), use_container_width=True)

        dcols = st.columns(min(len(numeric_cols), 2))
        for i, field in enumerate(numeric_fields):
            name = field["name"]
            label = field.get("label", name)
            if name in df_filtered.columns:
                series = pd.to_numeric(df_filtered[name], errors="coerce").dropna()
                if not series.empty:
                    with dcols[i % 2]:
                        st.markdown(f"**{label}**")
                        st.bar_chart(series.value_counts(
                            bins=min(10, len(series.unique()))).sort_index())
                        ca, cb, cc = st.columns(3)
                        ca.metric("Moyenne", f"{series.mean():.2f}")
                        cb.metric("Médiane", f"{series.median():.2f}")
                        cc.metric("Écart-type", f"{series.std():.2f}")

        if len(numeric_cols) >= 2:
            st.markdown(f"<div style='font-weight:700; color:{t['text']}; margin:1.5rem 0 0.8rem;'>🔗 Corrélations</div>",
                        unsafe_allow_html=True)
            try:
                corr = num_df.rename(columns={
                    f["name"]: f.get("label", f["name"]) for f in numeric_fields
                }).corr().round(2)
                st.dataframe(corr, use_container_width=True)
            except Exception:
                st.info("Impossible d'afficher les corrélations.")

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
                ca, cb = st.columns(2)
                with ca:
                    st.markdown(f"**{label}**")
                    st.dataframe(counts, use_container_width=True, hide_index=True)
                with cb:
                    st.bar_chart(counts.set_index(label)["Nombre"])

    if "created_at" in df_filtered.columns:
        st.markdown(f"<div style='font-weight:700; color:{t['text']}; margin:1.5rem 0 0.8rem;'>📅 Évolution</div>",
                    unsafe_allow_html=True)
        df_filtered = df_filtered.copy()
        df_filtered["created_at"] = pd.to_datetime(df_filtered["created_at"])
        daily = df_filtered.groupby(df_filtered["created_at"].dt.date).size().reset_index()
        daily.columns = ["Date", "Saisies"]
        st.line_chart(daily.set_index("Date"))


def export_dataframe(df: pd.DataFrame, domain: str) -> None:
    """Boutons d'export CSV et Excel.

    Args:
        df: DataFrame à exporter.
        domain: Nom du domaine.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"{domain}_{timestamp}"
    col1, col2 = st.columns(2)
    with col1:
        st.download_button("⬇ CSV",
                           data=df.to_csv(index=False).encode("utf-8-sig"),
                           file_name=f"{base_name}.csv",
                           mime="text/csv",
                           use_container_width=True)
    with col2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=domain[:31])
        st.download_button("⬇ Excel",
                           data=buffer.getvalue(),
                           file_name=f"{base_name}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)
def render_admin_section(conn, t: dict) -> None:
    """Section Admin — connexion + création + gestion.

    Args:
        conn: Connexion psycopg2.
        t: Thème actif.
    """
    if not is_logged_in():
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown(f"""
            <div style="background:{t['card']}; border:2px solid {t['accent']};
                border-radius:14px; padding:1.5rem; margin-bottom:1rem;">
                <div style="font-size:1.5rem; margin-bottom:0.5rem;">👑</div>
                <div style="font-weight:700; color:{t['text']}; font-size:0.95rem;">
                    Administrateur
                </div>
                <div style="color:{t['text_secondary']}; font-size:0.78rem; margin-top:0.4rem;">
                    Réservé au créateur de l'application.<br>
                    Accès total à tous les formulaires.
                </div>
            </div>
            """, unsafe_allow_html=True)
        with col_b:
            st.markdown(f"""
            <div style="background:{t['card']}; border:1px solid {t['border']};
                border-radius:14px; padding:1.5rem; margin-bottom:1rem;">
                <div style="font-size:1.5rem; margin-bottom:0.5rem;">👤</div>
                <div style="font-weight:700; color:{t['text']}; font-size:0.95rem;">
                    Gestionnaire de formulaire
                </div>
                <div style="color:{t['text_secondary']}; font-size:0.78rem; margin-top:0.4rem;">
                    Pour les profs, chercheurs ou toute personne<br>
                    qui veut créer et gérer sa propre étude.
                </div>
            </div>
            """, unsafe_allow_html=True)

        login_type = st.radio(
            "Je suis :",
            ["👑 Administrateur", "👤 Gestionnaire de formulaire"],
            key="login_type_main",
            horizontal=True,
        )

        if login_type == "👑 Administrateur":
            pwd = st.text_input(
                "Mot de passe admin",
                type="password",
                key="admin_pwd_main",
                placeholder="Votre mot de passe administrateur"
            )
            if st.button("→ Connexion Admin", key="btn_admin_main",
                         use_container_width=True):
                if pwd == st.secrets.get("ADMIN_PASSWORD", ""):
                    st.session_state.role = "admin"
                    st.session_state.user_id = st.secrets.get("ADMIN_ID", "admin")
                    st.rerun()
                else:
                    st.error("❌ Mot de passe incorrect")
        else:
            st.markdown(f"""
            <div style="background:{t['secondary_bg']}; border-left:3px solid {t['accent']};
                border-radius:0 8px 8px 0; padding:0.8rem 1rem; margin-bottom:1rem;
                font-size:0.82rem; color:{t['text_secondary']};">
                💡 <strong>Première visite ?</strong> Choisissez n'importe quel mot de passe.
                Vous serez connecté immédiatement.<br><br>
                <strong>Déjà un formulaire ?</strong> Entrez le même mot de passe
                qu'à la création pour retrouver vos données.
            </div>
            """, unsafe_allow_html=True)

            pwd_input = st.text_input(
                "Votre mot de passe",
                type="password",
                key="creator_pwd_main",
                placeholder="Ex: monétude2025"
            )
            if st.button("→ Accéder à mon espace", key="btn_creator_main",
                         use_container_width=True):
                if not pwd_input.strip():
                    st.error("❌ Entrez un mot de passe pour continuer")
                else:
                    all_schemas = load_schemas_db(conn)
                    matched_creator = None
                    for domain, schema in all_schemas.items():
                        if verify_creator_password(conn, domain, pwd_input.strip()):
                            matched_creator = schema.get("_creator_id", domain)
                            break
                    if matched_creator:
                        st.session_state.role = "creator"
                        st.session_state.user_id = matched_creator
                        st.rerun()
                    else:
                        st.session_state.role = "creator"
                        st.session_state.user_id = pwd_input.strip()
                        st.rerun()
        return

    # SI CONNECTÉ
    role_label = "👑 Administrateur" if is_admin() else f"👤 {st.session_state.user_id}"
    col_info, col_logout = st.columns([3, 1])
    with col_info:
        st.markdown(f"""
        <div style="background:{t['secondary_bg']}; border:1px solid {t['border']};
            border-radius:12px; padding:1rem 1.5rem; margin-bottom:1.5rem;">
            <div style="font-size:0.7rem; color:{t['text_secondary']};
                text-transform:uppercase; letter-spacing:0.08em;">Connecté en tant que</div>
            <div style="font-weight:700; color:{t['text']}; font-size:1rem;
                margin-top:4px;">{role_label}</div>
        </div>
        """, unsafe_allow_html=True)
    with col_logout:
        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
        if st.button("🚪 Déconnexion", key="btn_logout_main", use_container_width=True):
            st.session_state.role = "participant"
            st.session_state.user_id = None
            st.rerun()

    st.markdown(f"""
    <div style="font-weight:700; color:{t['text']}; font-size:1.05rem;
        margin-bottom:1rem; padding-bottom:0.5rem; border-bottom:2px solid {t['divider']};">
        🚀 Créer un nouveau formulaire
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        form_title = st.text_input("📌 Titre *", placeholder="Ex : Bien-être étudiant 2025")
        domain_name = st.text_input("🔑 Identifiant unique *",
                                    placeholder="Ex : bienetre_2025",
                                    help="Lettres, chiffres, underscores uniquement")
    with col2:
        form_description = st.text_area("📝 Description",
                                        placeholder="Objectif de l'étude...", height=100)
        creator_pwd = st.text_input(
            "🔒 Mot de passe du formulaire *",
            type="password",
            placeholder="Le même mot de passe que votre connexion",
            help="Utilisez le même mot de passe que celui de votre connexion"
        )

    nb_fields = st.number_input("Nombre de champs", min_value=1, max_value=20, value=3, step=1)
    fields = []

    for i in range(int(nb_fields)):
        with st.container():
            st.markdown(f"""
            <div style="background:{t['card']}; border:1px solid {t['border']};
                border-radius:12px; padding:1rem; margin-bottom:0.8rem;">
                <div style="font-weight:600; color:{t['accent']}; margin-bottom:0.5rem;">
                    Champ {i+1}
                </div>
            """, unsafe_allow_html=True)

            c1, c2, c3 = st.columns(3)
            with c1:
                fl = st.text_input("Libellé *", placeholder="Ex: Âge", key=f"label_{i}")
                fn = st.text_input("Nom interne *", placeholder="Ex: age", key=f"name_{i}")
            with c2:
                ft = st.selectbox("Type", ["Texte", "Nombre entier", "Nombre décimal",
                                           "Date", "Liste déroulante"], key=f"type_{i}")
                fr = st.checkbox("Obligatoire", value=True, key=f"req_{i}")
            with c3:
                fh = st.text_input("Aide", placeholder="Optionnel", key=f"help_{i}")
                fo, fmin, fmax = "", None, None
                if ft == "Liste déroulante":
                    fo = st.text_input("Options (virgules)",
                                       placeholder="Oui, Non", key=f"opt_{i}")
                elif ft in ("Nombre entier", "Nombre décimal"):
                    fmin = st.number_input("Min", value=0, key=f"min_{i}")
                    fmax = st.number_input("Max", value=100, key=f"max_{i}")

            st.markdown("</div>", unsafe_allow_html=True)

            type_map = {"Texte": "str", "Nombre entier": "int",
                        "Nombre décimal": "float", "Date": "date",
                        "Liste déroulante": "str"}
            fd: dict[str, Any] = {
                "name": fn.strip().replace(" ", "_"),
                "label": fl.strip(),
                "type": type_map[ft],
                "required": fr,
            }
            if fh: fd["help"] = fh
            if ft == "Liste déroulante" and fo:
                fd["options"] = [o.strip() for o in fo.split(",") if o.strip()]
            if ft in ("Nombre entier", "Nombre décimal") and fmin is not None:
                fd["min_value"] = fmin
                fd["max_value"] = fmax
            fields.append(fd)

    if st.button("🚀 Créer et publier le formulaire", use_container_width=True):
        errors = []
        if not form_title.strip(): errors.append("Titre obligatoire.")
        if not domain_name.strip(): errors.append("Identifiant obligatoire.")
        if not domain_name.strip().replace("_", "").isalnum():
            errors.append("Identifiant : lettres, chiffres, underscores uniquement.")
        if not creator_pwd.strip(): errors.append("Mot de passe obligatoire.")
        for i, f in enumerate(fields):
            if not f.get("label"): errors.append(f"Libellé champ {i+1} manquant.")
            if not f.get("name"): errors.append(f"Nom interne champ {i+1} manquant.")

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
                save_schema_db(conn, domain_name.strip(), schema,
                               creator_id=st.session_state.user_id,
                               creator_password=creator_pwd.strip(),
                               is_public=False)
                share_url = f"{get_app_url()}/?study={domain_name.strip()}"
                st.success(f"✅ Formulaire **{form_title}** créé avec succès !")
                st.markdown(f"""
                <div style="background:{t['secondary_bg']}; border:1.5px solid {t['border']};
                    border-radius:12px; padding:1.5rem; margin-top:1rem;">
                    <div style="font-weight:700; color:{t['accent']}; margin-bottom:0.5rem;">
                        🔗 Lien à partager aux participants
                    </div>
                    <div style="background:{t['card']}; border:1px solid {t['border']};
                        border-radius:8px; padding:0.8rem; font-family:monospace;
                        font-size:0.85rem; color:{t['accent']}; word-break:break-all;">
                        {share_url}
                    </div>
                    <div style="color:{t['text_secondary']}; font-size:0.78rem; margin-top:0.8rem;">
                        ⚠️ Envoyez ce lien uniquement à vos participants.
                    </div>
                </div>
                """, unsafe_allow_html=True)
                st.balloons()
                st.rerun()
            except Exception as exc:
                st.error(f"❌ Erreur : {exc}")

    st.markdown(f"""
    <div style="font-weight:700; color:{t['text']}; font-size:1.05rem;
        margin:2rem 0 1rem; padding-bottom:0.5rem; border-bottom:2px solid {t['divider']};">
        📁 {"Tous les formulaires" if is_admin() else "Mes formulaires"}
    </div>
    """, unsafe_allow_html=True)

    schemas = load_schemas_db(conn) if is_admin() else load_schemas_for_user(
        conn, st.session_state.user_id)

    if not schemas:
        st.info("Aucun formulaire pour l'instant. Créez-en un ci-dessus !")
    else:
        for domain, schema in schemas.items():
            gradient = get_domain_gradient(domain)
            share_url = f"{get_app_url()}/?study={domain}"
            creator = schema.get("_creator_id", "inconnu")

            col_info, col_del = st.columns([4, 1])
            with col_info:
                st.markdown(f"""
                <div style="background:{t['card']}; border:1px solid {t['border']};
                    border-radius:14px; padding:1.5rem; position:relative;
                    overflow:hidden; margin-bottom:0.8rem;">
                    <div style="position:absolute; top:0; left:0; width:4px; height:100%;
                        background:{gradient};"></div>
                    <div style="padding-left:1rem;">
                        <div style="font-weight:700; color:{t['text']}; font-size:1.05rem;">
                            {schema.get('title', domain)}
                        </div>
                        <div style="color:{t['text_secondary']}; font-size:0.8rem; margin-top:0.2rem;">
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
                            border-radius:8px; padding:0.5rem 0.8rem; margin-top:0.8rem;
                            font-family:monospace; font-size:0.75rem; color:{t['accent']};
                            word-break:break-all;">
                            🔗 {share_url}
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            with col_del:
                st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
                if st.button("🗑️", key=f"del_{domain}", use_container_width=True,
                             help="Supprimer ce formulaire"):
                    try:
                        delete_schema_db(conn, domain)
                        st.success("Supprimé !")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"❌ {exc}")
def main() -> None:
    """Point d'entrée principal."""
    init_session()

    try:
        conn = get_connection()
        ensure_schemas_table(conn)
        active_users = get_active_users(conn)
    except Exception as exc:
        st.error(f"❌ Erreur connexion : {exc}")
        st.stop()

    t = THEMES[st.session_state.theme]
    apply_theme(t)

    query_params = st.query_params
    url_study = query_params.get("study", None)

    # COMPTEUR UTILISATEURS ACTIFS
    active_users = get_active_users(conn)

    # HEADER avec compteur intégré
    st.markdown(f"""
    <div style="background:{t['hero']}; border-radius:20px; padding:2rem 2.5rem;
        margin-bottom:1.5rem; position:relative; overflow:hidden;">
        <div style="position:absolute; top:-40px; right:-40px; width:200px; height:200px;
            background:rgba(255 255 255 / 0.06); border-radius:50%;"></div>
        <div style="position:relative; display:flex; justify-content:space-between;
            align-items:flex-start; flex-wrap:wrap; gap:1rem;">
            <div>
                <div style="font-size:0.65rem; letter-spacing:0.15em; text-transform:uppercase;
                    color:rgba(255 255 255 / 0.6); margin-bottom:0.4rem;">
                    Plateforme de collecte de données
                </div>
                <div style="font-family:'Playfair Display',serif; font-size:2rem;
                    font-weight:800; color:white; line-height:1.1; margin-bottom:0.4rem;">
                    DataCollect
                    <span style="display:inline-block; background:rgba(255 255 255 / 0.2);
                        color:white; font-size:0.55rem; font-weight:600; padding:3px 10px;
                        border-radius:20px; letter-spacing:0.1em; text-transform:uppercase;
                        vertical-align:middle; margin-left:8px;
                        border:1px solid rgba(255 255 255 / 0.3);">Universal</span>
                </div>
                <div style="color:rgba(255 255 255 / 0.7); font-size:0.8rem;">
                    Réalisé par Botagne Julien Claude Daniel
                </div>
            </div>
            <div style="background:rgba(255 255 255 / 0.15); border-radius:14px;
                padding:1rem 1.5rem; text-align:center; min-width:120px;">
                <div style="font-size:2rem; font-weight:800; color:white;">
                    🟢 {active_users}
                </div>
                <div style="font-size:0.65rem; text-transform:uppercase;
                    color:rgba(255 255 255 / 0.7); letter-spacing:0.08em; margin-top:4px;">
                    Actifs maintenant
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # SÉLECTEUR DE THÈME
    theme_col, _ = st.columns([2, 6])
    with theme_col:
        st.session_state.theme = st.selectbox(
            "🎨 Thème",
            list(THEMES.keys()),
            index=list(THEMES.keys()).index(st.session_state.theme),
            key="theme_select"
        )
        t = THEMES[st.session_state.theme]
        apply_theme(t)

    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    # SECTION ADMIN — toujours visible sur la page
    render_admin_section(conn, t)

    st.markdown(f"""
    <hr style="border:none; border-top:2px solid {t['divider']}; margin:2rem 0;">
    """, unsafe_allow_html=True)

    # RÉSOLUTION DU DOMAINE
    if url_study:
        schema = load_schema_by_domain(conn, url_study)
        if schema:
            domain = url_study
        else:
            st.error("❌ Formulaire introuvable. Le lien est invalide.")
            st.stop()
    elif is_logged_in():
        all_schemas = load_schemas_db(conn) if is_admin() else load_schemas_for_user(
            conn, st.session_state.user_id)
        available = list(all_schemas.keys())
        if available:
            selected = st.selectbox(
                "📋 Sélectionner un formulaire",
                available,
                format_func=lambda x: all_schemas[x].get("title", x)
            )
            domain = selected
            schema = all_schemas.get(domain)
        else:
            domain = None
            schema = None
    else:
        all_schemas = {}
        domain = None
        schema = None

    # ONGLETS SAISIE / DONNÉES / STATS
    tab_form, tab_data, tab_stats = st.tabs([
        "✏️  Saisie", "📋  Données", "📊  Statistiques"
    ])

    if not schema:
        with tab_form:
            st.markdown(f"""
            <div style="background:{t['card']}; border:1px solid {t['border']};
                border-radius:16px; padding:3rem; text-align:center; margin-top:1rem;">
                <div style="font-size:3rem;">📋</div>
                <div style="font-size:1.1rem; font-weight:600;
                    color:{t['text_secondary']}; margin-top:1rem;">
                    {"Connectez-vous ci-dessus pour créer un formulaire"
                     if not is_logged_in()
                     else "Sélectionnez ou créez un formulaire ci-dessus"}
                </div>
            </div>
            """, unsafe_allow_html=True)
        return

    schema_fields: list[dict] = schema.get("fields", [])
    form_title: str = schema.get("title", domain.replace("_", " ").title())
    form_description: str = schema.get("description", "")
    domain_gradient = get_domain_gradient(domain)
    share_url = f"{get_app_url()}/?study={domain}"

    try:
        ensure_table(conn, domain, schema_fields)
    except Exception as exc:
        st.error(f"❌ Erreur base de données : {exc}")
        st.stop()

    try:
        model_cls = build_model(schema_fields)
    except Exception as exc:
        st.error(f"❌ Erreur validation : {exc}")
        st.stop()
    # ONGLET SAISIE
    with tab_form:
        st.markdown(f"""
        <div style="background:{domain_gradient}; border-radius:16px;
            padding:2rem 2.5rem; margin-bottom:1.5rem; position:relative; overflow:hidden;">
            <div style="position:absolute; top:-30px; right:-30px; width:160px; height:160px;
                background:rgba(255 255 255 / 0.07); border-radius:50%;"></div>
            <div style="position:relative;">
                <div style="font-size:0.65rem; letter-spacing:0.12em; text-transform:uppercase;
                    color:rgba(255 255 255 / 0.6); margin-bottom:0.4rem;">📋 Formulaire</div>
                <div style="font-family:'Playfair Display',serif; font-size:1.8rem;
                    font-weight:700; color:white; margin-bottom:0.4rem;">{form_title}</div>
                <div style="color:rgba(255 255 255 / 0.8); font-size:0.85rem;">
                    {form_description or 'Remplissez le formulaire ci-dessous.'}
                </div>
                <div style="margin-top:1rem; display:flex; gap:0.6rem; flex-wrap:wrap;">
                    <span style="background:rgba(255 255 255 / 0.2); color:white;
                        padding:3px 12px; border-radius:20px; font-size:0.72rem;">
                        {len(schema_fields)} champs
                    </span>
                    <span style="background:rgba(255 255 255 / 0.2); color:white;
                        padding:3px 12px; border-radius:20px; font-size:0.72rem;">
                        🔒 Données permanentes
                    </span>
                </div>
            </div>
        </div>

        <div style="background:{t['secondary_bg']}; border:1.5px solid {t['border']};
            border-radius:12px; padding:1.2rem 1.5rem; margin-bottom:1.5rem;">
            <div style="font-weight:600; color:{t['accent']}; font-size:0.85rem;
                margin-bottom:0.4rem;">🔗 Partager cette étude</div>
            <div style="background:{t['card']}; border:1px solid {t['border']};
                border-radius:8px; padding:0.7rem 1rem; font-family:monospace;
                font-size:0.78rem; color:{t['accent']}; word-break:break-all;">
                {share_url}
            </div>
        </div>
        """, unsafe_allow_html=True)

        with st.form(key=f"form_{domain}", clear_on_submit=True):
            raw_values: dict[str, Any] = {}
            field_chunks = [schema_fields[i:i+2] for i in range(0, len(schema_fields), 2)]
            for chunk in field_chunks:
                cols = st.columns(len(chunk))
                for j, field in enumerate(chunk):
                    with cols[j]:
                        try:
                            raw_values[field["name"]] = render_field(field)
                        except Exception as exc:
                            st.warning(f"Champ `{field.get('name')}` : {exc}")
            submitted = st.form_submit_button("✅ Soumettre ma réponse",
                                              use_container_width=True)

        if submitted:
            try:
                instance, errors = validate_data(model_cls, raw_values)
                if errors:
                    st.error("❌ Corrigez les erreurs :")
                    for err in errors:
                        st.markdown(f"• {err}")
                else:
                    row_id = insert_row(conn, domain, instance.model_dump())
                    st.success(f"✅ Réponse #{row_id} enregistrée ! Merci.")
                    st.balloons()
            except Exception as exc:
                st.error(f"❌ Erreur : {exc}")
                with st.expander("Détails"):
                    st.code(traceback.format_exc())

    # ONGLET DONNÉES
    with tab_data:
        domain_creator = schema.get("_creator_id", "")
        can_access = is_admin() or st.session_state.user_id == domain_creator
        if not can_access:
            st.warning("🔐 Accès réservé au créateur et à l'administrateur.")
        else:
            try:
                df = fetch_all(conn, domain)
            except Exception as exc:
                st.error(f"Erreur : {exc}")
                df = pd.DataFrame()

            if df.empty:
                st.info("📭 Aucune donnée collectée pour l'instant.")
            else:
                st.markdown(f"""
                <span style="background:{t['tag_bg']}; color:{t['tag_text']};
                    padding:4px 14px; border-radius:20px; font-weight:600; font-size:0.85rem;">
                    {len(df)} réponse(s)
                </span>
                """, unsafe_allow_html=True)
                st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)
                st.dataframe(df, use_container_width=True, hide_index=True)
                export_dataframe(df, domain)

    # ONGLET STATISTIQUES
    with tab_stats:
        domain_creator = schema.get("_creator_id", "")
        can_access = is_admin() or st.session_state.user_id == domain_creator
        if not can_access:
            st.warning("🔐 Accès réservé au créateur et à l'administrateur.")
        else:
            st.markdown(f"""
            <div style="background:{domain_gradient}; border-radius:14px;
                padding:1.5rem 2rem; margin-bottom:1.5rem;">
                <div style="font-size:0.65rem; letter-spacing:0.12em; text-transform:uppercase;
                    color:rgba(255 255 255 / 0.6); margin-bottom:0.3rem;">
                    📊 Tableau de bord analytique
                </div>
                <div style="font-family:'Playfair Display',serif; font-size:1.4rem;
                    font-weight:700; color:white;">{form_title}</div>
            </div>
            """, unsafe_allow_html=True)
            try:
                df_stats = fetch_all(conn, domain)
                render_statistics(df_stats, schema_fields, t)
            except Exception as exc:
                st.error(f"❌ Erreur : {exc}")


if __name__ == "__main__":
    main()
