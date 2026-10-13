"""Personalization Console - serves live session state out of Lakebase.

Each row in live.sessions is a user's current session, continuously upserted by
the Real-Time Mode pipeline. Serving a next-best action is then just a point
lookup against Postgres (clickstream-sessions / databricks_postgres /
live.sessions) - no cache, no feature-store round trip.

The connection uses a short-lived credential minted by the running app's service
principal via WorkspaceClient(), so there is no static password. Tokens last
~1 hour, so we re-mint on a fixed cadence and reconnect on any connection error.

Local preview: set CONSOLE_MOCK=1 to render the full UI against synthetic
session data with no Lakebase connection. The production path (CONSOLE_MOCK
unset) is unchanged - it always talks to Lakebase.
"""

import os
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

INSTANCE_NAME = "clickstream-sessions"
DB_NAME = "databricks_postgres"
SCHEMA = "live"
TABLE = "sessions"
CRED_TTL_SECONDS = 45 * 60  # re-mint well before the ~1h expiry
REFRESH_SECONDS = 3

MOCK = os.getenv("CONSOLE_MOCK") == "1"

# Stage palette - faint tint background plus an accent color, so it reads on
# both the light and dark Databricks themes (no hardcoded page background).
STAGE_COLORS = {
    "converting": ("rgba(52, 168, 83, 0.16)", "#1e8e3e"),
    "engaged": ("rgba(26, 115, 232, 0.16)", "#1a73e8"),
    "browsing": ("rgba(140, 140, 150, 0.16)", "#80868b"),
    "needs help": ("rgba(234, 67, 53, 0.16)", "#d93025"),
}
ACCENT = "#FF3621"  # Databricks orange

st.set_page_config(
    page_title="Personalization Console",
    page_icon="*",
    layout="wide",
)


# --------------------------------------------------------------------------
# Connection: mint a fresh Postgres credential as the app service principal.
# --------------------------------------------------------------------------
@st.cache_resource
def _workspace_client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _pg_role(w) -> str:
    """Resolve the Postgres role to authenticate as.

    When the Lakebase `database` app resource is attached, Databricks injects
    PGUSER = the service principal's client id, which is the role it creates on
    the instance. Prefer that. Fall back to the caller's identity for local /
    user-driven runs (e.g. steven.yu).
    """
    pg_user = os.getenv("PGUSER")
    if pg_user:
        return pg_user
    return w.current_user.me().user_name


def _build_connection():
    """Mint a credential and open a psycopg connection. Caller owns the lifecycle."""
    import psycopg
    from psycopg.rows import dict_row

    w = _workspace_client()
    inst = w.database.get_database_instance(name=INSTANCE_NAME)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()), instance_names=[INSTANCE_NAME]
    )
    user = _pg_role(w)
    conn_string = (
        f"host={inst.read_write_dns} dbname={DB_NAME} "
        f"user={user} password={cred.token} sslmode=require"
    )
    conn = psycopg.connect(conn_string, row_factory=dict_row, connect_timeout=10)
    conn.autocommit = True
    return conn, user


def get_connection():
    """Return a live connection, (re)minting the credential past its TTL."""
    state = st.session_state
    now = time.time()
    needs_new = (
        "pg_conn" not in state
        or state.get("pg_conn") is None
        or (now - state.get("pg_conn_minted_at", 0)) > CRED_TTL_SECONDS
        or state.get("pg_conn").closed
    )
    if needs_new:
        old = state.get("pg_conn")
        if old is not None:
            try:
                old.close()
            except Exception:
                pass
        conn, user = _build_connection()
        state["pg_conn"] = conn
        state["pg_conn_minted_at"] = now
        state["pg_role"] = user
    return state["pg_conn"]


def run_query(sql: str, params=None) -> pd.DataFrame:
    """Execute a query, reconnecting once on a dropped/expired connection."""
    import psycopg

    for attempt in range(2):
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                rows = cur.fetchall()
            return pd.DataFrame(rows)
        except (psycopg.OperationalError, psycopg.InterfaceError):
            st.session_state["pg_conn"] = None
            if attempt == 1:
                raise
    return pd.DataFrame()


# --------------------------------------------------------------------------
# Data access. Each loader hits Lakebase in production, or returns synthetic
# rows in mock mode so the layout can be previewed locally. The SQL is the
# real serving query - unchanged from the validated pipeline.
# --------------------------------------------------------------------------
FQ_TABLE = f"{SCHEMA}.{TABLE}"


def load_metrics() -> pd.DataFrame:
    if MOCK:
        return _mock_metrics()
    return run_query(
        f"""
        SELECT
          COUNT(*) FILTER (WHERE status = 'online')                AS active_users,
          COUNT(*) FILTER (WHERE funnel_stage = 'converting')      AS converting,
          COUNT(*) FILTER (WHERE needs_help)                       AS needs_help,
          EXTRACT(EPOCH FROM (now() - MAX(last_updated)))          AS freshest_secs,
          COUNT(*)                                                 AS total_rows
        FROM {FQ_TABLE}
        """
    )


def load_grid() -> pd.DataFrame:
    if MOCK:
        return _mock_grid()
    return run_query(
        f"""
        SELECT user_id, current_surface, funnel_stage, engagement_score,
               EXTRACT(EPOCH FROM (now() - last_updated)) AS last_active_secs,
               status, needs_help
        FROM {FQ_TABLE}
        WHERE status = 'online'
        ORDER BY engagement_score DESC
        LIMIT 500
        """
    )


def load_detail(user_id: str) -> pd.DataFrame:
    if MOCK:
        return _mock_detail(user_id)
    return run_query(f"SELECT * FROM {FQ_TABLE} WHERE user_id = %s", (user_id,))


# --------------------------------------------------------------------------
# Mock data for local preview. Deterministic base population, lightly jittered
# each refresh so the console feels live without a backing stream.
# --------------------------------------------------------------------------
SURFACE_WEIGHTS = {
    "home": 5, "search": 4, "catalog": 4, "feature_x": 3,
    "account": 2, "pricing": 2, "support": 1, "checkout": 1,
}


def _surface_to_stage(surface: str):
    if surface == "support":
        return "browsing", True
    if surface == "checkout":
        return "converting", False
    if surface in ("pricing", "account"):
        return "engaged", False
    return "browsing", False


@st.cache_data
def _mock_base() -> pd.DataFrame:
    import random

    rng = random.Random(42)
    weighted = [s for s, w in SURFACE_WEIGHTS.items() for _ in range(w)]
    ids, seen = [], set()
    while len(ids) < 200:
        uid = f"user_{rng.randint(1000, 9999)}"
        if uid not in seen:
            seen.add(uid)
            ids.append(uid)
    rows = []
    for uid in ids:
        surface = rng.choice(weighted)
        stage, needs = _surface_to_stage(surface)
        clicks = rng.randint(1, 40)
        bonus = {"converting": 30, "engaged": 15, "browsing": 0}[stage]
        score = min(100, clicks * 2 + bonus)
        rows.append({
            "user_id": uid, "current_surface": surface, "funnel_stage": stage,
            "needs_help": needs, "click_count": clicks, "engagement_score": score,
        })
    return pd.DataFrame(rows)


def _mock_snapshot() -> pd.DataFrame:
    import random

    df = _mock_base().copy()
    # New seed roughly every refresh tick so counts and freshness drift.
    r = random.Random(int(time.time()) // REFRESH_SECONDS)
    df["status"] = ["online" if r.random() < 0.72 else "offline" for _ in range(len(df))]
    df["last_active_secs"] = [
        r.uniform(0.2, 18.0) if s == "online" else r.uniform(40.0, 320.0)
        for s in df["status"]
    ]
    now = pd.Timestamp.now(tz="UTC")
    df["last_updated"] = [now - pd.Timedelta(seconds=float(x)) for x in df["last_active_secs"]]
    df["start_time"] = df["last_updated"] - pd.to_timedelta(df["click_count"] * 4, unit="s")
    df["end_time"] = df["last_updated"]
    return df


def _mock_metrics() -> pd.DataFrame:
    df = _mock_snapshot()
    online = df[df["status"] == "online"]
    freshest = float(online["last_active_secs"].min()) if not online.empty else None
    return pd.DataFrame([{
        "active_users": int((df["status"] == "online").sum()),
        "converting": int((df["funnel_stage"] == "converting").sum()),
        "needs_help": int(df["needs_help"].sum()),
        "freshest_secs": freshest,
        "total_rows": int(len(df)),
    }])


def _mock_grid() -> pd.DataFrame:
    df = _mock_snapshot()
    cols = ["user_id", "current_surface", "funnel_stage", "engagement_score",
            "last_active_secs", "status", "needs_help"]
    return (
        df[df["status"] == "online"][cols]
        .sort_values("engagement_score", ascending=False)
        .reset_index(drop=True)
    )


def _mock_detail(user_id: str) -> pd.DataFrame:
    df = _mock_snapshot()
    return df[df["user_id"] == user_id].reset_index(drop=True)


# --------------------------------------------------------------------------
# Domain logic
# --------------------------------------------------------------------------
def _acted() -> dict:
    """user_id -> action kind taken this session ('support' | 'incentive').
    Client-side demo state, never written back to Lakebase."""
    return st.session_state.setdefault("acted", {})


def _available_action(r):
    """The operator action offered for this session's next-best-action, or None
    when the NBA is automated (no human button). Returns (kind, button_label,
    taken_label)."""
    if r["needs_help"]:
        return ("support", "Route to support", "Support in progress, live agent connected")
    if r["funnel_stage"] == "converting":
        return ("incentive", "Send assist / incentive", "Assist offer sent")
    return None


def _support_acted_ids() -> set:
    return {uid for uid, kind in _acted().items() if kind == "support"}


def next_best_action(r) -> str:
    """The personalization decision computed from this user's live session state."""
    act = _available_action(r)
    if act and _acted().get(r["user_id"]) == act[0]:
        return act[2]                                  # the 'action taken' state
    if r["needs_help"]:
        return "Offer live support"
    if r["funnel_stage"] == "converting":
        return "Nudge to complete (assist or incentive)"
    if r["funnel_stage"] == "engaged":
        return f"Surface tailored content for '{r['current_surface']}'"
    return "Highlight popular feature / onboarding"


def nba_why(r) -> str:
    done = _acted().get(r["user_id"])
    if done == "support":
        return "you routed live support to this session"
    if done == "incentive":
        return "you sent an assist / incentive to this session"
    if r["needs_help"]:
        return "needs_help flag is set on this session"
    if r["funnel_stage"] == "converting":
        return "user is in the converting funnel stage, close to a decision"
    if r["funnel_stage"] == "engaged":
        return f"user is engaged and actively on the '{r['current_surface']}' surface"
    return "user is browsing, not yet engaged"


def stage_label(r) -> str:
    """The label shown for a session - needs-help outranks the funnel stage. A
    routed (support-acted) session is no longer 'needs help' - it's being handled."""
    if r["needs_help"] and r["user_id"] not in _support_acted_ids():
        return "needs help"
    return r["funnel_stage"]


# --------------------------------------------------------------------------
# Presentation helpers
# --------------------------------------------------------------------------
def _inject_css():
    st.markdown(
        f"""
        <style>
        .block-container {{ padding-top: 1.6rem; }}
        .pc-hero {{
            display: flex; align-items: center; justify-content: space-between;
            border-bottom: 3px solid {ACCENT}; padding-bottom: 12px; margin-bottom: 8px;
        }}
        .pc-hero h1 {{ font-size: 1.55rem; margin: 0; font-weight: 700; }}
        .pc-hero .sub {{ font-size: 0.82rem; opacity: 0.65; margin-top: 2px; }}
        .pc-live {{ display: flex; align-items: center; gap: 8px; font-size: 0.9rem; font-weight: 600; }}
        .pc-dot {{
            height: 11px; width: 11px; border-radius: 50%;
            box-shadow: 0 0 0 0 rgba(52,168,83,0.7);
            animation: pcpulse 1.8s infinite;
        }}
        @keyframes pcpulse {{
            0%   {{ box-shadow: 0 0 0 0 rgba(52,168,83,0.55); }}
            70%  {{ box-shadow: 0 0 0 10px rgba(52,168,83,0); }}
            100% {{ box-shadow: 0 0 0 0 rgba(52,168,83,0); }}
        }}
        .pc-card {{
            border: 1px solid rgba(140,140,150,0.25); border-radius: 12px;
            padding: 14px 18px; height: 100%;
        }}
        .pc-card .label {{ font-size: 0.78rem; text-transform: uppercase;
            letter-spacing: 0.04em; opacity: 0.6; }}
        .pc-card .value {{ font-size: 2.0rem; font-weight: 700; line-height: 1.1; }}
        .pc-card .foot {{ font-size: 0.76rem; opacity: 0.55; }}
        .pc-pill {{ display: inline-block; padding: 3px 12px; border-radius: 999px;
            font-size: 0.78rem; font-weight: 600; }}
        .pc-nba {{
            border-radius: 12px; padding: 16px 20px; margin: 6px 0 4px 0;
            border-left: 5px solid {ACCENT};
            background: linear-gradient(90deg, rgba(255,54,33,0.12), rgba(255,54,33,0.02));
        }}
        .pc-nba.resolved {{
            border-left-color: #1e8e3e;
            background: linear-gradient(90deg, rgba(52,168,83,0.16), rgba(52,168,83,0.02));
        }}
        /* detail-pane action buttons - support = red, incentive = green */
        div[class*="st-key-actbtn_"] > button {{
            width: 100%; height: 38px; border-radius: 8px; font-weight: 600;
            color: #ffffff; border: 1px solid transparent; transition: all 0.12s ease; margin-top: 2px;
        }}
        div[class*="st-key-actbtn_support"] > button {{ background: #d93025; border-color: #d93025; }}
        div[class*="st-key-actbtn_support"] > button:hover {{ background: #b3170a; border-color: #b3170a; }}
        div[class*="st-key-actbtn_incentive"] > button {{ background: #1e8e3e; border-color: #1e8e3e; }}
        div[class*="st-key-actbtn_incentive"] > button:hover {{ background: #176a2e; border-color: #176a2e; }}
        .pc-log {{ margin-top: 22px; border-top: 1px solid rgba(140,140,150,0.2); padding-top: 10px;
            max-height: 160px; overflow-y: auto; }}
        .pc-log .h {{ font-size: 0.74rem; text-transform: uppercase; letter-spacing: 0.04em; opacity: 0.55; }}
        .pc-log .e {{ font-size: 0.82rem; opacity: 0.8; padding: 2px 0; }}
        .pc-log .e .t {{ color: #1a73e8; font-variant-numeric: tabular-nums; }}
        .pc-nba .k {{ font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em;
            opacity: 0.7; font-weight: 700; }}
        .pc-nba .v {{ font-size: 1.15rem; font-weight: 700; margin-top: 2px; }}
        .pc-nba .why {{ font-size: 0.8rem; opacity: 0.65; margin-top: 6px; }}
        .pc-nba .note {{ font-size: 0.72rem; opacity: 0.5; font-style: italic; margin-top: 8px;
            border-top: 1px dashed rgba(140,140,150,0.25); padding-top: 6px; }}
        .pc-kv {{ display: flex; justify-content: space-between; padding: 7px 0;
            border-bottom: 1px solid rgba(140,140,150,0.18); font-size: 0.9rem; }}
        .pc-kv .k {{ opacity: 0.6; }}
        .pc-bar-track {{ background: rgba(140,140,150,0.22); border-radius: 999px;
            height: 9px; width: 100%; overflow: hidden; }}
        .pc-bar-fill {{ height: 9px; border-radius: 999px;
            background: linear-gradient(90deg, {ACCENT}, #ff7a5c); }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _pill(label: str) -> str:
    bg, fg = STAGE_COLORS.get(label, STAGE_COLORS["browsing"])
    return f'<span class="pc-pill" style="background:{bg}; color:{fg};">{label}</span>'


def _metric_card(col, label, value, accent=None, foot=None):
    color = f"color:{accent};" if accent else ""
    foot_html = f'<div class="foot">{foot}</div>' if foot else ""
    col.markdown(
        f'<div class="pc-card"><div class="label">{label}</div>'
        f'<div class="value" style="{color}">{value}</div>{foot_html}</div>',
        unsafe_allow_html=True,
    )


def _freshness_badge(freshest):
    if freshest is None:
        return "#80868b", "no data"
    f = float(freshest)
    color = "#34a853" if f < 2 else ("#f9ab00" if f < 5 else "#d93025")
    return color, f"live · {f:.1f}s"


# --------------------------------------------------------------------------
# UI - rendered inside an auto-refreshing fragment so the whole page does not
# flash on every tick.
# --------------------------------------------------------------------------
def _picked_user(resp):
    """Pull the selected user id out of an AgGrid selection response. AgGrid
    returns the rendered ROW DATA, so the key is the displayed column name
    "user" (we rename user_id -> user for the grid), not "user_id". Handles both
    the list-of-dicts and DataFrame selection shapes across st_aggrid versions."""
    picked = getattr(resp, "selected_rows", None)
    if picked is None:
        try:
            picked = resp["selected_rows"]
        except Exception:
            picked = None
    if isinstance(picked, pd.DataFrame):
        return picked.iloc[0].get("user") if not picked.empty else None
    if picked:
        return picked[0].get("user")
    return None


SEGMENTS = ["All", "Converting", "Needs help", "Engaged"]


def _apply_segment(grid: pd.DataFrame, seg: str) -> pd.DataFrame:
    if grid.empty or seg == "All":
        return grid
    if seg == "Converting":
        return grid[grid["funnel_stage"] == "converting"]
    if seg == "Needs help":
        return grid[grid["needs_help"] & ~grid["user_id"].isin(_support_acted_ids())]
    if seg == "Engaged":
        return grid[grid["funnel_stage"] == "engaged"]
    return grid


def _detect_theme() -> str:
    """Active (toggle-aware) Streamlit theme - 'light' or 'dark', else 'base'.
    Folded into the AgGrid key so the grid remounts and re-themes on a toggle."""
    try:
        return st.context.theme.type or "base"
    except Exception:
        return "base"


try:
    _BANNER_TEMPLATE = (Path(__file__).parent / "banner.html").read_text(encoding="utf-8")
except Exception:
    _BANNER_TEMPLATE = ""


def _render_banner(theme: str, active: int):
    """Architecture banner at the top of the console. Theme-aware (dark/light), and
    the data-rail flow speed tracks the live active-session count - a busier stream
    visibly flows faster (0.6s when busy, up to 1.8s when quiet)."""
    if not _BANNER_TEMPLATE:
        return
    scheme = "light" if theme == "light" else "dark"
    flow_secs = max(0.6, min(1.8, 1.8 - active / 150.0))
    st.markdown(
        _BANNER_TEMPLATE.replace("__SCHEME__", scheme).replace("__FLOWDUR__", f"{flow_secs:.2f}s"),
        unsafe_allow_html=True,
    )


def _render_online_grid(display: pd.DataFrame, theme: str = "base"):
    """AgGrid table - click anywhere on a row (no checkbox) to drill the detail
    in. AgGrid returns the selected ROW DATA, so we read the user_id directly
    and there is no row-index drift on refresh. Writes session_state["selected_user"]."""
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

    gb = GridOptionsBuilder.from_dataframe(display)
    gb.configure_selection(selection_mode="single", use_checkbox=False)
    gb.configure_grid_options(rowHeight=34, headerHeight=36, suppressCellFocus=True)
    gb.configure_column("last active", type=["numericColumn"],
                        valueFormatter=JsCode("function(p){return (p.value).toFixed(1)+' s';}"))
    # Stage cell tinted by meaning (mirrors STAGE_COLORS).
    gb.configure_column("stage", cellStyle=JsCode("""
        function(p){
          var m={'converting':['rgba(52,168,83,0.18)','#1e8e3e'],
                 'engaged':['rgba(26,115,232,0.18)','#1a73e8'],
                 'browsing':['rgba(140,140,150,0.18)','#9aa0a6'],
                 'needs help':['rgba(234,67,53,0.20)','#e06055']};
          var c=m[p.value]||['transparent','inherit'];
          return {backgroundColor:c[0],color:c[1],fontWeight:600};
        }"""))
    # Engagement rendered as an inline progress bar.
    gb.configure_column("engagement", cellRenderer=JsCode("""
        class B{init(p){var v=Math.max(0,Math.min(100,p.value||0));
          var e=document.createElement('div');
          e.style.cssText='display:flex;align-items:center;gap:6px;height:100%';
          e.innerHTML='<div style="flex:1;background:rgba(140,140,150,0.25);border-radius:4px;height:8px;overflow:hidden">'
            +'<div style="width:'+v+'%;height:8px;background:#1a73e8;border-radius:4px"></div></div>'
            +'<span style="font-variant-numeric:tabular-nums">'+v+'</span>';
          this.e=e;}getGui(){return this.e;}}"""))

    # Folding the detected theme into the key forces AgGrid to remount when the
    # theme flips (it won't re-theme in place). Relies on theme detection working.
    resp = AgGrid(
        display,
        gridOptions=gb.build(),
        update_on=["selectionChanged"],
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=True,
        height=470,
        theme="streamlit",
        key=f"online_aggrid_{theme}",
    )

    uid = _picked_user(resp)
    if uid:
        st.session_state["selected_user"] = uid


def _take_action(uid: str, kind: str, label: str):
    """Act on the inspected session (route support / send incentive). Client-side
    demo only - records the action, logs it, toasts. Nothing written to Lakebase."""
    _acted()[uid] = kind
    log = st.session_state.setdefault("action_log", [])
    log.insert(0, (datetime.now().strftime("%H:%M:%S"), f"{label} · {uid}"))
    st.toast(f"{label} · {uid}")
    st.rerun(scope="fragment")


def _render_action_log():
    log = st.session_state.get("action_log", [])
    if not log:
        return
    rows = "".join(
        f'<div class="e"><span class="t">{t}</span> &nbsp; {msg}</div>' for t, msg in log[:8]
    )
    st.markdown(f'<div class="pc-log"><div class="h">Action log</div>{rows}</div>',
                unsafe_allow_html=True)


@st.fragment(run_every=REFRESH_SECONDS)
def render_console():
    try:
        metrics = load_metrics()
    except Exception as e:
        st.error("Lakebase connection not ready yet.")
        st.exception(e)
        return

    if metrics.empty or int(metrics.iloc[0]["total_rows"] or 0) == 0:
        st.info("Waiting for the stream... no session rows yet.")
        return

    m = metrics.iloc[0]
    freshest = m["freshest_secs"]
    dot_color, live_text = _freshness_badge(freshest)
    role = st.session_state.get("pg_role", os.getenv("PGUSER", "local"))
    _theme = _detect_theme()  # drives the banner palette + the AgGrid remount-on-toggle

    # The live online population is the single source for the banner flow rate, the
    # KPIs, the table, and the detail - so every number on screen agrees.
    grid = load_grid()
    converting_count = int((grid["funnel_stage"] == "converting").sum()) if not grid.empty else 0
    needs_help_count = (
        int((grid["needs_help"] & ~grid["user_id"].isin(_support_acted_ids())).sum())
        if not grid.empty else 0
    )

    # Architecture banner - the data-rail flow speed tracks live active sessions.
    _render_banner(_theme, len(grid))

    # Hero
    st.markdown(
        f"""
        <div class="pc-hero">
          <div>
            <h1>Personalization Console</h1>
            <div class="sub">Live session state, served sub-second straight from Lakebase</div>
          </div>
          <div class="pc-live">
            <span class="pc-dot" style="background:{dot_color};"></span>{live_text}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Metric strip - each card carries a one-line read of what the number means.
    c1, c2, c3, c4 = st.columns(4)
    _metric_card(c1, "Active users", len(grid),
                 foot="live concurrency right now")
    _metric_card(c2, "Converting", converting_count,
                 accent=STAGE_COLORS["converting"][1], foot="revenue-proximal, protect these")
    _metric_card(c3, "Needs help", needs_help_count,
                 accent=STAGE_COLORS["needs help"][1] if needs_help_count else STAGE_COLORS["converting"][1],
                 foot="friction happening now")
    fresh_str = f"{float(freshest):.1f}s" if freshest is not None else "n/a"
    _metric_card(c4, "Freshest update", fresh_str, accent=dot_color,
                 foot="serving latency vs Postgres now()")

    st.write("")

    # Segment filter - focus the live sessions table on a cohort.
    seg = st.segmented_control(
        "Focus the sessions", options=SEGMENTS, default="All", key="seg_filter",
    ) or "All"

    filtered = _apply_segment(grid, seg)
    left, right = st.columns([3, 2], gap="large")

    with left:
        st.markdown(f"##### Live online sessions · {seg} ({len(filtered)})")
        if filtered.empty:
            st.info("No sessions in this segment right now.")
        else:
            display = filtered.copy()
            display["stage"] = display.apply(stage_label, axis=1)
            display["last active"] = display["last_active_secs"].astype(float).round(1)
            display = display[
                ["user_id", "current_surface", "stage", "engagement_score", "last active"]
            ].rename(columns={
                "user_id": "user", "current_surface": "surface",
                "engagement_score": "engagement",
            })
            _render_online_grid(display, theme=_theme)

    # Resolve the drilled-in user: an explicit pick (grid row or queue button)
    # wins, otherwise fall back to the most engaged session in view.
    selected_user = st.session_state.get("selected_user")
    if not selected_user:
        if not filtered.empty:
            selected_user = filtered.iloc[0]["user_id"]
        elif not grid.empty:
            selected_user = grid.iloc[0]["user_id"]

    with right:
        st.markdown("##### Session detail")
        if selected_user is None:
            st.caption("Select a session to see its next best action.")
        else:
            detail = load_detail(selected_user)
            if detail.empty:
                st.caption("That session is no longer present.")
            else:
                r = detail.iloc[0]
                act = _available_action(r)
                done = bool(act and _acted().get(r["user_id"]) == act[0])
                resolved = " resolved" if done else ""
                st.markdown(f"**{r['user_id']}** &nbsp; {_pill(stage_label(r))}",
                            unsafe_allow_html=True)
                st.markdown(
                    f'<div class="pc-nba{resolved}"><div class="k">Next best action</div>'
                    f'<div class="v">{next_best_action(r)}</div>'
                    f'<div class="why">Why: {nba_why(r)}</div>'
                    f'<div class="note">Decision logic is illustrative - plug in your '
                    f'own rules or a served model here.</div></div>',
                    unsafe_allow_html=True,
                )
                # The action for the inspected session: an NBA that warrants a human move (route
                # support / send incentive) gets a button. support = red, incentive = green.
                if act and not done:
                    kind, label, _ = act
                    with st.container(key=f"actbtn_{kind}"):
                        if st.button(label, key=f"act_{r['user_id']}"):
                            _take_action(r["user_id"], kind, label)
                score = int(r["engagement_score"])
                st.markdown(
                    f'<div class="pc-kv"><span class="k">Current surface</span>'
                    f'<span>{_pill(r["current_surface"])}</span></div>'
                    f'<div class="pc-kv"><span class="k">Funnel stage</span>'
                    f'<span>{_pill(r["funnel_stage"])}</span></div>'
                    f'<div class="pc-kv"><span class="k">Status</span>'
                    f'<span>{r["status"]}</span></div>'
                    f'<div class="pc-kv"><span class="k">Click count</span>'
                    f'<span>{int(r["click_count"])}</span></div>'
                    f'<div class="pc-kv"><span class="k">Last updated</span>'
                    f'<span>{str(r["last_updated"])[:19]}</span></div>',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div style="margin-top:12px;"><div class="label" '
                    f'style="font-size:0.78rem;opacity:0.6;">Engagement score '
                    f'&nbsp; <b>{score}</b>/100</div>'
                    f'<div class="pc-bar-track"><div class="pc-bar-fill" '
                    f'style="width:{score}%;"></div></div></div>',
                    unsafe_allow_html=True,
                )
                st.caption("Read live from Lakebase Postgres - no cache, no batch.")

        _render_action_log()

    st.caption(
        f"Auto-refreshing every {REFRESH_SECONDS}s. Connected as Postgres role "
        f"`{role}`{' (mock data)' if MOCK else ' via a minted short-lived credential'}."
    )


def _startup_selftest():
    """One-time connectivity self-test, logged to stdout so the Lakebase read path is visible in the app logs."""
    if MOCK or st.session_state.get("_selftest_done"):
        return
    st.session_state["_selftest_done"] = True
    try:
        df = run_query(
            "SELECT COUNT(*) AS n, "
            "EXTRACT(EPOCH FROM (now() - MAX(last_updated))) AS fresh "
            f"FROM {SCHEMA}.{TABLE}"
        )
        n = int(df.iloc[0]["n"])
        fresh = df.iloc[0]["fresh"]
        print(
            f"[SELFTEST] Lakebase OK as role={st.session_state.get('pg_role')} "
            f"rows={n} freshest_secs={float(fresh):.2f}",
            flush=True,
        )
    except Exception as e:  # noqa: BLE001
        print(f"[SELFTEST] Lakebase FAILED: {type(e).__name__}: {e}", flush=True)


_inject_css()
_startup_selftest()
render_console()
