# app.py
# FundGov360 v5 — Fund Data Governance Platform
# Streamlit multi-page application | Bronze / Silver / Gold medallion architecture

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from utils.data_generator import load_all_data
from utils.rule_engine import (
    init_rule_engine_state, get_rules_df, run_all_rules,
    compute_dq_score, get_rules_summary_df, generate_alerts,
    get_failures_summary, get_sla_breach_rules, export_rules_to_df,
    import_rules_from_df, validate_rule_dict, add_rule, update_rule,
    delete_rule, toggle_rule, get_rules, RULE_TYPES, SEVERITIES,
    DATASETS, DATASET_FIELDS, RULE_TEMPLATES, gen_rule_trends,
)
from utils.conflict_resolver import (
    init_resolver_state, get_conflicts_df, get_open_conflicts_df,
    get_resolution_stats, auto_resolve_conflicts, resolve_conflict,
    escalate_conflict, reject_conflict, reassign_conflict,
    get_audit_trail, build_golden_record_summary, simulate_new_conflict,
    detect_conflicts, CONFLICT_TYPES, PRIORITY_LEVELS,
    RESOLUTION_METHODS, RESOLVER_NAMES,
)

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────

st.set_page_config(
    page_title    = "FundGov360",
    page_icon     = "🏦",
    layout        = "wide",
    initial_sidebar_state = "expanded",
)

# ─────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────

st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #16213e 100%);
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        border-left: 4px solid #4fc3f7;
        margin-bottom: 0.5rem;
    }
    .metric-card h3 { color: #90caf9; font-size: 0.85rem; margin: 0; }
    .metric-card h1 { color: #ffffff; font-size: 2rem; margin: 0.2rem 0; }
    .metric-card p  { color: #b0bec5; font-size: 0.75rem; margin: 0; }

    .badge-critical { background:#ef5350; color:white; border-radius:4px; padding:2px 8px; font-size:0.75rem; }
    .badge-high     { background:#ff9800; color:white; border-radius:4px; padding:2px 8px; font-size:0.75rem; }
    .badge-medium   { background:#fdd835; color:#333; border-radius:4px; padding:2px 8px; font-size:0.75rem; }
    .badge-low      { background:#66bb6a; color:white; border-radius:4px; padding:2px 8px; font-size:0.75rem; }

    .gold-banner   { background: linear-gradient(90deg,#b8860b,#ffd700); color:#1a1a1a; padding:6px 12px; border-radius:6px; font-weight:700; }
    .silver-banner { background: linear-gradient(90deg,#708090,#c0c0c0); color:#1a1a1a; padding:6px 12px; border-radius:6px; font-weight:700; }
    .bronze-banner { background: linear-gradient(90deg,#8b4513,#cd7f32); color:#fff; padding:6px 12px; border-radius:6px; font-weight:700; }

    [data-testid="stSidebar"] { background: #0d1b2a; }
    [data-testid="stSidebar"] .stMarkdown p { color: #90caf9; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# SESSION STATE — DATA BOOTSTRAP
# ─────────────────────────────────────────────

@st.cache_data(show_spinner="⏳ Generating synthetic fund data...")
def load_data():
    return load_all_data(nav_days=365, n_transactions=500)

if "data" not in st.session_state:
    st.session_state["data"] = load_data()

data         = st.session_state["data"]
funds_df     = data["funds"]
sf_df        = data["sub_funds"]
sc_df        = data["share_classes"]
nav_df       = data["nav"]
ytd_df       = data["ytd"]
port_df      = data["portfolio"]
tx_df        = data["transactions"]
reg_df       = data["registration"]
stewards_df  = data["stewards"]
catalog_df   = data["catalog"]
lineage_df   = data["lineage"]
nodes_df     = data["lineage_nodes"]
profiling_df = data["profiling"]

# Initialise engines
init_rule_engine_state()
init_resolver_state()

# ─────────────────────────────────────────────
# SIDEBAR NAVIGATION
# ─────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🏦 FundGov360")
    st.markdown("**Fund Data Governance Platform**")
    st.markdown("---")

    page = st.radio(
        "Navigate",
        options=[
            "📊 Dashboard",
            "📈 NAV Monitor",
            "💰 AUM Tracker",
            "📁 Portfolio Holdings",
            "🔄 Transactions",
            "🌍 Registration Matrix",
            "🗂️ Static Data",
            "🔧 DQ Rule Manager",
            "⚔️ Conflict Resolver",
            "📖 Data Catalog",
            "🔬 Data Profiling",
            "🔗 Data Lineage",
            "👤 Stewardship",
        ],
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown("### 📅 Data Snapshot")
    st.caption(f"As of: **{datetime.today().strftime('%d %b %Y')}**")
    st.caption(f"Funds: **{len(funds_df)}** | Sub-funds: **{len(sf_df)}** | Share classes: **{len(sc_df)}**")
    st.caption(f"NAV records: **{len(nav_df):,}**")
    st.caption(f"Transactions: **{len(tx_df):,}**")

    st.markdown("---")
    st.markdown('<span class="bronze-banner">🥉 Bronze</span> &nbsp; <span class="silver-banner">🥈 Silver</span> &nbsp; <span class="gold-banner">🥇 Gold</span>', unsafe_allow_html=True)
    st.caption("Medallion Architecture")

# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────

def metric_card(label: str, value: str, note: str = "") -> str:
    return f"""
    <div class="metric-card">
        <h3>{label}</h3>
        <h1>{value}</h1>
        <p>{note}</p>
    </div>"""

def fmt_currency(v, currency="USD") -> str:
    if v is None:
        return "—"
    if abs(v) >= 1e9:
        return f"{currency} {v/1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"{currency} {v/1e6:.2f}M"
    return f"{currency} {v:,.2f}"

def severity_color(sev: str) -> str:
    return {"Critical": "#ef5350", "High": "#ff9800", "Medium": "#fdd835", "Low": "#66bb6a"}.get(sev, "#90caf9")

def layer_color(layer: str) -> str:
    return {"Bronze": "#cd7f32", "Silver": "#c0c0c0", "Gold": "#ffd700"}.get(layer, "#90caf9")

# ─────────────────────────────────────────────
# PAGE: DASHBOARD
# ─────────────────────────────────────────────

if page == "📊 Dashboard":
    st.title("📊 FundGov360 — Executive Dashboard")
    st.caption("Real-time fund data governance overview | Medallion architecture: Bronze → Silver → Gold")

    # ── KPI Row 1
    total_aum = nav_df.groupby("sc_id")["aum"].last().sum()
    open_conflicts = len(get_open_conflicts_df())
    dq_results = run_all_rules(nav_df, sc_df, tx_df, port_df, reg_df)
    dq_score   = compute_dq_score(dq_results)
    sla_breaches = dq_score.get("sla_breaches", 0)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(metric_card("Total AUM", fmt_currency(total_aum), f"{len(funds_df)} funds · {len(sc_df)} share classes"), unsafe_allow_html=True)
    with c2:
        score_val = dq_score.get("weighted_score", 0)
        color_score = "🟢" if score_val >= 95 else ("🟡" if score_val >= 85 else "🔴")
        st.markdown(metric_card("DQ Weighted Score", f"{color_score} {score_val}%", f"SLA breaches: {sla_breaches}"), unsafe_allow_html=True)
    with c3:
        st.markdown(metric_card("Open Conflicts", str(open_conflicts), "Awaiting resolution"), unsafe_allow_html=True)
    with c4:
        settled_pct = round(len(tx_df[tx_df["settlement_status"] == "Settled"]) / len(tx_df) * 100, 1) if len(tx_df) else 0
        st.markdown(metric_card("Settlement Rate", f"{settled_pct}%", f"{len(tx_df):,} total transactions"), unsafe_allow_html=True)

    st.markdown("---")

    # ── Row 2: AUM by Fund + DQ by Severity
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("💼 AUM by Fund")
        aum_fund = nav_df.groupby("fund_id")["aum"].sum().reset_index()
        aum_fund = aum_fund.merge(funds_df[["fund_id", "fund_name"]], on="fund_id")
        fig_aum = px.pie(
            aum_fund, values="aum", names="fund_name",
            color_discrete_sequence=px.colors.sequential.Blues_r,
            hole=0.45,
        )
        fig_aum.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white", height=320)
        st.plotly_chart(fig_aum, use_container_width=True)

    with col_r:
        st.subheader("🛡️ DQ Score by Severity")
        by_sev = dq_score.get("by_severity", {})
        sev_df = pd.DataFrame([
            {"Severity": k, "Avg Pass Rate": v["avg_pass_rate"], "Rules": v["rules"]}
            for k, v in by_sev.items()
        ])
        if not sev_df.empty:
            fig_dq = px.bar(
                sev_df, x="Severity", y="Avg Pass Rate", color="Severity",
                color_discrete_map={"Critical":"#ef5350","High":"#ff9800","Medium":"#fdd835","Low":"#66bb6a"},
                text="Avg Pass Rate",
            )
            fig_dq.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_dq.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="white", height=320, showlegend=False,
                yaxis=dict(range=[0, 105]),
            )
            st.plotly_chart(fig_dq, use_container_width=True)

    # ── Row 3: NAV trend + Conflict breakdown
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("📈 NAV Trend — Top 5 Share Classes")
        top5_sc = sc_df.head(5)["sc_id"].tolist()
        nav_top = nav_df[nav_df["sc_id"].isin(top5_sc)].copy()
        nav_top["date"] = pd.to_datetime(nav_top["date"])
        fig_nav = px.line(
            nav_top, x="date", y="nav", color="sc_id",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_nav.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="white", height=280, legend_title="Share Class",
        )
        st.plotly_chart(fig_nav, use_container_width=True)

    with col_b:
        st.subheader("⚔️ Conflicts by Type")
        stats = get_resolution_stats()
        by_type = stats.get("by_type", {})
        if by_type:
            type_df = pd.DataFrame(list(by_type.items()), columns=["Type", "Count"]).sort_values("Count", ascending=True).tail(10)
            fig_ct = px.bar(
                type_df, x="Count", y="Type", orientation="h",
                color="Count", color_continuous_scale="Reds",
            )
            fig_ct.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="white", height=280, coloraxis_showscale=False,
            )
            st.plotly_chart(fig_ct, use_container_width=True)

    # ── Active Alerts
    st.subheader("🚨 Active DQ Alerts")
    alerts_df = generate_alerts(dq_results)
    if not alerts_df.empty:
        st.dataframe(alerts_df.head(10), use_container_width=True, hide_index=True)
    else:
        st.success("✅ No active DQ alerts — all rules passing SLA.")

# ─────────────────────────────────────────────
# PAGE: NAV MONITOR
# ─────────────────────────────────────────────

elif page == "📈 NAV Monitor":
    st.title("📈 NAV Monitor")
    st.caption("Daily NAV per share class · Gold layer · Source: Fund Administrator / Bloomberg")

    # Filters
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        sel_fund = st.selectbox("Fund", ["All"] + funds_df["fund_name"].tolist())
    with col_f2:
        date_min = pd.to_datetime(nav_df["date"]).min().date()
        date_max = pd.to_datetime(nav_df["date"]).max().date()
        date_range = st.date_input("Date Range", value=(date_max - timedelta(days=90), date_max),
                                   min_value=date_min, max_value=date_max)
    with col_f3:
        sel_currency = st.multiselect("Currency", nav_df["currency"].unique().tolist(),
                                       default=nav_df["currency"].unique().tolist())

    # Filter data
    nav_view = nav_df.copy()
    nav_view["date"] = pd.to_datetime(nav_view["date"])
    if sel_fund != "All":
        fid = funds_df[funds_df["fund_name"] == sel_fund]["fund_id"].values[0]
        nav_view = nav_view[nav_view["fund_id"] == fid]
    if len(date_range) == 2:
        nav_view = nav_view[(nav_view["date"].dt.date >= date_range[0]) &
                            (nav_view["date"].dt.date <= date_range[1])]
    if sel_currency:
        nav_view = nav_view[nav_view["currency"].isin(sel_currency)]

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    latest_nav = nav_view.groupby("sc_id")["nav"].last()
    k1.metric("Share Classes", len(latest_nav))
    k2.metric("Avg NAV", f"{latest_nav.mean():.4f}")
    k3.metric("Min NAV", f"{latest_nav.min():.4f}")
    k4.metric("Max NAV", f"{latest_nav.max():.4f}")

    st.markdown("---")

    # NAV chart
    sc_options = nav_view["sc_id"].unique().tolist()
    sel_sc = st.multiselect("Select Share Classes to chart", sc_options, default=sc_options[:5])
    if sel_sc:
        chart_data = nav_view[nav_view["sc_id"].isin(sel_sc)]
        fig = px.line(
            chart_data, x="date", y="nav", color="sc_id",
            title="NAV Over Time",
            color_discrete_sequence=px.colors.qualitative.Plotly,
        )
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="white", height=400)
        st.plotly_chart(fig, use_container_width=True)

    # YTD Performance
    st.subheader("📊 YTD Performance")
    ytd_merged = ytd_df.merge(sc_df[["sc_id", "sc_name", "currency"]], on="sc_id", how="left")
    ytd_merged["ytd_pct"] = ytd_merged["ytd_pct"].round(2)
    fig_ytd = px.bar(
        ytd_merged.sort_values("ytd_pct", ascending=False).head(20),
        x="sc_name", y="ytd_pct", color="ytd_pct",
        color_continuous_scale="RdYlGn",
        title="YTD Return (%) per Share Class",
    )
    fig_ytd.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="white", height=380, xaxis_tickangle=-45)
    st.plotly_chart(fig_ytd, use_container_width=True)

    # Raw table
    with st.expander("🔍 Raw NAV Records"):
        st.dataframe(nav_view.sort_values("date", ascending=False).head(500),
                     use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# PAGE: AUM TRACKER
# ─────────────────────────────────────────────

elif page == "💰 AUM Tracker":
    st.title("💰 AUM Tracker")
    st.caption("Assets Under Management · Gold layer · Aggregated from NAV × shares outstanding")

    # Latest AUM snapshot
    latest_nav_per_sc = nav_df.sort_values("date").groupby("sc_id").last().reset_index()
    aum_sc = latest_nav_per_sc[["sc_id", "fund_id", "aum", "currency"]].copy()
    aum_sc = aum_sc.merge(sc_df[["sc_id", "sc_name", "sub_fund_id"]], on="sc_id")
    aum_sc = aum_sc.merge(sf_df[["sub_fund_id", "sub_fund_name", "asset_class"]], on="sub_fund_id")
    aum_sc = aum_sc.merge(funds_df[["fund_id", "fund_name"]], on="fund_id")

    total_aum = aum_sc["aum"].sum()
    k1, k2, k3 = st.columns(3)
    k1.metric("Total AUM", fmt_currency(total_aum))
    k2.metric("Largest Fund", aum_sc.groupby("fund_name")["aum"].sum().idxmax())
    k3.metric("# Active Share Classes", len(sc_df[sc_df["status"] == "Active"]))

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("AUM by Asset Class")
        aum_ac = aum_sc.groupby("asset_class")["aum"].sum().reset_index()
        fig1 = px.pie(aum_ac, values="aum", names="asset_class",
                      color_discrete_sequence=px.colors.sequential.Teal, hole=0.4)
        fig1.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=320)
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("AUM by Fund")
        aum_f = aum_sc.groupby("fund_name")["aum"].sum().reset_index().sort_values("aum", ascending=False)
        fig2 = px.bar(aum_f, x="fund_name", y="aum", color="fund_name",
                      color_discrete_sequence=px.colors.qualitative.Pastel)
        fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                           font_color="white", height=320, showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("📋 AUM Breakdown by Share Class")
    aum_display = aum_sc[["fund_name", "sub_fund_name", "sc_name", "currency", "aum"]].copy()
    aum_display["aum"] = aum_display["aum"].apply(lambda x: fmt_currency(x))
    st.dataframe(aum_display.sort_values("fund_name"), use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# PAGE: PORTFOLIO HOLDINGS
# ─────────────────────────────────────────────

elif page == "📁 Portfolio Holdings":
    st.title("📁 Portfolio Holdings")
    st.caption("Holdings by geography, sector, asset class · Silver layer · Source: Custodian / Fund Admin")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        sel_sf = st.selectbox("Sub-Fund", ["All"] + sf_df["sub_fund_name"].tolist())
    with col_f2:
        sel_ac = st.multiselect("Asset Class", port_df["asset_class"].unique().tolist(),
                                 default=port_df["asset_class"].unique().tolist())

    port_view = port_df.copy()
    if sel_sf != "All":
        sfid = sf_df[sf_df["sub_fund_name"] == sel_sf]["sub_fund_id"].values[0]
        port_view = port_view[port_view["sub_fund_id"] == sfid]
    if sel_ac:
        port_view = port_view[port_view["asset_class"].isin(sel_ac)]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Positions", len(port_view))
    k2.metric("Total Market Value", fmt_currency(port_view["market_value"].sum()))
    k3.metric("Avg Weight", f"{port_view['weight_pct'].mean():.2f}%")
    k4.metric("Geographies", port_view["geography"].nunique())

    st.markdown("---")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("By Geography")
        geo = port_view.groupby("geography")["market_value"].sum().reset_index()
        fig_geo = px.pie(geo, values="market_value", names="geography", hole=0.4,
                         color_discrete_sequence=px.colors.sequential.Plasma_r)
        fig_geo.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=300)
        st.plotly_chart(fig_geo, use_container_width=True)

    with col2:
        st.subheader("By Sector")
        sec = port_view.groupby("sector")["market_value"].sum().reset_index().sort_values("market_value", ascending=True)
        fig_sec = px.bar(sec, x="market_value", y="sector", orientation="h",
                         color="market_value", color_continuous_scale="Viridis")
        fig_sec.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              font_color="white", height=300, coloraxis_showscale=False)
        st.plotly_chart(fig_sec, use_container_width=True)

    with col3:
        st.subheader("By Asset Class")
        ac = port_view.groupby("asset_class")["market_value"].sum().reset_index()
        fig_ac = px.pie(ac, values="market_value", names="asset_class", hole=0.4,
                        color_discrete_sequence=px.colors.qualitative.Set3)
        fig_ac.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=300)
        st.plotly_chart(fig_ac, use_container_width=True)

    st.subheader("🔝 Top 20 Holdings by Market Value")
    top20 = port_view.nlargest(20, "market_value")[
        ["security_name", "isin", "asset_class", "geography", "sector", "currency", "market_value", "weight_pct"]
    ].copy()
    top20["market_value"] = top20["market_value"].apply(lambda x: fmt_currency(x))
    st.dataframe(top20, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# PAGE: TRANSACTIONS
# ─────────────────────────────────────────────

elif page == "🔄 Transactions":
    st.title("🔄 Transactions")
    st.caption("Buy / Sell / Subscription / Redemption · Silver layer · Source: Transfer Agent / Custodian")

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        sel_type = st.multiselect("Transaction Type", tx_df["tx_type"].unique().tolist(),
                                   default=tx_df["tx_type"].unique().tolist())
    with col_f2:
        sel_status = st.multiselect("Settlement Status", tx_df["settlement_status"].unique().tolist(),
                                     default=tx_df["settlement_status"].unique().tolist())
    with col_f3:
        show_errors = st.checkbox("Show Error Flagged Only", value=False)

    tx_view = tx_df.copy()
    if sel_type:
        tx_view = tx_view[tx_view["tx_type"].isin(sel_type)]
    if sel_status:
        tx_view = tx_view[tx_view["settlement_status"].isin(sel_status)]
    if show_errors:
        tx_view = tx_view[tx_view["error_flag"] == True]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Transactions", f"{len(tx_view):,}")
    k2.metric("Total Volume", fmt_currency(tx_view["amount"].sum()))
    settled_pct = round(len(tx_view[tx_view["settlement_status"] == "Settled"]) / len(tx_view) * 100, 1) if len(tx_view) else 0
    k3.metric("Settlement Rate", f"{settled_pct}%")
    k4.metric("Errors Flagged", f"{tx_view['error_flag'].sum()}")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Volume by Transaction Type")
        vol_type = tx_view.groupby("tx_type")["amount"].sum().reset_index()
        fig_vt = px.bar(vol_type, x="tx_type", y="amount", color="tx_type",
                        color_discrete_sequence=px.colors.qualitative.Pastel)
        fig_vt.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             font_color="white", height=300, showlegend=False)
        st.plotly_chart(fig_vt, use_container_width=True)

    with col2:
        st.subheader("Settlement Status Breakdown")
        ss = tx_view["settlement_status"].value_counts().reset_index()
        ss.columns = ["Status", "Count"]
        fig_ss = px.pie(ss, values="Count", names="Status", hole=0.4,
                        color_discrete_map={"Settled":"#66bb6a","Pending":"#fdd835","Failed":"#ef5350","Cancelled":"#90caf9"})
        fig_ss.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=300)
        st.plotly_chart(fig_ss, use_container_width=True)

    st.subheader("📋 Transaction Table")
    st.dataframe(
        tx_view.sort_values("tx_date", ascending=False).head(200)[
            ["tx_id","sc_id","tx_type","tx_date","settlement_date","amount",
             "currency","settlement_status","investor_id","error_flag","error_reason"]
        ],
        use_container_width=True, hide_index=True,
    )

# ─────────────────────────────────────────────
# PAGE: REGISTRATION MATRIX
# ─────────────────────────────────────────────

elif page == "🌍 Registration Matrix":
    st.title("🌍 Registration Matrix")
    st.caption("Per-jurisdiction registration status per share class · Gold layer · Source: Legal & Compliance")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        sel_jur = st.multiselect("Jurisdiction", reg_df["jurisdiction"].unique().tolist(),
                                  default=reg_df["jurisdiction"].unique().tolist()[:8])
    with col_f2:
        sel_reg_status = st.multiselect("Status", reg_df["reg_status"].unique().tolist(),
                                         default=reg_df["reg_status"].unique().tolist())

    reg_view = reg_df.copy()
    if sel_jur:
        reg_view = reg_view[reg_view["jurisdiction"].isin(sel_jur)]
    if sel_reg_status:
        reg_view = reg_view[reg_view["reg_status"].isin(sel_reg_status)]

    k1, k2, k3 = st.columns(3)
    k1.metric("Registered", len(reg_view[reg_view["reg_status"] == "Registered"]))
    k2.metric("Pending",    len(reg_view[reg_view["reg_status"] == "Pending"]))
    k3.metric("Restricted", len(reg_view[reg_view["reg_status"] == "Restricted"]))

    st.subheader("🗺️ Registration Heatmap")
    pivot = reg_view.pivot_table(
        index="jurisdiction", columns="reg_status", values="sc_id", aggfunc="count", fill_value=0
    )
    fig_heat = px.imshow(
        pivot, color_continuous_scale="Blues",
        title="Registration Count by Jurisdiction × Status",
    )
    fig_heat.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=420)
    st.plotly_chart(fig_heat, use_container_width=True)

    st.subheader("📋 Registration Detail")
    st.dataframe(
        reg_view[["sc_id","isin","jurisdiction","reg_status","reg_date","expiry_date","local_regulator","last_updated"]],
        use_container_width=True, hide_index=True,
    )

# ─────────────────────────────────────────────
# PAGE: STATIC DATA
# ─────────────────────────────────────────────

elif page == "🗂️ Static Data":
    st.title("🗂️ Static Data Master")
    st.caption("Fund / Sub-Fund / Share Class hierarchy · Gold layer")

    tab1, tab2, tab3 = st.tabs(["🏦 Funds", "📂 Sub-Funds", "📄 Share Classes"])

    with tab1:
        st.dataframe(funds_df, use_container_width=True, hide_index=True)

    with tab2:
        sf_display = sf_df.merge(funds_df[["fund_id","fund_name"]], on="fund_id")
        st.dataframe(sf_display, use_container_width=True, hide_index=True)

    with tab3:
        sc_display = sc_df.merge(sf_df[["sub_fund_id","sub_fund_name"]], on="sub_fund_id")
        sc_display = sc_display.merge(funds_df[["fund_id","fund_name"]], on="fund_id")
        st.dataframe(sc_display, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# PAGE: DQ RULE MANAGER
# ─────────────────────────────────────────────

elif page == "🔧 DQ Rule Manager":
    st.title("🔧 Data Quality Rule Manager")
    st.caption("Define, execute and monitor DQ rules · Inspired by Talend DQ / Great Expectations / Atlan")

    tab_run, tab_results, tab_trends, tab_manage, tab_import = st.tabs([
        "▶️ Run Rules", "📊 Results", "📈 Trends", "⚙️ Manage Rules", "📥 Import/Export"
    ])

    # ── Tab: Run Rules
    with tab_run:
        st.subheader("▶️ Execute Rule Suite")
        col_r1, col_r2 = st.columns([3, 1])
        with col_r1:
            active_only = st.checkbox("Active rules only", value=True)
        with col_r2:
            run_btn = st.button("🚀 Run All Rules", type="primary", use_container_width=True)

        if run_btn or "dq_results" not in st.session_state:
            with st.spinner("Running DQ rules..."):
                results = run_all_rules(nav_df, sc_df, tx_df, port_df, reg_df, active_only=active_only)
                st.session_state["dq_results"] = results
            st.success(f"✅ Executed {len(results)} rules.")

        results = st.session_state.get("dq_results", {})
        if results:
            score = compute_dq_score(results)
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Overall Score",    f"{score['overall_score']}%")
            m2.metric("Weighted Score",   f"{score['weighted_score']}%")
            m3.metric("Total Checked",    f"{score['total_checks']:,}")
            m4.metric("Total Failed",     f"{score['total_failed']:,}")
            m5.metric("SLA Breaches",     str(score['sla_breaches']))

            st.subheader("🚨 Active Alerts")
            alerts = generate_alerts(results)
            if not alerts.empty:
                st.dataframe(alerts, use_container_width=True, hide_index=True)
            else:
                st.success("✅ No alerts — all rules passing.")

    # ── Tab: Results
    with tab_results:
        st.subheader("📊 Rule Results")
        results = st.session_state.get("dq_results", {})
        if not results:
            st.info("Run rules first in the ▶️ Run Rules tab.")
        else:
            summary = get_rules_summary_df(results)
            st.dataframe(summary, use_container_width=True, hide_index=True)

            st.subheader("❌ Failure Records")
            failures = get_failures_summary(results)
            if not failures.empty:
                st.dataframe(failures.head(200), use_container_width=True, hide_index=True)
            else:
                st.success("✅ No failure records.")

    # ── Tab: Trends
    with tab_trends:
        st.subheader("📈 DQ Pass-Rate Trends (30 days)")
        trends_df = gen_rule_trends(days=30)
        sel_rule = st.selectbox("Select Rule", trends_df["rule_id"].unique().tolist())
        rule_trend = trends_df[trends_df["rule_id"] == sel_rule]
        sla_target = rule_trend["sla_target"].iloc[0]
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=rule_trend["date"], y=rule_trend["pass_rate"],
            mode="lines+markers", name="Pass Rate", line=dict(color="#4fc3f7"),
        ))
        fig_trend.add_hline(y=sla_target, line_dash="dash", line_color="#ef5350",
                            annotation_text=f"SLA {sla_target}%")
        fig_trend.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="white", height=360, yaxis=dict(range=[70, 101]),
        )
        st.plotly_chart(fig_trend, use_container_width=True)

    # ── Tab: Manage Rules
    with tab_manage:
        st.subheader("⚙️ Rule Library")
        rules_df_display = get_rules_df()
        st.dataframe(rules_df_display[[
            "rule_id","rule_name","dataset","field","rule_type","severity","active","sla_pass_rate","owner"
        ]], use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("➕ Add New Rule")
        with st.form("add_rule_form"):
            fc1, fc2 = st.columns(2)
            with fc1:
                new_id      = st.text_input("Rule ID", placeholder="e.g. MY-001")
                new_name    = st.text_input("Rule Name")
                new_dataset = st.selectbox("Dataset", DATASETS)
                new_field   = st.selectbox("Field", DATASET_FIELDS.get(DATASETS[0], []))
                new_type    = st.selectbox("Rule Type", RULE_TYPES)
            with fc2:
                new_sev     = st.selectbox("Severity", SEVERITIES)
                new_min     = st.number_input("Threshold Min", value=None, format="%.4f")
                new_max     = st.number_input("Threshold Max", value=None, format="%.4f")
                new_regex   = st.text_input("Regex Pattern", placeholder=r"^[A-Z]{2}...")
                new_formula = st.text_input("Formula Expression", placeholder="amount > 0")
                new_sla     = st.number_input("SLA Pass Rate %", min_value=0.0, max_value=100.0, value=95.0)
                new_owner   = st.text_input("Owner", value="Data Management")
                new_desc    = st.text_area("Description")

            submitted = st.form_submit_button("Add Rule", type="primary")
            if submitted:
                new_rule = {
                    "rule_id": new_id, "rule_name": new_name, "description": new_desc,
                    "dataset": new_dataset, "field": new_field, "rule_type": new_type,
                    "threshold_min": new_min, "threshold_max": new_max,
                    "regex_pattern": new_regex or None, "formula_expr": new_formula or None,
                    "severity": new_sev, "active": True, "category": "Custom",
                    "owner": new_owner, "sla_pass_rate": new_sla,
                    "created_date": datetime.today().strftime("%Y-%m-%d"),
                    "last_modified": datetime.today().strftime("%Y-%m-%d"),
                }
                errs = validate_rule_dict(new_rule)
                if errs:
                    for e in errs:
                        st.error(e)
                else:
                    try:
                        add_rule(new_rule)
                        st.success(f"✅ Rule {new_id} added.")
                    except ValueError as e:
                        st.error(str(e))

        st.markdown("---")
        st.subheader("🔄 Toggle / Delete Rule")
        col_t1, col_t2, col_t3 = st.columns(3)
        with col_t1:
            toggle_id = st.selectbox("Rule to Toggle", get_rules_df()["rule_id"].tolist(), key="toggle_sel")
        with col_t2:
            delete_id = st.selectbox("Rule to Delete", get_rules_df()["rule_id"].tolist(), key="delete_sel")
        with col_t3:
            st.markdown("&nbsp;", unsafe_allow_html=True)
            if st.button("Toggle Active/Inactive"):
                toggle_rule(toggle_id)
                st.success(f"Toggled {toggle_id}")
            if st.button("🗑️ Delete Rule", type="secondary"):
                delete_rule(delete_id)
                st.warning(f"Deleted {delete_id}")

    # ── Tab: Import/Export
    with tab_import:
        st.subheader("📤 Export Rules")
        export_df = export_rules_to_df()
        csv = export_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Rules CSV", data=csv,
                           file_name="fundgov360_rules.csv", mime="text/csv")

        st.markdown("---")
        st.subheader("📥 Import Rules from CSV")
        uploaded = st.file_uploader("Upload rules CSV", type=["csv"])
        if uploaded:
            import_df = pd.read_csv(uploaded)
            imported, skipped, errors = import_rules_from_df(import_df)
            st.success(f"✅ Imported: {imported} | Skipped: {skipped}")
            for e in errors:
                st.warning(e)

# ─────────────────────────────────────────────
# PAGE: CONFLICT RESOLVER
# ─────────────────────────────────────────────

elif page == "⚔️ Conflict Resolver":
    st.title("⚔️ Data Conflict Resolver")
    st.caption("Detect, triage and resolve data conflicts · Inspired by Informatica MDM / Talend MDM")

    tab_overview, tab_queue, tab_resolve, tab_golden, tab_audit, tab_simulate = st.tabs([
        "📊 Overview", "📋 Conflict Queue", "✅ Resolve", "🥇 Golden Records", "📜 Audit Trail", "🧪 Simulate"
    ])

    # ── Tab: Overview
    with tab_overview:
        stats = get_resolution_stats()
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("Total Conflicts",    stats.get("total", 0))
        m2.metric("Open",               stats.get("open", 0))
        m3.metric("Resolved",           stats.get("resolved", 0))
        m4.metric("Escalated",          stats.get("escalated", 0))
        m5.metric("SLA Breaches",       stats.get("sla_breaches", 0))
        m6.metric("Auto-Resolved",      stats.get("auto_resolved", 0))

        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("By Priority")
            by_pri = stats.get("by_priority", {})
            if by_pri:
                pri_df = pd.DataFrame(list(by_pri.items()), columns=["Priority", "Count"])
                fig_p = px.bar(pri_df, x="Priority", y="Count", color="Priority",
                               color_discrete_map={"P1 – Critical":"#ef5350","P2 – High":"#ff9800",
                                                   "P3 – Medium":"#fdd835","P4 – Low":"#66bb6a"})
                fig_p.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                    font_color="white", height=300, showlegend=False)
                st.plotly_chart(fig_p, use_container_width=True)

        with col2:
            st.subheader("By Dataset")
            by_ds = stats.get("by_dataset", {})
            if by_ds:
                ds_df = pd.DataFrame(list(by_ds.items()), columns=["Dataset", "Count"])
                fig_d = px.pie(ds_df, values="Count", names="Dataset", hole=0.4,
                               color_discrete_sequence=px.colors.qualitative.Set2)
                fig_d.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=300)
                st.plotly_chart(fig_d, use_container_width=True)

        # Live detection
        st.markdown("---")
        st.subheader("🔍 Detect Live Conflicts from Data")
        if st.button("🔎 Run Conflict Detection", type="primary"):
            new_conflicts = detect_conflicts(nav_df, sc_df, tx_df, port_df, reg_df)
            if new_conflicts:
                st.session_state["conflicts"].extend(new_conflicts)
                st.success(f"✅ Detected and added {len(new_conflicts)} new conflicts.")
            else:
                st.info("No new conflicts detected in current data snapshot.")

    # ── Tab: Queue
    with tab_queue:
        st.subheader("📋 Open Conflict Queue")
        open_df = get_open_conflicts_df()
        if open_df.empty:
            st.success("✅ No open conflicts.")
        else:
            display_cols = ["conflict_id","priority","conflict_type","title","dataset","field",
                            "source_a","value_a","source_b","value_b","status","assigned_to","detected_at"]
            st.dataframe(open_df[display_cols], use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("⚡ Auto-Resolver")
        resolver_name = st.text_input("Resolver Name", value="FundGov360 Auto-Resolver")
        if st.button("🤖 Auto-Resolve All Eligible", type="primary"):
            resolved, skipped = auto_resolve_conflicts(resolver_name=resolver_name)
            st.success(f"✅ Auto-resolved: {resolved} | Skipped: {skipped}")

    # ── Tab: Resolve
    with tab_resolve:
        st.subheader("✅ Manual Resolution")
        conflicts_df = get_conflicts_df()
        open_ids = conflicts_df[conflicts_df["status"].isin(["Open","Under Review","Escalated"])]["conflict_id"].tolist()

        if not open_ids:
            st.success("✅ No open conflicts to resolve.")
        else:
            sel_cid = st.selectbox("Select Conflict", open_ids)
            conflict_row = conflicts_df[conflicts_df["conflict_id"] == sel_cid].iloc[0]

            st.markdown(f"**{conflict_row['title']}**")
            st.caption(conflict_row["description"])
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                st.info(f"**Source A:** {conflict_row['source_a']} → `{conflict_row['value_a']}`")
            with col_d2:
                st.warning(f"**Source B:** {conflict_row['source_b']} → `{conflict_row['value_b']}`")
            st.caption(f"💡 Recommended: `{conflict_row['recommended_value']}` — {conflict_row['resolution_note']}")

            with st.form("resolve_form"):
                final_val   = st.text_input("Final Value", value=str(conflict_row.get("recommended_value") or ""))
                method      = st.selectbox("Resolution Method", RESOLUTION_METHODS)
                resolved_by = st.text_input("Resolved By", value="Clément Denorme")
                comment     = st.text_area("Resolution Comment")
                col_btn1, col_btn2, col_btn3 = st.columns(3)
                with col_btn1:
                    submitted_res = st.form_submit_button("✅ Resolve", type="primary")
                with col_btn2:
                    submitted_esc = st.form_submit_button("⬆️ Escalate")
                with col_btn3:
                    submitted_rej = st.form_submit_button("❌ Reject")

            if submitted_res:
                resolve_conflict(sel_cid, final_val, method, resolved_by, comment)
                st.success(f"✅ Conflict {sel_cid} resolved.")
            if submitted_esc:
                escalate_conflict(sel_cid, resolved_by, comment)
                st.warning(f"⬆️ Conflict {sel_cid} escalated.")
            if submitted_rej:
                reject_conflict(sel_cid, resolved_by, comment)
                st.error(f"❌ Conflict {sel_cid} rejected.")

    # ── Tab: Golden Records
    with tab_golden:
        st.subheader("🥇 Golden Record Status by Dataset")
        golden_df = build_golden_record_summary()
        st.dataframe(golden_df, use_container_width=True, hide_index=True)

        fig_gr = px.bar(
            golden_df, x="Dataset", y="Golden Record %",
            color="Golden Record %", color_continuous_scale="RdYlGn",
            range_color=[60, 100], text="Golden Record %",
        )
        fig_gr.update_traces(texttemplate="%{text}%", textposition="outside")
        fig_gr.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             font_color="white", height=360, coloraxis_showscale=False)
        st.plotly_chart(fig_gr, use_container_width=True)

    # ── Tab: Audit Trail
    with tab_audit:
        st.subheader("📜 Audit Trail")
        filter_cid = st.text_input("Filter by Conflict ID (leave blank for all)", value="")
        audit_df   = get_audit_trail(conflict_id=filter_cid if filter_cid else None)
        if audit_df.empty:
            st.info("No audit entries yet.")
        else:
            st.dataframe(audit_df, use_container_width=True, hide_index=True)

    # ── Tab: Simulate
    with tab_simulate:
        st.subheader("🧪 Conflict Simulator")
        sim_type = st.selectbox("Conflict Type to Simulate", ["(random)"] + list(CONFLICT_TYPES))
        if st.button("💉 Inject Simulated Conflict", type="primary"):
            ct = None if sim_type == "(random)" else sim_type
            new_c = simulate_new_conflict(conflict_type=ct)
            st.success(f"✅ Injected: {new_c['conflict_id']} — {new_c['title']}")

# ─────────────────────────────────────────────
# PAGE: DATA CATALOG
# ─────────────────────────────────────────────

elif page == "📖 Data Catalog":
    st.title("📖 Data Catalog")
    st.caption("Asset inventory with stewardship, classification, quality scores · Inspired by Atlan / Alation")

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        sel_asset_type = st.multiselect("Asset Type", catalog_df["asset_type"].unique().tolist(),
                                         default=catalog_df["asset_type"].unique().tolist())
    with col_f2:
        sel_domain = st.multiselect("Domain", catalog_df["domain"].unique().tolist(),
                                     default=catalog_df["domain"].unique().tolist())
    with col_f3:
        cert_only = st.checkbox("Certified Only", value=False)

    cat_view = catalog_df.copy()
    if sel_asset_type:
        cat_view = cat_view[cat_view["asset_type"].isin(sel_asset_type)]
    if sel_domain:
        cat_view = cat_view[cat_view["domain"].isin(sel_domain)]
    if cert_only:
        cat_view = cat_view[cat_view["certified"] == True]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Assets",     len(cat_view))
    k2.metric("Certified",        cat_view["certified"].sum())
    k3.metric("Avg DQ Score",     f"{cat_view['quality_score'].mean():.1f}%")
    k4.metric("Avg Completeness", f"{cat_view['completeness_pct'].mean():.1f}%")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("DQ Score Distribution")
        fig_qs = px.histogram(cat_view, x="quality_score", nbins=20,
                              color_discrete_sequence=["#4fc3f7"],
                              title="Quality Score Distribution")
        fig_qs.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             font_color="white", height=300)
        st.plotly_chart(fig_qs, use_container_width=True)

    with col2:
        st.subheader("Assets by Steward Role")
        role_cnt = cat_view["steward_role"].value_counts().reset_index()
        role_cnt.columns = ["Role", "Count"]
        fig_role = px.pie(role_cnt, values="Count", names="Role", hole=0.4,
                          color_discrete_sequence=px.colors.qualitative.Pastel)
        fig_role.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=300)
        st.plotly_chart(fig_role, use_container_width=True)

    st.subheader("📋 Asset Inventory")
    display_cols = ["asset_id","asset_type","asset_name","domain","sub_domain","source_system",
                    "data_layer","steward_name","steward_role","quality_score","completeness_pct",
                    "certified","tags","last_profiled"]
    st.dataframe(cat_view[display_cols], use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# PAGE: DATA PROFILING
# ─────────────────────────────────────────────

elif page == "🔬 Data Profiling":
    st.title("🔬 Data Profiling")
    st.caption("Field-level statistics and data quality metrics · Inspired by Talend Data Quality / Atlan Profiler")

    sel_dataset = st.selectbox("Dataset", profiling_df["dataset"].unique().tolist())
    prof_view   = profiling_df[profiling_df["dataset"] == sel_dataset].copy()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Fields Profiled",   len(prof_view))
    k2.metric("Avg Completeness",  f"{prof_view['completeness_pct'].mean():.1f}%")
    k3.metric("Avg DQ Score",      f"{prof_view['overall_dq_score'].mean():.1f}%")
    k4.metric("Avg Conformity",    f"{prof_view['conformity_pct'].mean():.1f}%")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Completeness by Field")
        fig_comp = px.bar(
            prof_view.sort_values("completeness_pct"), x="completeness_pct", y="field_name",
            orientation="h", color="completeness_pct", color_continuous_scale="RdYlGn",
            range_color=[70, 100],
        )
        fig_comp.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font_color="white", height=360, coloraxis_showscale=False)
        st.plotly_chart(fig_comp, use_container_width=True)

    with col2:
        st.subheader("Overall DQ Score by Field")
        fig_dq = px.bar(
            prof_view.sort_values("overall_dq_score"), x="overall_dq_score", y="field_name",
            orientation="h", color="overall_dq_score", color_continuous_scale="Blues",
            range_color=[70, 100],
        )
        fig_dq.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             font_color="white", height=360, coloraxis_showscale=False)
        st.plotly_chart(fig_dq, use_container_width=True)

    st.subheader("📋 Profiling Statistics")
    st.dataframe(prof_view[[
        "field_name","data_type","nullable","row_count","null_count_pct","unique_count_pct",
        "completeness_pct","conformity_pct","validity_pct","overall_dq_score",
        "min_value","max_value","mean_value","top_value","top_value_freq_pct","last_profiled"
    ]], use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# PAGE: DATA LINEAGE
# ─────────────────────────────────────────────

elif page == "🔗 Data Lineage":
    st.title("🔗 Data Lineage")
    st.caption("Bronze → Silver → Gold pipeline · Inspired by Atlan Lineage / Apache Atlas")

    tab_graph, tab_edges, tab_nodes = st.tabs(["🗺️ Lineage Graph", "🔗 Edge Table", "📋 Node Catalog"])

    with tab_graph:
        st.subheader("Pipeline Lineage Graph")
        # Build Sankey diagram
        node_list = nodes_df["node_id"].tolist()
        labels    = [f"{r['dataset']} ({r['layer']})" for _, r in nodes_df.iterrows()]
        node_idx  = {nid: i for i, nid in enumerate(node_list)}
        colors_by_layer = [layer_color(nodes_df.loc[nodes_df["node_id"] == nid, "layer"].values[0]) for nid in node_list]

        src_idx = [node_idx[r["source_node_id"]] for _, r in lineage_df.iterrows()]
        tgt_idx = [node_idx[r["target_node_id"]] for _, r in lineage_df.iterrows()]

        fig_sankey = go.Figure(go.Sankey(
            node=dict(label=labels, color=colors_by_layer, pad=20, thickness=20),
            link=dict(source=src_idx, target=tgt_idx, value=[1]*len(src_idx),
                      color="rgba(79,195,247,0.3)"),
        ))
        fig_sankey.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=500,
            title_text="FundGov360 — Data Lineage Sankey",
        )
        st.plotly_chart(fig_sankey, use_container_width=True)

        # Layer summary
        st.subheader("📊 Layer Summary")
        layer_counts = nodes_df["layer"].value_counts().reset_index()
        layer_counts.columns = ["Layer", "Nodes"]
        for _, row in layer_counts.iterrows():
            color = layer_color(row["Layer"])
            st.markdown(
                f'<span style="color:{color};font-size:1.1rem;font-weight:700">'
                f'🔹 {row["Layer"]}</span> — {row["Nodes"]} node(s)',
                unsafe_allow_html=True
            )

    with tab_edges:
        st.subheader("🔗 Lineage Edges")
        edge_display = lineage_df[[
            "source_node_id","source_dataset","source_layer","source_system",
            "target_node_id","target_dataset","target_layer","target_system",
            "transformation","last_run","status"
        ]].copy()
        edge_display["status"] = edge_display["status"].apply(
            lambda s: f"✅ {s}" if s == "Success" else (f"⚠️ {s}" if s == "Warning" else f"🔴 {s}")
        )
        st.dataframe(edge_display, use_container_width=True, hide_index=True)

    with tab_nodes:
        st.subheader("📋 Node Catalog")
        st.dataframe(nodes_df, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# PAGE: STEWARDSHIP
# ─────────────────────────────────────────────

elif page == "👤 Stewardship":
    st.title("👤 Data Stewardship")
    st.caption("Data owners, stewards and business owners · Inspired by Alation Stewardship / Atlan")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        sel_role = st.multiselect("Role", stewards_df["role"].unique().tolist(),
                                   default=stewards_df["role"].unique().tolist())
    with col_f2:
        sel_dept = st.multiselect("Department", stewards_df["department"].unique().tolist(),
                                   default=stewards_df["department"].unique().tolist())

    stew_view = stewards_df.copy()
    if sel_role:
        stew_view = stew_view[stew_view["role"].isin(sel_role)]
    if sel_dept:
        stew_view = stew_view[stew_view["department"].isin(sel_dept)]

    k1, k2, k3 = st.columns(3)
    k1.metric("Total Stewards", len(stew_view))
    k2.metric("Active",         stew_view["active"].sum())
    k3.metric("Departments",    stew_view["department"].nunique())

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Stewards by Role")
        role_df = stew_view["role"].value_counts().reset_index()
        role_df.columns = ["Role", "Count"]
        fig_r = px.bar(role_df, x="Role", y="Count", color="Role",
                       color_discrete_sequence=px.colors.qualitative.Pastel)
        fig_r.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            font_color="white", height=300, showlegend=False)
        st.plotly_chart(fig_r, use_container_width=True)

    with col2:
        st.subheader("Stewards by Department")
        dept_df = stew_view["department"].value_counts().reset_index()
        dept_df.columns = ["Department", "Count"]
        fig_d = px.pie(dept_df, values="Count", names="Department", hole=0.4,
                       color_discrete_sequence=px.colors.qualitative.Set2)
        fig_d.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=300)
        st.plotly_chart(fig_d, use_container_width=True)

    st.subheader("👥 Steward Directory")
    st.dataframe(
        stew_view[["steward_id","name","email","role","department","location","active","assigned_since"]],
        use_container_width=True, hide_index=True,
    )

    st.markdown("---")
    st.subheader("📋 Asset Ownership Map")
    ownership = catalog_df[["asset_name","asset_type","steward_name","steward_role","quality_score","certified"]].copy()
    ownership["certified"] = ownership["certified"].apply(lambda x: "✅" if x else "—")
    st.dataframe(ownership, use_container_width=True, hide_index=True)
