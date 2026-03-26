
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sys, os

sys.path.insert(0, os.path.dirname(__file__))
from utils.rule_engine import (
    init_rules, get_next_rule_id, test_rule_against_data,
    RULE_TYPES, DOMAINS, ENTITY_TYPES, SEVERITIES, ACTIONS
)
from utils.conflict_resolver import (
    init_conflicts, RESOLUTION_STRATEGIES, TRUSTED_SOURCE_HIERARCHY,
    SOURCES, ATTRIBUTE_CATALOGUE
)
from utils.data_generator import (
    FUNDS, gen_sub_funds, gen_share_classes, gen_nav_data,
    gen_portfolio, gen_transactions, gen_registration_matrix,
    gen_imports, gen_data_quality
)

st.set_page_config(
    page_title="FundGov360 | Data Governance Platform",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  .metric-card {background:#1E2329;border-radius:10px;padding:16px 20px;border-left:4px solid #0066CC;}
  .bronze-card {background:#1E2329;border-radius:10px;padding:16px 20px;border-left:4px solid #CD7F32;}
  .silver-card {background:#1E2329;border-radius:10px;padding:16px 20px;border-left:4px solid #C0C0C0;}
  .gold-card   {background:#1E2329;border-radius:10px;padding:16px 20px;border-left:4px solid #FFD700;}
  .status-ok   {color:#00C851;font-weight:bold;}
  .status-warn {color:#FF8800;font-weight:bold;}
  .status-err  {color:#FF4444;font-weight:bold;}
  .layer-badge-bronze {background:#CD7F32;color:white;padding:2px 10px;border-radius:12px;font-size:12px;}
  .layer-badge-silver {background:#808080;color:white;padding:2px 10px;border-radius:12px;font-size:12px;}
  .layer-badge-gold   {background:#B8860B;color:white;padding:2px 10px;border-radius:12px;font-size:12px;}
  div[data-testid="stMetricValue"] {font-size:28px !important;}
  .stTabs [data-baseweb="tab-list"] {gap: 8px;}
  .stTabs [data-baseweb="tab"] {background:#1E2329;border-radius:6px 6px 0 0;padding:8px 18px;}
  .block-container {padding-top:1.5rem;}
</style>
""", unsafe_allow_html=True)


# ── Session-state data cache ──────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading fund data…")
def load_all_data():
    funds_df     = pd.DataFrame(FUNDS)
    sub_funds_df = gen_sub_funds(FUNDS)
    sc_df        = gen_share_classes(sub_funds_df)
    nav_df       = gen_nav_data(sc_df, days=365)
    port_df      = gen_portfolio(sub_funds_df, days=60)
    tx_df        = gen_transactions(sub_funds_df, days=180)
    reg_df       = gen_registration_matrix(sc_df)
    imp_df       = gen_imports(days=90)
    dq_df        = gen_data_quality(sub_funds_df, sc_df)
    return funds_df, sub_funds_df, sc_df, nav_df, port_df, tx_df, reg_df, imp_df, dq_df

funds_df, sub_funds_df, sc_df, nav_df, port_df, tx_df, reg_df, imp_df, dq_df = load_all_data()

init_rules()
init_conflicts(sc_df, sub_funds_df, reg_df)

# ── Sidebar navigation ────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏦 FundGov360")
    st.markdown("*Data Governance Platform*")
    st.markdown("---")
    page = st.radio(
        "Navigation",
        ["🏠 Overview Dashboard",
         "🔄 Data Layers",
         "📥 Import Monitor",
         "📊 NAV & Valuations",
         "📁 Portfolio & Holdings",
         "💸 Transactions",
         "🌍 Registration Matrix",
         "📈 Analytics & Trends",
         "✅ Data Governance",
         "🔧 DQ Rule Manager",
         "⚔️ Conflict Resolver"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown("**Last Refresh**")
    st.markdown(f"`{datetime.now().strftime('%Y-%m-%d %H:%M')}`")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown("**Filters**")
    sel_funds = st.multiselect("Fund", [f["fund_name"] for f in FUNDS],
                               default=[f["fund_name"] for f in FUNDS])
    date_range = st.date_input("Date range",
                               value=(datetime(2026,1,1), datetime(2026,3,25)),
                               min_value=datetime(2025,3,25),
                               max_value=datetime(2026,3,25))

selected_fund_ids = [f["fund_id"] for f in FUNDS if f["fund_name"] in sel_funds]


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: OVERVIEW DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────
if page == "🏠 Overview Dashboard":
    st.title("🏠 Overview Dashboard")
    st.caption("Real-time view of your fund data ecosystem")

    total_aum = sub_funds_df[sub_funds_df["fund_id"].isin(selected_fund_ids)]["aum_usd"].sum()
    total_funds = len(selected_fund_ids)
    total_scs = len(sc_df[sc_df["fund_id"].isin(selected_fund_ids)])
    imp_today = imp_df[imp_df["date"] == "2026-03-25"]
    sla_ok = len(imp_today[imp_today["status"] == "On Time"])
    sla_total = len(imp_today)
    sla_rate = round(sla_ok / sla_total * 100, 1) if sla_total else 0
    missing = len(imp_today[imp_today["status"] == "Missing"])
    dq_score = round(dq_df["score"].mean() * 100, 1)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total AUM", f"${total_aum/1e9:.2f}B")
    col2.metric("Funds Monitored", total_funds)
    col3.metric("Share Classes", total_scs)
    col4.metric("SLA Compliance (Today)", f"{sla_rate}%", delta=f"{sla_rate-92:.1f}% vs target")
    col5.metric("Avg Data Quality", f"{dq_score}%")

    st.markdown("---")
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("📥 Import Status — Last 30 Days")
        imp_30 = imp_df[imp_df["date"] >= "2026-02-23"]
        status_cnt = imp_30.groupby("status").size().reset_index(name="count")
        color_map = {"On Time": "#00C851", "Late": "#FF8800", "Critical Delay": "#FF4444", "Missing": "#9E9E9E"}
        fig = px.bar(status_cnt, x="status", y="count", color="status",
                     color_discrete_map=color_map, text="count")
        fig.update_layout(showlegend=False, height=300, plot_bgcolor="rgba(0,0,0,0)",
                          paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                          margin=dict(t=20, b=20, l=10, r=10))
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("🎯 SLA Compliance Trend (90 days)")
        daily_sla = imp_df.groupby("date").apply(
            lambda x: round(len(x[x["status"]=="On Time"]) / len(x) * 100, 1)
        ).reset_index()
        daily_sla.columns = ["date", "sla_pct"]
        daily_sla = daily_sla.tail(60)
        fig2 = px.line(daily_sla, x="date", y="sla_pct")
        fig2.add_hline(y=92, line_dash="dash", line_color="orange", annotation_text="Target 92%")
        fig2.update_layout(height=300, plot_bgcolor="rgba(0,0,0,0)",
                           paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                           margin=dict(t=20, b=20, l=10, r=10))
        fig2.update_traces(fill="tozeroy", fillcolor="rgba(0,102,204,0.15)", line_color="#0066CC")
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")
    col_c, col_d = st.columns(2)

    with col_c:
        st.subheader("🏦 AUM Distribution by Fund")
        aum_data = sub_funds_df[sub_funds_df["fund_id"].isin(selected_fund_ids)].groupby("fund_id")["aum_usd"].sum().reset_index()
        aum_data["fund_name"] = aum_data["fund_id"].map({f["fund_id"]: f["fund_name"] for f in FUNDS})
        fig3 = px.pie(aum_data, values="aum_usd", names="fund_name", hole=0.45)
        fig3.update_layout(height=300, paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                           legend=dict(orientation="v", x=1.0),
                           margin=dict(t=20, b=20, l=10, r=10))
        st.plotly_chart(fig3, use_container_width=True)

    with col_d:
        st.subheader("⚠️ Today's Alerts")
        alerts = imp_today[imp_today["status"].isin(["Missing","Critical Delay","Late"])].head(10)
        if len(alerts) == 0:
            st.success("✅ No alerts today")
        else:
            for _, row in alerts.iterrows():
                icon = "🔴" if row["status"] == "Missing" else ("🟠" if row["status"] == "Critical Delay" else "🟡")
                st.markdown(f"{icon} **{row['doc_type']}** — {row['fund_name']} — *{row['status']}*"
                            + (f" (+{row['delay_hours']:.1f}h)" if row['delay_hours'] else ""))


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: DATA LAYERS
# ─────────────────────────────────────────────────────────────────────────────
elif page == "🔄 Data Layers":
    st.title("🔄 Data Layers — Bronze / Silver / Gold")
    st.caption("Medallion architecture: raw ingestion → cleansed → business-ready")

    col1, col2, col3 = st.columns(3)

    with col1:
        bronze_records = len(imp_df[imp_df["status"] != "Missing"])
        st.markdown(f"""<div class="bronze-card">
        <h3>🥉 Bronze Layer</h3>
        <b>Raw Ingestion</b><br>
        Records: <b>{bronze_records:,}</b><br>
        Sources: Fund Admins, Transfer Agents, Custodians<br>
        Format: CSV, XML, SWIFT, FTP Push<br>
        <span style="color:#CD7F32">Retention: 7 years</span>
        </div>""", unsafe_allow_html=True)

    with col2:
        silver_records = int(bronze_records * 0.97)
        st.markdown(f"""<div class="silver-card">
        <h3>🥈 Silver Layer</h3>
        <b>Validated & Enriched</b><br>
        Records: <b>{silver_records:,}</b><br>
        Rules applied: 142 validation checks<br>
        Enriched with: Reference Data, LEI, ISIN<br>
        <span style="color:#C0C0C0">Deduplication: Active</span>
        </div>""", unsafe_allow_html=True)

    with col3:
        gold_records = int(silver_records * 0.99)
        st.markdown(f"""<div class="gold-card">
        <h3>🥇 Gold Layer</h3>
        <b>Business-Ready Analytics</b><br>
        Records: <b>{gold_records:,}</b><br>
        Golden Records: NAV, AUM, Portfolio<br>
        Consumer: Dashboards, Reports, APIs<br>
        <span style="color:#FFD700">SLA: T+2h delivery</span>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("📊 Layer Pipeline Health")

    pipeline_data = {
        "Layer": ["Bronze", "Silver", "Gold"],
        "Records In": [bronze_records, bronze_records, silver_records],
        "Records Out": [bronze_records, silver_records, gold_records],
        "Pass Rate %": [100.0, round(silver_records/bronze_records*100,2), round(gold_records/silver_records*100,2)],
        "Avg Latency (min)": [2, 8, 15],
        "Last Run": ["2026-03-25 16:42", "2026-03-25 16:50", "2026-03-25 17:05"],
        "Status": ["✅ Healthy", "✅ Healthy", "✅ Healthy"],
    }
    st.dataframe(pd.DataFrame(pipeline_data), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("🔍 Data Flow by Document Type")
    doc_stats = imp_df[imp_df["status"] != "Missing"].groupby("doc_type").agg(
        records=("records_count", "sum"),
        avg_quality=("data_quality_score", "mean"),
        count=("import_id", "count")
    ).reset_index()
    doc_stats["avg_quality_pct"] = (doc_stats["avg_quality"] * 100).round(2)
    fig = px.scatter(doc_stats, x="count", y="avg_quality_pct", size="records",
                     color="doc_type", text="doc_type",
                     labels={"count":"Import Count","avg_quality_pct":"Avg Quality Score %","records":"Total Records"})
    fig.update_traces(textposition="top center")
    fig.update_layout(height=420, plot_bgcolor="rgba(0,0,0,0)",
                      paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("🏗️ Data Architecture Schema")
    st.markdown("""
    ```
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                        SOURCES (External)                               │
    │  Fund Admin │ Transfer Agent │ Custodian │ Bloomberg │ FactSet │ SWIFT  │
    └──────────────────────────┬──────────────────────────────────────────────┘
                               │ Raw Files (CSV, XML, SWIFT MT950/536)
    ┌──────────────────────────▼──────────────────────────────────────────────┐
    │                    🥉 BRONZE LAYER (Raw Ingest)                          │
    │  nav_raw │ portfolio_raw │ transaction_raw │ static_raw │ reg_raw        │
    │  • No transformation  • Timestamp + Source metadata  • Full audit trail │
    └──────────────────────────┬──────────────────────────────────────────────┘
                               │ Validation Engine (142 rules)
    ┌──────────────────────────▼──────────────────────────────────────────────┐
    │                    🥈 SILVER LAYER (Curated)                             │
    │  nav_clean │ portfolio_clean │ tx_enriched │ fund_master │ sc_master     │
    │  • ISIN/LEI enrichment  • Dedup  • DQ scoring  • Cross-validation       │
    └──────────────────────────┬──────────────────────────────────────────────┘
                               │ Business Rules & Aggregation
    ┌──────────────────────────▼──────────────────────────────────────────────┐
    │                    🥇 GOLD LAYER (Business-Ready)                        │
    │  golden_nav │ golden_portfolio │ aum_timeseries │ reg_matrix_final       │
    │  • Golden Record per Share Class  • T+2h SLA  • API / BI exposed        │
    └─────────────────────────────────────────────────────────────────────────┘
    ```
    """)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: IMPORT MONITOR
# ─────────────────────────────────────────────────────────────────────────────
elif page == "📥 Import Monitor":
    st.title("📥 Import Monitor")
    st.caption("Track every document reception — SLA compliance, delays, and missing files")

    imp_filtered = imp_df[imp_df["fund_id"].isin(selected_fund_ids)].copy()
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        imp_filtered = imp_filtered[
            (imp_filtered["date"] >= str(date_range[0])) &
            (imp_filtered["date"] <= str(date_range[1]))
        ]

    col1, col2, col3, col4 = st.columns(4)
    total = len(imp_filtered)
    on_time = len(imp_filtered[imp_filtered["status"]=="On Time"])
    late = len(imp_filtered[imp_filtered["status"]=="Late"])
    critical = len(imp_filtered[imp_filtered["status"]=="Critical Delay"])
    missing = len(imp_filtered[imp_filtered["status"]=="Missing"])
    col1.metric("Total Expected", total)
    col2.metric("On Time", on_time, f"{on_time/total*100:.1f}%")
    col3.metric("Late / Critical", f"{late + critical}", f"-{(late+critical)/total*100:.1f}%")
    col4.metric("Missing", missing, delta=f"-{missing}", delta_color="inverse")

    st.markdown("---")
    tab1, tab2, tab3, tab4 = st.tabs(["📅 Daily Heatmap", "📊 By Document Type", "📋 Detail Log", "⏱️ Delay Analysis"])

    with tab1:
        heatmap_data = imp_filtered.groupby(["date","doc_type"]).apply(
            lambda x: round(len(x[x["status"]=="On Time"])/len(x)*100, 0)
        ).reset_index()
        heatmap_data.columns = ["date","doc_type","sla_pct"]
        pivot = heatmap_data.pivot(index="doc_type", columns="date", values="sla_pct")
        fig = px.imshow(pivot, color_continuous_scale=["#FF4444","#FF8800","#FFD700","#00C851"],
                        aspect="auto", zmin=0, zmax=100,
                        labels={"color":"SLA %"})
        fig.update_layout(height=400, plot_bgcolor="rgba(0,0,0,0)",
                          paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                          margin=dict(t=20,b=50,l=10,r=10))
        fig.update_xaxes(tickangle=45, tickfont=dict(size=8))
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        by_type = imp_filtered.groupby(["doc_type","status"]).size().reset_index(name="count")
        color_map = {"On Time":"#00C851","Late":"#FF8800","Critical Delay":"#FF4444","Missing":"#9E9E9E"}
        fig2 = px.bar(by_type, x="doc_type", y="count", color="status",
                      color_discrete_map=color_map, barmode="stack")
        fig2.update_layout(height=400, plot_bgcolor="rgba(0,0,0,0)",
                           paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                           xaxis_tickangle=30)
        st.plotly_chart(fig2, use_container_width=True)

    with tab3:
        st.dataframe(
            imp_filtered.sort_values("date", ascending=False)[
                ["date","fund_name","doc_type","expected_time","received_time",
                 "delay_hours","status","records_count","data_quality_score"]
            ].style.applymap(
                lambda v: "color: #00C851" if v=="On Time" else
                          ("color: #FF8800" if v=="Late" else
                          ("color: #FF4444" if v in ["Missing","Critical Delay"] else "")),
                subset=["status"]
            ),
            use_container_width=True, height=500
        )

    with tab4:
        late_df = imp_filtered[imp_filtered["delay_hours"].notna() & (imp_filtered["delay_hours"] > 0)]
        if len(late_df) > 0:
            fig3 = px.box(late_df, x="doc_type", y="delay_hours", color="doc_type",
                          points="outliers",
                          labels={"delay_hours":"Delay (hours)","doc_type":"Document Type"})
            fig3.update_layout(height=420, plot_bgcolor="rgba(0,0,0,0)",
                               paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                               showlegend=False, xaxis_tickangle=30)
            fig3.add_hline(y=0, line_dash="dash", line_color="white", annotation_text="SLA Limit")
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No delay data available for selected filters.")


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: NAV & VALUATIONS
# ─────────────────────────────────────────────────────────────────────────────
elif page == "📊 NAV & Valuations":
    st.title("📊 NAV & Valuations")

    nav_filtered = nav_df[nav_df["fund_id"].isin(selected_fund_ids)].copy()
    nav_filtered["date"] = pd.to_datetime(nav_filtered["date"])

    col1, col2 = st.columns(2)
    sel_subfund = col1.selectbox("Select Sub-Fund",
        sorted(sub_funds_df[sub_funds_df["fund_id"].isin(selected_fund_ids)]["sub_fund_id"].unique()))
    sc_options = sc_df[sc_df["sub_fund_id"] == sel_subfund]["share_class_id"].tolist()
    sel_sc = col2.selectbox("Select Share Class", sc_options)

    nav_sc = nav_filtered[nav_filtered["share_class_id"] == sel_sc].sort_values("date")

    if len(nav_sc) > 0:
        col1, col2, col3, col4 = st.columns(4)
        latest = nav_sc.iloc[-1]
        prev = nav_sc.iloc[-2] if len(nav_sc) > 1 else latest
        ytd_start = nav_sc[nav_sc["date"] >= f"{nav_sc['date'].dt.year.max()}-01-01"].iloc[0] if len(nav_sc) > 0 else latest
        col1.metric("Latest NAV", f"{latest['nav']:.4f} {latest['currency']}")
        col2.metric("1D Change", f"{latest['nav'] - prev['nav']:+.4f}",
                    f"{(latest['nav']/prev['nav']-1)*100:+.2f}%")
        col3.metric("YTD Performance", f"{(latest['nav']/ytd_start['nav']-1)*100:+.2f}%")
        col4.metric("AUM", f"${latest['aum']/1e6:.1f}M")

        tab1, tab2, tab3 = st.tabs(["📈 NAV Evolution", "📊 AUM Evolution", "📋 Data Table"])

        with tab1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=nav_sc["date"], y=nav_sc["nav"], mode="lines",
                                      line=dict(color="#0066CC", width=2), name="NAV",
                                      fill="tozeroy", fillcolor="rgba(0,102,204,0.12)"))
            fig.update_layout(height=420, plot_bgcolor="rgba(0,0,0,0)",
                               paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                               margin=dict(t=20,b=20,l=10,r=10),
                               xaxis_title="Date", yaxis_title="NAV")
            st.plotly_chart(fig, use_container_width=True)

        with tab2:
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(x=nav_sc["date"], y=nav_sc["aum"]/1e6,
                                   marker_color="#0066CC", name="AUM (M)"))
            fig2.update_layout(height=420, plot_bgcolor="rgba(0,0,0,0)",
                                paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                                xaxis_title="Date", yaxis_title="AUM (USD M)")
            st.plotly_chart(fig2, use_container_width=True)

        with tab3:
            st.dataframe(nav_sc[["date","nav","aum","shares_outstanding","currency"]].sort_values("date", ascending=False),
                         use_container_width=True, height=450)
    else:
        st.warning("No NAV data available for this selection.")

    st.markdown("---")
    st.subheader("🏦 Total AUM by Fund (All Share Classes)")
    aum_latest = nav_df[nav_df["date"] == "2026-03-25"].groupby("fund_id")["aum"].sum().reset_index()
    aum_latest["fund_name"] = aum_latest["fund_id"].map({f["fund_id"]: f["fund_name"] for f in FUNDS})
    fig3 = px.bar(aum_latest[aum_latest["fund_id"].isin(selected_fund_ids)],
                  x="fund_name", y="aum", color="fund_name",
                  labels={"aum":"AUM (USD)","fund_name":"Fund"})
    fig3.update_layout(height=350, plot_bgcolor="rgba(0,0,0,0)",
                       paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                       showlegend=False, xaxis_tickangle=20)
    st.plotly_chart(fig3, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: PORTFOLIO & HOLDINGS
# ─────────────────────────────────────────────────────────────────────────────
elif page == "📁 Portfolio & Holdings":
    st.title("📁 Portfolio & Holdings")

    port_filtered = port_df[port_df["fund_id"].isin(selected_fund_ids)].copy()
    latest_date = port_filtered["date"].max()
    port_latest = port_filtered[port_filtered["date"] == latest_date]

    st.info(f"Showing latest portfolio snapshot: **{latest_date}**  —  "
            f"{len(port_latest):,} positions across {port_latest['sub_fund_id'].nunique()} sub-funds")

    tab1, tab2, tab3, tab4 = st.tabs(["🌍 By Geography", "🏭 By Sector", "📦 By Asset Class", "📋 Holdings Table"])

    with tab1:
        geo = port_latest.groupby("country")["market_value_usd"].sum().reset_index().sort_values("market_value_usd", ascending=False)
        fig = px.choropleth(geo, locations="country", color="market_value_usd",
                             color_continuous_scale="Blues",
                             labels={"market_value_usd":"Market Value USD"})
        fig.update_layout(height=450, paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                          geo=dict(bgcolor="rgba(0,0,0,0)", landcolor="#1E2329", showframe=False))
        st.plotly_chart(fig, use_container_width=True)
        col1, col2 = st.columns(2)
        with col1:
            top_geo = geo.head(10)
            fig2 = px.bar(top_geo, x="country", y="market_value_usd",
                          labels={"market_value_usd":"Market Value USD","country":"Country"})
            fig2.update_layout(height=300, plot_bgcolor="rgba(0,0,0,0)",
                               paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA")
            st.plotly_chart(fig2, use_container_width=True)

    with tab2:
        sec_data = port_latest.groupby("sector")["market_value_usd"].sum().reset_index()
        fig = px.pie(sec_data, values="market_value_usd", names="sector", hole=0.4)
        fig.update_layout(height=420, paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                          legend=dict(orientation="v", x=1.0))
        st.plotly_chart(fig, use_container_width=True)

    with tab3:
        ac_data = port_latest.groupby("asset_class")["market_value_usd"].sum().reset_index().sort_values("market_value_usd", ascending=True)
        fig = px.bar(ac_data, x="market_value_usd", y="asset_class", orientation="h",
                     color="asset_class",
                     labels={"market_value_usd":"Market Value (USD)","asset_class":"Asset Class"})
        fig.update_layout(height=420, plot_bgcolor="rgba(0,0,0,0)",
                          paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with tab4:
        sel_sf_port = st.selectbox("Filter by Sub-Fund",
            ["All"] + sorted(port_latest["sub_fund_id"].unique().tolist()))
        tbl = port_latest if sel_sf_port == "All" else port_latest[port_latest["sub_fund_id"] == sel_sf_port]
        st.dataframe(
            tbl[["sub_fund_id","security_name","asset_class","sector","country",
                 "quantity","price","market_value_usd","weight_pct"]].sort_values("market_value_usd", ascending=False),
            use_container_width=True, height=500
        )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: TRANSACTIONS
# ─────────────────────────────────────────────────────────────────────────────
elif page == "💸 Transactions":
    st.title("💸 Transactions")

    tx_filtered = tx_df[tx_df["fund_id"].isin(selected_fund_ids)].copy()
    tx_filtered["date"] = pd.to_datetime(tx_filtered["date"])
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        tx_filtered = tx_filtered[
            (tx_filtered["date"] >= pd.to_datetime(date_range[0])) &
            (tx_filtered["date"] <= pd.to_datetime(date_range[1]))
        ]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Transactions", f"{len(tx_filtered):,}")
    col2.metric("Total Volume", f"${tx_filtered['gross_amount'].sum()/1e9:.2f}B")
    col3.metric("Pending", len(tx_filtered[tx_filtered["status"]=="Pending"]))
    col4.metric("Failed", len(tx_filtered[tx_filtered["status"]=="Failed"]))

    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["📊 Volume by Type", "📈 Daily Activity", "📋 Transaction Log"])

    with tab1:
        vol_type = tx_filtered.groupby("tx_type")["gross_amount"].sum().reset_index().sort_values("gross_amount", ascending=False)
        fig = px.bar(vol_type, x="tx_type", y="gross_amount", color="tx_type",
                     labels={"gross_amount":"Volume (USD)","tx_type":"Transaction Type"})
        fig.update_layout(height=400, plot_bgcolor="rgba(0,0,0,0)",
                          paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        daily_tx = tx_filtered.groupby("date").agg(
            count=("tx_id","count"), volume=("gross_amount","sum")).reset_index()
        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        fig2.add_trace(go.Bar(x=daily_tx["date"], y=daily_tx["count"], name="# Transactions",
                               marker_color="rgba(0,102,204,0.7)"), secondary_y=False)
        fig2.add_trace(go.Scatter(x=daily_tx["date"], y=daily_tx["volume"]/1e6,
                                   name="Volume (M)", line=dict(color="#FFD700", width=2)),
                        secondary_y=True)
        fig2.update_layout(height=420, plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA")
        fig2.update_yaxes(title_text="# Transactions", secondary_y=False)
        fig2.update_yaxes(title_text="Volume (USD M)", secondary_y=True)
        st.plotly_chart(fig2, use_container_width=True)

    with tab3:
        status_filter = st.selectbox("Filter by Status", ["All", "Settled", "Pending", "Failed"])
        tbl = tx_filtered if status_filter == "All" else tx_filtered[tx_filtered["status"] == status_filter]
        st.dataframe(
            tbl.sort_values("date", ascending=False)[
                ["date","fund_id","sub_fund_id","tx_type","security_name",
                 "quantity","price","gross_amount","currency","status","broker"]
            ].style.applymap(
                lambda v: "color:#FF4444" if v=="Failed" else ("color:#FF8800" if v=="Pending" else ""),
                subset=["status"]
            ),
            use_container_width=True, height=500
        )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: REGISTRATION MATRIX
# ─────────────────────────────────────────────────────────────────────────────
elif page == "🌍 Registration Matrix":
    st.title("🌍 Registration Matrix")
    st.caption("Share class registration status across jurisdictions")

    reg_filtered = reg_df[reg_df["fund_id"].isin(selected_fund_ids)].copy()

    col1, col2, col3 = st.columns(3)
    total_reg = len(reg_filtered)
    registered = len(reg_filtered[reg_filtered["registered"]])
    active = len(reg_filtered[reg_filtered["status"]=="Active"])
    col1.metric("Total Entries", f"{total_reg:,}")
    col2.metric("Registered", f"{registered:,}", f"{registered/total_reg*100:.1f}%")
    col3.metric("Active Registrations", f"{active:,}")

    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["🗺️ Registration Map", "📊 By Country", "📋 Detail Table"])

    with tab1:
        country_reg = reg_filtered[reg_filtered["registered"]].groupby("country_name").size().reset_index(name="registrations")
        fig = px.choropleth(country_reg, locations="country_name", locationmode="country names",
                             color="registrations", color_continuous_scale="Blues",
                             labels={"registrations":"# Registrations"})
        fig.update_layout(height=500, paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                          geo=dict(bgcolor="rgba(0,0,0,0)", showframe=False))
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        by_country = reg_filtered.groupby(["country_name","status"]).size().reset_index(name="count")
        fig2 = px.bar(by_country, x="country_name", y="count", color="status", barmode="stack",
                      color_discrete_map={"Active":"#00C851","Under Review":"#FF8800",
                                          "Pending":"#0066CC","Not Registered":"#555555"})
        fig2.update_layout(height=420, plot_bgcolor="rgba(0,0,0,0)",
                           paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                           xaxis_tickangle=30)
        st.plotly_chart(fig2, use_container_width=True)

    with tab3:
        fund_filter = st.selectbox("Filter by Fund", ["All"] + [f["fund_name"] for f in FUNDS])
        tbl = reg_filtered
        if fund_filter != "All":
            fund_id_filter = [f["fund_id"] for f in FUNDS if f["fund_name"] == fund_filter][0]
            tbl = reg_filtered[reg_filtered["fund_id"] == fund_id_filter]
        pivot_tbl = tbl.pivot_table(index="share_class_id", columns="country_name",
                                     values="status", aggfunc="first").fillna("—")
        st.dataframe(pivot_tbl, use_container_width=True, height=500)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: ANALYTICS & TRENDS
# ─────────────────────────────────────────────────────────────────────────────
elif page == "📈 Analytics & Trends":
    st.title("📈 Analytics & Trends")
    st.caption("AI-powered insights, anomaly detection and historical trend analysis")

    tab1, tab2, tab3, tab4 = st.tabs(["🔮 Predictive Insights", "🚨 Anomaly Detection", "📊 AUM Trends", "🔁 Correlation Matrix"])

    with tab1:
        st.subheader("🔮 Predictive Insights — Next 30 Days")
        nav_pred_data = nav_df[nav_df["fund_id"].isin(selected_fund_ids)].copy()
        nav_pred_data["date"] = pd.to_datetime(nav_pred_data["date"])
        total_aum_ts = nav_pred_data.groupby("date")["aum"].sum().reset_index()
        total_aum_ts = total_aum_ts.sort_values("date")
        last_30 = total_aum_ts.tail(30)
        avg_daily_change = last_30["aum"].pct_change().mean()
        std_daily = last_30["aum"].pct_change().std()
        last_aum = total_aum_ts.iloc[-1]["aum"]
        last_date = total_aum_ts.iloc[-1]["date"]
        future_dates = [last_date + timedelta(days=i) for i in range(1, 31)]
        np.random.seed(123)
        future_aum = [last_aum]
        for _ in range(30):
            future_aum.append(future_aum[-1] * (1 + np.random.normal(avg_daily_change, std_daily)))
        future_aum = future_aum[1:]
        upper = [v * 1.02 for v in future_aum]
        lower = [v * 0.98 for v in future_aum]
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=total_aum_ts["date"].tolist(), y=total_aum_ts["aum"].tolist()/1e9,
                                  mode="lines", name="Historical AUM", line=dict(color="#0066CC", width=2),
                                  fill="tozeroy", fillcolor="rgba(0,102,204,0.1)"))
        fig.add_trace(go.Scatter(x=future_dates, y=[v/1e9 for v in future_aum],
                                  mode="lines", name="Forecast", line=dict(color="#FFD700", width=2, dash="dash")))
        fig.add_trace(go.Scatter(x=future_dates + future_dates[::-1],
                                  y=[v/1e9 for v in upper] + [v/1e9 for v in lower][::-1],
                                  fill="toself", fillcolor="rgba(255,215,0,0.1)",
                                  line=dict(color="rgba(0,0,0,0)"), name="95% CI"))
        fig.update_layout(height=450, plot_bgcolor="rgba(0,0,0,0)",
                           paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                           xaxis_title="Date", yaxis_title="Total AUM (B USD)",
                           legend=dict(orientation="h", y=1.05, x=0.3))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("**📌 Key Insights (auto-generated)**")
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"📈 **AUM Trend**: Average daily growth of {avg_daily_change*100:+.3f}% over last 30 days")
            st.info(f"🎯 **30-Day Forecast**: AUM expected to reach **${future_aum[-1]/1e9:.2f}B** (±2% CI)")
        with col2:
            st.info(f"⚡ **Volatility**: Daily AUM std dev = {std_daily*100:.3f}%")
            st.warning(f"⚠️ **SLA Risk**: {len(imp_df[imp_df['status']=='Late'])/(len(imp_df))*100:.1f}% of imports breached SLA historically")

    with tab2:
        st.subheader("🚨 NAV Anomaly Detection (Z-Score > 3σ)")
        nav_anom = nav_df[nav_df["fund_id"].isin(selected_fund_ids)].copy()
        nav_anom["date"] = pd.to_datetime(nav_anom["date"])
        nav_anom_sc = nav_anom.groupby("share_class_id").apply(
            lambda x: x.sort_values("date").assign(
                daily_ret=x.sort_values("date")["nav"].pct_change(),
            )
        ).reset_index(drop=True)
        nav_anom_sc["zscore"] = nav_anom_sc.groupby("share_class_id")["daily_ret"].transform(
            lambda x: (x - x.mean()) / x.std()
        )
        anomalies = nav_anom_sc[nav_anom_sc["zscore"].abs() > 3].dropna()
        st.markdown(f"**{len(anomalies)} anomalous NAV movements detected** (Z-Score > 3σ)")
        if len(anomalies) > 0:
            sample_sc = anomalies["share_class_id"].value_counts().index[0]
            sc_data = nav_anom_sc[nav_anom_sc["share_class_id"] == sample_sc].sort_values("date")
            sc_anom = anomalies[anomalies["share_class_id"] == sample_sc]
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=sc_data["date"], y=sc_data["nav"], mode="lines",
                                      name="NAV", line=dict(color="#0066CC")))
            fig.add_trace(go.Scatter(x=sc_anom["date"], y=sc_anom["nav"], mode="markers",
                                      marker=dict(color="#FF4444", size=10, symbol="x"),
                                      name="Anomaly (3σ)"))
            fig.update_layout(height=400, plot_bgcolor="rgba(0,0,0,0)",
                               paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                               title=f"NAV Anomalies — {sample_sc}")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(anomalies[["date","share_class_id","fund_id","nav","daily_ret","zscore"]].sort_values("zscore", key=abs, ascending=False).head(20),
                         use_container_width=True)

    with tab3:
        st.subheader("📊 AUM Trend by Fund")
        aum_trend = nav_df[nav_df["fund_id"].isin(selected_fund_ids)].copy()
        aum_trend["date"] = pd.to_datetime(aum_trend["date"])
        aum_trend_daily = aum_trend.groupby(["date","fund_id"])["aum"].sum().reset_index()
        aum_trend_daily["fund_name"] = aum_trend_daily["fund_id"].map({f["fund_id"]: f["fund_name"] for f in FUNDS})
        fig = px.area(aum_trend_daily, x="date", y="aum", color="fund_name",
                       labels={"aum":"AUM (USD)","date":"Date","fund_name":"Fund"})
        fig.update_layout(height=450, plot_bgcolor="rgba(0,0,0,0)",
                           paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                           legend=dict(orientation="h", y=1.08, x=0))
        st.plotly_chart(fig, use_container_width=True)

    with tab4:
        st.subheader("🔁 Import SLA Correlation by Document Type × Fund")
        sla_pivot = imp_df.groupby(["fund_name","doc_type"]).apply(
            lambda x: round(len(x[x["status"]=="On Time"])/len(x)*100,1)
        ).reset_index()
        sla_pivot.columns = ["fund_name","doc_type","sla_pct"]
        corr_pivot = sla_pivot.pivot(index="fund_name", columns="doc_type", values="sla_pct")
        fig = px.imshow(corr_pivot, color_continuous_scale="RdYlGn", zmin=70, zmax=100,
                        text_auto=True, aspect="auto", labels={"color":"SLA %"})
        fig.update_layout(height=380, plot_bgcolor="rgba(0,0,0,0)",
                           paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA")
        st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: DATA GOVERNANCE
# ─────────────────────────────────────────────────────────────────────────────
elif page == "✅ Data Governance":
    st.title("✅ Data Governance")
    st.caption("Data quality scoring, business rules, lineage and ownership")

    tab1, tab2, tab3, tab4 = st.tabs(["🏅 Quality Scores", "📏 Business Rules", "🔗 Data Lineage", "👥 Stewardship"])

    with tab1:
        st.subheader("Data Quality Scores by Entity & Dimension")
        dq_pivot = dq_df.pivot_table(index="entity", columns="dimension", values="score").round(3)
        avg_scores = dq_df.groupby("dimension")["score"].mean().reset_index()
        avg_scores["score_pct"] = (avg_scores["score"] * 100).round(1)
        col1, col2 = st.columns([2,1])
        with col1:
            fig = px.imshow(dq_pivot * 100, color_continuous_scale="RdYlGn",
                             zmin=75, zmax=100, text_auto=True, aspect="auto",
                             labels={"color":"Quality %"})
            fig.update_layout(height=600, plot_bgcolor="rgba(0,0,0,0)",
                               paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                               margin=dict(t=20,b=20,l=10,r=10))
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.markdown("**Avg Score by Dimension**")
            for _, row in avg_scores.iterrows():
                color = "#00C851" if row["score_pct"] >= 95 else ("#FF8800" if row["score_pct"] >= 90 else "#FF4444")
                st.markdown(f"<span style='color:{color}'>●</span> **{row['dimension']}**: {row['score_pct']}%", unsafe_allow_html=True)

    with tab2:
        st.subheader("📏 Active Business Rules")
        rules = pd.DataFrame([
            {"Rule ID":"BR-001","Domain":"NAV","Description":"NAV must be positive and non-zero","Severity":"Critical","Entities Affected":f"{len(sc_df)}","Pass Rate":"99.2%","Layer":"Silver"},
            {"Rule ID":"BR-002","Domain":"NAV","Description":"NAV date must be business day","Severity":"High","Entities Affected":f"{len(sc_df)}","Pass Rate":"100%","Layer":"Bronze"},
            {"Rule ID":"BR-003","Domain":"Portfolio","Description":"Portfolio weights must sum to 100% ±0.01%","Severity":"Critical","Entities Affected":f"{len(sub_funds_df)}","Pass Rate":"97.8%","Layer":"Silver"},
            {"Rule ID":"BR-004","Domain":"Transaction","Description":"Transaction gross amount = quantity × price","Severity":"High","Entities Affected":"All TX","Pass Rate":"99.9%","Layer":"Bronze"},
            {"Rule ID":"BR-005","Domain":"Static Data","Description":"ISIN must be 12 chars and pass checksum","Severity":"Critical","Entities Affected":f"{len(sc_df)}","Pass Rate":"100%","Layer":"Bronze"},
            {"Rule ID":"BR-006","Domain":"Registration","Description":"Expiry date must be > registration date","Severity":"Medium","Entities Affected":f"{len(reg_df[reg_df['registered']])}","Pass Rate":"98.5%","Layer":"Silver"},
            {"Rule ID":"BR-007","Domain":"NAV","Description":"NAV daily change must be within ±15%","Severity":"High","Entities Affected":f"{len(sc_df)}","Pass Rate":"99.7%","Layer":"Silver"},
            {"Rule ID":"BR-008","Domain":"AUM","Description":"AUM must reconcile with custodian ±0.1%","Severity":"Critical","Entities Affected":f"{len(sub_funds_df)}","Pass Rate":"95.3%","Layer":"Gold"},
            {"Rule ID":"BR-009","Domain":"Fund Static","Description":"Fund currency must match sub-fund currency","Severity":"Low","Entities Affected":f"{len(funds_df)}","Pass Rate":"100%","Layer":"Silver"},
            {"Rule ID":"BR-010","Domain":"Transaction","Description":"No duplicate TX_ID within same fund","Severity":"Critical","Entities Affected":"All TX","Pass Rate":"100%","Layer":"Bronze"},
        ])
        st.dataframe(rules.style.applymap(
            lambda v: "color:#FF4444" if v=="Critical" else ("color:#FF8800" if v=="High" else ("color:#FFD700" if v=="Medium" else "")),
            subset=["Severity"]
        ), use_container_width=True, hide_index=True)

    with tab3:
        st.subheader("🔗 Data Lineage")
        st.markdown("""
        ```
        [Fund Admin System]  →  CSV/XML Upload  →  [Bronze: nav_raw]
                                                        │
                                              Validation & Enrichment
                                                        │
                                              [Silver: nav_clean]
                                            (ISIN enriched + DQ scored)
                                                        │
                                    ┌───────────────────┤
                                    │                   │
                              [Gold: golden_nav]  [Gold: aum_timeseries]
                               (per share class)   (per sub-fund)
                                    │                   │
                             BI Dashboard         API Consumers
                             (FundGov360)        (Client Reports)

        [Custodian] → SWIFT MT950 → [Bronze: portfolio_raw]
                                          │
                                  ISIN + Sector + Country enrichment
                                          │
                               [Silver: portfolio_clean]
                                          │
                              [Gold: golden_portfolio]
                               (weighted positions + risk metrics)
                                          │
                              Risk Reports / Compliance Reports
        ```
        """)

    with tab4:
        st.subheader("👥 Data Stewardship")
        stewards = pd.DataFrame([
            {"Domain":"NAV & Valuations","Data Steward":"Marie Dupont","Team":"Fund Accounting","Escalation":"Head of Valuations","Review Cycle":"Daily","Status":"🟢 Active"},
            {"Domain":"Portfolio","Data Steward":"Thomas Müller","Team":"Portfolio Management","Escalation":"CIO Office","Review Cycle":"Daily","Status":"🟢 Active"},
            {"Domain":"Transactions","Data Steward":"Sophie Martin","Team":"Operations","Escalation":"COO Office","Review Cycle":"Daily","Status":"🟢 Active"},
            {"Domain":"Registration Matrix","Data Steward":"Luca Rossi","Team":"Legal & Compliance","Escalation":"CCO Office","Review Cycle":"Monthly","Status":"🟢 Active"},
            {"Domain":"Fund Static Data","Data Steward":"Emma Johnson","Team":"Data Management","Escalation":"CDO Office","Review Cycle":"Weekly","Status":"🟢 Active"},
            {"Domain":"Reference Data","Data Steward":"Pierre Bernard","Team":"Data Management","Escalation":"CDO Office","Review Cycle":"Weekly","Status":"🟡 Under Review"},
        ])
        st.dataframe(stewards, use_container_width=True, hide_index=True)
        st.markdown("---")
        st.info("**Data Governance Framework**: This platform follows the DAMA-DMBOK2 framework. "
                "All data domains are assigned a Data Owner (executive accountability) and a Data Steward (operational responsibility). "
                "DQ issues above threshold trigger automatic JIRA tickets routed to the responsible steward.")


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: DQ RULE MANAGER
# ─────────────────────────────────────────────────────────────────────────────
elif page == "🔧 DQ Rule Manager":
    st.title("🔧 DQ Rule Manager")
    st.caption("Create, manage and test data quality rules across all fund data domains")

    rules_df = st.session_state["dq_rules"]
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Rule Catalogue", "➕ Add New Rule", "🧪 Test a Rule", "📊 Execution History"])

    # ── TAB 1: RULE CATALOGUE ─────────────────────────────────────────────────
    with tab1:
        col1, col2, col3, col4 = st.columns(4)
        total_rules = len(rules_df)
        active_rules = len(rules_df[rules_df["active"] == True])
        critical_rules = len(rules_df[rules_df["severity"] == "Critical"])
        domains_covered = rules_df["domain"].nunique()
        col1.metric("Total Rules", total_rules)
        col2.metric("Active", active_rules)
        col3.metric("Critical", critical_rules)
        col4.metric("Domains Covered", domains_covered)

        st.markdown("---")
        # Filters
        fc1, fc2, fc3 = st.columns(3)
        flt_domain   = fc1.selectbox("Filter by Domain",   ["All"] + sorted(rules_df["domain"].unique().tolist()))
        flt_severity = fc2.selectbox("Filter by Severity", ["All"] + SEVERITIES)
        flt_active   = fc3.selectbox("Active only", ["All", "Active", "Inactive"])

        disp = rules_df.copy()
        if flt_domain   != "All": disp = disp[disp["domain"]   == flt_domain]
        if flt_severity != "All": disp = disp[disp["severity"] == flt_severity]
        if flt_active   == "Active":   disp = disp[disp["active"] == True]
        if flt_active   == "Inactive": disp = disp[disp["active"] == False]

        def sev_color(v):
            return ("color:#FF4444" if v=="Critical" else
                    "color:#FF8800" if v=="High" else
                    "color:#FFD700" if v=="Medium" else "")

        st.dataframe(
            disp[["rule_id","rule_name","domain","entity_type","attribute","rule_type",
                  "severity","action","active","created_by","created_at","tags"]]
            .style.applymap(sev_color, subset=["severity"]),
            use_container_width=True, height=480, hide_index=True
        )

        st.markdown("---")
        st.subheader("🗑️ Toggle / Delete Rule")
        col_a, col_b = st.columns(2)
        sel_toggle = col_a.selectbox("Select Rule to Toggle Active/Inactive", rules_df["rule_id"].tolist())
        if col_a.button("🔁 Toggle Status"):
            idx = st.session_state["dq_rules"].index[st.session_state["dq_rules"]["rule_id"] == sel_toggle][0]
            st.session_state["dq_rules"].at[idx, "active"] = not st.session_state["dq_rules"].at[idx, "active"]
            new_status = "Active" if st.session_state["dq_rules"].at[idx, "active"] else "Inactive"
            st.success(f"✅ Rule {sel_toggle} is now **{new_status}**")
            st.rerun()

        sel_delete = col_b.selectbox("Select Rule to Delete", rules_df["rule_id"].tolist(), key="del_rule")
        if col_b.button("🗑️ Delete Rule", type="secondary"):
            st.session_state["dq_rules"] = st.session_state["dq_rules"][
                st.session_state["dq_rules"]["rule_id"] != sel_delete
            ].reset_index(drop=True)
            st.success(f"✅ Rule {sel_delete} deleted.")
            st.rerun()

    # ── TAB 2: ADD NEW RULE ───────────────────────────────────────────────────
    with tab2:
        st.subheader("➕ Create a New DQ Rule")
        with st.form("new_rule_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            rule_name = c1.text_input("Rule Name *", placeholder="e.g. NAV must be positive")
            domain    = c2.selectbox("Domain *", DOMAINS)

            c3, c4 = st.columns(2)
            entity_type = c3.selectbox("Entity Type *", ENTITY_TYPES)
            attribute   = c4.text_input("Attribute *", placeholder="e.g. nav, isin, currency, status")

            c5, c6 = st.columns(2)
            rule_type   = c5.selectbox("Rule Type *", RULE_TYPES)
            severity    = c6.selectbox("Severity *", SEVERITIES)

            expression = st.text_area("Rule Expression *",
                placeholder="e.g.  nav > 0  |  ^[A-Z]{2}[A-Z0-9]{10}$  |  abs(sum(weight_pct) - 100) <= 0.01",
                height=80)

            c7, c8, c9 = st.columns(3)
            action     = c7.selectbox("Action on Failure", ACTIONS)
            active_new = c8.checkbox("Active", value=True)
            tags_new   = c9.text_input("Tags (comma-separated)", placeholder="nav,valuation")

            description_new = st.text_area("Description / Business Justification",
                placeholder="Explain why this rule exists and what it protects against", height=80)

            c_user, _ = st.columns(2)
            created_by_new = c_user.text_input("Created By", value="admin")

            submitted = st.form_submit_button("💾 Save Rule", type="primary", use_container_width=True)

        if submitted:
            if not rule_name or not attribute or not expression:
                st.error("❌ Please fill in Rule Name, Attribute, and Expression.")
            else:
                new_rule = {
                    "rule_id":      get_next_rule_id(),
                    "rule_name":    rule_name,
                    "domain":       domain,
                    "entity_type":  entity_type,
                    "attribute":    attribute,
                    "rule_type":    rule_type,
                    "expression":   expression,
                    "severity":     severity,
                    "action":       action,
                    "active":       active_new,
                    "created_by":   created_by_new,
                    "created_at":   datetime.now().strftime("%Y-%m-%d"),
                    "tags":         tags_new,
                }
                new_row = pd.DataFrame([new_rule])
                st.session_state["dq_rules"] = pd.concat(
                    [st.session_state["dq_rules"], new_row], ignore_index=True
                )
                st.success(f"✅ Rule **{new_rule['rule_id']} — {rule_name}** created successfully!")
                st.rerun()

        st.markdown("---")
        st.markdown("**📖 Rule Type Reference**")
        ref_data = {
            "Rule Type": RULE_TYPES,
            "Description": [
                "Field must not be null/empty",
                "Value must be within min/max bounds",
                "Value must match regular expression pattern",
                "Derived field must equal formula result",
                "First date must be before second date",
                "Date must be within allowed range",
                "Sum/count across records must meet condition",
                "Value must be unique within scope",
                "Value must exist in reference list",
                "Two values from different sources must match",
                "Custom Python/SQL expression",
            ]
        }
        st.dataframe(pd.DataFrame(ref_data), use_container_width=True, hide_index=True)

    # ── TAB 3: TEST A RULE ────────────────────────────────────────────────────
    with tab3:
        st.subheader("🧪 Test Rule Against Live Data")
        active_rules_list = rules_df[rules_df["active"] == True]
        sel_rule_id = st.selectbox(
            "Select Rule to Test",
            active_rules_list["rule_id"].tolist(),
            format_func=lambda rid: f"{rid} — {rules_df[rules_df['rule_id']==rid]['rule_name'].values[0]}"
        )
        sel_rule = rules_df[rules_df["rule_id"] == sel_rule_id].iloc[0].to_dict()

        st.markdown(f"""
        | Field | Value |
        |---|---|
        | **Domain** | {sel_rule['domain']} |
        | **Entity Type** | {sel_rule['entity_type']} |
        | **Attribute** | `{sel_rule['attribute']}` |
        | **Expression** | `{sel_rule['expression']}` |
        | **Severity** | {sel_rule['severity']} |
        | **Action on Failure** | {sel_rule['action']} |
        """)

        if st.button("▶️ Run Rule Test", type="primary"):
            with st.spinner("Running rule against data…"):
                fails_df, stats = test_rule_against_data(sel_rule, nav_df, sc_df, tx_df, port_df)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Records Tested", stats["total"])
            c2.metric("Passed", stats["passed"])
            c3.metric("Failed", stats["failed"])
            c4.metric("Pass Rate", f"{stats['pass_rate']}%",
                      delta=f"{stats['pass_rate']-99:.2f}% vs 99% target",
                      delta_color="normal" if stats['pass_rate'] >= 99 else "inverse")

            # Log execution
            st.session_state["rule_exec_log"].append({
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "rule_id": sel_rule_id,
                "rule_name": sel_rule["rule_name"],
                "records_tested": stats["total"],
                "passed": stats["passed"],
                "failed": stats["failed"],
                "pass_rate": stats["pass_rate"],
            })

            if stats["failed"] > 0 and len(fails_df) > 0:
                st.markdown(f"**⚠️ {stats['failed']} violations found:**")
                st.dataframe(fails_df, use_container_width=True, height=350)
            elif stats["failed"] == 0:
                st.success("✅ All records passed this rule!")

    # ── TAB 4: EXECUTION HISTORY ──────────────────────────────────────────────
    with tab4:
        st.subheader("📊 Rule Execution History")
        if len(st.session_state["rule_exec_log"]) == 0:
            st.info("No rule executions yet. Go to the **Test a Rule** tab to run your first test.")
        else:
            log_df = pd.DataFrame(st.session_state["rule_exec_log"])
            c1, c2, c3 = st.columns(3)
            c1.metric("Tests Run", len(log_df))
            c2.metric("Avg Pass Rate", f"{log_df['pass_rate'].mean():.2f}%")
            c3.metric("Total Violations", log_df["failed"].sum())
            st.dataframe(log_df.sort_values("timestamp", ascending=False),
                         use_container_width=True, height=400, hide_index=True)
            if st.button("🗑️ Clear History"):
                st.session_state["rule_exec_log"] = []
                st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: CONFLICT RESOLVER
# ─────────────────────────────────────────────────────────────────────────────
elif page == "⚔️ Conflict Resolver":
    st.title("⚔️ Data Conflict Resolver")
    st.caption("Detect, investigate and resolve attribute conflicts across multiple data sources — feeds the Golden Record")

    conflicts = st.session_state["conflicts"]
    open_conflicts = conflicts[conflicts["status"] == "Open"]
    resolved = conflicts[conflicts["status"] == "Resolved"]

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Conflicts", len(conflicts))
    col2.metric("Open", len(open_conflicts),
                delta=f"{len(open_conflicts[open_conflicts['severity']=='Critical'])} Critical",
                delta_color="inverse")
    col3.metric("In Review", len(conflicts[conflicts["status"]=="In Review"]))
    col4.metric("Resolved", len(resolved))
    col5.metric("Resolution Rate", f"{len(resolved)/len(conflicts)*100:.0f}%")

    st.markdown("---")
    tab1, tab2, tab3, tab4 = st.tabs(["🚨 Open Conflicts", "🔍 Resolve a Conflict", "🏆 Golden Record View", "📜 Resolution Log"])

    # ── TAB 1: OPEN CONFLICTS ─────────────────────────────────────────────────
    with tab1:
        fc1, fc2, fc3 = st.columns(3)
        flt_sev  = fc1.selectbox("Severity",     ["All","Critical","High","Medium","Low"], key="cf_sev")
        flt_type = fc2.selectbox("Entity Type",  ["All","ShareClass","SubFund","Registration","NAV","Transaction"], key="cf_type")
        flt_ctype= fc3.selectbox("Conflict Type",["All"] + sorted(conflicts["conflict_type"].unique().tolist()), key="cf_ctype")

        disp_cf = conflicts[conflicts["status"].isin(["Open","In Review"])].copy()
        if flt_sev   != "All": disp_cf = disp_cf[disp_cf["severity"]      == flt_sev]
        if flt_type  != "All": disp_cf = disp_cf[disp_cf["entity_type"]   == flt_type]
        if flt_ctype != "All": disp_cf = disp_cf[disp_cf["conflict_type"] == flt_ctype]

        def severity_badge(row):
            sev_colors = {"Critical":"#FF4444","High":"#FF8800","Medium":"#FFD700","Low":"#00C851"}
            c = sev_colors.get(row["severity"],"#9E9E9E")
            return [f"color:{c}" if col == "severity" else "" for col in row.index]

        st.dataframe(
            disp_cf[["conflict_id","entity_type","entity_id","attribute","conflict_type",
                      "source_a","value_a","source_b","value_b","severity","detected_at","status"]]
            .style.apply(severity_badge, axis=1),
            use_container_width=True, height=480, hide_index=True
        )

        # Visual: conflicts by type
        st.markdown("---")
        col_a, col_b = st.columns(2)
        with col_a:
            by_severity = conflicts.groupby("severity").size().reset_index(name="count")
            color_map_sev = {"Critical":"#FF4444","High":"#FF8800","Medium":"#FFD700","Low":"#00C851"}
            fig = px.pie(by_severity, values="count", names="severity", hole=0.4,
                         color="severity", color_discrete_map=color_map_sev,
                         title="By Severity")
            fig.update_layout(height=300, paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                               margin=dict(t=40,b=10,l=10,r=10))
            st.plotly_chart(fig, use_container_width=True)
        with col_b:
            by_entity = conflicts.groupby("entity_type").size().reset_index(name="count")
            fig2 = px.bar(by_entity, x="entity_type", y="count", color="entity_type",
                          title="By Entity Type")
            fig2.update_layout(height=300, plot_bgcolor="rgba(0,0,0,0)",
                                paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                                showlegend=False, margin=dict(t=40,b=10,l=10,r=10))
            st.plotly_chart(fig2, use_container_width=True)

    # ── TAB 2: RESOLVE A CONFLICT ─────────────────────────────────────────────
    with tab2:
        st.subheader("🔍 Conflict Investigation & Resolution")
        open_ids = conflicts[conflicts["status"].isin(["Open","In Review"])]["conflict_id"].tolist()
        if len(open_ids) == 0:
            st.success("🎉 All conflicts have been resolved!")
        else:
            sel_conf_id = st.selectbox(
                "Select Conflict to Resolve",
                open_ids,
                format_func=lambda cid: f"{cid} — {conflicts[conflicts['conflict_id']==cid]['entity_type'].values[0]} · {conflicts[conflicts['conflict_id']==cid]['attribute'].values[0]} · {conflicts[conflicts['conflict_id']==cid]['severity'].values[0]}"
            )
            conf = conflicts[conflicts["conflict_id"] == sel_conf_id].iloc[0]

            # Conflict card
            sev_color = {"Critical":"#FF4444","High":"#FF8800","Medium":"#FFD700","Low":"#00C851"}
            sc_hex = sev_color.get(conf["severity"], "#9E9E9E")
            st.markdown(f"""
            <div style="background:#1E2329;border-radius:10px;padding:20px;border-left:5px solid {sc_hex};margin-bottom:16px">
            <h4>⚔️ {conf['conflict_id']} — <span style="color:{sc_hex}">{conf['severity']}</span> | {conf['conflict_type']}</h4>
            <p style="color:#aaa">{conf['description']}</p>
            <b>Entity:</b> {conf['entity_type']} &nbsp;|&nbsp; <b>ID:</b> <code>{conf['entity_id']}</code> &nbsp;|&nbsp;
            <b>Attribute:</b> <code>{conf['attribute']}</code> &nbsp;|&nbsp; <b>Detected:</b> {conf['detected_at']}
            </div>
            """, unsafe_allow_html=True)

            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(f"""
                <div style="background:#1E2329;border-radius:8px;padding:14px;border:2px solid #0066CC">
                <b>🅐 {conf['source_a']}</b><br>
                <span style="font-size:24px;color:#0066CC"><b>{conf['value_a']}</b></span>
                </div>""", unsafe_allow_html=True)
            with col_b:
                st.markdown(f"""
                <div style="background:#1E2329;border-radius:8px;padding:14px;border:2px solid #FF8800">
                <b>🅑 {conf['source_b']}</b><br>
                <span style="font-size:24px;color:#FF8800"><b>{conf['value_b']}</b></span>
                </div>""", unsafe_allow_html=True)

            # Trusted source suggestion
            domain_key = ("NAV" if conf["entity_type"]=="NAV" else
                          "Registration" if conf["entity_type"]=="Registration" else
                          "Static Data")
            trusted_order = TRUSTED_SOURCE_HIERARCHY.get(domain_key, [])
            if conf["source_a"] in trusted_order and conf["source_b"] in trusted_order:
                a_rank = trusted_order.index(conf["source_a"])
                b_rank = trusted_order.index(conf["source_b"])
                suggestion = conf["source_a"] if a_rank < b_rank else conf["source_b"]
                st.info(f"💡 **Suggested**: Accept **{suggestion}** — ranked #{min(a_rank,b_rank)+1} in trusted source hierarchy for {domain_key} data")
            elif conf["source_a"] in trusted_order:
                st.info(f"💡 **Suggested**: Accept **{conf['source_a']}** — ranked in trusted source hierarchy for {domain_key}")

            st.markdown("---")
            with st.form(f"resolve_form_{sel_conf_id}"):
                c1, c2 = st.columns(2)
                strategy = c1.selectbox("Resolution Strategy", RESOLUTION_STRATEGIES)
                resolved_by = c2.text_input("Your Name / Initials", value="admin")
                manual_value = st.text_input(
                    "Override Value (only if 'Manual Override')",
                    placeholder="Type the correct value to use as the Golden Record value"
                )
                comment = st.text_area("Comment / Justification", placeholder="Why did you choose this resolution?", height=80)
                submit_res = st.form_submit_button("✅ Resolve Conflict", type="primary", use_container_width=True)

            if submit_res:
                if strategy == "Accept Source A":
                    final_value = conf["value_a"]
                elif strategy == "Accept Source B":
                    final_value = conf["value_b"]
                elif strategy == "Manual Override":
                    final_value = manual_value if manual_value else conf["value_a"]
                elif strategy == "Mark as Equivalent":
                    final_value = conf["value_a"]
                elif strategy == "Escalate to Data Owner":
                    final_value = None
                else:
                    final_value = conf["value_a"]

                idx = st.session_state["conflicts"].index[
                    st.session_state["conflicts"]["conflict_id"] == sel_conf_id
                ][0]
                new_status = "Escalated" if strategy == "Escalate to Data Owner" else "Resolved"
                st.session_state["conflicts"].at[idx, "status"]              = new_status
                st.session_state["conflicts"].at[idx, "resolved_value"]      = final_value
                st.session_state["conflicts"].at[idx, "resolution_strategy"] = strategy
                st.session_state["conflicts"].at[idx, "resolved_by"]         = resolved_by
                st.session_state["conflicts"].at[idx, "resolved_at"]         = datetime.now().strftime("%Y-%m-%d %H:%M")
                st.session_state["conflicts"].at[idx, "comment"]             = comment

                st.session_state["resolution_log"].append({
                    "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "conflict_id": sel_conf_id,
                    "entity_type": conf["entity_type"],
                    "entity_id":   conf["entity_id"],
                    "attribute":   conf["attribute"],
                    "strategy":    strategy,
                    "final_value": final_value,
                    "resolved_by": resolved_by,
                    "comment":     comment,
                    "status":      new_status,
                })

                if new_status == "Resolved":
                    st.success(f"✅ Conflict **{sel_conf_id}** resolved → Golden Record value: **{final_value}**")
                else:
                    st.warning(f"⚠️ Conflict **{sel_conf_id}** escalated to Data Owner.")
                st.rerun()

    # ── TAB 3: GOLDEN RECORD VIEW ─────────────────────────────────────────────
    with tab3:
        st.subheader("🏆 Golden Record — Resolved Attributes")
        st.caption("Attributes whose conflicts have been resolved and are now canonical values in the Gold layer")

        resolved_cf = conflicts[conflicts["status"] == "Resolved"].copy()
        if len(resolved_cf) == 0:
            st.info("No resolved conflicts yet. Resolve conflicts in the **Resolve a Conflict** tab.")
        else:
            gr_view = resolved_cf[["entity_type","entity_id","attribute","resolved_value",
                                    "resolution_strategy","resolved_by","resolved_at","comment"]].copy()
            gr_view.columns = ["Entity Type","Entity ID","Attribute","Golden Value",
                                "Strategy","Resolved By","Resolved At","Comment"]
            st.dataframe(gr_view, use_container_width=True, height=500, hide_index=True)

            st.markdown("---")
            # By entity type breakdown
            entity_counts = resolved_cf.groupby("entity_type").size().reset_index(name="resolved")
            st.subheader("Resolved Conflicts by Entity Type")
            fig = px.bar(entity_counts, x="entity_type", y="resolved", color="entity_type",
                         labels={"resolved":"Resolved Count","entity_type":"Entity Type"})
            fig.update_layout(height=300, plot_bgcolor="rgba(0,0,0,0)",
                               paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
                               showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # ── TAB 4: RESOLUTION LOG ─────────────────────────────────────────────────
    with tab4:
        st.subheader("📜 Full Resolution Audit Log")
        st.caption("Immutable audit trail of all conflict resolution decisions")
        if len(st.session_state["resolution_log"]) == 0:
            st.info("No resolutions recorded yet.")
        else:
            log_df = pd.DataFrame(st.session_state["resolution_log"])
            st.dataframe(log_df.sort_values("timestamp", ascending=False),
                         use_container_width=True, height=500, hide_index=True)
            csv = log_df.to_csv(index=False)
            st.download_button("📥 Export Audit Log (CSV)", data=csv,
                               file_name=f"resolution_log_{datetime.now().strftime('%Y%m%d')}.csv",
                               mime="text/csv")
