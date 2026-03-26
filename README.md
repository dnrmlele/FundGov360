# 🏦 FundGov360 — Fund Data Governance Platform

> A data governance & monitoring platform for fund data (NAV, Portfolio, Transactions, Registration Matrix, Static Data) following the **Bronze / Silver / Gold** medallion architecture.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://your-app.streamlit.app)

---

## 📌 Features

| Module | Description |
|---|---|
| 🏠 **Overview Dashboard** | KPIs, SLA compliance, AUM overview, real-time alerts |
| 🔄 **Data Layers** | Bronze / Silver / Gold pipeline health & architecture |
| 📥 **Import Monitor** | SLA tracking, delay analysis, heatmap, document logs |
| 📊 **NAV & Valuations** | NAV evolution, AUM trends, per share class |
| 📁 **Portfolio & Holdings** | Geography, sector, asset class breakdowns |
| 💸 **Transactions** | Volume by type, daily activity, settlement status |
| 🌍 **Registration Matrix** | Jurisdiction registration status & choropleth map |
| 📈 **Analytics & Trends** | AUM forecast, anomaly detection (Z-score), correlations |
| ✅ **Data Governance** | DQ heatmap, business rules engine, data lineage, stewardship |

---

## 🏗️ Architecture

```
Sources (Fund Admin, Custodian, TA, Bloomberg)
      ↓
🥉 Bronze Layer  — Raw ingestion, no transformation, full audit trail
      ↓
🥈 Silver Layer  — Validated, enriched (ISIN/LEI/Sector), DQ scored
      ↓
🥇 Gold Layer    — Golden Records, business-ready, API/BI exposed
```

---

## 🚀 Quick Start

### Local
```bash
git clone https://github.com/YOUR_USERNAME/fund-governance-platform.git
cd fund-governance-platform
pip install -r requirements.txt
streamlit run app.py
```

### Streamlit Cloud
1. Fork this repo
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Click **New App** → connect your repo → set `app.py` as main file
4. Deploy 🎉

---

## 📂 Project Structure

```
fund_governance_platform/
├── app.py                    # Main Streamlit application
├── requirements.txt          # Python dependencies
├── README.md
├── .streamlit/
│   └── config.toml           # Dark theme + branding
└── utils/
    └── data_generator.py     # Synthetic fund data generation
```

---

## 🔧 Tech Stack

- **Streamlit** — UI framework
- **Pandas / NumPy** — Data processing
- **Plotly** — Interactive visualizations
- **Faker / SciPy** — Synthetic data generation

---

## 📈 Data Domain Coverage

- **NAV** — Daily NAV per share class, YTD performance, AUM
- **Portfolio** — Holdings by geography, sector, asset class
- **Transactions** — Buy/Sell/Subscription/Redemption, settlement status
- **Registration Matrix** — Per-jurisdiction registration status
- **Static Data** — Fund, Sub-Fund, Share Class master data
