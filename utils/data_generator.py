
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

FUNDS = [
    {"fund_id": "F001", "fund_name": "Lumina Global Equity Fund", "domicile": "Luxembourg", "currency": "USD", "manager": "Lumina AM"},
    {"fund_id": "F002", "fund_name": "Atlas Fixed Income SICAV", "domicile": "Ireland", "currency": "EUR", "manager": "Atlas Capital"},
    {"fund_id": "F003", "fund_name": "Nexus Multi-Asset Fund", "domicile": "Luxembourg", "currency": "EUR", "manager": "Nexus Group"},
    {"fund_id": "F004", "fund_name": "Orion EM Opportunities", "domicile": "Cayman", "currency": "USD", "manager": "Orion Partners"},
    {"fund_id": "F005", "fund_name": "Vega Sustainable Growth", "domicile": "Luxembourg", "currency": "EUR", "manager": "Vega ESG AM"},
]

SUB_FUNDS_TEMPLATE = [
    ("Core", "Equity"), ("Growth", "Equity"), ("Income", "Bond"),
    ("Balanced", "Mixed"), ("Absolute Return", "Alternatives"),
]
SHARE_CLASS_TYPES = ["A", "B", "I", "R", "Z", "C"]
COUNTRIES = ["LU", "DE", "FR", "GB", "US", "CH", "IT", "ES", "NL", "BE", "SE", "DK", "AT"]

ASSET_CLASSES = ["Equity", "Fixed Income", "Real Estate", "Commodities", "Cash", "Derivatives", "Private Equity"]
SECTORS = ["Technology", "Financials", "Healthcare", "Consumer Disc.", "Industrials",
           "Energy", "Materials", "Utilities", "Real Estate", "Comm. Services"]
SECURITIES = [f"SEC{str(i).zfill(4)}" for i in range(1, 51)]
SEC_NAMES = ["Apple Inc", "Microsoft Corp", "Amazon.com", "NVIDIA Corp", "Alphabet Inc",
             "Meta Platforms", "Tesla Inc", "Berkshire Hathaway", "JP Morgan Chase", "UnitedHealth Group",
             "LVMH", "ASML Holding", "SAP SE", "Nestle SA", "Roche Holding",
             "TotalEnergies", "Siemens AG", "BNP Paribas", "Airbus SE", "Deutsche Telekom",
             "Volkswagen AG", "AXA SA", "Unilever PLC", "BP PLC", "Shell PLC",
             "Toyota Motor", "Sony Group", "Softbank", "Samsung Electronics", "TSMC",
             "Alibaba Group", "Tencent Holdings", "JD.com", "Baidu Inc", "Xiaomi Corp",
             "iShares Core S&P500", "Vanguard FTSE All-World", "Amundi MSCI EM", "SPDR Gold Shares", "iShares Euro Gov Bond",
             "US Treasury 10Y", "Bund 10Y", "OAT 10Y", "BTP 10Y", "Gilt 10Y",
             "EUR/USD FX Forward", "Gold Spot", "Brent Crude Future", "S&P500 Future", "Euro Stoxx 50 Future"]

DOC_TYPES = [
    {"type": "NAV", "sla_hours": 2, "frequency": "daily"},
    {"type": "Portfolio", "sla_hours": 4, "frequency": "daily"},
    {"type": "Transaction", "sla_hours": 3, "frequency": "daily"},
    {"type": "Registration Matrix", "sla_hours": 48, "frequency": "monthly"},
    {"type": "Static Data", "sla_hours": 24, "frequency": "weekly"},
    {"type": "AUM Report", "sla_hours": 6, "frequency": "daily"},
    {"type": "Risk Report", "sla_hours": 8, "frequency": "daily"},
    {"type": "Compliance Report", "sla_hours": 24, "frequency": "weekly"},
]


def gen_sub_funds(funds):
    rows = []
    for fund in funds:
        n = random.randint(2, 4)
        for i, (name_sfx, strategy) in enumerate(random.sample(SUB_FUNDS_TEMPLATE, n)):
            rows.append({
                "sub_fund_id": f"{fund['fund_id']}-SF{i+1:02d}",
                "fund_id": fund["fund_id"],
                "sub_fund_name": f"{fund['fund_name'].split()[0]} {name_sfx}",
                "strategy": strategy,
                "currency": fund["currency"],
                "inception_date": (datetime(2018,1,1) + timedelta(days=random.randint(0,1000))).strftime("%Y-%m-%d"),
                "status": random.choice(["Active","Active","Active","Closed"]),
                "aum_usd": round(random.uniform(50e6, 2e9), 0),
            })
    return pd.DataFrame(rows)


def gen_share_classes(sub_funds_df):
    rows = []
    for _, sf in sub_funds_df.iterrows():
        n = random.randint(2, 5)
        for sc_type in random.sample(SHARE_CLASS_TYPES, n):
            currency = random.choice([sf["currency"], "USD", "EUR", "GBP"])
            isin_suffix = "".join([str(random.randint(0,9)) for _ in range(10)])
            rows.append({
                "share_class_id": f"{sf['sub_fund_id']}-{sc_type}",
                "sub_fund_id": sf["sub_fund_id"],
                "fund_id": sf["fund_id"],
                "share_class_type": sc_type,
                "isin": f"LU{isin_suffix}",
                "currency": currency,
                "nav_frequency": random.choice(["Daily","Daily","Daily","Weekly"]),
                "min_investment": random.choice([1000, 5000, 10000, 100000, 1000000]),
                "mgmt_fee_pct": round(random.uniform(0.25, 1.5), 2),
                "status": random.choice(["Active","Active","Active","Inactive"]),
            })
    return pd.DataFrame(rows)


def gen_nav_data(share_classes_df, days=365):
    rows = []
    end_date = datetime(2026, 3, 25)
    start_date = end_date - timedelta(days=days)
    dates = pd.bdate_range(start_date, end_date)
    for _, sc in share_classes_df.iterrows():
        base_nav = round(random.uniform(50, 500), 2)
        nav = base_nav
        aum = random.uniform(1e6, 200e6)
        for dt in dates:
            ret = np.random.normal(0.0003, 0.008)
            nav = round(nav * (1 + ret), 4)
            aum = aum * (1 + np.random.normal(0.0001, 0.002))
            rows.append({
                "nav_id": f"{sc['share_class_id']}-{dt.strftime('%Y%m%d')}",
                "share_class_id": sc["share_class_id"],
                "sub_fund_id": sc["sub_fund_id"],
                "fund_id": sc["fund_id"],
                "date": dt.strftime("%Y-%m-%d"),
                "nav": round(nav, 4),
                "aum": round(aum, 0),
                "currency": sc["currency"],
                "shares_outstanding": round(aum / nav, 0),
            })
    return pd.DataFrame(rows)


def gen_portfolio(sub_funds_df, days=60):
    rows = []
    end_date = datetime(2026, 3, 25)
    start_date = end_date - timedelta(days=days)
    dates = pd.bdate_range(start_date, end_date)
    for _, sf in sub_funds_df.iterrows():
        if sf["status"] != "Active":
            continue
        n_sec = random.randint(15, 35)
        sec_ids = random.sample(range(len(SECURITIES)), n_sec)
        weights = np.random.dirichlet(np.ones(n_sec))
        total_aum = sf["aum_usd"]
        for dt in dates[::5]:
            for i, (sec_idx, w) in enumerate(zip(sec_ids, weights)):
                mv = total_aum * w * np.random.normal(1, 0.05)
                price = round(random.uniform(10, 500), 2)
                rows.append({
                    "portfolio_id": f"{sf['sub_fund_id']}-{dt.strftime('%Y%m%d')}-{i}",
                    "sub_fund_id": sf["sub_fund_id"],
                    "fund_id": sf["fund_id"],
                    "date": dt.strftime("%Y-%m-%d"),
                    "security_id": SECURITIES[sec_idx],
                    "security_name": SEC_NAMES[sec_idx],
                    "asset_class": random.choice(ASSET_CLASSES),
                    "sector": random.choice(SECTORS),
                    "country": random.choice(COUNTRIES),
                    "quantity": round(mv / price, 0),
                    "price": price,
                    "market_value_usd": round(mv, 2),
                    "weight_pct": round(w * 100, 4),
                })
    return pd.DataFrame(rows)


def gen_transactions(sub_funds_df, days=180):
    rows = []
    end_date = datetime(2026, 3, 25)
    start_date = end_date - timedelta(days=days)
    tx_types = ["Buy", "Sell", "Subscription", "Redemption", "Corporate Action", "FX Spot", "Dividend"]
    for _, sf in sub_funds_df.iterrows():
        n_tx = random.randint(50, 200)
        for i in range(n_tx):
            dt = start_date + timedelta(days=random.randint(0, days))
            sec_idx = random.randint(0, len(SECURITIES)-1)
            qty = round(random.uniform(100, 50000), 0)
            price = round(random.uniform(5, 600), 2)
            rows.append({
                "tx_id": f"TX-{sf['sub_fund_id']}-{i:04d}",
                "sub_fund_id": sf["sub_fund_id"],
                "fund_id": sf["fund_id"],
                "date": dt.strftime("%Y-%m-%d"),
                "tx_type": random.choice(tx_types),
                "security_id": SECURITIES[sec_idx],
                "security_name": SEC_NAMES[sec_idx],
                "quantity": qty,
                "price": price,
                "gross_amount": round(qty * price, 2),
                "currency": sf["currency"],
                "status": random.choice(["Settled","Settled","Settled","Pending","Failed"]),
                "broker": random.choice(["Goldman Sachs","JP Morgan","UBS","Deutsche Bank","BNP Paribas","Citi","Morgan Stanley"]),
                "counterparty": f"CTPY-{random.randint(1,20):03d}",
            })
    return pd.DataFrame(rows)


def gen_registration_matrix(share_classes_df):
    rows = []
    for _, sc in share_classes_df.iterrows():
        for country in COUNTRIES:
            is_registered = random.random() > 0.35
            rows.append({
                "share_class_id": sc["share_class_id"],
                "fund_id": sc["fund_id"],
                "isin": sc["isin"],
                "country": country,
                "country_name": {"LU":"Luxembourg","DE":"Germany","FR":"France","GB":"United Kingdom",
                                  "US":"United States","CH":"Switzerland","IT":"Italy","ES":"Spain",
                                  "NL":"Netherlands","BE":"Belgium","SE":"Sweden","DK":"Denmark","AT":"Austria"}[country],
                "registered": is_registered,
                "registration_date": (datetime(2018,1,1) + timedelta(days=random.randint(0,2000))).strftime("%Y-%m-%d") if is_registered else None,
                "expiry_date": (datetime(2026,1,1) + timedelta(days=random.randint(0,730))).strftime("%Y-%m-%d") if is_registered else None,
                "status": random.choice(["Active","Active","Active","Under Review","Pending"]) if is_registered else "Not Registered",
            })
    return pd.DataFrame(rows)


def gen_imports(days=90):
    rows = []
    end_date = datetime(2026, 3, 25)
    start_date = end_date - timedelta(days=days)
    dates = pd.bdate_range(start_date, end_date)
    import_id = 1
    for dt in dates:
        for doc in DOC_TYPES:
            if doc["frequency"] == "daily":
                pass
            elif doc["frequency"] == "weekly" and dt.weekday() != 0:
                continue
            elif doc["frequency"] == "monthly" and dt.day != 1:
                continue
            for fund in FUNDS:
                expected_dt = dt + timedelta(hours=random.randint(14, 18))
                delay_prob = random.random()
                if delay_prob < 0.75:
                    delay_hours = random.uniform(-0.5, doc["sla_hours"] * 0.8)
                    status = "On Time"
                elif delay_prob < 0.88:
                    delay_hours = doc["sla_hours"] + random.uniform(0, 4)
                    status = "Late"
                elif delay_prob < 0.95:
                    delay_hours = doc["sla_hours"] + random.uniform(4, 24)
                    status = "Critical Delay"
                else:
                    delay_hours = None
                    status = "Missing"
                rows.append({
                    "import_id": f"IMP-{import_id:06d}",
                    "date": dt.strftime("%Y-%m-%d"),
                    "fund_id": fund["fund_id"],
                    "fund_name": fund["fund_name"],
                    "doc_type": doc["type"],
                    "frequency": doc["frequency"],
                    "sla_hours": doc["sla_hours"],
                    "expected_time": expected_dt.strftime("%Y-%m-%d %H:%M"),
                    "received_time": (expected_dt + timedelta(hours=delay_hours)).strftime("%Y-%m-%d %H:%M") if delay_hours is not None else None,
                    "delay_hours": round(delay_hours, 2) if delay_hours is not None else None,
                    "status": status,
                    "file_size_kb": round(random.uniform(10, 5000), 1) if status != "Missing" else None,
                    "records_count": random.randint(10, 50000) if status != "Missing" else None,
                    "data_quality_score": round(random.uniform(0.75, 1.0), 3) if status != "Missing" else None,
                })
                import_id += 1
    return pd.DataFrame(rows)


def gen_data_quality(sub_funds_df, share_classes_df):
    dimensions = ["Completeness", "Accuracy", "Timeliness", "Consistency", "Uniqueness", "Validity"]
    entities = (
        [{"entity": f["fund_name"], "entity_type": "Fund", "layer": "Gold"} for f in FUNDS] +
        [{"entity": row["sub_fund_name"], "entity_type": "Sub-Fund", "layer": "Silver"} for _, row in sub_funds_df.iterrows()] +
        [{"entity": row["share_class_id"], "entity_type": "Share Class", "layer": "Silver"} for _, row in share_classes_df.head(20).iterrows()]
    )
    rows = []
    for ent in entities:
        for dim in dimensions:
            base = random.uniform(0.82, 0.99)
            rows.append({
                "entity": ent["entity"],
                "entity_type": ent["entity_type"],
                "layer": ent["layer"],
                "dimension": dim,
                "score": round(base, 4),
                "issues_count": random.randint(0, 20),
                "last_checked": (datetime(2026,3,25) - timedelta(hours=random.randint(0,48))).strftime("%Y-%m-%d %H:%M"),
            })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    print("Data generator module loaded.")
