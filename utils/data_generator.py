# utils/data_generator.py
# FundGov360 v5 — Synthetic Data Generator
# Inspired by Talend (data quality), Atlan (lineage), Alation (stewardship & cataloging)

import pandas as pd
import numpy as np
import random
import string
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
np.random.seed(42)
random.seed(42)

# ─────────────────────────────────────────────
# STATIC MASTER DATA
# ─────────────────────────────────────────────

FUNDS = [
    {"fund_id": "F001", "fund_name": "Apex Global Fund", "domicile": "LU", "legal_form": "SICAV", "currency": "USD"},
    {"fund_id": "F002", "fund_name": "Atlas Income Fund", "domicile": "IE", "legal_form": "ICAV",  "currency": "EUR"},
    {"fund_id": "F003", "fund_name": "Orion EM Fund",     "domicile": "KY", "legal_form": "LP",    "currency": "USD"},
]

SUB_FUNDS = [
    {"sub_fund_id": "SF001", "fund_id": "F001", "sub_fund_name": "Apex Global Equity",       "asset_class": "Equity",       "strategy": "Long Only"},
    {"sub_fund_id": "SF002", "fund_id": "F001", "sub_fund_name": "Apex Global Fixed Income",  "asset_class": "Fixed Income", "strategy": "Active"},
    {"sub_fund_id": "SF003", "fund_id": "F002", "sub_fund_name": "Atlas Core Income",          "asset_class": "Fixed Income", "strategy": "Passive"},
    {"sub_fund_id": "SF004", "fund_id": "F002", "sub_fund_name": "Atlas High Yield",           "asset_class": "Fixed Income", "strategy": "Active"},
    {"sub_fund_id": "SF005", "fund_id": "F003", "sub_fund_name": "Orion EM Equity",            "asset_class": "Equity",       "strategy": "Long Only"},
    {"sub_fund_id": "SF006", "fund_id": "F003", "sub_fund_name": "Orion EM Debt",              "asset_class": "Fixed Income", "strategy": "Active"},
]

SHARE_CLASS_TEMPLATES = [
    {"suffix": "A-USD", "currency": "USD", "investor_type": "Retail",        "min_investment": 1_000},
    {"suffix": "B-USD", "currency": "USD", "investor_type": "Institutional", "min_investment": 1_000_000},
    {"suffix": "C-EUR", "currency": "EUR", "investor_type": "Retail",        "min_investment": 1_000},
    {"suffix": "D-EUR", "currency": "EUR", "investor_type": "Institutional", "min_investment": 500_000},
    {"suffix": "I-GBP", "currency": "GBP", "investor_type": "Institutional", "min_investment": 1_000_000},
]

JURISDICTIONS = [
    "Luxembourg", "Germany", "France", "United Kingdom", "Switzerland",
    "Netherlands", "Belgium", "Spain", "Italy", "Austria",
    "Sweden", "Denmark", "Norway", "Singapore", "Hong Kong",
]

GEOGRAPHIES   = ["North America", "Europe", "Asia Pacific", "Emerging Markets", "Middle East & Africa", "Latin America"]
SECTORS       = ["Technology", "Financials", "Healthcare", "Consumer Discretionary", "Industrials",
                 "Energy", "Materials", "Utilities", "Real Estate", "Communication Services"]
ASSET_CLASSES = ["Equity", "Government Bond", "Corporate Bond", "Cash & Equivalents",
                 "Real Estate", "Commodities", "Derivatives", "Money Market"]
CURRENCIES    = ["USD", "EUR", "GBP", "JPY", "CHF", "SGD", "HKD"]

TX_TYPES      = ["Buy", "Sell", "Subscription", "Redemption"]
SETTLE_STATUS = ["Settled", "Pending", "Failed", "Cancelled"]
SETTLE_WEIGHTS = [0.70, 0.18, 0.08, 0.04]

REG_STATUSES  = ["Registered", "Pending", "Restricted", "Not Registered"]
REG_WEIGHTS   = [0.55, 0.15, 0.10, 0.20]

DATA_SOURCES  = ["Fund Administrator", "Custodian", "Transfer Agent", "Bloomberg", "Reuters", "Internal"]
STEWARD_ROLES = ["Data Owner", "Data Steward", "Business Owner", "Compliance Officer"]
LINEAGE_LAYERS = ["Bronze", "Silver", "Gold"]


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _isin_prefix(domicile: str) -> str:
    """Return ISO country prefix for ISIN based on fund domicile."""
    mapping = {"LU": "LU", "IE": "IE", "KY": "KY"}
    return mapping.get(domicile, "XX")


def _random_isin(prefix: str) -> str:
    digits = "".join(random.choices(string.digits, k=9))
    check  = random.choice(string.digits)
    return f"{prefix}{digits}{check}"


def _date_range(days: int = 365) -> list[datetime]:
    end   = datetime.today()
    start = end - timedelta(days=days)
    return [start + timedelta(days=i) for i in range(days)]


# ─────────────────────────────────────────────
# LEVEL 1 — FUNDS
# ─────────────────────────────────────────────

def gen_funds() -> pd.DataFrame:
    """Return master fund table."""
    df = pd.DataFrame(FUNDS)
    df["inception_date"] = [
        datetime(2010, 3, 15),
        datetime(2014, 7, 1),
        datetime(2018, 11, 20),
    ]
    df["aum_usd"] = [4_200_000_000, 2_800_000_000, 950_000_000]
    df["fund_manager"] = ["Apex Capital", "Atlas AM", "Orion Partners"]
    df["custodian"]    = ["State Street", "BNP Paribas", "Citi"]
    df["fund_admin"]   = ["Northern Trust", "CACEIS", "Maples"]
    df["lei"]          = [
        "529900" + "".join(random.choices(string.ascii_uppercase + string.digits, k=14))
        for _ in FUNDS
    ]
    return df


# ─────────────────────────────────────────────
# LEVEL 2 — SUB-FUNDS
# ─────────────────────────────────────────────

def gen_sub_funds(funds_df: pd.DataFrame) -> pd.DataFrame:
    """Return sub-fund table linked to fund master."""
    df = pd.DataFrame(SUB_FUNDS)
    df = df.merge(funds_df[["fund_id", "domicile"]], on="fund_id", how="left")
    df["isin_base"]      = df["domicile"].apply(lambda d: _random_isin(_isin_prefix(d)))
    df["inception_date"] = pd.to_datetime([
        "2010-06-01", "2012-03-15", "2014-09-01",
        "2016-01-10", "2018-12-01", "2020-05-20",
    ])
    df["benchmark"] = [
        "MSCI World", "Bloomberg Global Agg", "Bloomberg Euro Agg",
        "ICE BofA HY Index", "MSCI EM", "JPM EMBI",
    ]
    df["management_fee_pct"] = [0.75, 0.50, 0.30, 0.65, 0.85, 0.60]
    df["performance_fee_pct"] = [10.0, 0.0, 0.0, 15.0, 15.0, 10.0]
    df["nav_frequency"] = ["Daily"] * 4 + ["Weekly", "Weekly"]
    df["status"] = "Active"
    return df


# ─────────────────────────────────────────────
# LEVEL 3 — SHARE CLASSES
# ─────────────────────────────────────────────

def gen_share_classes(sub_funds_df: pd.DataFrame) -> pd.DataFrame:
    """Return share class table linked to sub-fund master."""
    rows = []
    sc_counter = 1
    for _, sf in sub_funds_df.iterrows():
        templates = random.sample(SHARE_CLASS_TEMPLATES, k=random.randint(2, 4))
        for tmpl in templates:
            prefix = _isin_prefix(sf["domicile"])
            rows.append({
                "sc_id":          f"SC{sc_counter:03d}",
                "sub_fund_id":    sf["sub_fund_id"],
                "fund_id":        sf["fund_id"],
                "sc_name":        f"{sf['sub_fund_name']} {tmpl['suffix']}",
                "isin":           _random_isin(prefix),
                "currency":       tmpl["currency"],
                "investor_type":  tmpl["investor_type"],
                "min_investment": tmpl["min_investment"],
                "distribution":   random.choice(["Accumulating", "Distributing"]),
                "hedged":         random.choice([True, False]),
                "inception_date": sf["inception_date"] + timedelta(days=random.randint(0, 180)),
                "status":         random.choices(["Active", "Closed", "Suspended"], weights=[0.85, 0.10, 0.05])[0],
                "shares_outstanding": random.randint(100_000, 50_000_000),
                "transfer_agent": random.choice(["RBC IS", "CACEIS TA", "Northern Trust TA", "BNP Paribas TA"]),
            })
            sc_counter += 1
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# NAV TIME SERIES
# ─────────────────────────────────────────────

def gen_nav(sc_df: pd.DataFrame, days: int = 365) -> pd.DataFrame:
    """Generate daily NAV per share class for the past `days` days."""
    dates = _date_range(days)
    rows  = []
    for _, sc in sc_df.iterrows():
        nav = round(random.uniform(80.0, 200.0), 4)
        for dt in dates:
            change  = np.random.normal(0.0003, 0.008)
            nav     = max(0.01, round(nav * (1 + change), 4))
            aum_val = round(nav * sc["shares_outstanding"] * random.uniform(0.95, 1.05), 2)
            rows.append({
                "date":           dt.date(),
                "sc_id":          sc["sc_id"],
                "sub_fund_id":    sc["sub_fund_id"],
                "fund_id":        sc["fund_id"],
                "isin":           sc["isin"],
                "currency":       sc["currency"],
                "nav":            nav,
                "aum":            aum_val,
                "shares_outstanding": sc["shares_outstanding"],
                "source":         random.choice(["Fund Administrator", "Bloomberg"]),
                "validated":      random.choices([True, False], weights=[0.92, 0.08])[0],
                "data_layer":     "Gold",
            })
    return pd.DataFrame(rows)


def gen_ytd_performance(nav_df: pd.DataFrame) -> pd.DataFrame:
    """Compute YTD performance from NAV series."""
    df = nav_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    current_year = df["date"].dt.year.max()
    df_year = df[df["date"].dt.year == current_year].copy()

    jan1 = df_year[df_year["date"].dt.dayofyear == df_year.groupby("sc_id")["date"].transform(
        lambda x: x.dt.dayofyear.min()
    )]
    first_nav = jan1[["sc_id", "nav"]].rename(columns={"nav": "nav_start"})

    latest = df_year.sort_values("date").groupby("sc_id").last().reset_index()[["sc_id", "date", "nav"]]
    latest = latest.merge(first_nav, on="sc_id", how="left")
    latest["ytd_pct"] = ((latest["nav"] - latest["nav_start"]) / latest["nav_start"] * 100).round(2)
    return latest[["sc_id", "date", "nav", "nav_start", "ytd_pct"]]


# ─────────────────────────────────────────────
# PORTFOLIO HOLDINGS
# ─────────────────────────────────────────────

def gen_portfolio(sub_funds_df: pd.DataFrame, n_holdings: int = 15) -> pd.DataFrame:
    """Generate portfolio holdings per sub-fund."""
    rows = []
    for _, sf in sub_funds_df.iterrows():
        total_weight = 0.0
        n = random.randint(n_holdings - 3, n_holdings + 5)
        weights = np.random.dirichlet(np.ones(n) * 2)
        for i, w in enumerate(weights):
            weight = round(float(w) * 100, 2)
            total_weight += weight
            rows.append({
                "sub_fund_id":   sf["sub_fund_id"],
                "fund_id":       sf["fund_id"],
                "position_id":   f"{sf['sub_fund_id']}_P{i+1:03d}",
                "security_name": fake.company() + random.choice([" Corp", " PLC", " SA", " AG", " Ltd", " Inc"]),
                "isin":          _random_isin(random.choice(["US", "DE", "FR", "GB", "JP"])),
                "asset_class":   sf["asset_class"] if random.random() > 0.2 else random.choice(ASSET_CLASSES),
                "geography":     random.choice(GEOGRAPHIES),
                "sector":        random.choice(SECTORS),
                "currency":      random.choice(CURRENCIES),
                "market_value":  round(random.uniform(100_000, 10_000_000), 2),
                "weight_pct":    weight,
                "quantity":      random.randint(1_000, 500_000),
                "price":         round(random.uniform(5.0, 500.0), 4),
                "maturity_date": (datetime.today() + timedelta(days=random.randint(180, 3650))).date()
                    if sf["asset_class"] == "Fixed Income" else None,
                "rating":        random.choice(["AAA", "AA", "A", "BBB", "BB", "B", "NR"])
                    if sf["asset_class"] == "Fixed Income" else None,
                "coupon_pct":    round(random.uniform(1.0, 8.0), 2)
                    if sf["asset_class"] == "Fixed Income" else None,
                "valuation_date": datetime.today().date(),
                "source":        random.choice(DATA_SOURCES),
                "data_layer":    "Silver",
            })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# TRANSACTIONS
# ─────────────────────────────────────────────

def gen_transactions(sc_df: pd.DataFrame, n: int = 500) -> pd.DataFrame:
    """Generate synthetic fund transactions."""
    rows = []
    sc_list = sc_df.to_dict("records")
    for i in range(n):
        sc    = random.choice(sc_list)
        tx_dt = datetime.today() - timedelta(days=random.randint(0, 365))
        tx_type = random.choice(TX_TYPES)
        amount  = round(random.uniform(5_000, 5_000_000), 2)
        nav_val = round(random.uniform(80.0, 200.0), 4)
        rows.append({
            "tx_id":          f"TX{i+1:05d}",
            "sc_id":          sc["sc_id"],
            "sub_fund_id":    sc["sub_fund_id"],
            "fund_id":        sc["fund_id"],
            "isin":           sc["isin"],
            "tx_type":        tx_type,
            "tx_date":        tx_dt.date(),
            "settlement_date": (tx_dt + timedelta(days=random.choice([1, 2, 3]))).date(),
            "amount":         amount,
            "currency":       sc["currency"],
            "nav_at_tx":      nav_val,
            "units":          round(amount / nav_val, 4),
            "counterparty":   fake.company(),
            "investor_id":    f"INV{random.randint(1000, 9999)}",
            "settlement_status": random.choices(SETTLE_STATUS, weights=SETTLE_WEIGHTS)[0],
            "source":         random.choice(["Transfer Agent", "Custodian"]),
            "data_layer":     "Silver",
            "error_flag":     random.choices([False, True], weights=[0.95, 0.05])[0],
            "error_reason":   None,
        })
    df = pd.DataFrame(rows)
    # Inject realistic error reasons
    mask = df["error_flag"]
    df.loc[mask, "error_reason"] = random.choices(
        ["Missing investor ID", "NAV mismatch", "Duplicate transaction", "Invalid ISIN", "Settlement date error"],
        k=mask.sum()
    )
    return df


# ─────────────────────────────────────────────
# REGISTRATION MATRIX
# ─────────────────────────────────────────────

def gen_registration_matrix(sc_df: pd.DataFrame) -> pd.DataFrame:
    """Generate per-jurisdiction registration status for each share class."""
    rows = []
    for _, sc in sc_df.iterrows():
        for jur in JURISDICTIONS:
            status = random.choices(REG_STATUSES, weights=REG_WEIGHTS)[0]
            rows.append({
                "sc_id":        sc["sc_id"],
                "sub_fund_id":  sc["sub_fund_id"],
                "fund_id":      sc["fund_id"],
                "isin":         sc["isin"],
                "jurisdiction": jur,
                "reg_status":   status,
                "reg_date":     (datetime.today() - timedelta(days=random.randint(30, 1800))).date()
                    if status == "Registered" else None,
                "expiry_date":  (datetime.today() + timedelta(days=random.randint(90, 730))).date()
                    if status == "Registered" else None,
                "local_regulator": fake.company() + " Authority",
                "last_updated": (datetime.today() - timedelta(days=random.randint(0, 90))).date(),
                "source":       "Legal & Compliance",
                "data_layer":   "Gold",
            })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# STEWARDSHIP  (Alation-inspired)
# ─────────────────────────────────────────────

def gen_stewards(n: int = 12) -> pd.DataFrame:
    """Generate data steward profiles à la Alation Stewardship."""
    rows = []
    for i in range(n):
        rows.append({
            "steward_id":    f"STW{i+1:03d}",
            "name":          fake.name(),
            "email":         fake.company_email(),
            "department":    random.choice(["Data Management", "Compliance", "Operations", "Finance", "Risk"]),
            "role":          random.choice(STEWARD_ROLES),
            "phone":         fake.phone_number(),
            "location":      random.choice(["Luxembourg", "Dublin", "London", "Paris", "Frankfurt"]),
            "active":        random.choices([True, False], weights=[0.90, 0.10])[0],
            "assigned_since": (datetime.today() - timedelta(days=random.randint(30, 1800))).date(),
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# DATA CATALOG  (Atlan-inspired)
# ─────────────────────────────────────────────

def gen_data_catalog(
    sub_funds_df: pd.DataFrame,
    sc_df: pd.DataFrame,
    stewards_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate a data asset catalog linking sub-funds & share classes to
    stewards, sources, and business glossary terms — à la Atlan / Alation.
    """
    assets = []

    # Sub-fund assets
    for _, sf in sub_funds_df.iterrows():
        steward = stewards_df.sample(1).iloc[0]
        assets.append({
            "asset_id":          f"DA_{sf['sub_fund_id']}",
            "asset_type":        "Sub-Fund",
            "asset_name":        sf["sub_fund_name"],
            "domain":            "Fund Master Data",
            "sub_domain":        sf["asset_class"],
            "description":       f"Master data entity for {sf['sub_fund_name']} — {sf['strategy']} strategy.",
            "source_system":     random.choice(DATA_SOURCES),
            "data_layer":        "Gold",
            "pii_flag":          False,
            "classification":    "Internal",
            "steward_id":        steward["steward_id"],
            "steward_name":      steward["name"],
            "steward_role":      steward["role"],
            "quality_score":     round(random.uniform(70.0, 99.0), 1),
            "completeness_pct":  round(random.uniform(80.0, 100.0), 1),
            "last_profiled":     (datetime.today() - timedelta(days=random.randint(0, 30))).date(),
            "tags":              ", ".join(random.sample(["NAV", "AUM", "ISIN", "Benchmark", "Fee"], k=3)),
            "glossary_terms":    "Net Asset Value; Sub-Fund; Share Class",
            "certified":         random.choice([True, False]),
        })

    # Share class assets
    for _, sc in sc_df.iterrows():
        steward = stewards_df.sample(1).iloc[0]
        assets.append({
            "asset_id":          f"DA_{sc['sc_id']}",
            "asset_type":        "Share Class",
            "asset_name":        sc["sc_name"],
            "domain":            "Fund Master Data",
            "sub_domain":        sc["investor_type"],
            "description":       f"Share class {sc['isin']} — {sc['currency']} {sc['distribution']}.",
            "source_system":     random.choice(DATA_SOURCES),
            "data_layer":        "Gold",
            "pii_flag":          False,
            "classification":    "Internal",
            "steward_id":        steward["steward_id"],
            "steward_name":      steward["name"],
            "steward_role":      steward["role"],
            "quality_score":     round(random.uniform(65.0, 99.0), 1),
            "completeness_pct":  round(random.uniform(75.0, 100.0), 1),
            "last_profiled":     (datetime.today() - timedelta(days=random.randint(0, 30))).date(),
            "tags":              ", ".join(random.sample(["ISIN", "Currency", "Hedged", "Distribution", "TA"], k=3)),
            "glossary_terms":    "ISIN; Share Class; Transfer Agent",
            "certified":         random.choice([True, False]),
        })

    return pd.DataFrame(assets)


# ─────────────────────────────────────────────
# DATA LINEAGE  (Atlan / Talend-inspired)
# ─────────────────────────────────────────────

LINEAGE_NODES = [
    # Bronze sources
    {"node_id": "N01", "layer": "Bronze", "system": "Fund Administrator",    "dataset": "raw_nav",           "description": "Raw NAV files from Fund Admin (CSV/SWIFT)"},
    {"node_id": "N02", "layer": "Bronze", "system": "Bloomberg",             "dataset": "raw_prices",        "description": "Security prices & reference data"},
    {"node_id": "N03", "layer": "Bronze", "system": "Transfer Agent",        "dataset": "raw_transactions",  "description": "Subscription / Redemption raw feed"},
    {"node_id": "N04", "layer": "Bronze", "system": "Custodian",             "dataset": "raw_holdings",      "description": "Portfolio holdings from custodian"},
    {"node_id": "N05", "layer": "Bronze", "system": "Legal & Compliance",    "dataset": "raw_registrations", "description": "Jurisdiction registration status export"},
    # Silver transformations
    {"node_id": "N06", "layer": "Silver", "system": "FundGov360 ETL",        "dataset": "nav_cleansed",      "description": "Validated & deduplicated NAV records"},
    {"node_id": "N07", "layer": "Silver", "system": "FundGov360 ETL",        "dataset": "tx_enriched",       "description": "Transactions enriched with counterparty & status"},
    {"node_id": "N08", "layer": "Silver", "system": "FundGov360 ETL",        "dataset": "portfolio_mapped",  "description": "Holdings mapped to security master"},
    {"node_id": "N09", "layer": "Silver", "system": "FundGov360 ETL",        "dataset": "reg_matrix",        "description": "Normalised registration matrix"},
    # Gold outputs
    {"node_id": "N10", "layer": "Gold",   "system": "FundGov360 Dashboard",  "dataset": "golden_nav",        "description": "Authoritative NAV golden record"},
    {"node_id": "N11", "layer": "Gold",   "system": "FundGov360 Dashboard",  "dataset": "golden_portfolio",  "description": "Consolidated portfolio golden record"},
    {"node_id": "N12", "layer": "Gold",   "system": "FundGov360 Dashboard",  "dataset": "golden_tx",         "description": "Settled & reconciled transactions"},
    {"node_id": "N13", "layer": "Gold",   "system": "FundGov360 Dashboard",  "dataset": "golden_reg",        "description": "Current jurisdiction registration status"},
]

LINEAGE_EDGES = [
    ("N01", "N06"), ("N02", "N06"), ("N06", "N10"),
    ("N03", "N07"), ("N07", "N12"),
    ("N04", "N08"), ("N02", "N08"), ("N08", "N11"),
    ("N05", "N09"), ("N09", "N13"),
]


def gen_lineage() -> pd.DataFrame:
    """Return a lineage edge table suitable for graph rendering."""
    nodes = {n["node_id"]: n for n in LINEAGE_NODES}
    rows  = []
    for src_id, tgt_id in LINEAGE_EDGES:
        src = nodes[src_id]
        tgt = nodes[tgt_id]
        rows.append({
            "source_node_id":   src_id,
            "source_dataset":   src["dataset"],
            "source_layer":     src["layer"],
            "source_system":    src["system"],
            "target_node_id":   tgt_id,
            "target_dataset":   tgt["dataset"],
            "target_layer":     tgt["layer"],
            "target_system":    tgt["system"],
            "transformation":   f"{src['dataset']} → {tgt['dataset']}",
            "last_run":         (datetime.today() - timedelta(hours=random.randint(1, 48))).strftime("%Y-%m-%d %H:%M"),
            "status":           random.choices(["Success", "Warning", "Failed"], weights=[0.85, 0.10, 0.05])[0],
        })
    return pd.DataFrame(rows)


def gen_lineage_nodes() -> pd.DataFrame:
    """Return the lineage node catalog."""
    return pd.DataFrame(LINEAGE_NODES)


# ─────────────────────────────────────────────
# DATA PROFILING STATS  (Talend Data Quality-inspired)
# ─────────────────────────────────────────────

PROFILING_FIELDS = [
    # (dataset, field_name, expected_type, nullable)
    ("nav",           "nav",              "float",   False),
    ("nav",           "aum",              "float",   False),
    ("nav",           "isin",             "string",  False),
    ("nav",           "currency",         "string",  False),
    ("nav",           "source",           "string",  True),
    ("share_classes", "isin",             "string",  False),
    ("share_classes", "currency",         "string",  False),
    ("share_classes", "shares_outstanding","int",    False),
    ("share_classes", "min_investment",   "int",     False),
    ("share_classes", "inception_date",   "date",    False),
    ("transactions",  "amount",           "float",   False),
    ("transactions",  "settlement_status","string",  False),
    ("transactions",  "tx_date",          "date",    False),
    ("transactions",  "investor_id",      "string",  True),
    ("portfolio",     "weight_pct",       "float",   False),
    ("portfolio",     "market_value",     "float",   False),
    ("portfolio",     "geography",        "string",  False),
    ("portfolio",     "sector",           "string",  False),
    ("registration",  "reg_status",       "string",  False),
    ("registration",  "jurisdiction",     "string",  False),
]


def gen_profiling_stats() -> pd.DataFrame:
    """
    Generate synthetic data profiling statistics per field —
    mimicking Talend Data Quality / Atlan profiling panels.
    """
    rows = []
    for dataset, field, dtype, nullable in PROFILING_FIELDS:
        null_pct    = round(random.uniform(0.0, 5.0) if nullable else random.uniform(0.0, 1.5), 2)
        unique_pct  = round(random.uniform(10.0, 100.0), 1)
        completeness = round(100.0 - null_pct, 2)
        conformity  = round(random.uniform(88.0, 100.0), 2)
        validity    = round(random.uniform(90.0, 100.0), 2)
        rows.append({
            "dataset":            dataset,
            "field_name":         field,
            "data_type":          dtype,
            "nullable":           nullable,
            "row_count":          random.randint(1_000, 50_000),
            "null_count_pct":     null_pct,
            "unique_count_pct":   unique_pct,
            "completeness_pct":   completeness,
            "conformity_pct":     conformity,
            "validity_pct":       validity,
            "overall_dq_score":   round((completeness + conformity + validity) / 3, 2),
            "min_value":          round(random.uniform(0.01, 10.0), 4) if dtype in ("float", "int") else None,
            "max_value":          round(random.uniform(500.0, 5_000_000.0), 2) if dtype in ("float", "int") else None,
            "mean_value":         round(random.uniform(50.0, 500_000.0), 2) if dtype in ("float", "int") else None,
            "std_dev":            round(random.uniform(1.0, 50_000.0), 2) if dtype in ("float", "int") else None,
            "top_value":          None if dtype in ("float", "int") else random.choice(
                ["USD", "EUR", "Active", "Settled", "Registered", "North America"]
            ),
            "top_value_freq_pct": round(random.uniform(15.0, 60.0), 1),
            "last_profiled":      (datetime.today() - timedelta(days=random.randint(0, 7))).date(),
            "profiling_source":   "FundGov360 Profiler v5",
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# RULE TEST RUNNER  (used by rule_engine.py)
# ─────────────────────────────────────────────

def test_rule_against_data(
    rule: dict,
    nav_df: pd.DataFrame,
    sc_df: pd.DataFrame,
    tx_df: pd.DataFrame,
    port_df: pd.DataFrame,
    reg_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """
    Run a single DQ rule dict against the appropriate dataset.
    Returns (failures_df, stats_dict).

    rule keys expected: rule_id, rule_name, dataset, field, rule_type,
                        threshold_min, threshold_max, regex_pattern,
                        formula_expr, severity.
    """
    dataset_map = {
        "nav":          nav_df,
        "share_classes": sc_df,
        "transactions": tx_df,
        "portfolio":    port_df,
        "registration": reg_df,
    }

    target = dataset_map.get(rule.get("dataset", ""), pd.DataFrame())
    if target.empty:
        return pd.DataFrame(), {"checked": 0, "failed": 0, "pass_rate": 0.0}

    field       = rule.get("field", "")
    rule_type   = rule.get("rule_type", "NOT_NULL")
    failures    = pd.DataFrame()

    if field not in target.columns:
        return pd.DataFrame(), {"checked": 0, "failed": 0, "pass_rate": 0.0, "error": f"Field '{field}' not found"}

    checked = len(target)

    if rule_type == "NOT_NULL":
        failures = target[target[field].isna()].copy()

    elif rule_type == "RANGE":
        lo = rule.get("threshold_min")
        hi = rule.get("threshold_max")
        mask = pd.Series([False] * checked, index=target.index)
        if lo is not None:
            mask |= target[field] < lo
        if hi is not None:
            mask |= target[field] > hi
        failures = target[mask].copy()

    elif rule_type == "REGEX":
        pattern = rule.get("regex_pattern", "")
        if pattern:
            import re
            failures = target[~target[field].astype(str).str.match(pattern, na=False)].copy()

    elif rule_type == "FORMULA":
        expr = rule.get("formula_expr", "")
        if expr:
            try:
                mask = ~target.eval(expr)
                failures = target[mask].copy()
            except Exception as e:
                return pd.DataFrame(), {"checked": checked, "failed": 0, "pass_rate": 100.0, "error": str(e)}

    elif rule_type == "DATE":
        try:
            col = pd.to_datetime(target[field], errors="coerce")
            failures = target[col.isna()].copy()
        except Exception:
            failures = pd.DataFrame()

    if not failures.empty:
        failures["__rule_id__"]   = rule.get("rule_id", "UNKNOWN")
        failures["__rule_name__"] = rule.get("rule_name", "")
        failures["__severity__"]  = rule.get("severity", "Medium")
        failures["__field__"]     = field

    failed    = len(failures)
    pass_rate = round((1 - failed / checked) * 100, 2) if checked else 0.0

    stats = {
        "rule_id":   rule.get("rule_id"),
        "checked":   checked,
        "failed":    failed,
        "passed":    checked - failed,
        "pass_rate": pass_rate,
        "severity":  rule.get("severity", "Medium"),
    }
    return failures, stats


# ─────────────────────────────────────────────
# CONVENIENCE: load all datasets at once
# ─────────────────────────────────────────────

def load_all_data(nav_days: int = 365, n_transactions: int = 500) -> dict:
    """
    Bootstrap all synthetic datasets in dependency order.
    Returns a dict of DataFrames keyed by dataset name.

    Usage:
        from utils.data_generator import load_all_data
        data = load_all_data()
        nav_df  = data["nav"]
        sc_df   = data["share_classes"]
        ...
    """
    funds_df    = gen_funds()
    sf_df       = gen_sub_funds(funds_df)
    sc_df       = gen_share_classes(sf_df)
    nav_df      = gen_nav(sc_df, days=nav_days)
    ytd_df      = gen_ytd_performance(nav_df)
    port_df     = gen_portfolio(sf_df)
    tx_df       = gen_transactions(sc_df, n=n_transactions)
    reg_df      = gen_registration_matrix(sc_df)
    stewards_df = gen_stewards()
    catalog_df  = gen_data_catalog(sf_df, sc_df, stewards_df)
    lineage_df  = gen_lineage()
    nodes_df    = gen_lineage_nodes()
    profiling_df = gen_profiling_stats()

    return {
        "funds":          funds_df,
        "sub_funds":      sf_df,
        "share_classes":  sc_df,
        "nav":            nav_df,
        "ytd":            ytd_df,
        "portfolio":      port_df,
        "transactions":   tx_df,
        "registration":   reg_df,
        "stewards":       stewards_df,
        "catalog":        catalog_df,
        "lineage":        lineage_df,
        "lineage_nodes":  nodes_df,
        "profiling":      profiling_df,
    }
