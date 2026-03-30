
"""
contract_engine.py — FundGov360 Data Contract Engine
Covers: contract registry, schema, quality rules, SLA, breach history,
        lifecycle management (Draft → Review → Active → Breached/Expired)
"""
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import uuid

random.seed(55)
np.random.seed(55)

# ── Constants ─────────────────────────────────────────────────────────────────
CONTRACT_STATUSES  = ["Active", "Draft", "In Review", "Breached", "Expired", "Deprecated"]
CONTRACT_DOMAINS   = ["NAV", "Portfolio", "Transaction", "Static Data", "Registration", "AUM"]
PRODUCERS          = ["Fund Administrator", "Custodian", "Transfer Agent",
                      "Bloomberg", "FactSet", "Internal System"]
CONSUMERS          = ["Gold Layer — NAV", "Gold Layer — Portfolio", "Gold Layer — Transactions",
                      "KID / KIID Team", "EPT Reporting", "Registration Team",
                      "Global Tax", "Client Reporting", "Risk & Compliance", "BI Dashboard"]
DATA_TYPES         = ["NUMERIC", "STRING", "DATE", "BOOLEAN", "INTEGER"]
BREACH_TYPES       = ["SLA Delivery", "Data Quality", "Schema Violation",
                      "Freshness Breach", "Completeness", "Referential Integrity"]
SEVERITIES         = ["Critical", "High", "Medium", "Low"]
STEWARD_NAMES      = ["Alice Mercer", "Bruno Leclercq", "Céline Fontaine",
                      "David Kwame", "Eva Richter", "François Dupont",
                      "Grace Liu", "Hamid Rashidi", "Isabelle Morin", "Jan Kowalski"]

# ── Contract templates (realistic fund ops contracts) ─────────────────────────
CONTRACT_TEMPLATES = [
    {
        "contract_name": "Fund Admin → NAV Golden Record",
        "domain": "NAV", "producer": "Fund Administrator",
        "consumer": "Gold Layer — NAV",
        "description": "Daily NAV per share class delivered by the fund administrator. "
                       "Covers all active share classes across all domiciled funds. "
                       "Golden record used downstream by KID, EPT, and BI reporting.",
        "sla_delivery_h": 2, "sla_freshness_h": 4, "sla_availability_pct": 99.5,
        "fields": [
            ("nav", "NUMERIC", False, "nav > 0", "Net Asset Value per share", False, True),
            ("shares_outstanding", "NUMERIC", False, "shares_outstanding >= 0", "Total shares outstanding", False, False),
            ("aum", "NUMERIC", False, "aum > 0", "Assets under management", False, True),
            ("currency", "STRING", False, "ISO 4217 code", "NAV currency", False, False),
            ("date", "DATE", False, "date <= today", "Valuation date", False, False),
            ("share_class_id", "STRING", False, "valid ISIN format", "Share class identifier", False, False),
        ],
        "rules": [
            ("NAV must be positive", "RANGE", "nav > 0", "Critical", 100),
            ("NAV daily change within ±10%", "RANGE", "abs(pct_change(nav)) <= 0.10", "High", 99),
            ("Currency must be ISO 4217", "REFERENTIAL", "currency IN valid_ccys", "High", 100),
            ("No duplicate NAV per share class per date", "UNIQUENESS", "unique(share_class_id, date)", "Critical", 100),
            ("AUM = NAV × shares_outstanding (±0.1%)", "FORMULA", "abs(aum - nav*shares_outstanding)/aum <= 0.001", "Medium", 99),
        ],
    },
    {
        "contract_name": "Custodian → Portfolio Positions",
        "domain": "Portfolio", "producer": "Custodian",
        "consumer": "Gold Layer — Portfolio",
        "description": "End-of-day portfolio positions per sub-fund from the custodian. "
                       "Covers all asset classes. Used for risk, regulatory reporting, and KID.",
        "sla_delivery_h": 4, "sla_freshness_h": 8, "sla_availability_pct": 99.0,
        "fields": [
            ("isin", "STRING", False, "regex [A-Z]{2}[0-9]{10}", "Security ISIN", False, False),
            ("quantity", "NUMERIC", False, "quantity != 0", "Position quantity", False, False),
            ("market_value_usd", "NUMERIC", False, "market_value_usd > 0", "Market value in USD", False, True),
            ("weight_pct", "NUMERIC", False, "0 < weight_pct <= 100", "Portfolio weight %", False, False),
            ("sector", "STRING", True, None, "GICS sector classification", False, False),
            ("country", "STRING", False, "ISO 3166 alpha-2", "Issuer country", False, False),
            ("asset_class", "STRING", False, "in reference list", "Asset class category", False, False),
        ],
        "rules": [
            ("ISIN must match regex", "REGEX", "isin ~ [A-Z]{2}[0-9]{10}", "Critical", 100),
            ("Portfolio weights sum to 100% per sub-fund", "FORMULA", "sum(weight_pct) ~= 100 per sub_fund", "Critical", 100),
            ("No single position > 35% (UCITS)", "RANGE", "max(weight_pct) <= 35", "High", 100),
            ("Market value must be positive", "RANGE", "market_value_usd > 0", "High", 100),
        ],
    },
    {
        "contract_name": "Transfer Agent → Transactions",
        "domain": "Transaction", "producer": "Transfer Agent",
        "consumer": "Gold Layer — Transactions",
        "description": "Daily subscription, redemption, buy, and sell transactions from the TA. "
                       "Used for AUM tracking, settlement monitoring, and regulatory reporting.",
        "sla_delivery_h": 3, "sla_freshness_h": 6, "sla_availability_pct": 99.0,
        "fields": [
            ("tx_id", "STRING", False, "unique", "Transaction identifier", False, False),
            ("tx_type", "STRING", False, "in [Buy,Sell,Subscription,Redemption]", "Transaction type", False, False),
            ("gross_amount", "NUMERIC", False, "gross_amount > 0", "Gross transaction amount", False, True),
            ("quantity", "NUMERIC", False, "quantity > 0", "Units transacted", False, False),
            ("price", "NUMERIC", False, "price > 0", "Executed price", False, True),
            ("status", "STRING", False, "in [Settled,Pending,Failed,Cancelled]", "Settlement status", False, False),
            ("tx_date", "DATE", False, "tx_date <= today", "Transaction date", False, False),
        ],
        "rules": [
            ("gross_amount = quantity × price (±0.01)", "FORMULA", "abs(gross_amount - quantity*price) <= 0.01", "Critical", 100),
            ("Transaction date not in future", "DATE", "tx_date <= today", "High", 100),
            ("Status must not be null", "NOT_NULL", "status IS NOT NULL", "High", 100),
            ("Transaction ID must be unique", "UNIQUENESS", "unique(tx_id)", "Critical", 100),
        ],
    },
    {
        "contract_name": "Fund Admin → Static Data (Share Classes)",
        "domain": "Static Data", "producer": "Fund Administrator",
        "consumer": "Gold Layer — NAV",
        "description": "Master data for all share classes including ISIN, currency, fee structure, "
                       "and registration details. Source of truth for all downstream processes.",
        "sla_delivery_h": 24, "sla_freshness_h": 48, "sla_availability_pct": 99.9,
        "fields": [
            ("share_class_id", "STRING", False, "unique", "Internal share class ID", False, False),
            ("isin", "STRING", False, "regex [A-Z]{2}[0-9]{10}", "ISIN code", False, False),
            ("currency", "STRING", False, "ISO 4217", "Share class currency", False, False),
            ("mgmt_fee_pct", "NUMERIC", False, "0 <= mgmt_fee_pct <= 5", "Management fee %", False, True),
            ("nav_frequency", "STRING", False, "in [Daily,Weekly,Monthly]", "NAV calculation frequency", False, False),
            ("status", "STRING", False, "in [Active,Inactive,Pending]", "Share class status", False, False),
        ],
        "rules": [
            ("ISIN must be valid format", "REGEX", "isin ~ [A-Z]{2}[0-9]{10}", "Critical", 100),
            ("Management fee between 0% and 5%", "RANGE", "0 <= mgmt_fee_pct <= 5", "High", 100),
            ("No duplicate ISIN", "UNIQUENESS", "unique(isin)", "Critical", 100),
            ("Currency must be ISO 4217", "REFERENTIAL", "currency IN valid_ccys", "High", 100),
        ],
    },
    {
        "contract_name": "Fund Admin → Registration Matrix",
        "domain": "Registration", "producer": "Fund Administrator",
        "consumer": "Registration Team",
        "description": "Per-country registration status for each share class. "
                       "Drives KID language versions, EPT scope, and distribution eligibility.",
        "sla_delivery_h": 48, "sla_freshness_h": 168, "sla_availability_pct": 98.0,
        "fields": [
            ("share_class_id", "STRING", False, "valid reference", "Share class reference", False, False),
            ("country", "STRING", False, "ISO 3166 alpha-2", "Registration country", False, False),
            ("status", "STRING", False, "in [Registered,Pending,De-registered]", "Registration status", False, False),
            ("registration_date", "DATE", True, "date <= today", "Date of registration", False, False),
            ("regulator", "STRING", False, None, "National competent authority", False, False),
        ],
        "rules": [
            ("Status must not be null", "NOT_NULL", "status IS NOT NULL", "Critical", 100),
            ("Country must be ISO 3166", "REFERENTIAL", "country IN iso_countries", "High", 100),
            ("No duplicate (share_class_id, country)", "UNIQUENESS", "unique(share_class_id, country)", "Critical", 100),
        ],
    },
    {
        "contract_name": "Bloomberg → NAV Vendor Feed",
        "domain": "NAV", "producer": "Bloomberg",
        "consumer": "Gold Layer — NAV",
        "description": "Vendor NAV feed used for cross-validation and conflict detection "
                       "against the primary Fund Administrator feed.",
        "sla_delivery_h": 3, "sla_freshness_h": 6, "sla_availability_pct": 98.5,
        "fields": [
            ("isin", "STRING", False, "valid ISIN", "Share class ISIN", False, False),
            ("nav", "NUMERIC", False, "nav > 0", "Vendor NAV", False, True),
            ("date", "DATE", False, "date <= today", "Valuation date", False, False),
            ("source", "STRING", False, "= Bloomberg", "Data source tag", False, False),
        ],
        "rules": [
            ("NAV must be positive", "RANGE", "nav > 0", "Critical", 100),
            ("NAV variance vs Fund Admin < 0.5%", "FORMULA", "abs(nav_bb - nav_fa)/nav_fa < 0.005", "High", 95),
        ],
    },
    {
        "contract_name": "Internal System → AUM Reporting",
        "domain": "AUM", "producer": "Internal System",
        "consumer": "BI Dashboard",
        "description": "Aggregated AUM per fund and sub-fund computed from the Gold NAV layer. "
                       "Feeds into management reporting and regulatory submissions.",
        "sla_delivery_h": 6, "sla_freshness_h": 12, "sla_availability_pct": 99.0,
        "fields": [
            ("fund_id", "STRING", False, "valid reference", "Fund identifier", False, False),
            ("sub_fund_id", "STRING", False, "valid reference", "Sub-fund identifier", False, False),
            ("aum_usd", "NUMERIC", False, "aum_usd >= 0", "Total AUM in USD", False, True),
            ("date", "DATE", False, "date <= today", "Reporting date", False, False),
            ("currency", "STRING", False, "ISO 4217", "Reporting currency", False, False),
        ],
        "rules": [
            ("AUM must be non-negative", "RANGE", "aum_usd >= 0", "Critical", 100),
            ("No missing fund references", "NOT_NULL", "fund_id IS NOT NULL", "High", 100),
        ],
    },
]


def _make_contract_id(i):
    return f"DC-{i+1:03d}"


def gen_contracts(stewards=None):
    """Generate the full contract registry as a DataFrame."""
    rows = []
    if stewards is None:
        steward_pool = STEWARD_NAMES
    else:
        steward_pool = stewards["name"].tolist() if hasattr(stewards, "tolist") is False else stewards["name"].tolist()

    for i, tpl in enumerate(CONTRACT_TEMPLATES):
        owner = random.choice(steward_pool)
        created = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 180))
        effective = created + timedelta(days=random.randint(7, 30))
        expiry = effective + timedelta(days=random.randint(180, 730))
        last_val = datetime(2026, 3, 25) - timedelta(hours=random.randint(0, 48))

        # Weighted status
        status = random.choices(
            CONTRACT_STATUSES,
            weights=[60, 10, 8, 12, 5, 5], k=1
        )[0]

        breach_count = random.randint(0, 3) if status in ["Active", "Breached"] else 0
        if status == "Breached":
            breach_count = max(breach_count, 1)

        health = round(max(0.4, 1.0 - breach_count * 0.12 - random.uniform(0, 0.08)), 3)

        rows.append({
            "contract_id":         _make_contract_id(i),
            "contract_name":       tpl["contract_name"],
            "version":             f"1.{random.randint(0, 5)}.0",
            "status":              status,
            "domain":              tpl["domain"],
            "producer":            tpl["producer"],
            "consumer":            tpl["consumer"],
            "description":         tpl["description"],
            "owner":               owner,
            "created_date":        created.strftime("%Y-%m-%d"),
            "effective_date":      effective.strftime("%Y-%m-%d"),
            "expiry_date":         expiry.strftime("%Y-%m-%d"),
            "sla_delivery_h":      tpl["sla_delivery_h"],
            "sla_freshness_h":     tpl["sla_freshness_h"],
            "sla_availability_pct":tpl["sla_availability_pct"],
            "last_validated":      last_val.strftime("%Y-%m-%d %H:%M"),
            "breach_count":        breach_count,
            "health_score":        health,
            "n_fields":            len(tpl["fields"]),
            "n_rules":             len(tpl["rules"]),
        })

    return pd.DataFrame(rows)


def gen_contract_schema(contract_id):
    """Return schema fields for a given contract."""
    idx = int(contract_id.split("-")[1]) - 1
    if idx >= len(CONTRACT_TEMPLATES):
        return pd.DataFrame()
    tpl = CONTRACT_TEMPLATES[idx]
    rows = []
    for field_name, dtype, nullable, constraint, desc, pii, sensitive in tpl["fields"]:
        rows.append({
            "field_name":  field_name,
            "data_type":   dtype,
            "nullable":    "✅" if nullable else "❌",
            "constraint":  constraint or "—",
            "description": desc,
            "pii":         "🔒" if pii else "",
            "sensitive":   "🔐" if sensitive else "",
        })
    return pd.DataFrame(rows)


def gen_contract_rules(contract_id):
    """Return quality rules for a given contract."""
    idx = int(contract_id.split("-")[1]) - 1
    if idx >= len(CONTRACT_TEMPLATES):
        return pd.DataFrame()
    tpl = CONTRACT_TEMPLATES[idx]
    rows = []
    for j, (name, rtype, expr, severity, threshold) in enumerate(tpl["rules"]):
        pass_rate = round(random.uniform(threshold - 5, threshold) / 100, 4)
        rows.append({
            "rule_id":    f"{contract_id}-QR{j+1:02d}",
            "rule_name":  name,
            "type":       rtype,
            "expression": expr,
            "severity":   severity,
            "threshold":  f"{threshold}%",
            "last_pass_rate": f"{pass_rate*100:.2f}%",
            "status":     "✅ Pass" if pass_rate >= threshold/100 - 0.01 else "❌ Fail",
        })
    return pd.DataFrame(rows)


def gen_breach_history(contracts_df):
    """Generate breach history for all contracts."""
    rows = []
    for _, c in contracts_df.iterrows():
        if c["breach_count"] == 0:
            continue
        for b in range(int(c["breach_count"])):
            breach_date = datetime(2026, 1, 1) + timedelta(days=random.randint(0, 83))
            btype = random.choice(BREACH_TYPES)
            sev   = random.choice(["Critical", "High", "Medium"])
            resolved = random.random() > 0.3
            res_date = (breach_date + timedelta(hours=random.randint(1, 72))).strftime("%Y-%m-%d %H:%M") if resolved else None
            res_by   = random.choice(STEWARD_NAMES) if resolved else None

            if btype == "SLA Delivery":
                desc = f"Feed delivered {random.randint(1, 8)}h late (SLA: {c['sla_delivery_h']}h)"
            elif btype == "Data Quality":
                desc = f"Rule failure rate exceeded threshold on {random.choice(['nav','isin','gross_amount','weight_pct'])} field"
            elif btype == "Schema Violation":
                desc = f"Unexpected null values in mandatory field"
            elif btype == "Freshness Breach":
                desc = f"Data not refreshed within {c['sla_freshness_h']}h SLA window"
            else:
                desc = f"Data quality threshold breached — {random.randint(1,5)}% records affected"

            rows.append({
                "breach_id":      f"BRH-{len(rows)+1:04d}",
                "contract_id":    c["contract_id"],
                "contract_name":  c["contract_name"],
                "domain":         c["domain"],
                "breach_date":    breach_date.strftime("%Y-%m-%d %H:%M"),
                "breach_type":    btype,
                "severity":       sev,
                "description":    desc,
                "resolved":       resolved,
                "resolved_date":  res_date or "—",
                "resolved_by":    res_by or "—",
                "status":         "✅ Resolved" if resolved else "🔴 Open",
            })

    if not rows:
        return pd.DataFrame(columns=["breach_id","contract_id","contract_name","domain",
                                     "breach_date","breach_type","severity","description",
                                     "resolved","resolved_date","resolved_by","status"])
    return pd.DataFrame(rows).sort_values("breach_date", ascending=False).reset_index(drop=True)


def new_contract_dict():
    """Return an empty contract dict for the create form."""
    return {
        "contract_id":          f"DC-{random.randint(100,999)}",
        "contract_name":        "",
        "version":              "1.0.0",
        "status":               "Draft",
        "domain":               CONTRACT_DOMAINS[0],
        "producer":             PRODUCERS[0],
        "consumer":             CONSUMERS[0],
        "description":          "",
        "owner":                "",
        "effective_date":       datetime.today().strftime("%Y-%m-%d"),
        "expiry_date":          (datetime.today() + timedelta(days=365)).strftime("%Y-%m-%d"),
        "sla_delivery_h":       4,
        "sla_freshness_h":      8,
        "sla_availability_pct": 99.0,
    }
