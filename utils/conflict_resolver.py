# utils/conflict_resolver.py
# FundGov360 v5 — Data Conflict Resolver
# Inspired by Informatica MDM, Talend MDM, Atlan lineage conflict resolution

import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta
import streamlit as st

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

CONFLICT_TYPES = [
    "NAV_DISCREPANCY",
    "AUM_MISMATCH",
    "ISIN_DUPLICATE",
    "MISSING_NAV",
    "SETTLEMENT_FAILURE",
    "REGISTRATION_INCONSISTENCY",
    "SHARE_CLASS_MISMATCH",
    "PORTFOLIO_WEIGHT_BREACH",
    "CURRENCY_MISMATCH",
    "TRANSACTION_DUPLICATE",
    "COUNTERPARTY_MISMATCH",
    "STATIC_DATA_CONFLICT",
    "BENCHMARK_MISMATCH",
    "FEE_DISCREPANCY",
    "VALUATION_DATE_GAP",
    "MISSING_ISIN",
    "INVESTOR_ID_MISSING",
    "EXPIRY_DATE_BREACH",
    "STALE_PRICE",
    "WEIGHT_SUM_BREACH",
    "INVALID_STATUS",
    "INCEPTION_DATE_CONFLICT",
]

CONFLICT_STATUSES = ["Open", "Under Review", "Resolved", "Escalated", "Auto-Resolved", "Rejected"]

RESOLUTION_METHODS = [
    "Source Priority",
    "Latest Timestamp",
    "Manual Override",
    "Golden Record Merge",
    "Majority Vote",
    "Weighted Average",
    "Escalated to Owner",
    "Auto-Resolved",
    "Rejected — Duplicate",
]

PRIORITY_LEVELS = ["P1 – Critical", "P2 – High", "P3 – Medium", "P4 – Low"]

DATA_SOURCES = ["Fund Administrator", "Custodian", "Transfer Agent", "Bloomberg", "Reuters", "Internal"]

DEPARTMENTS   = ["Data Management", "Compliance", "Operations", "Finance", "Risk", "Fund Accounting"]
RESOLVER_NAMES = [
    "Sophie Martin", "James O'Brien", "Clara Muller", "Hugo Lefevre",
    "Amara Diallo", "Luca Bianchi", "Mei Lin", "Patrick Walsh",
    "Fatima El-Amin", "Björn Lindqvist",
]

# Source trust hierarchy (higher = more trusted)
SOURCE_TRUST = {
    "Fund Administrator": 5,
    "Bloomberg":          4,
    "Custodian":          4,
    "Reuters":            3,
    "Transfer Agent":     3,
    "Internal":           2,
}

# Auto-resolvable conflict types and their strategies
AUTO_RESOLVE_STRATEGIES = {
    "NAV_DISCREPANCY":             "Source Priority",
    "AUM_MISMATCH":                "Weighted Average",
    "MISSING_NAV":                 "Latest Timestamp",
    "SETTLEMENT_FAILURE":          "Latest Timestamp",
    "ISIN_DUPLICATE":              "Rejected — Duplicate",
    "TRANSACTION_DUPLICATE":       "Rejected — Duplicate",
    "PORTFOLIO_WEIGHT_BREACH":     "Source Priority",
    "STALE_PRICE":                 "Latest Timestamp",
    "WEIGHT_SUM_BREACH":           "Source Priority",
    "CURRENCY_MISMATCH":           "Source Priority",
    "MISSING_ISIN":                "Escalated to Owner",
    "INVESTOR_ID_MISSING":         "Escalated to Owner",
}

# ─────────────────────────────────────────────
# CONFLICT SCENARIO LIBRARY  (22 scenarios)
# ─────────────────────────────────────────────

CONFLICT_SCENARIOS = [
    {
        "scenario_id":   "CS-001",
        "conflict_type": "NAV_DISCREPANCY",
        "title":         "NAV mismatch between Fund Admin and Bloomberg",
        "description":   "Fund Administrator reports NAV = 142.35, Bloomberg reports 141.90 for the same share class and date.",
        "dataset":       "nav",
        "field":         "nav",
        "source_a":      "Fund Administrator",
        "value_a":       142.35,
        "source_b":      "Bloomberg",
        "value_b":       141.90,
        "delta":         0.45,
        "delta_pct":     0.32,
        "priority":      "P2 – High",
        "auto_resolvable": True,
        "resolution_strategy": "Source Priority",
        "recommended_value": 142.35,
        "resolution_note": "Fund Administrator has highest trust level (5). Bloomberg value rejected.",
    },
    {
        "scenario_id":   "CS-002",
        "conflict_type": "AUM_MISMATCH",
        "title":         "AUM discrepancy across two sources",
        "description":   "Custodian reports AUM = €1.24B, Fund Admin reports €1.19B for same sub-fund.",
        "dataset":       "nav",
        "field":         "aum",
        "source_a":      "Custodian",
        "value_a":       1_240_000_000,
        "source_b":      "Fund Administrator",
        "value_b":       1_190_000_000,
        "delta":         50_000_000,
        "delta_pct":     4.03,
        "priority":      "P2 – High",
        "auto_resolvable": True,
        "resolution_strategy": "Weighted Average",
        "recommended_value": 1_215_000_000,
        "resolution_note": "Weighted average applied pending investigation. Escalation triggered if delta > 5%.",
    },
    {
        "scenario_id":   "CS-003",
        "conflict_type": "ISIN_DUPLICATE",
        "title":         "Duplicate ISIN across two share classes",
        "description":   "Two share class records share the same ISIN LU0123456789. One is a data entry error.",
        "dataset":       "share_classes",
        "field":         "isin",
        "source_a":      "Internal",
        "value_a":       "LU0123456789",
        "source_b":      "Internal",
        "value_b":       "LU0123456789",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P1 – Critical",
        "auto_resolvable": True,
        "resolution_strategy": "Rejected — Duplicate",
        "recommended_value": None,
        "resolution_note": "Newer record flagged for deletion. Golden record retains older inception date.",
    },
    {
        "scenario_id":   "CS-004",
        "conflict_type": "MISSING_NAV",
        "title":         "NAV missing for T-1 business day",
        "description":   "Share class SC015 has no NAV record for yesterday. Previous NAV carried forward pending resolution.",
        "dataset":       "nav",
        "field":         "nav",
        "source_a":      "Fund Administrator",
        "value_a":       None,
        "source_b":      "Bloomberg",
        "value_b":       98.12,
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P1 – Critical",
        "auto_resolvable": True,
        "resolution_strategy": "Latest Timestamp",
        "recommended_value": 98.12,
        "resolution_note": "Bloomberg value used as interim. Fund Admin confirmation pending.",
    },
    {
        "scenario_id":   "CS-005",
        "conflict_type": "SETTLEMENT_FAILURE",
        "title":         "Transaction settlement past T+3 threshold",
        "description":   "Transaction TX00342 was booked on 2025-11-01 but remains unsettled after 5 business days.",
        "dataset":       "transactions",
        "field":         "settlement_status",
        "source_a":      "Transfer Agent",
        "value_a":       "Pending",
        "source_b":      "Custodian",
        "value_b":       "Failed",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P1 – Critical",
        "auto_resolvable": False,
        "resolution_strategy": "Escalated to Owner",
        "recommended_value": "Failed",
        "resolution_note": "Custodian status takes precedence. Ops team notified.",
    },
    {
        "scenario_id":   "CS-006",
        "conflict_type": "REGISTRATION_INCONSISTENCY",
        "title":         "Registration status conflict — Luxembourg",
        "description":   "Compliance system shows 'Registered' for SC020 in Luxembourg, but Legal export shows 'Pending'.",
        "dataset":       "registration",
        "field":         "reg_status",
        "source_a":      "Internal",
        "value_a":       "Registered",
        "source_b":      "Legal & Compliance",
        "value_b":       "Pending",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P1 – Critical",
        "auto_resolvable": False,
        "resolution_strategy": "Escalated to Owner",
        "recommended_value": "Pending",
        "resolution_note": "Conservative value applied. Compliance officer alerted.",
    },
    {
        "scenario_id":   "CS-007",
        "conflict_type": "PORTFOLIO_WEIGHT_BREACH",
        "title":         "Portfolio total weight exceeds 100%",
        "description":   "Sub-fund SF003 total portfolio weights sum to 103.2% — 3.2% overage across 4 positions.",
        "dataset":       "portfolio",
        "field":         "weight_pct",
        "source_a":      "Custodian",
        "value_a":       103.2,
        "source_b":      "Fund Administrator",
        "value_b":       100.0,
        "delta":         3.2,
        "delta_pct":     3.2,
        "priority":      "P2 – High",
        "auto_resolvable": True,
        "resolution_strategy": "Source Priority",
        "recommended_value": 100.0,
        "resolution_note": "Fund Admin weights normalised to 100%. Custodian error flagged.",
    },
    {
        "scenario_id":   "CS-008",
        "conflict_type": "CURRENCY_MISMATCH",
        "title":         "Currency mismatch on NAV record",
        "description":   "NAV reported in GBP by Fund Admin but share class currency is EUR.",
        "dataset":       "nav",
        "field":         "currency",
        "source_a":      "Fund Administrator",
        "value_a":       "GBP",
        "source_b":      "Internal",
        "value_b":       "EUR",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P2 – High",
        "auto_resolvable": True,
        "resolution_strategy": "Source Priority",
        "recommended_value": "EUR",
        "resolution_note": "Golden record currency EUR retained. Fund Admin NAV rejected pending correction.",
    },
    {
        "scenario_id":   "CS-009",
        "conflict_type": "TRANSACTION_DUPLICATE",
        "title":         "Duplicate transaction TX00512",
        "description":   "Transaction TX00512 appears twice in the Transfer Agent feed with identical amount, date, and investor ID.",
        "dataset":       "transactions",
        "field":         "tx_id",
        "source_a":      "Transfer Agent",
        "value_a":       "TX00512",
        "source_b":      "Transfer Agent",
        "value_b":       "TX00512",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P1 – Critical",
        "auto_resolvable": True,
        "resolution_strategy": "Rejected — Duplicate",
        "recommended_value": None,
        "resolution_note": "Second record suppressed. Transfer Agent notified to fix feed.",
    },
    {
        "scenario_id":   "CS-010",
        "conflict_type": "STATIC_DATA_CONFLICT",
        "title":         "Sub-fund inception date mismatch",
        "description":   "Bloomberg reports inception date 2014-09-01, Fund Admin reports 2014-07-15 for SF003.",
        "dataset":       "share_classes",
        "field":         "inception_date",
        "source_a":      "Bloomberg",
        "value_a":       "2014-09-01",
        "source_b":      "Fund Administrator",
        "value_b":       "2014-07-15",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P3 – Medium",
        "auto_resolvable": False,
        "resolution_strategy": "Manual Override",
        "recommended_value": "2014-07-15",
        "resolution_note": "Fund Admin prospectus date preferred. Bloomberg reference data update requested.",
    },
    {
        "scenario_id":   "CS-011",
        "conflict_type": "BENCHMARK_MISMATCH",
        "title":         "Benchmark reference conflict for SF001",
        "description":   "Internal system uses 'MSCI World NR USD', Bloomberg uses 'MSCI World TR USD'.",
        "dataset":       "nav",
        "field":         "source",
        "source_a":      "Internal",
        "value_a":       "MSCI World NR USD",
        "source_b":      "Bloomberg",
        "value_b":       "MSCI World TR USD",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P3 – Medium",
        "auto_resolvable": False,
        "resolution_strategy": "Escalated to Owner",
        "recommended_value": "MSCI World NR USD",
        "resolution_note": "Fund prospectus specifies NR. Bloomberg mapping corrected.",
    },
    {
        "scenario_id":   "CS-012",
        "conflict_type": "FEE_DISCREPANCY",
        "title":         "Management fee rate conflict",
        "description":   "Fund Admin reports management fee 0.75%, Bloomberg shows 0.85% for SF001 Class A.",
        "dataset":       "share_classes",
        "field":         "min_investment",
        "source_a":      "Fund Administrator",
        "value_a":       0.75,
        "source_b":      "Bloomberg",
        "value_b":       0.85,
        "delta":         0.10,
        "delta_pct":     13.3,
        "priority":      "P3 – Medium",
        "auto_resolvable": False,
        "resolution_strategy": "Manual Override",
        "recommended_value": 0.75,
        "resolution_note": "Fund prospectus fee schedule confirmed at 0.75%. Bloomberg data correction submitted.",
    },
    {
        "scenario_id":   "CS-013",
        "conflict_type": "VALUATION_DATE_GAP",
        "title":         "Portfolio valuation date lag — 3 days",
        "description":   "Portfolio holdings for SF005 have valuation date T-3 vs expected T-1.",
        "dataset":       "portfolio",
        "field":         "valuation_date",
        "source_a":      "Custodian",
        "value_a":       "T-3",
        "source_b":      "Fund Administrator",
        "value_b":       "T-1",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P2 – High",
        "auto_resolvable": False,
        "resolution_strategy": "Escalated to Owner",
        "recommended_value": "T-1",
        "resolution_note": "Custodian delivery SLA breach. Operations team alerted.",
    },
    {
        "scenario_id":   "CS-014",
        "conflict_type": "MISSING_ISIN",
        "title":         "ISIN missing on portfolio holding",
        "description":   "Position SF002_P014 has no ISIN — possibly OTC instrument or data entry error.",
        "dataset":       "portfolio",
        "field":         "isin",
        "source_a":      "Custodian",
        "value_a":       None,
        "source_b":      "Bloomberg",
        "value_b":       None,
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P2 – High",
        "auto_resolvable": True,
        "resolution_strategy": "Escalated to Owner",
        "recommended_value": None,
        "resolution_note": "Data Steward assigned. Bloomberg lookup initiated.",
    },
    {
        "scenario_id":   "CS-015",
        "conflict_type": "INVESTOR_ID_MISSING",
        "title":         "Investor ID absent on redemption",
        "description":   "Transaction TX00688 (Redemption, €2.1M) has no investor ID — AML compliance risk.",
        "dataset":       "transactions",
        "field":         "investor_id",
        "source_a":      "Transfer Agent",
        "value_a":       None,
        "source_b":      "Internal",
        "value_b":       None,
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P1 – Critical",
        "auto_resolvable": True,
        "resolution_strategy": "Escalated to Owner",
        "recommended_value": None,
        "resolution_note": "Compliance officer notified. Transaction frozen pending ID verification.",
    },
    {
        "scenario_id":   "CS-016",
        "conflict_type": "EXPIRY_DATE_BREACH",
        "title":         "Registration expiry date past for SC030 in Germany",
        "description":   "Passporting registration for SC030 in Germany expired 30 days ago with no renewal on record.",
        "dataset":       "registration",
        "field":         "expiry_date",
        "source_a":      "Legal & Compliance",
        "value_a":       "2025-09-30",
        "source_b":      "Internal",
        "value_b":       None,
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P1 – Critical",
        "auto_resolvable": False,
        "resolution_strategy": "Escalated to Owner",
        "recommended_value": None,
        "resolution_note": "Distribution in Germany suspended. Legal team escalated immediately.",
    },
    {
        "scenario_id":   "CS-017",
        "conflict_type": "STALE_PRICE",
        "title":         "Security price not updated for 5 business days",
        "description":   "Position SF001_P007 (Pharma Corp PLC) shows last price update 5 days ago — illiquid bond.",
        "dataset":       "portfolio",
        "field":         "price",
        "source_a":      "Bloomberg",
        "value_a":       98.45,
        "source_b":      "Custodian",
        "value_b":       97.80,
        "delta":         0.65,
        "delta_pct":     0.66,
        "priority":      "P3 – Medium",
        "auto_resolvable": True,
        "resolution_strategy": "Latest Timestamp",
        "recommended_value": 97.80,
        "resolution_note": "Custodian provides more recent mark. Bloomberg stale flag raised.",
    },
    {
        "scenario_id":   "CS-018",
        "conflict_type": "WEIGHT_SUM_BREACH",
        "title":         "Sub-fund total allocation below 95%",
        "description":   "SF006 portfolio weights sum to 91.3% — potential uninvested cash not classified.",
        "dataset":       "portfolio",
        "field":         "weight_pct",
        "source_a":      "Custodian",
        "value_a":       91.3,
        "source_b":      "Fund Administrator",
        "value_b":       98.7,
        "delta":         7.4,
        "delta_pct":     7.4,
        "priority":      "P2 – High",
        "auto_resolvable": True,
        "resolution_strategy": "Source Priority",
        "recommended_value": 98.7,
        "resolution_note": "Fund Admin positions used. Missing 1.3% allocated to Cash & Equivalents.",
    },
    {
        "scenario_id":   "CS-019",
        "conflict_type": "COUNTERPARTY_MISMATCH",
        "title":         "Counterparty name discrepancy on transaction",
        "description":   "TX00789: Transfer Agent shows 'JP Morgan AM', Custodian shows 'JPMorgan Asset Management'.",
        "dataset":       "transactions",
        "field":         "counterparty",
        "source_a":      "Transfer Agent",
        "value_a":       "JP Morgan AM",
        "source_b":      "Custodian",
        "value_b":       "JPMorgan Asset Management",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P4 – Low",
        "auto_resolvable": True,
        "resolution_strategy": "Golden Record Merge",
        "recommended_value": "JPMorgan Asset Management",
        "resolution_note": "Legal entity name standardised using LEI lookup. Master reference updated.",
    },
    {
        "scenario_id":   "CS-020",
        "conflict_type": "SHARE_CLASS_MISMATCH",
        "title":         "Hedged flag inconsistency on share class",
        "description":   "SC025 flagged as Hedged=True in internal system but Bloomberg shows no hedge suffix.",
        "dataset":       "share_classes",
        "field":         "hedged",
        "source_a":      "Internal",
        "value_a":       True,
        "source_b":      "Bloomberg",
        "value_b":       False,
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P3 – Medium",
        "auto_resolvable": False,
        "resolution_strategy": "Manual Override",
        "recommended_value": True,
        "resolution_note": "Prospectus confirms currency hedge. Bloomberg reference correction submitted.",
    },
    {
        "scenario_id":   "CS-021",
        "conflict_type": "INVALID_STATUS",
        "title":         "Share class status value out of domain",
        "description":   "SC031 has status = 'Dormant' which is not in the accepted domain [Active, Closed, Suspended].",
        "dataset":       "share_classes",
        "field":         "status",
        "source_a":      "Internal",
        "value_a":       "Dormant",
        "source_b":      "Fund Administrator",
        "value_b":       "Closed",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P2 – High",
        "auto_resolvable": True,
        "resolution_strategy": "Source Priority",
        "recommended_value": "Closed",
        "resolution_note": "Fund Admin value 'Closed' applied. Internal system reference data corrected.",
    },
    {
        "scenario_id":   "CS-022",
        "conflict_type": "INCEPTION_DATE_CONFLICT",
        "title":         "Share class inception date predates sub-fund launch",
        "description":   "SC008 inception date 2010-01-01 is earlier than parent sub-fund SF002 launch 2012-03-15.",
        "dataset":       "share_classes",
        "field":         "inception_date",
        "source_a":      "Internal",
        "value_a":       "2010-01-01",
        "source_b":      "Fund Administrator",
        "value_b":       "2012-06-01",
        "delta":         None,
        "delta_pct":     None,
        "priority":      "P2 – High",
        "auto_resolvable": False,
        "resolution_strategy": "Manual Override",
        "recommended_value": "2012-06-01",
        "resolution_note": "Child cannot predate parent. Fund Admin date applied. Lineage inconsistency logged.",
    },
]

# ─────────────────────────────────────────────
# SESSION STATE MANAGEMENT
# ─────────────────────────────────────────────

def init_resolver_state() -> None:
    """Initialise Streamlit session state for the conflict resolver."""
    if "conflicts" not in st.session_state:
        st.session_state["conflicts"] = _generate_initial_conflicts()
    if "audit_trail" not in st.session_state:
        st.session_state["audit_trail"] = []
    if "resolution_stats" not in st.session_state:
        st.session_state["resolution_stats"] = {}


def _generate_initial_conflicts() -> list[dict]:
    """Seed the conflict queue from scenarios with randomised metadata."""
    conflicts = []
    statuses_pool = ["Open", "Open", "Open", "Under Review", "Escalated"]
    for i, sc in enumerate(CONFLICT_SCENARIOS):
        days_ago = random.randint(0, 30)
        conflicts.append({
            "conflict_id":         f"CF-{i+1:04d}",
            "scenario_id":         sc["scenario_id"],
            "conflict_type":       sc["conflict_type"],
            "title":               sc["title"],
            "description":         sc["description"],
            "dataset":             sc["dataset"],
            "field":               sc["field"],
            "source_a":            sc["source_a"],
            "value_a":             sc["value_a"],
            "source_b":            sc["source_b"],
            "value_b":             sc["value_b"],
            "delta":               sc["delta"],
            "delta_pct":           sc["delta_pct"],
            "priority":            sc["priority"],
            "status":              random.choice(statuses_pool),
            "auto_resolvable":     sc["auto_resolvable"],
            "resolution_strategy": sc["resolution_strategy"],
            "recommended_value":   sc["recommended_value"],
            "resolution_note":     sc["resolution_note"],
            "assigned_to":         random.choice(RESOLVER_NAMES),
            "department":          random.choice(DEPARTMENTS),
            "detected_at":         (datetime.now() - timedelta(days=days_ago, hours=random.randint(0, 12))).strftime("%Y-%m-%d %H:%M"),
            "resolved_at":         None,
            "resolved_by":         None,
            "resolution_method":   None,
            "final_value":         None,
            "resolution_comment":  None,
            "sla_hours":           _get_sla_hours(sc["priority"]),
            "sla_breached":        random.choices([False, True], weights=[0.75, 0.25])[0],
            "escalation_count":    random.randint(0, 2),
            "linked_rule_id":      None,
        })
    return conflicts


def _get_sla_hours(priority: str) -> int:
    """Return SLA resolution target in hours by priority."""
    return {
        "P1 – Critical": 4,
        "P2 – High":     24,
        "P3 – Medium":   72,
        "P4 – Low":      168,
    }.get(priority, 72)


def get_conflicts() -> list[dict]:
    init_resolver_state()
    return st.session_state["conflicts"]


def get_conflicts_df() -> pd.DataFrame:
    return pd.DataFrame(get_conflicts())


# ─────────────────────────────────────────────
# CONFLICT DETECTION  (from live data)
# ─────────────────────────────────────────────

def detect_conflicts(
    nav_df: pd.DataFrame,
    sc_df: pd.DataFrame,
    tx_df: pd.DataFrame,
    port_df: pd.DataFrame,
    reg_df: pd.DataFrame,
) -> list[dict]:
    """
    Detect data conflicts from live datasets.
    Returns a list of new conflict dicts ready to be appended to the queue.
    """
    detected = []
    run_ts   = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── NAV: missing values
    if not nav_df.empty and "nav" in nav_df.columns:
        missing = nav_df[nav_df["nav"].isna()]
        if not missing.empty:
            for _, row in missing.head(5).iterrows():
                detected.append(_make_conflict(
                    conflict_type  = "MISSING_NAV",
                    title          = f"Missing NAV — {row.get('sc_id', '?')} on {row.get('date', '?')}",
                    description    = f"No NAV value found for share class {row.get('sc_id')} on {row.get('date')}.",
                    dataset        = "nav",
                    field          = "nav",
                    source_a       = row.get("source", "Unknown"),
                    value_a        = None,
                    source_b       = "Expected",
                    value_b        = "Non-null NAV",
                    priority       = "P1 – Critical",
                    auto_resolvable = True,
                    detected_at    = run_ts,
                ))

    # ── NAV: range breach
    if not nav_df.empty and "nav" in nav_df.columns:
        breach = nav_df[(nav_df["nav"] < 0.01) | (nav_df["nav"] > 100_000)]
        for _, row in breach.head(3).iterrows():
            detected.append(_make_conflict(
                conflict_type  = "NAV_DISCREPANCY",
                title          = f"NAV out of valid range — {row.get('sc_id')}",
                description    = f"NAV = {row.get('nav')} is outside valid range [0.01, 100000].",
                dataset        = "nav",
                field          = "nav",
                source_a       = row.get("source", "Unknown"),
                value_a        = row.get("nav"),
                source_b       = "Validation Rule BR-002",
                value_b        = "[0.01, 100000]",
                priority       = "P1 – Critical",
                auto_resolvable = False,
                detected_at    = run_ts,
            ))

    # ── Transactions: duplicate tx_id
    if not tx_df.empty and "tx_id" in tx_df.columns:
        dupes = tx_df[tx_df["tx_id"].duplicated(keep=False)]
        if not dupes.empty:
            for tx_id in dupes["tx_id"].unique()[:3]:
                detected.append(_make_conflict(
                    conflict_type  = "TRANSACTION_DUPLICATE",
                    title          = f"Duplicate transaction {tx_id}",
                    description    = f"Transaction {tx_id} appears multiple times in the feed.",
                    dataset        = "transactions",
                    field          = "tx_id",
                    source_a       = "Transfer Agent",
                    value_a        = tx_id,
                    source_b       = "Transfer Agent",
                    value_b        = tx_id,
                    priority       = "P1 – Critical",
                    auto_resolvable = True,
                    detected_at    = run_ts,
                ))

    # ── Transactions: missing investor ID
    if not tx_df.empty and "investor_id" in tx_df.columns:
        missing_inv = tx_df[tx_df["investor_id"].isna()]
        if not missing_inv.empty:
            detected.append(_make_conflict(
                conflict_type  = "INVESTOR_ID_MISSING",
                title          = f"Investor ID missing on {len(missing_inv)} transaction(s)",
                description    = f"{len(missing_inv)} transactions have no investor ID — potential AML risk.",
                dataset        = "transactions",
                field          = "investor_id",
                source_a       = "Transfer Agent",
                value_a        = None,
                source_b       = "Compliance",
                value_b        = "Required",
                priority       = "P1 – Critical",
                auto_resolvable = True,
                detected_at    = run_ts,
            ))

    # ── Portfolio: weight sum breach per sub-fund
    if not port_df.empty and "weight_pct" in port_df.columns:
        weight_sums = port_df.groupby("sub_fund_id")["weight_pct"].sum()
        for sf_id, total in weight_sums.items():
            if total > 100.5 or total < 90.0:
                detected.append(_make_conflict(
                    conflict_type  = "WEIGHT_SUM_BREACH",
                    title          = f"Portfolio weight breach — {sf_id} ({total:.1f}%)",
                    description    = f"Total portfolio weight for {sf_id} = {total:.2f}%, expected ~100%.",
                    dataset        = "portfolio",
                    field          = "weight_pct",
                    source_a       = "Custodian",
                    value_a        = round(total, 2),
                    source_b       = "Expected",
                    value_b        = 100.0,
                    priority       = "P2 – High",
                    auto_resolvable = True,
                    detected_at    = run_ts,
                ))

    # ── Share classes: duplicate ISIN
    if not sc_df.empty and "isin" in sc_df.columns:
        dupe_isin = sc_df[sc_df["isin"].duplicated(keep=False)]
        if not dupe_isin.empty:
            for isin in dupe_isin["isin"].unique()[:3]:
                detected.append(_make_conflict(
                    conflict_type  = "ISIN_DUPLICATE",
                    title          = f"Duplicate ISIN {isin} on share classes",
                    description    = f"ISIN {isin} is assigned to more than one share class record.",
                    dataset        = "share_classes",
                    field          = "isin",
                    source_a       = "Internal",
                    value_a        = isin,
                    source_b       = "Internal",
                    value_b        = isin,
                    priority       = "P1 – Critical",
                    auto_resolvable = True,
                    detected_at    = run_ts,
                ))

    # ── Registration: expired registrations
    if not reg_df.empty and "expiry_date" in reg_df.columns:
        today = datetime.today().date()
        reg_copy = reg_df.copy()
        reg_copy["expiry_date"] = pd.to_datetime(reg_copy["expiry_date"], errors="coerce").dt.date
        expired = reg_copy[
            (reg_copy["expiry_date"].notna()) &
            (reg_copy["expiry_date"] < today) &
            (reg_copy["reg_status"] == "Registered")
        ]
        if not expired.empty:
            detected.append(_make_conflict(
                conflict_type  = "EXPIRY_DATE_BREACH",
                title          = f"{len(expired)} registration(s) past expiry date",
                description    = f"{len(expired)} share class registrations are expired but still marked 'Registered'.",
                dataset        = "registration",
                field          = "expiry_date",
                source_a       = "Legal & Compliance",
                value_a        = f"{len(expired)} records",
                source_b       = "Expected",
                value_b        = "Valid or renewed",
                priority       = "P1 – Critical",
                auto_resolvable = False,
                detected_at    = run_ts,
            ))

    return detected


def _make_conflict(
    conflict_type: str,
    title: str,
    description: str,
    dataset: str,
    field: str,
    source_a: str,
    value_a,
    source_b: str,
    value_b,
    priority: str,
    auto_resolvable: bool,
    detected_at: str,
) -> dict:
    """Factory function for new conflict dicts."""
    strategy = AUTO_RESOLVE_STRATEGIES.get(conflict_type, "Escalated to Owner")
    return {
        "conflict_id":         f"CF-{uuid.uuid4().hex[:8].upper()}",
        "scenario_id":         None,
        "conflict_type":       conflict_type,
        "title":               title,
        "description":         description,
        "dataset":             dataset,
        "field":               field,
        "source_a":            source_a,
        "value_a":             value_a,
        "source_b":            source_b,
        "value_b":             value_b,
        "delta":               None,
        "delta_pct":           None,
        "priority":            priority,
        "status":              "Open",
        "auto_resolvable":     auto_resolvable,
        "resolution_strategy": strategy,
        "recommended_value":   None,
        "resolution_note":     None,
        "assigned_to":         random.choice(RESOLVER_NAMES),
        "department":          random.choice(DEPARTMENTS),
        "detected_at":         detected_at,
        "resolved_at":         None,
        "resolved_by":         None,
        "resolution_method":   None,
        "final_value":         None,
        "resolution_comment":  None,
        "sla_hours":           _get_sla_hours(priority),
        "sla_breached":        False,
        "escalation_count":    0,
        "linked_rule_id":      None,
    }


# ─────────────────────────────────────────────
# AUTO-RESOLVER  (Informatica MDM-inspired)
# ─────────────────────────────────────────────

def auto_resolve_conflicts(resolver_name: str = "FundGov360 Auto-Resolver") -> tuple[int, int]:
    """
    Attempt to auto-resolve all Open conflicts marked auto_resolvable=True.
    Returns (resolved_count, skipped_count).
    """
    init_resolver_state()
    resolved = 0
    skipped  = 0
    ts       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for conflict in st.session_state["conflicts"]:
        if conflict["status"] != "Open":
            skipped += 1
            continue
        if not conflict.get("auto_resolvable", False):
            skipped += 1
            continue

        strategy = conflict.get("resolution_strategy", "Source Priority")
        final_value, note = _apply_strategy(conflict, strategy)

        conflict["status"]             = "Auto-Resolved"
        conflict["resolution_method"]  = strategy
        conflict["final_value"]        = final_value
        conflict["resolved_at"]        = ts
        conflict["resolved_by"]        = resolver_name
        conflict["resolution_comment"] = note

        _log_audit(
            conflict_id    = conflict["conflict_id"],
            action         = "AUTO_RESOLVED",
            performed_by   = resolver_name,
            details        = f"Strategy: {strategy} | Final value: {final_value} | {note}",
            timestamp      = ts,
        )
        resolved += 1

    _update_resolution_stats()
    return resolved, skipped


def _apply_strategy(conflict: dict, strategy: str) -> tuple:
    """Apply a resolution strategy and return (final_value, note)."""
    source_a   = conflict.get("source_a", "")
    source_b   = conflict.get("source_b", "")
    value_a    = conflict.get("value_a")
    value_b    = conflict.get("value_b")
    trust_a    = SOURCE_TRUST.get(source_a, 1)
    trust_b    = SOURCE_TRUST.get(source_b, 1)

    if strategy == "Source Priority":
        if trust_a >= trust_b:
            return value_a, f"'{source_a}' selected (trust={trust_a}) over '{source_b}' (trust={trust_b})."
        else:
            return value_b, f"'{source_b}' selected (trust={trust_b}) over '{source_a}' (trust={trust_a})."

    elif strategy == "Weighted Average":
        try:
            w_total = trust_a + trust_b
            avg     = round((float(value_a) * trust_a + float(value_b) * trust_b) / w_total, 4)
            return avg, f"Weighted average: ({value_a}×{trust_a} + {value_b}×{trust_b}) / {w_total} = {avg}."
        except (TypeError, ValueError):
            return value_a, f"Weighted average failed — defaulted to source A value."

    elif strategy == "Latest Timestamp":
        non_null = value_b if value_a is None else value_a
        return non_null, "Most recent non-null value selected."

    elif strategy == "Rejected — Duplicate":
        return None, "Duplicate record suppressed. Original retained in golden record."

    elif strategy == "Golden Record Merge":
        merged = value_b if value_b else value_a
        return merged, f"Golden record updated to standardised value from '{source_b}'."

    elif strategy == "Escalated to Owner":
        return None, "Auto-resolution not possible. Escalated to data owner."

    else:
        return value_a, f"Default: source A value retained (strategy: {strategy})."


# ─────────────────────────────────────────────
# MANUAL RESOLUTION
# ─────────────────────────────────────────────

def resolve_conflict(
    conflict_id:    str,
    final_value,
    resolution_method: str,
    resolved_by:    str,
    comment:        str = "",
) -> bool:
    """
    Manually resolve a conflict by ID.
    Returns True if found and updated, False otherwise.
    """
    init_resolver_state()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for conflict in st.session_state["conflicts"]:
        if conflict["conflict_id"] == conflict_id:
            conflict["status"]             = "Resolved"
            conflict["final_value"]        = final_value
            conflict["resolution_method"]  = resolution_method
            conflict["resolved_at"]        = ts
            conflict["resolved_by"]        = resolved_by
            conflict["resolution_comment"] = comment

            _log_audit(
                conflict_id  = conflict_id,
                action       = "MANUALLY_RESOLVED",
                performed_by = resolved_by,
                details      = f"Method: {resolution_method} | Value: {final_value} | {comment}",
                timestamp    = ts,
            )
            _update_resolution_stats()
            return True
    return False


def escalate_conflict(conflict_id: str, escalated_by: str, reason: str = "") -> bool:
    """Escalate a conflict, increment its escalation counter."""
    init_resolver_state()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for conflict in st.session_state["conflicts"]:
        if conflict["conflict_id"] == conflict_id:
            conflict["status"]           = "Escalated"
            conflict["escalation_count"] = conflict.get("escalation_count", 0) + 1

            _log_audit(
                conflict_id  = conflict_id,
                action       = "ESCALATED",
                performed_by = escalated_by,
                details      = f"Escalation #{conflict['escalation_count']} | Reason: {reason}",
                timestamp    = ts,
            )
            return True
    return False


def reject_conflict(conflict_id: str, rejected_by: str, reason: str = "") -> bool:
    """Reject (dismiss) a conflict as invalid or out of scope."""
    init_resolver_state()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for conflict in st.session_state["conflicts"]:
        if conflict["conflict_id"] == conflict_id:
            conflict["status"]             = "Rejected"
            conflict["resolved_at"]        = ts
            conflict["resolved_by"]        = rejected_by
            conflict["resolution_comment"] = reason

            _log_audit(
                conflict_id  = conflict_id,
                action       = "REJECTED",
                performed_by = rejected_by,
                details      = f"Reason: {reason}",
                timestamp    = ts,
            )
            return True
    return False


def reassign_conflict(conflict_id: str, new_assignee: str, reassigned_by: str) -> bool:
    """Reassign a conflict to a different resolver."""
    init_resolver_state()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for conflict in st.session_state["conflicts"]:
        if conflict["conflict_id"] == conflict_id:
            old_assignee = conflict.get("assigned_to", "—")
            conflict["assigned_to"] = new_assignee
            conflict["status"]      = "Under Review" if conflict["status"] == "Open" else conflict["status"]

            _log_audit(
                conflict_id  = conflict_id,
                action       = "REASSIGNED",
                performed_by = reassigned_by,
                details      = f"From: {old_assignee} → To: {new_assignee}",
                timestamp    = ts,
            )
            return True
    return False


# ─────────────────────────────────────────────
# AUDIT TRAIL  (Atlan lineage-inspired)
# ─────────────────────────────────────────────

def _log_audit(
    conflict_id:  str,
    action:       str,
    performed_by: str,
    details:      str,
    timestamp:    str,
) -> None:
    """Append an entry to the audit trail."""
    init_resolver_state()
    st.session_state["audit_trail"].append({
        "audit_id":     f"AUD-{uuid.uuid4().hex[:8].upper()}",
        "conflict_id":  conflict_id,
        "action":       action,
        "performed_by": performed_by,
        "details":      details,
        "timestamp":    timestamp,
    })


def get_audit_trail(conflict_id: str = None) -> pd.DataFrame:
    """
    Return audit trail as DataFrame.
    If conflict_id is provided, filter to that conflict only.
    """
    init_resolver_state()
    df = pd.DataFrame(st.session_state["audit_trail"])
    if df.empty:
        return df
    if conflict_id:
        df = df[df["conflict_id"] == conflict_id].reset_index(drop=True)
    return df.sort_values("timestamp", ascending=False).reset_index(drop=True)


# ─────────────────────────────────────────────
# STATISTICS & KPIs
# ─────────────────────────────────────────────

def _update_resolution_stats() -> None:
    """Recompute resolution statistics and store in session state."""
    conflicts = get_conflicts()
    total     = len(conflicts)
    by_status = {}
    for c in conflicts:
        s = c.get("status", "Open")
        by_status[s] = by_status.get(s, 0) + 1

    open_c        = by_status.get("Open", 0)
    resolved_c    = by_status.get("Resolved", 0) + by_status.get("Auto-Resolved", 0)
    escalated_c   = by_status.get("Escalated", 0)
    sla_breaches  = sum(1 for c in conflicts if c.get("sla_breached", False))
    critical_open = sum(1 for c in conflicts if c.get("status") == "Open" and c.get("priority") == "P1 – Critical")
    auto_resolved = by_status.get("Auto-Resolved", 0)

    # Average resolution time (hours) for resolved conflicts
    res_times = []
    for c in conflicts:
        if c.get("resolved_at") and c.get("detected_at"):
            try:
                det = datetime.strptime(c["detected_at"], "%Y-%m-%d %H:%M")
                res = datetime.strptime(c["resolved_at"], "%Y-%m-%d %H:%M:%S")
                res_times.append((res - det).total_seconds() / 3600)
            except ValueError:
                pass

    avg_resolution_hours = round(np.mean(res_times), 1) if res_times else None

    st.session_state["resolution_stats"] = {
        "total":                total,
        "open":                 open_c,
        "resolved":             resolved_c,
        "escalated":            escalated_c,
        "rejected":             by_status.get("Rejected", 0),
        "under_review":         by_status.get("Under Review", 0),
        "auto_resolved":        auto_resolved,
        "sla_breaches":         sla_breaches,
        "critical_open":        critical_open,
        "resolution_rate_pct":  round(resolved_c / total * 100, 1) if total else 0.0,
        "auto_resolve_rate_pct": round(auto_resolved / total * 100, 1) if total else 0.0,
        "avg_resolution_hours": avg_resolution_hours,
        "by_status":            by_status,
        "by_priority":          _count_by(conflicts, "priority"),
        "by_type":              _count_by(conflicts, "conflict_type"),
        "by_dataset":           _count_by(conflicts, "dataset"),
    }


def _count_by(conflicts: list, key: str) -> dict:
    counts = {}
    for c in conflicts:
        v = c.get(key, "Unknown")
        counts[v] = counts.get(v, 0) + 1
    return counts


def get_resolution_stats() -> dict:
    init_resolver_state()
    _update_resolution_stats()
    return st.session_state.get("resolution_stats", {})


def get_open_conflicts_df() -> pd.DataFrame:
    df = get_conflicts_df()
    if df.empty:
        return df
    return df[df["status"].isin(["Open", "Under Review", "Escalated"])].reset_index(drop=True)


def get_conflicts_by_priority(priority: str) -> pd.DataFrame:
    df = get_conflicts_df()
    if df.empty:
        return df
    return df[df["priority"] == priority].reset_index(drop=True)


def get_sla_breached_conflicts() -> pd.DataFrame:
    df = get_conflicts_df()
    if df.empty:
        return df
    return df[df["sla_breached"] == True].reset_index(drop=True)


# ─────────────────────────────────────────────
# GOLDEN RECORD BUILDER  (MDM-inspired)
# ─────────────────────────────────────────────

def build_golden_record_summary() -> pd.DataFrame:
    """
    Summarise the golden record status per dataset based on
    conflict resolution outcomes.
    """
    conflicts  = get_conflicts()
    datasets   = ["nav", "share_classes", "transactions", "portfolio", "registration"]
    rows       = []

    for ds in datasets:
        ds_conflicts = [c for c in conflicts if c.get("dataset") == ds]
        total        = len(ds_conflicts)
        resolved     = sum(1 for c in ds_conflicts if c.get("status") in ("Resolved", "Auto-Resolved"))
        open_c       = sum(1 for c in ds_conflicts if c.get("status") == "Open")
        critical     = sum(1 for c in ds_conflicts if c.get("priority") == "P1 – Critical" and c.get("status") == "Open")
        golden_pct   = round((resolved / total * 100), 1) if total else 100.0

        rows.append({
            "Dataset":              ds.replace("_", " ").title(),
            "Total Conflicts":      total,
            "Resolved":             resolved,
            "Open":                 open_c,
            "Critical Open":        critical,
            "Golden Record %":      golden_pct,
            "Golden Record Status": "✅ Clean" if critical == 0 and open_c == 0
                                    else ("⚠️ Partial" if critical == 0 else "🔴 At Risk"),
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# CONFLICT SIMULATION  (for demo / testing)
# ─────────────────────────────────────────────

def simulate_new_conflict(conflict_type: str = None) -> dict:
    """
    Inject a fresh synthetic conflict into the queue.
    Useful for demo and stress-testing the resolver UI.
    """
    init_resolver_state()
    if not conflict_type:
        conflict_type = random.choice(CONFLICT_TYPES)

    # Pick a matching scenario if available
    matching = [s for s in CONFLICT_SCENARIOS if s["conflict_type"] == conflict_type]
    base     = random.choice(matching) if matching else CONFLICT_SCENARIOS[0]
    ts       = datetime.now().strftime("%Y-%m-%d %H:%M")

    new_conflict = _make_conflict(
        conflict_type  = conflict_type,
        title          = f"[SIM] {base['title']}",
        description    = f"Simulated conflict: {base['description']}",
        dataset        = base["dataset"],
        field          = base["field"],
        source_a       = base["source_a"],
        value_a        = base["value_a"],
        source_b       = base["source_b"],
        value_b        = base["value_b"],
        priority       = random.choice(PRIORITY_LEVELS),
        auto_resolvable = base["auto_resolvable"],
        detected_at    = ts,
    )

    st.session_state["conflicts"].append(new_conflict)
    _log_audit(
        conflict_id  = new_conflict["conflict_id"],
        action       = "SIMULATED",
        performed_by = "FundGov360 Simulator",
        details      = f"Injected simulated conflict of type {conflict_type}",
        timestamp    = ts,
    )
    return new_conflict
