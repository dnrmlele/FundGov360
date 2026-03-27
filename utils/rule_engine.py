# utils/rule_engine.py
# FundGov360 v5 — Data Quality Rule Engine
# Inspired by Talend Data Quality, Atlan DQ, Great Expectations

import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
import streamlit as st

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

RULE_TYPES = ["NOT_NULL", "RANGE", "REGEX", "FORMULA", "DATE", "UNIQUENESS", "REFERENTIAL", "CUSTOM"]

SEVERITIES = ["Critical", "High", "Medium", "Low"]

DATASETS = ["nav", "share_classes", "transactions", "portfolio", "registration"]

DATASET_FIELDS = {
    "nav": [
        "nav", "aum", "isin", "currency", "date", "sc_id",
        "sub_fund_id", "fund_id", "source", "validated", "shares_outstanding",
    ],
    "share_classes": [
        "sc_id", "sub_fund_id", "fund_id", "sc_name", "isin", "currency",
        "investor_type", "min_investment", "distribution", "hedged",
        "inception_date", "status", "shares_outstanding", "transfer_agent",
    ],
    "transactions": [
        "tx_id", "sc_id", "sub_fund_id", "fund_id", "isin", "tx_type",
        "tx_date", "settlement_date", "amount", "currency", "nav_at_tx",
        "units", "counterparty", "investor_id", "settlement_status",
        "source", "error_flag",
    ],
    "portfolio": [
        "sub_fund_id", "fund_id", "position_id", "security_name", "isin",
        "asset_class", "geography", "sector", "currency", "market_value",
        "weight_pct", "quantity", "price", "valuation_date", "source",
    ],
    "registration": [
        "sc_id", "sub_fund_id", "fund_id", "isin", "jurisdiction",
        "reg_status", "reg_date", "expiry_date", "local_regulator",
        "last_updated", "source",
    ],
}

# ─────────────────────────────────────────────
# DEFAULT RULE LIBRARY  (16 production-grade rules)
# ─────────────────────────────────────────────

DEFAULT_RULES = [
    # NAV rules
    {
        "rule_id":       "BR-001",
        "rule_name":     "NAV Not Null",
        "description":   "NAV value must never be null or missing.",
        "dataset":       "nav",
        "field":         "nav",
        "rule_type":     "NOT_NULL",
        "threshold_min": None,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "Critical",
        "active":        True,
        "category":      "Completeness",
        "owner":         "Data Management",
        "sla_pass_rate":  99.5,
        "created_date":  "2024-01-15",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "BR-002",
        "rule_name":     "NAV Positive Range",
        "description":   "NAV must be strictly positive (> 0) and below 100,000.",
        "dataset":       "nav",
        "field":         "nav",
        "rule_type":     "RANGE",
        "threshold_min": 0.01,
        "threshold_max": 100_000.0,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "Critical",
        "active":        True,
        "category":      "Validity",
        "owner":         "Data Management",
        "sla_pass_rate":  99.9,
        "created_date":  "2024-01-15",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "BR-003",
        "rule_name":     "AUM Positive",
        "description":   "AUM must be a positive number.",
        "dataset":       "nav",
        "field":         "aum",
        "rule_type":     "RANGE",
        "threshold_min": 0.01,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "High",
        "active":        True,
        "category":      "Validity",
        "owner":         "Data Management",
        "sla_pass_rate":  99.0,
        "created_date":  "2024-01-15",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "BR-004",
        "rule_name":     "NAV Currency Not Null",
        "description":   "Currency field must be populated for every NAV record.",
        "dataset":       "nav",
        "field":         "currency",
        "rule_type":     "NOT_NULL",
        "threshold_min": None,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "High",
        "active":        True,
        "category":      "Completeness",
        "owner":         "Data Management",
        "sla_pass_rate":  99.5,
        "created_date":  "2024-02-01",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "BR-005",
        "rule_name":     "ISIN Format",
        "description":   "ISIN must match ^[A-Z]{2}[A-Z0-9]{9}[0-9]$ (ISO 6166).",
        "dataset":       "nav",
        "field":         "isin",
        "rule_type":     "REGEX",
        "threshold_min": None,
        "threshold_max": None,
        "regex_pattern": r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$",
        "formula_expr":  None,
        "severity":      "Critical",
        "active":        True,
        "category":      "Conformity",
        "owner":         "Data Management",
        "sla_pass_rate":  100.0,
        "created_date":  "2024-02-10",
        "last_modified": "2025-09-15",
    },
    # Share class rules
    {
        "rule_id":       "SC-001",
        "rule_name":     "Share Class ISIN Not Null",
        "description":   "Every share class must have an ISIN.",
        "dataset":       "share_classes",
        "field":         "isin",
        "rule_type":     "NOT_NULL",
        "threshold_min": None,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "Critical",
        "active":        True,
        "category":      "Completeness",
        "owner":         "Fund Operations",
        "sla_pass_rate":  100.0,
        "created_date":  "2024-01-20",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "SC-002",
        "rule_name":     "Minimum Investment Positive",
        "description":   "Minimum investment must be > 0.",
        "dataset":       "share_classes",
        "field":         "min_investment",
        "rule_type":     "RANGE",
        "threshold_min": 1,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "Medium",
        "active":        True,
        "category":      "Validity",
        "owner":         "Fund Operations",
        "sla_pass_rate":  99.0,
        "created_date":  "2024-03-01",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "SC-003",
        "rule_name":     "Shares Outstanding Positive",
        "description":   "Shares outstanding must be a positive integer.",
        "dataset":       "share_classes",
        "field":         "shares_outstanding",
        "rule_type":     "RANGE",
        "threshold_min": 1,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "High",
        "active":        True,
        "category":      "Validity",
        "owner":         "Fund Operations",
        "sla_pass_rate":  99.5,
        "created_date":  "2024-03-05",
        "last_modified": "2025-06-01",
    },
    # Transaction rules
    {
        "rule_id":       "TX-001",
        "rule_name":     "Transaction Amount Positive",
        "description":   "Transaction amount must be strictly positive.",
        "dataset":       "transactions",
        "field":         "amount",
        "rule_type":     "RANGE",
        "threshold_min": 0.01,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "Critical",
        "active":        True,
        "category":      "Validity",
        "owner":         "Operations",
        "sla_pass_rate":  99.9,
        "created_date":  "2024-01-15",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "TX-002",
        "rule_name":     "Settlement Date After Trade Date",
        "description":   "Settlement date must be >= transaction date.",
        "dataset":       "transactions",
        "field":         "settlement_date",
        "rule_type":     "FORMULA",
        "threshold_min": None,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  "settlement_date >= tx_date",
        "severity":      "High",
        "active":        True,
        "category":      "Consistency",
        "owner":         "Operations",
        "sla_pass_rate":  99.5,
        "created_date":  "2024-02-20",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "TX-003",
        "rule_name":     "Investor ID Not Null",
        "description":   "Investor ID must be present on all transactions.",
        "dataset":       "transactions",
        "field":         "investor_id",
        "rule_type":     "NOT_NULL",
        "threshold_min": None,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "High",
        "active":        True,
        "category":      "Completeness",
        "owner":         "Compliance",
        "sla_pass_rate":  99.0,
        "created_date":  "2024-04-01",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "TX-004",
        "rule_name":     "Valid Settlement Status",
        "description":   "Settlement status must be one of: Settled, Pending, Failed, Cancelled.",
        "dataset":       "transactions",
        "field":         "settlement_status",
        "rule_type":     "REGEX",
        "threshold_min": None,
        "threshold_max": None,
        "regex_pattern": r"^(Settled|Pending|Failed|Cancelled)$",
        "formula_expr":  None,
        "severity":      "Medium",
        "active":        True,
        "category":      "Conformity",
        "owner":         "Operations",
        "sla_pass_rate":  98.0,
        "created_date":  "2024-04-10",
        "last_modified": "2025-06-01",
    },
    # Portfolio rules
    {
        "rule_id":       "PF-001",
        "rule_name":     "Portfolio Weight Range",
        "description":   "Individual position weight must be between 0% and 100%.",
        "dataset":       "portfolio",
        "field":         "weight_pct",
        "rule_type":     "RANGE",
        "threshold_min": 0.0,
        "threshold_max": 100.0,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "Critical",
        "active":        True,
        "category":      "Validity",
        "owner":         "Portfolio Management",
        "sla_pass_rate":  100.0,
        "created_date":  "2024-01-15",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "PF-002",
        "rule_name":     "Market Value Positive",
        "description":   "Market value of any holding must be positive.",
        "dataset":       "portfolio",
        "field":         "market_value",
        "rule_type":     "RANGE",
        "threshold_min": 0.01,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "High",
        "active":        True,
        "category":      "Validity",
        "owner":         "Portfolio Management",
        "sla_pass_rate":  99.5,
        "created_date":  "2024-02-01",
        "last_modified": "2025-06-01",
    },
    # Registration rules
    {
        "rule_id":       "RG-001",
        "rule_name":     "Registration Status Valid",
        "description":   "Registration status must be one of: Registered, Pending, Restricted, Not Registered.",
        "dataset":       "registration",
        "field":         "reg_status",
        "rule_type":     "REGEX",
        "threshold_min": None,
        "threshold_max": None,
        "regex_pattern": r"^(Registered|Pending|Restricted|Not Registered)$",
        "formula_expr":  None,
        "severity":      "High",
        "active":        True,
        "category":      "Conformity",
        "owner":         "Compliance",
        "sla_pass_rate":  99.0,
        "created_date":  "2024-03-15",
        "last_modified": "2025-06-01",
    },
    {
        "rule_id":       "RG-002",
        "rule_name":     "Jurisdiction Not Null",
        "description":   "Every registration record must reference a jurisdiction.",
        "dataset":       "registration",
        "field":         "jurisdiction",
        "rule_type":     "NOT_NULL",
        "threshold_min": None,
        "threshold_max": None,
        "regex_pattern": None,
        "formula_expr":  None,
        "severity":      "Critical",
        "active":        True,
        "category":      "Completeness",
        "owner":         "Compliance",
        "sla_pass_rate":  100.0,
        "created_date":  "2024-03-20",
        "last_modified": "2025-06-01",
    },
]

# ─────────────────────────────────────────────
# RULE TEMPLATES  (for UI-driven rule creation)
# ─────────────────────────────────────────────

RULE_TEMPLATES = {
    "NOT_NULL": {
        "label":       "Not Null / Completeness",
        "description": "Checks that a field has no missing or null values.",
        "icon":        "🔴",
        "fields_needed": ["dataset", "field", "severity"],
        "example":     "nav field must not be null",
    },
    "RANGE": {
        "label":       "Range / Boundary Check",
        "description": "Validates that a numeric field falls within [min, max].",
        "icon":        "📏",
        "fields_needed": ["dataset", "field", "threshold_min", "threshold_max", "severity"],
        "example":     "NAV must be between 0.01 and 100,000",
    },
    "REGEX": {
        "label":       "Format / Regex Pattern",
        "description": "Checks that a string field matches a regular expression.",
        "icon":        "🔤",
        "fields_needed": ["dataset", "field", "regex_pattern", "severity"],
        "example":     "ISIN must match ^[A-Z]{2}[A-Z0-9]{9}[0-9]$",
    },
    "FORMULA": {
        "label":       "Formula / Cross-Field",
        "description": "Evaluates a pandas-compatible expression across multiple fields.",
        "icon":        "🧮",
        "fields_needed": ["dataset", "field", "formula_expr", "severity"],
        "example":     "settlement_date >= tx_date",
    },
    "DATE": {
        "label":       "Date Validity",
        "description": "Checks that a date field is parseable and not null.",
        "icon":        "📅",
        "fields_needed": ["dataset", "field", "severity"],
        "example":     "tx_date must be a valid date",
    },
    "UNIQUENESS": {
        "label":       "Uniqueness",
        "description": "Checks that field values are unique across the dataset.",
        "icon":        "🔑",
        "fields_needed": ["dataset", "field", "severity"],
        "example":     "tx_id must be unique",
    },
    "REFERENTIAL": {
        "label":       "Referential Integrity",
        "description": "Checks that a foreign key value exists in a reference set.",
        "icon":        "🔗",
        "fields_needed": ["dataset", "field", "severity"],
        "example":     "sc_id in transactions must exist in share_classes",
    },
    "CUSTOM": {
        "label":       "Custom / Advanced",
        "description": "Custom Python lambda or advanced expression.",
        "icon":        "⚙️",
        "fields_needed": ["dataset", "field", "formula_expr", "severity"],
        "example":     "abs(nav - prev_nav) / prev_nav < 0.20  (20% NAV spike check)",
    },
}

# ─────────────────────────────────────────────
# SESSION STATE MANAGEMENT
# ─────────────────────────────────────────────

def init_rule_engine_state() -> None:
    """Initialise Streamlit session state keys for the rule engine."""
    if "rules" not in st.session_state:
        st.session_state["rules"] = DEFAULT_RULES.copy()
    if "rule_results" not in st.session_state:
        st.session_state["rule_results"] = {}
    if "rule_run_history" not in st.session_state:
        st.session_state["rule_run_history"] = []
    if "rule_trends" not in st.session_state:
        st.session_state["rule_trends"] = {}


def get_rules() -> list[dict]:
    init_rule_engine_state()
    return st.session_state["rules"]


def get_rules_df() -> pd.DataFrame:
    return pd.DataFrame(get_rules())


def add_rule(rule: dict) -> None:
    init_rule_engine_state()
    existing_ids = {r["rule_id"] for r in st.session_state["rules"]}
    if rule["rule_id"] in existing_ids:
        raise ValueError(f"Rule ID '{rule['rule_id']}' already exists.")
    st.session_state["rules"].append(rule)


def update_rule(rule_id: str, updates: dict) -> None:
    init_rule_engine_state()
    for i, r in enumerate(st.session_state["rules"]):
        if r["rule_id"] == rule_id:
            st.session_state["rules"][i] = {**r, **updates, "last_modified": datetime.today().strftime("%Y-%m-%d")}
            return
    raise ValueError(f"Rule ID '{rule_id}' not found.")


def delete_rule(rule_id: str) -> None:
    init_rule_engine_state()
    st.session_state["rules"] = [r for r in st.session_state["rules"] if r["rule_id"] != rule_id]


def toggle_rule(rule_id: str) -> None:
    init_rule_engine_state()
    for r in st.session_state["rules"]:
        if r["rule_id"] == rule_id:
            r["active"] = not r["active"]
            return


# ─────────────────────────────────────────────
# RULE EXECUTION ENGINE
# ─────────────────────────────────────────────

def _get_dataset(
    rule: dict,
    nav_df: pd.DataFrame,
    sc_df: pd.DataFrame,
    tx_df: pd.DataFrame,
    port_df: pd.DataFrame,
    reg_df: pd.DataFrame,
) -> pd.DataFrame:
    mapping = {
        "nav":           nav_df,
        "share_classes": sc_df,
        "transactions":  tx_df,
        "portfolio":     port_df,
        "registration":  reg_df,
    }
    return mapping.get(rule.get("dataset", ""), pd.DataFrame())


def run_rule(
    rule: dict,
    nav_df: pd.DataFrame,
    sc_df: pd.DataFrame,
    tx_df: pd.DataFrame,
    port_df: pd.DataFrame,
    reg_df: pd.DataFrame,
) -> dict:
    """
    Execute a single rule against the appropriate dataset.
    Returns a result dict with keys:
        rule_id, rule_name, dataset, field, rule_type, severity,
        checked, passed, failed, pass_rate, sla_met,
        failures_df, run_at, status, error
    """
    target    = _get_dataset(rule, nav_df, sc_df, tx_df, port_df, reg_df)
    field     = rule.get("field", "")
    rule_type = rule.get("rule_type", "NOT_NULL")
    result = {
        "rule_id":    rule["rule_id"],
        "rule_name":  rule["rule_name"],
        "dataset":    rule.get("dataset"),
        "field":      field,
        "rule_type":  rule_type,
        "severity":   rule.get("severity", "Medium"),
        "category":   rule.get("category", ""),
        "owner":      rule.get("owner", ""),
        "sla_pass_rate": rule.get("sla_pass_rate", 95.0),
        "checked":    0,
        "passed":     0,
        "failed":     0,
        "pass_rate":  0.0,
        "sla_met":    False,
        "failures_df": pd.DataFrame(),
        "run_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status":     "error",
        "error":      None,
    }

    if target.empty:
        result["error"] = f"Dataset '{rule.get('dataset')}' is empty or not found."
        return result

    if field not in target.columns:
        result["error"] = f"Field '{field}' not found in dataset '{rule.get('dataset')}'."
        return result

    checked  = len(target)
    failures = pd.DataFrame()

    try:
        if rule_type == "NOT_NULL":
            failures = target[target[field].isna()].copy()

        elif rule_type == "RANGE":
            lo   = rule.get("threshold_min")
            hi   = rule.get("threshold_max")
            mask = pd.Series(False, index=target.index)
            if lo is not None:
                mask |= pd.to_numeric(target[field], errors="coerce") < lo
            if hi is not None:
                mask |= pd.to_numeric(target[field], errors="coerce") > hi
            mask |= pd.to_numeric(target[field], errors="coerce").isna()
            failures = target[mask].copy()

        elif rule_type == "REGEX":
            pattern = rule.get("regex_pattern", "")
            if pattern:
                failures = target[
                    ~target[field].astype(str).str.match(pattern, na=False)
                ].copy()

        elif rule_type == "FORMULA":
            expr = rule.get("formula_expr", "")
            if expr:
                # Convert date columns for formula evaluation
                eval_df = target.copy()
                for col in ["tx_date", "settlement_date", "reg_date", "expiry_date", "inception_date"]:
                    if col in eval_df.columns:
                        eval_df[col] = pd.to_datetime(eval_df[col], errors="coerce")
                mask     = ~eval_df.eval(expr)
                failures = target[mask].copy()

        elif rule_type == "DATE":
            parsed   = pd.to_datetime(target[field], errors="coerce")
            failures = target[parsed.isna()].copy()

        elif rule_type == "UNIQUENESS":
            failures = target[target[field].duplicated(keep=False)].copy()

        elif rule_type == "REFERENTIAL":
            # For demo purposes: flag nulls as referential failures
            failures = target[target[field].isna()].copy()

        elif rule_type == "CUSTOM":
            expr = rule.get("formula_expr", "")
            if expr:
                try:
                    mask     = ~target.eval(expr)
                    failures = target[mask].copy()
                except Exception as e:
                    result["error"] = f"Custom expression error: {e}"
                    return result

        # Tag failures
        if not failures.empty:
            failures = failures.copy()
            failures["__rule_id__"]   = rule["rule_id"]
            failures["__rule_name__"] = rule["rule_name"]
            failures["__severity__"]  = rule.get("severity", "Medium")
            failures["__field__"]     = field
            failures["__dataset__"]   = rule.get("dataset")

        failed    = len(failures)
        passed    = checked - failed
        pass_rate = round((passed / checked) * 100, 2) if checked > 0 else 100.0
        sla       = rule.get("sla_pass_rate", 95.0)

        result.update({
            "checked":     checked,
            "passed":      passed,
            "failed":      failed,
            "pass_rate":   pass_rate,
            "sla_met":     pass_rate >= sla,
            "failures_df": failures,
            "status":      "ok",
            "error":       None,
        })

    except Exception as e:
        result["error"]  = str(e)
        result["status"] = "error"

    return result


def run_all_rules(
    nav_df: pd.DataFrame,
    sc_df: pd.DataFrame,
    tx_df: pd.DataFrame,
    port_df: pd.DataFrame,
    reg_df: pd.DataFrame,
    active_only: bool = True,
) -> dict[str, dict]:
    """
    Run all rules (or only active ones) and return a dict keyed by rule_id.
    Also appends a summary snapshot to st.session_state['rule_run_history'].
    """
    init_rule_engine_state()
    rules   = [r for r in get_rules() if r.get("active", True)] if active_only else get_rules()
    results = {}

    for rule in rules:
        res = run_rule(rule, nav_df, sc_df, tx_df, port_df, reg_df)
        results[rule["rule_id"]] = res

    # Persist results and history snapshot
    st.session_state["rule_results"] = results
    _append_run_history(results)

    return results


def _append_run_history(results: dict[str, dict]) -> None:
    """Append a summarised snapshot of a full rule run to history."""
    snapshot = {
        "run_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_rules":  len(results),
        "rules_passed": sum(1 for r in results.values() if r["failed"] == 0),
        "rules_failed": sum(1 for r in results.values() if r["failed"] > 0),
        "sla_breaches": sum(1 for r in results.values() if not r["sla_met"]),
        "avg_pass_rate": round(
            np.mean([r["pass_rate"] for r in results.values()]) if results else 0.0, 2
        ),
        "critical_failures": sum(
            1 for r in results.values() if r["failed"] > 0 and r["severity"] == "Critical"
        ),
    }
    st.session_state["rule_run_history"].append(snapshot)


# ─────────────────────────────────────────────
# TREND SIMULATION  (historical DQ pass rates)
# ─────────────────────────────────────────────

def gen_rule_trends(days: int = 30) -> pd.DataFrame:
    """
    Simulate historical pass-rate trend per rule for the past `days` days.
    Returns a long-format DataFrame: date × rule_id × pass_rate.
    """
    rules = get_rules()
    rows  = []
    dates = [(datetime.today() - timedelta(days=i)).date() for i in range(days - 1, -1, -1)]

    for rule in rules:
        base_rate = rule.get("sla_pass_rate", 95.0)
        rate      = min(base_rate + np.random.uniform(-2.0, 5.0), 100.0)
        for dt in dates:
            delta = np.random.normal(0, 0.8)
            rate  = float(np.clip(rate + delta, max(base_rate - 10.0, 70.0), 100.0))
            rows.append({
                "date":       dt,
                "rule_id":    rule["rule_id"],
                "rule_name":  rule["rule_name"],
                "dataset":    rule.get("dataset"),
                "severity":   rule.get("severity"),
                "pass_rate":  round(rate, 2),
                "sla_target": base_rate,
                "sla_met":    rate >= base_rate,
            })

    return pd.DataFrame(rows)


def get_trend_for_rule(rule_id: str, days: int = 30) -> pd.DataFrame:
    """Return trend data for a single rule."""
    df = gen_rule_trends(days=days)
    return df[df["rule_id"] == rule_id].reset_index(drop=True)


# ─────────────────────────────────────────────
# SUMMARY & SCORING  (dashboard KPIs)
# ─────────────────────────────────────────────

def compute_dq_score(results: dict[str, dict]) -> dict:
    """
    Compute an overall Data Quality score from rule results.
    Weights: Critical=4, High=3, Medium=2, Low=1.
    """
    weight_map = {"Critical": 4, "High": 3, "Medium": 2, "Low": 1}

    if not results:
        return {"overall_score": 0.0, "weighted_score": 0.0, "total_checks": 0, "by_severity": {}}

    weighted_sum    = 0.0
    total_weight    = 0.0
    by_severity     = {}

    for res in results.values():
        w  = weight_map.get(res.get("severity", "Medium"), 2)
        pr = res.get("pass_rate", 0.0)
        weighted_sum += w * pr
        total_weight += w * 100.0

        sev = res.get("severity", "Medium")
        if sev not in by_severity:
            by_severity[sev] = {"rules": 0, "avg_pass_rate": 0.0, "failures": 0}
        by_severity[sev]["rules"]    += 1
        by_severity[sev]["failures"] += res.get("failed", 0)
        by_severity[sev]["avg_pass_rate"] = round(
            by_severity[sev]["avg_pass_rate"] * (by_severity[sev]["rules"] - 1) / by_severity[sev]["rules"]
            + pr / by_severity[sev]["rules"], 2
        )

    weighted_score  = round((weighted_sum / total_weight) * 100, 2) if total_weight else 0.0
    simple_avg      = round(np.mean([r.get("pass_rate", 0.0) for r in results.values()]), 2)

    return {
        "overall_score":  simple_avg,
        "weighted_score": weighted_score,
        "total_checks":   sum(r.get("checked", 0) for r in results.values()),
        "total_failed":   sum(r.get("failed", 0) for r in results.values()),
        "sla_breaches":   sum(1 for r in results.values() if not r.get("sla_met", True)),
        "critical_open":  sum(1 for r in results.values() if r.get("failed", 0) > 0 and r.get("severity") == "Critical"),
        "by_severity":    by_severity,
    }


def get_failures_summary(results: dict[str, dict]) -> pd.DataFrame:
    """Return a flat DataFrame of all failures across all rules."""
    frames = []
    for res in results.values():
        fdf = res.get("failures_df", pd.DataFrame())
        if not fdf.empty:
            frames.append(fdf)
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


def get_rules_summary_df(results: dict[str, dict]) -> pd.DataFrame:
    """Return a summary DataFrame (one row per rule) suitable for display."""
    rows = []
    for res in results.values():
        rows.append({
            "Rule ID":    res["rule_id"],
            "Rule Name":  res["rule_name"],
            "Dataset":    res["dataset"],
            "Field":      res["field"],
            "Type":       res["rule_type"],
            "Severity":   res["severity"],
            "Category":   res.get("category", ""),
            "Owner":      res.get("owner", ""),
            "Checked":    res["checked"],
            "Passed":     res["passed"],
            "Failed":     res["failed"],
            "Pass Rate %": res["pass_rate"],
            "SLA Target": res.get("sla_pass_rate", 95.0),
            "SLA Met":    "✅" if res["sla_met"] else "❌",
            "Run At":     res["run_at"],
            "Status":     res["status"],
            "Error":      res.get("error") or "",
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# RULE VALIDATION  (before saving a new rule)
# ─────────────────────────────────────────────

def validate_rule_dict(rule: dict) -> list[str]:
    """
    Validate a rule dict before adding it to the engine.
    Returns a list of error strings (empty = valid).
    """
    errors = []
    required = ["rule_id", "rule_name", "dataset", "field", "rule_type", "severity"]
    for key in required:
        if not rule.get(key):
            errors.append(f"Missing required field: '{key}'")

    if rule.get("rule_type") not in RULE_TYPES:
        errors.append(f"rule_type must be one of {RULE_TYPES}")

    if rule.get("severity") not in SEVERITIES:
        errors.append(f"severity must be one of {SEVERITIES}")

    if rule.get("dataset") not in DATASETS:
        errors.append(f"dataset must be one of {DATASETS}")

    field   = rule.get("field", "")
    dataset = rule.get("dataset", "")
    valid_fields = DATASET_FIELDS.get(dataset, [])
    if field and valid_fields and field not in valid_fields:
        errors.append(f"Field '{field}' is not recognised for dataset '{dataset}'. Known fields: {valid_fields}")

    if rule.get("rule_type") == "REGEX" and not rule.get("regex_pattern"):
        errors.append("REGEX rules require a 'regex_pattern'.")

    if rule.get("rule_type") in ("FORMULA", "CUSTOM") and not rule.get("formula_expr"):
        errors.append(f"{rule.get('rule_type')} rules require a 'formula_expr'.")

    if rule.get("regex_pattern"):
        try:
            re.compile(rule["regex_pattern"])
        except re.error as e:
            errors.append(f"Invalid regex pattern: {e}")

    if rule.get("sla_pass_rate") is not None:
        sla = rule["sla_pass_rate"]
        if not (0.0 <= sla <= 100.0):
            errors.append("sla_pass_rate must be between 0 and 100.")

    return errors


# ─────────────────────────────────────────────
# RULE EXPORT / IMPORT
# ─────────────────────────────────────────────

def export_rules_to_df() -> pd.DataFrame:
    """Export all rules as a DataFrame (for CSV download)."""
    rules = get_rules()
    export_fields = [
        "rule_id", "rule_name", "description", "dataset", "field",
        "rule_type", "threshold_min", "threshold_max", "regex_pattern",
        "formula_expr", "severity", "active", "category", "owner",
        "sla_pass_rate", "created_date", "last_modified",
    ]
    rows = []
    for r in rules:
        rows.append({k: r.get(k, "") for k in export_fields})
    return pd.DataFrame(rows)


def import_rules_from_df(df: pd.DataFrame) -> tuple[int, int, list[str]]:
    """
    Import rules from a DataFrame (e.g. uploaded CSV).
    Returns (imported_count, skipped_count, error_messages).
    """
    imported = 0
    skipped  = 0
    errors   = []

    for _, row in df.iterrows():
        rule = row.to_dict()
        # Type coercion
        for num_field in ["threshold_min", "threshold_max", "sla_pass_rate"]:
            if rule.get(num_field) not in (None, "", "None"):
                try:
                    rule[num_field] = float(rule[num_field])
                except (ValueError, TypeError):
                    rule[num_field] = None
            else:
                rule[num_field] = None

        rule["active"] = str(rule.get("active", "True")).strip().lower() in ("true", "1", "yes")

        errs = validate_rule_dict(rule)
        if errs:
            skipped += 1
            errors.append(f"Rule {rule.get('rule_id', '?')}: {'; '.join(errs)}")
            continue

        try:
            add_rule(rule)
            imported += 1
        except ValueError as e:
            skipped += 1
            errors.append(str(e))

    return imported, skipped, errors


# ─────────────────────────────────────────────
# ALERT GENERATION  (Talend-style issue triage)
# ─────────────────────────────────────────────

def generate_alerts(results: dict[str, dict]) -> pd.DataFrame:
    """
    Generate a prioritised alert table from rule results.
    Severity rank: Critical > High > Medium > Low.
    """
    severity_rank = {"Critical": 1, "High": 2, "Medium": 3, "Low": 4}
    rows = []

    for res in results.values():
        if res.get("failed", 0) == 0:
            continue
        sev = res.get("severity", "Medium")
        rows.append({
            "Priority":    severity_rank.get(sev, 3),
            "Severity":    sev,
            "Rule ID":     res["rule_id"],
            "Rule Name":   res["rule_name"],
            "Dataset":     res["dataset"],
            "Field":       res["field"],
            "Failed Rows": res["failed"],
            "Pass Rate %": res["pass_rate"],
            "SLA Target":  res.get("sla_pass_rate", 95.0),
            "SLA Met":     "✅" if res["sla_met"] else "❌",
            "Owner":       res.get("owner", "—"),
            "Category":    res.get("category", "—"),
            "Run At":      res["run_at"],
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Priority").drop(columns="Priority").reset_index(drop=True)
    return df


def get_sla_breach_rules(results: dict[str, dict]) -> list[dict]:
    """Return list of result dicts where SLA was not met."""
    return [r for r in results.values() if not r.get("sla_met", True)]
