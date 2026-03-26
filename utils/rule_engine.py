
import pandas as pd
import numpy as np
import re
from datetime import datetime
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# DEFAULT RULE CATALOGUE
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_RULES = [
    {"rule_id":"BR-001","rule_name":"NAV Positive & Non-Zero","domain":"NAV","entity_type":"ShareClass",
     "attribute":"nav","rule_type":"RANGE","expression":"nav > 0","severity":"Critical",
     "action":"Reject","active":True,"created_by":"system","created_at":"2026-01-01","tags":"valuation,nav"},
    {"rule_id":"BR-002","rule_name":"NAV on Business Day","domain":"NAV","entity_type":"ShareClass",
     "attribute":"date","rule_type":"CUSTOM","expression":"date.weekday() not in [5,6]","severity":"High",
     "action":"Flag","active":True,"created_by":"system","created_at":"2026-01-01","tags":"timing,nav"},
    {"rule_id":"BR-003","rule_name":"Portfolio Weights Sum ≈ 100%","domain":"Portfolio","entity_type":"SubFund",
     "attribute":"weight_pct","rule_type":"AGGREGATE","expression":"abs(sum(weight_pct) - 100) <= 0.01","severity":"Critical",
     "action":"Reject","active":True,"created_by":"system","created_at":"2026-01-01","tags":"portfolio,weights"},
    {"rule_id":"BR-004","rule_name":"Transaction Amount = Qty × Price","domain":"Transaction","entity_type":"Transaction",
     "attribute":"gross_amount","rule_type":"FORMULA","expression":"abs(gross_amount - quantity*price) < 0.01","severity":"High",
     "action":"Flag","active":True,"created_by":"system","created_at":"2026-01-01","tags":"transaction,reconciliation"},
    {"rule_id":"BR-005","rule_name":"ISIN Format Validation","domain":"Static Data","entity_type":"ShareClass",
     "attribute":"isin","rule_type":"REGEX","expression":"^[A-Z]{2}[A-Z0-9]{10}$","severity":"Critical",
     "action":"Reject","active":True,"created_by":"system","created_at":"2026-01-01","tags":"static,isin"},
    {"rule_id":"BR-006","rule_name":"Registration Expiry > Start","domain":"Registration","entity_type":"ShareClass",
     "attribute":"expiry_date","rule_type":"DATE_ORDER","expression":"expiry_date > registration_date","severity":"Medium",
     "action":"Flag","active":True,"created_by":"system","created_at":"2026-01-01","tags":"registration,dates"},
    {"rule_id":"BR-007","rule_name":"NAV Daily Change ≤ ±15%","domain":"NAV","entity_type":"ShareClass",
     "attribute":"nav","rule_type":"RANGE","expression":"abs(pct_change) <= 0.15","severity":"High",
     "action":"Flag","active":True,"created_by":"system","created_at":"2026-01-01","tags":"nav,threshold"},
    {"rule_id":"BR-008","rule_name":"AUM Custodian Reconciliation ±0.1%","domain":"NAV","entity_type":"SubFund",
     "attribute":"aum","rule_type":"RECONCILIATION","expression":"abs(aum_internal - aum_custodian) / aum_internal <= 0.001","severity":"Critical",
     "action":"Reject","active":True,"created_by":"system","created_at":"2026-01-01","tags":"aum,reconciliation"},
    {"rule_id":"BR-009","rule_name":"No Duplicate ISIN per Sub-Fund","domain":"Static Data","entity_type":"ShareClass",
     "attribute":"isin","rule_type":"UNIQUENESS","expression":"count(isin) per sub_fund_id == 1","severity":"Critical",
     "action":"Reject","active":True,"created_by":"system","created_at":"2026-01-01","tags":"static,uniqueness"},
    {"rule_id":"BR-010","rule_name":"Share Class Currency ISO4217","domain":"Static Data","entity_type":"ShareClass",
     "attribute":"currency","rule_type":"REFERENTIAL","expression":"currency IN ('EUR','USD','GBP','CHF','JPY','SEK','DKK','NOK','CAD','AUD')","severity":"High",
     "action":"Flag","active":True,"created_by":"system","created_at":"2026-01-01","tags":"static,currency"},
    {"rule_id":"BR-011","rule_name":"Transaction Date Not Future","domain":"Transaction","entity_type":"Transaction",
     "attribute":"date","rule_type":"DATE_RANGE","expression":"date <= today()","severity":"High",
     "action":"Reject","active":True,"created_by":"system","created_at":"2026-01-01","tags":"transaction,timing"},
    {"rule_id":"BR-012","rule_name":"Subscription/Redemption Net Flow","domain":"Transaction","entity_type":"SubFund",
     "attribute":"net_flow","rule_type":"AGGREGATE","expression":"abs(subscriptions - redemptions) matches reported_net_flow","severity":"Medium",
     "action":"Warn","active":True,"created_by":"system","created_at":"2026-01-01","tags":"transaction,flow"},
]

RULE_TYPES = ["NOT_NULL","RANGE","REGEX","FORMULA","DATE_ORDER","DATE_RANGE","AGGREGATE","UNIQUENESS","REFERENTIAL","RECONCILIATION","CUSTOM"]
DOMAINS = ["NAV","Portfolio","Transaction","Static Data","Registration","AUM","Risk","Compliance"]
ENTITY_TYPES = ["Fund","SubFund","ShareClass","Transaction","Position","Registration"]
SEVERITIES = ["Critical","High","Medium","Low"]
ACTIONS = ["Reject","Flag","Warn","Log Only"]

# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION ENGINE (simulated against synthetic data)
# ─────────────────────────────────────────────────────────────────────────────
def test_rule_against_data(rule, nav_df, sc_df, tx_df, port_df):
    """Simulate rule execution and return pass/fail stats."""
    import random
    random.seed(hash(rule["rule_id"]) % 1000)
    domain = rule["domain"]
    rule_type = rule["rule_type"]
    results = []

    if domain == "NAV" and rule_type == "RANGE" and "nav > 0" in rule["expression"]:
        sample = nav_df.sample(min(500, len(nav_df)))
        for _, row in sample.iterrows():
            passed = row["nav"] > 0
            results.append({"entity_id": row["share_class_id"], "date": row["date"],
                             "value": row["nav"], "passed": passed,
                             "message": "" if passed else f"NAV={row['nav']} is not positive"})

    elif domain == "Static Data" and rule_type == "REGEX":
        pattern = rule["expression"]
        for _, row in sc_df.iterrows():
            val = row.get("isin","")
            passed = bool(re.match(pattern, str(val)))
            results.append({"entity_id": row["share_class_id"], "date": "N/A",
                             "value": val, "passed": passed,
                             "message": "" if passed else f"ISIN '{val}' does not match pattern"})

    elif domain == "Transaction" and "gross_amount" in rule["expression"]:
        sample = tx_df.sample(min(300, len(tx_df)))
        for _, row in sample.iterrows():
            diff = abs(row["gross_amount"] - row["quantity"] * row["price"])
            passed = diff < 1.0
            results.append({"entity_id": row["tx_id"], "date": row["date"],
                             "value": round(diff, 4), "passed": passed,
                             "message": "" if passed else f"Amount diff = {diff:.2f}"})

    elif domain == "Static Data" and rule_type == "REFERENTIAL":
        valid_ccys = ['EUR','USD','GBP','CHF','JPY','SEK','DKK','NOK','CAD','AUD']
        for _, row in sc_df.iterrows():
            passed = row["currency"] in valid_ccys
            results.append({"entity_id": row["share_class_id"], "date": "N/A",
                             "value": row["currency"], "passed": passed,
                             "message": "" if passed else f"Currency '{row['currency']}' not valid ISO4217"})

    else:
        # Simulate generic result
        n = random.randint(100, 500)
        fail_rate = random.uniform(0.001, 0.03)
        for i in range(n):
            passed = random.random() > fail_rate
            results.append({"entity_id": f"ENT-{i:04d}", "date": "2026-03-25",
                             "value": "—", "passed": passed, "message": "" if passed else "Rule violation detected"})

    df = pd.DataFrame(results)
    if len(df) == 0:
        return pd.DataFrame(), {"total": 0, "passed": 0, "failed": 0, "pass_rate": 100.0}
    total = len(df)
    passed_n = df["passed"].sum()
    failed_n = total - passed_n
    stats = {"total": total, "passed": int(passed_n), "failed": int(failed_n),
             "pass_rate": round(passed_n/total*100, 2)}
    return df[~df["passed"]].head(50), stats


def init_rules():
    if "dq_rules" not in st.session_state:
        st.session_state["dq_rules"] = pd.DataFrame(DEFAULT_RULES)
    if "rule_exec_log" not in st.session_state:
        st.session_state["rule_exec_log"] = []

def get_next_rule_id():
    rules = st.session_state.get("dq_rules", pd.DataFrame())
    if len(rules) == 0:
        return "BR-001"
    ids = rules["rule_id"].str.extract(r"BR-(\d+)").astype(float)
    next_num = int(ids.max().iloc[0]) + 1
    return f"BR-{next_num:03d}"
