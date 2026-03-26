
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# CONFLICT TYPES & SOURCES
# ─────────────────────────────────────────────────────────────────────────────
SOURCES = ["Fund Administrator", "Transfer Agent", "Custodian", "Bloomberg", "FactSet", "Internal System"]

ATTRIBUTE_CATALOGUE = {
    "ShareClass": ["share_class_name","currency","nav_frequency","min_investment","mgmt_fee_pct","isin","status"],
    "SubFund":    ["sub_fund_name","strategy","currency","inception_date","status","aum","domicile"],
    "Fund":       ["fund_name","legal_form","domicile","currency","manager","regulator"],
    "Registration":["registration_status","registration_date","expiry_date","distributor_name"],
    "NAV":        ["nav_value","shares_outstanding","aum"],
    "Transaction":["settlement_date","counterparty","broker","gross_amount","status"],
}

CONFLICT_SCENARIOS = [
    # ShareClass name inconsistencies
    {"entity_type":"ShareClass","attribute":"share_class_name","src_a":"Fund Administrator","src_b":"Transfer Agent",
     "val_a":"Lumina Core - Class A (EUR)","val_b":"LUMINA CORE A EUR","severity":"Medium",
     "conflict_type":"Naming Convention","description":"Different naming conventions across systems"},
    {"entity_type":"ShareClass","attribute":"share_class_name","src_a":"Fund Administrator","src_b":"Bloomberg",
     "val_a":"Atlas Fixed Income I USD Acc","val_b":"Atlas FI-I USD","severity":"Low",
     "conflict_type":"Naming Convention","description":"Bloomberg shortname vs official name"},
    {"entity_type":"ShareClass","attribute":"mgmt_fee_pct","src_a":"Fund Administrator","src_b":"FactSet",
     "val_a":"0.75","val_b":"0.80","severity":"High",
     "conflict_type":"Value Discrepancy","description":"Management fee differs between FA and FactSet"},
    {"entity_type":"ShareClass","attribute":"currency","src_a":"Transfer Agent","src_b":"Custodian",
     "val_a":"EUR","val_b":"USD","severity":"Critical",
     "conflict_type":"Critical Mismatch","description":"Share class currency mismatch — potential booking error"},
    {"entity_type":"ShareClass","attribute":"nav_frequency","src_a":"Fund Administrator","src_b":"Transfer Agent",
     "val_a":"Daily","val_b":"Weekly","severity":"High",
     "conflict_type":"Attribute Conflict","description":"NAV frequency differs between Fund Admin and TA"},
    {"entity_type":"ShareClass","attribute":"min_investment","src_a":"Fund Administrator","src_b":"Bloomberg",
     "val_a":"100000","val_b":"10000","severity":"Medium",
     "conflict_type":"Value Discrepancy","description":"Min investment amount discrepancy"},
    # Registration
    {"entity_type":"Registration","attribute":"registration_status","src_a":"Fund Administrator","src_b":"Internal System",
     "val_a":"Active","val_b":"Pending","severity":"High",
     "conflict_type":"Status Conflict","description":"Registration status differs: FA says Active, Internal says Pending"},
    {"entity_type":"Registration","attribute":"registration_status","src_a":"Custodian","src_b":"Transfer Agent",
     "val_a":"Inactive","val_b":"Active","severity":"Critical",
     "conflict_type":"Critical Mismatch","description":"Critical: Custodian shows Inactive vs TA shows Active"},
    {"entity_type":"Registration","attribute":"expiry_date","src_a":"Fund Administrator","src_b":"Transfer Agent",
     "val_a":"2027-06-30","val_b":"2026-12-31","severity":"High",
     "conflict_type":"Date Discrepancy","description":"Registration expiry date is different across sources"},
    {"entity_type":"Registration","attribute":"distributor_name","src_a":"Fund Administrator","src_b":"Transfer Agent",
     "val_a":"BNP Paribas Wealth Mgmt","val_b":"BNPP Wealth Management","severity":"Low",
     "conflict_type":"Naming Convention","description":"Distributor name formatting differs"},
    # Sub-Fund
    {"entity_type":"SubFund","attribute":"inception_date","src_a":"Fund Administrator","src_b":"Bloomberg",
     "val_a":"2019-03-15","val_b":"2019-03-01","severity":"Medium",
     "conflict_type":"Date Discrepancy","description":"Inception date differs by 14 days"},
    {"entity_type":"SubFund","attribute":"strategy","src_a":"Fund Administrator","src_b":"FactSet",
     "val_a":"Long/Short Equity","val_b":"Absolute Return","severity":"High",
     "conflict_type":"Classification Conflict","description":"Investment strategy classification mismatch"},
    {"entity_type":"SubFund","attribute":"domicile","src_a":"Fund Administrator","src_b":"Bloomberg",
     "val_a":"Luxembourg","val_b":"Ireland","severity":"Critical",
     "conflict_type":"Critical Mismatch","description":"Sub-fund domicile differs between sources"},
    # NAV
    {"entity_type":"NAV","attribute":"nav_value","src_a":"Fund Administrator","src_b":"Custodian",
     "val_a":"245.3200","val_b":"245.3150","severity":"Medium",
     "conflict_type":"Value Discrepancy","description":"Small NAV discrepancy — rounding or timing"},
    {"entity_type":"NAV","attribute":"shares_outstanding","src_a":"Transfer Agent","src_b":"Custodian",
     "val_a":"1234567","val_b":"1234521","severity":"High",
     "conflict_type":"Reconciliation Break","description":"Shares outstanding break between TA and Custodian"},
]


def gen_conflicts(sc_df, sub_funds_df, reg_df, n=45):
    """Generate a realistic set of conflicts from scenario templates."""
    random.seed(99)
    np.random.seed(99)
    rows = []
    sc_list = sc_df["share_class_id"].tolist()
    sf_list = sub_funds_df["sub_fund_id"].tolist()

    for i in range(n):
        scenario = random.choice(CONFLICT_SCENARIOS)
        if scenario["entity_type"] == "ShareClass":
            entity_id = random.choice(sc_list)
        elif scenario["entity_type"] in ["SubFund","NAV","Registration"]:
            entity_id = random.choice(sf_list)
        else:
            entity_id = random.choice(sc_list)

        detected = datetime(2026, 1, 1) + timedelta(days=random.randint(0, 83))
        rows.append({
            "conflict_id": f"CONF-{i+1:04d}",
            "entity_type": scenario["entity_type"],
            "entity_id": entity_id,
            "attribute": scenario["attribute"],
            "conflict_type": scenario["conflict_type"],
            "source_a": scenario["src_a"],
            "value_a": scenario["val_a"],
            "source_b": scenario["src_b"],
            "value_b": scenario["val_b"],
            "severity": scenario["severity"],
            "description": scenario["description"],
            "detected_at": detected.strftime("%Y-%m-%d %H:%M"),
            "status": random.choice(["Open","Open","Open","In Review","Open"]),
            "resolved_value": None,
            "resolution_strategy": None,
            "resolved_by": None,
            "resolved_at": None,
            "comment": "",
        })
    return pd.DataFrame(rows)


def init_conflicts(sc_df, sub_funds_df, reg_df):
    if "conflicts" not in st.session_state:
        st.session_state["conflicts"] = gen_conflicts(sc_df, sub_funds_df, reg_df)
    if "resolution_log" not in st.session_state:
        st.session_state["resolution_log"] = []


RESOLUTION_STRATEGIES = [
    "Accept Source A",
    "Accept Source B",
    "Manual Override",
    "Mark as Equivalent",   # same meaning, different format
    "Escalate to Data Owner",
    "Defer / Accept Both",
]

TRUSTED_SOURCE_HIERARCHY = {
    "NAV":          ["Fund Administrator", "Custodian", "Bloomberg", "FactSet"],
    "Portfolio":    ["Custodian", "Fund Administrator", "Bloomberg"],
    "Registration": ["Fund Administrator", "Transfer Agent", "Internal System"],
    "Static Data":  ["Fund Administrator", "Bloomberg", "FactSet", "Transfer Agent"],
    "Transaction":  ["Custodian", "Transfer Agent", "Fund Administrator"],
}
