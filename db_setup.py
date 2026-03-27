# db_setup.py
# FundGov360 v5 — SQLite Database with Demo Data Generator
# Schema + Seeding + Utils for Streamlit integration

import sqlite3
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from faker import Faker
import os

# ✅ Fix 1: class seed BEFORE instance creation
Faker.seed(42)
fake = Faker()
np.random.seed(42)
random.seed(42)

DB_PATH = "fundgov360.db"


def create_database_and_seed(db_path: str = DB_PATH):
    """Create complete schema and populate with production-like demo data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # ─────────────────────────────────────────────
    # SCHEMA DEFINITION
    # ─────────────────────────────────────────────
    schema_sql = """
-- 1. CORE FUND HIERARCHY (Bronze → Gold)
CREATE TABLE IF NOT EXISTS fund (
    fund_id       TEXT PRIMARY KEY,
    fund_name     TEXT NOT NULL,
    lei           TEXT UNIQUE NOT NULL,
    legal_form    TEXT NOT NULL,
    domicile      TEXT NOT NULL,
    fund_manager  TEXT NOT NULL,
    custodian     TEXT NOT NULL,
    fund_admin    TEXT NOT NULL,
    inception_date DATE NOT NULL,
    currency      TEXT NOT NULL,
    aum_usd       DECIMAL(15,2),
    status        TEXT DEFAULT 'Active',
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sub_fund (
    sub_fund_id          TEXT PRIMARY KEY,
    fund_id              TEXT NOT NULL,
    sub_fund_name        TEXT NOT NULL,
    isin_base            TEXT,
    asset_class          TEXT NOT NULL,
    strategy             TEXT NOT NULL,
    benchmark            TEXT,
    inception_date       DATE NOT NULL,
    management_fee_pct   DECIMAL(5,2),
    performance_fee_pct  DECIMAL(5,2),
    nav_frequency        TEXT DEFAULT 'Daily',
    status               TEXT DEFAULT 'Active',
    data_layer           TEXT DEFAULT 'Gold',
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_id) REFERENCES fund(fund_id)
);

CREATE TABLE IF NOT EXISTS share_class (
    sc_id              TEXT PRIMARY KEY,
    sub_fund_id        TEXT NOT NULL,
    fund_id            TEXT NOT NULL,
    sc_name            TEXT NOT NULL,
    isin               TEXT UNIQUE NOT NULL,
    currency           TEXT NOT NULL,
    investor_type      TEXT NOT NULL,
    min_investment     DECIMAL(12,2),
    distribution       TEXT,
    hedged             BOOLEAN DEFAULT FALSE,
    inception_date     DATE NOT NULL,
    status             TEXT DEFAULT 'Active',
    shares_outstanding DECIMAL(15,0),
    transfer_agent     TEXT NOT NULL,
    data_layer         TEXT DEFAULT 'Gold',
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sub_fund_id) REFERENCES sub_fund(sub_fund_id),
    FOREIGN KEY (fund_id) REFERENCES fund(fund_id)
);

-- 2. TIME SERIES DATA (Silver → Gold)
CREATE TABLE IF NOT EXISTS nav (
    nav_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    sc_id              TEXT NOT NULL,
    fund_id            TEXT NOT NULL,
    sub_fund_id        TEXT NOT NULL,
    isin               TEXT NOT NULL,
    currency           TEXT NOT NULL,
    nav_date           DATE NOT NULL,
    nav                DECIMAL(10,4) NOT NULL,
    aum                DECIMAL(15,2) NOT NULL,
    shares_outstanding DECIMAL(15,0) NOT NULL,
    source             TEXT NOT NULL,
    validated          BOOLEAN DEFAULT FALSE,
    data_layer         TEXT DEFAULT 'Gold',
    UNIQUE(sc_id, nav_date),
    FOREIGN KEY (sc_id) REFERENCES share_class(sc_id)
);

CREATE TABLE IF NOT EXISTS portfolio_position (
    position_id    TEXT PRIMARY KEY,
    sub_fund_id    TEXT NOT NULL,
    fund_id        TEXT NOT NULL,
    isin           TEXT NOT NULL,
    security_name  TEXT NOT NULL,
    asset_class    TEXT NOT NULL,
    geography      TEXT NOT NULL,
    sector         TEXT NOT NULL,
    currency       TEXT NOT NULL,
    market_value   DECIMAL(15,2) NOT NULL,
    weight_pct     DECIMAL(5,2) NOT NULL,
    quantity       DECIMAL(15,0),
    price          DECIMAL(10,4),
    valuation_date DATE NOT NULL,
    source         TEXT NOT NULL,
    data_layer     TEXT DEFAULT 'Silver',
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sub_fund_id) REFERENCES sub_fund(sub_fund_id)
);

CREATE TABLE IF NOT EXISTS fund_transaction (
    tx_id             TEXT PRIMARY KEY,
    sc_id             TEXT NOT NULL,
    fund_id           TEXT NOT NULL,
    sub_fund_id       TEXT NOT NULL,
    isin              TEXT NOT NULL,
    tx_type           TEXT NOT NULL,
    tx_date           DATE NOT NULL,
    settlement_date   DATE NOT NULL,
    amount            DECIMAL(15,2) NOT NULL,
    currency          TEXT NOT NULL,
    nav_at_tx         DECIMAL(10,4),
    units             DECIMAL(15,4),
    counterparty      TEXT,
    investor_id       TEXT,
    settlement_status TEXT NOT NULL,
    source            TEXT NOT NULL,
    error_flag        BOOLEAN DEFAULT FALSE,
    error_reason      TEXT,
    data_layer        TEXT DEFAULT 'Silver',
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sc_id) REFERENCES share_class(sc_id)
);

CREATE TABLE IF NOT EXISTS registration (
    reg_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sc_id           TEXT NOT NULL,
    fund_id         TEXT NOT NULL,
    sub_fund_id     TEXT NOT NULL,
    isin            TEXT NOT NULL,
    jurisdiction    TEXT NOT NULL,
    reg_status      TEXT NOT NULL,
    reg_date        DATE,
    expiry_date     DATE,
    local_regulator TEXT,
    last_updated    DATE NOT NULL,
    source          TEXT NOT NULL,
    data_layer      TEXT DEFAULT 'Gold',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sc_id) REFERENCES share_class(sc_id)
);

-- 3. GOVERNANCE METADATA (Gold)
CREATE TABLE IF NOT EXISTS steward (
    steward_id     TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    email          TEXT NOT NULL,
    role           TEXT NOT NULL,
    department     TEXT NOT NULL,
    phone          TEXT,
    location       TEXT,
    active         BOOLEAN DEFAULT TRUE,
    assigned_since DATE NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team (
    team_id    TEXT PRIMARY KEY,
    team_name  TEXT UNIQUE NOT NULL,
    department TEXT NOT NULL,
    head       TEXT NOT NULL,
    focus      TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS taxonomy_domain (
    domain_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    domain_name TEXT UNIQUE NOT NULL,
    description TEXT NOT NULL,
    icon        TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS taxonomy_subdomain (
    subdomain_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    domain_id      INTEGER NOT NULL,
    subdomain_name TEXT NOT NULL,
    description    TEXT NOT NULL,
    FOREIGN KEY (domain_id) REFERENCES taxonomy_domain(domain_id)
);

CREATE TABLE IF NOT EXISTS taxonomy_concept (
    concept_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    subdomain_id INTEGER NOT NULL,
    concept_name TEXT NOT NULL,
    FOREIGN KEY (subdomain_id) REFERENCES taxonomy_subdomain(subdomain_id)
);

CREATE TABLE IF NOT EXISTS data_element (
    element_id       TEXT PRIMARY KEY,
    element_name     TEXT NOT NULL,
    display_name     TEXT NOT NULL,
    domain_id        INTEGER,
    subdomain_id     INTEGER,
    concept_id       INTEGER,
    description      TEXT NOT NULL,
    data_type        TEXT NOT NULL,
    format           TEXT,
    example_value    TEXT,
    nullable         BOOLEAN DEFAULT TRUE,
    pii              BOOLEAN DEFAULT FALSE,
    classification   TEXT NOT NULL,
    golden_source    TEXT NOT NULL,
    secondary_source TEXT,
    data_layer       TEXT DEFAULT 'Gold',
    owner_team       TEXT,
    steward          TEXT,
    regulatory_ref   TEXT,
    linked_rule_ids  TEXT,
    glossary_term    TEXT,
    status           TEXT DEFAULT 'Draft',
    criticality      TEXT DEFAULT 'Medium',
    last_reviewed    DATE,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (domain_id) REFERENCES taxonomy_domain(domain_id)
);

CREATE TABLE IF NOT EXISTS data_element_usage (
    usage_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    element_id     TEXT NOT NULL,
    consuming_team TEXT NOT NULL,
    usage_purpose  TEXT,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (element_id) REFERENCES data_element(element_id)
);

CREATE TABLE IF NOT EXISTS business_glossary (
    glossary_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    term           TEXT UNIQUE NOT NULL,
    abbreviation   TEXT,
    domain_id      INTEGER,
    subdomain_id   INTEGER,
    definition     TEXT NOT NULL,
    example        TEXT,
    related_terms  TEXT,
    regulatory_ref TEXT,
    data_type      TEXT,
    owner          TEXT NOT NULL,
    status         TEXT DEFAULT 'Draft',
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (domain_id) REFERENCES taxonomy_domain(domain_id)
);

-- 4. DQ & GOVERNANCE
CREATE TABLE IF NOT EXISTS dq_rule (
    rule_id       TEXT PRIMARY KEY,
    rule_name     TEXT NOT NULL,
    description   TEXT,
    dataset       TEXT NOT NULL,
    field         TEXT NOT NULL,
    rule_type     TEXT NOT NULL,
    threshold_min DECIMAL,
    threshold_max DECIMAL,
    regex_pattern TEXT,
    formula_expr  TEXT,
    severity      TEXT NOT NULL,
    active        BOOLEAN DEFAULT TRUE,
    category      TEXT,
    owner         TEXT,
    sla_pass_rate DECIMAL DEFAULT 95.0,
    created_date  DATE NOT NULL,
    last_modified DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS dq_run_result (
    result_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id    TEXT NOT NULL,
    run_at     TIMESTAMP NOT NULL,
    dataset    TEXT NOT NULL,
    checked    INTEGER NOT NULL,
    passed     INTEGER NOT NULL,
    failed     INTEGER NOT NULL,
    pass_rate  DECIMAL(5,2) NOT NULL,
    sla_met    BOOLEAN NOT NULL,
    FOREIGN KEY (rule_id) REFERENCES dq_rule(rule_id)
);

CREATE TABLE IF NOT EXISTS conflict (
    conflict_id        TEXT PRIMARY KEY,
    conflict_type      TEXT NOT NULL,
    title              TEXT NOT NULL,
    description        TEXT,
    dataset            TEXT NOT NULL,
    field              TEXT NOT NULL,
    source_a           TEXT NOT NULL,
    value_a            ANY,
    source_b           TEXT NOT NULL,
    value_b            ANY,
    priority           TEXT NOT NULL,
    status             TEXT DEFAULT 'Open',
    assigned_to        TEXT,
    department         TEXT,
    detected_at        TIMESTAMP NOT NULL,
    resolved_at        TIMESTAMP,
    resolved_by        TEXT,
    resolution_method  TEXT,
    final_value        ANY,
    resolution_comment TEXT,
    sla_hours          INTEGER,
    sla_breached       BOOLEAN DEFAULT FALSE,
    escalation_count   INTEGER DEFAULT 0,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS conflict_audit (
    audit_id     TEXT PRIMARY KEY,
    conflict_id  TEXT NOT NULL,
    action       TEXT NOT NULL,
    performed_by TEXT NOT NULL,
    details      TEXT,
    timestamp    TIMESTAMP NOT NULL,
    FOREIGN KEY (conflict_id) REFERENCES conflict(conflict_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_nav_sc_date    ON nav(sc_id, nav_date);
CREATE INDEX IF NOT EXISTS idx_nav_date       ON nav(nav_date);
CREATE INDEX IF NOT EXISTS idx_tx_settlement  ON fund_transaction(settlement_status, settlement_date);
CREATE INDEX IF NOT EXISTS idx_tx_date        ON fund_transaction(tx_date);
CREATE INDEX IF NOT EXISTS idx_port_subfund   ON portfolio_position(sub_fund_id);
CREATE INDEX IF NOT EXISTS idx_reg_sc_jur     ON registration(sc_id, jurisdiction);
CREATE INDEX IF NOT EXISTS idx_conflict_status   ON conflict(status);
CREATE INDEX IF NOT EXISTS idx_conflict_priority ON conflict(priority);
"""
    cursor.executescript(schema_sql)

    # ─────────────────────────────────────────────
    # CLEAR EXISTING DATA (clean demo slate)
    # ─────────────────────────────────────────────
    for tbl in [
        "conflict_audit", "conflict", "dq_run_result", "data_element_usage",
        "business_glossary", "data_element", "taxonomy_concept", "taxonomy_subdomain",
        "taxonomy_domain", "team", "steward", "registration", "fund_transaction",
        "portfolio_position", "nav", "share_class", "sub_fund", "fund",
    ]:
        cursor.execute(f"DELETE FROM {tbl}")

    # ─────────────────────────────────────────────
    # 1. FUND HIERARCHY
    # ─────────────────────────────────────────────
    funds = [
        ("F001", "Apex Global SICAV",     "529900HNOAA1KXQJUQ27", "SICAV", "LU", "Apex Capital",   "State Street", "Northern Trust", "2010-03-15", "USD", 4200000000, "Active"),
        ("F002", "Atlas Income ICAV",     "5493000ABC123DEF45678", "ICAV",  "IE", "Atlas AM",       "BNP Paribas",  "CACEIS",         "2014-07-01", "EUR", 2800000000, "Active"),
        ("F003", "Orion EM Partners LP",  "529900XYZ987654321098", "LP",    "KY", "Orion Partners", "Citi",         "Maples",         "2018-11-20", "USD",  950000000, "Active"),
    ]
    cursor.executemany(
        """INSERT INTO fund(fund_id,fund_name,lei,legal_form,domicile,fund_manager,
           custodian,fund_admin,inception_date,currency,aum_usd,status)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        funds,
    )

    sub_funds = [
        ("SF001","F001","Apex Global Equity",       "LU1234567890","Equity",       "Long Only","MSCI World",            "2010-06-01",0.75,10.0,"Daily", "Active","Gold"),
        ("SF002","F001","Apex Global Fixed Income", "LU0987654321","Fixed Income", "Active",   "Bloomberg Global Agg",  "2012-03-15",0.50, 0.0,"Daily", "Active","Gold"),
        ("SF003","F002","Atlas Core Income",        "IE1111111111","Fixed Income", "Passive",  "Bloomberg Euro Agg",    "2014-09-01",0.30, 0.0,"Weekly","Active","Gold"),
        ("SF004","F002","Atlas High Yield",         "IE2222222222","Fixed Income", "Active",   "ICE BofA HY Index",     "2016-01-10",0.65,15.0,"Daily", "Active","Gold"),
        ("SF005","F003","Orion EM Equity",          "KY3333333333","Equity",       "Long Only","MSCI EM",               "2018-12-01",0.85,15.0,"Daily", "Active","Gold"),
        ("SF006","F003","Orion EM Debt",            "KY4444444444","Fixed Income", "Active",   "JPM EMBI",              "2020-05-20",0.60,10.0,"Weekly","Active","Gold"),
    ]
    cursor.executemany(
        """INSERT INTO sub_fund(sub_fund_id,fund_id,sub_fund_name,isin_base,asset_class,
           strategy,benchmark,inception_date,management_fee_pct,performance_fee_pct,
           nav_frequency,status,data_layer)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        sub_funds,
    )

    share_classes = [
        ("SC001","SF001","F001","Apex Global Equity A-USD", "LU0123456789","USD","Retail",       1000,   "Accumulating",False,"2010-06-01","Active",8700000,"RBC IS",   "Gold"),
        ("SC002","SF001","F001","Apex Global Equity I-USD", "LU0234567890","USD","Institutional",1000000,"Accumulating",False,"2011-01-15","Active",2300000,"RBC IS",   "Gold"),
        ("SC003","SF002","F001","Apex Global FI C-EUR H",   "LU0345678901","EUR","Retail",       1000,   "Distributing", True,"2012-06-01","Active",1200000,"CACEIS TA","Gold"),
        ("SC004","SF003","F002","Atlas Core Income D-EUR",  "IE0456789012","EUR","Institutional",500000, "Accumulating",False,"2014-10-01","Active", 890000,"RBC IS",   "Gold"),
        ("SC005","SF004","F002","Atlas HY B-EUR",           "IE0567890123","EUR","Retail",       1000,   "Distributing", False,"2016-02-15","Active",3400000,"CACEIS TA","Gold"),
        ("SC006","SF005","F003","Orion EM Equity A-USD",    "KY0678901234","USD","Retail",       1000,   "Accumulating",False,"2019-01-01","Active",2100000,"Maples TA","Gold"),
        ("SC007","SF002","F001","Apex Global FI I-USD",     "LU0456789012","USD","Institutional",500000, "Accumulating",False,"2013-03-01","Active",1500000,"RBC IS",   "Gold"),
        ("SC008","SF004","F002","Atlas HY I-EUR",           "IE0678901234","EUR","Institutional",500000, "Accumulating",False,"2016-06-01","Active",1800000,"CACEIS TA","Gold"),
        ("SC009","SF005","F003","Orion EM Equity I-USD",    "KY0789012345","USD","Institutional",1000000,"Accumulating",False,"2019-06-01","Active",1200000,"Maples TA","Gold"),
        ("SC010","SF006","F003","Orion EM Debt A-USD",      "KY0890123456","USD","Retail",       1000,   "Distributing", False,"2020-06-01","Active", 950000,"Maples TA","Gold"),
        ("SC011","SF003","F002","Atlas Core Income R-EUR",  "IE0901234567","EUR","Retail",       1000,   "Distributing", False,"2015-01-10","Active",2200000,"CACEIS TA","Gold"),
        ("SC012","SF006","F003","Orion EM Debt I-USD",      "KY0912345678","USD","Institutional",500000, "Accumulating",False,"2020-09-01","Active", 700000,"Maples TA","Gold"),
    ]
    cursor.executemany(
        """INSERT INTO share_class(sc_id,sub_fund_id,fund_id,sc_name,isin,currency,
           investor_type,min_investment,distribution,hedged,inception_date,status,
           shares_outstanding,transfer_agent,data_layer)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        share_classes,
    )

    # ─────────────────────────────────────────────
    # 2. NAV TIME SERIES (365 days)
    # ─────────────────────────────────────────────
    sc_meta = {
        "SC001": {"fund_id":"F001","sub_fund_id":"SF001","isin":"LU0123456789","currency":"USD","nav_base":142.35,"shares":8700000},
        "SC002": {"fund_id":"F001","sub_fund_id":"SF001","isin":"LU0234567890","currency":"USD","nav_base":210.50,"shares":2300000},
        "SC003": {"fund_id":"F001","sub_fund_id":"SF002","isin":"LU0345678901","currency":"EUR","nav_base": 98.12,"shares":1200000},
        "SC004": {"fund_id":"F002","sub_fund_id":"SF003","isin":"IE0456789012","currency":"EUR","nav_base":105.80,"shares": 890000},
        "SC005": {"fund_id":"F002","sub_fund_id":"SF004","isin":"IE0567890123","currency":"EUR","nav_base": 87.45,"shares":3400000},
        "SC006": {"fund_id":"F003","sub_fund_id":"SF005","isin":"KY0678901234","currency":"USD","nav_base": 64.20,"shares":2100000},
        "SC007": {"fund_id":"F001","sub_fund_id":"SF002","isin":"LU0456789012","currency":"USD","nav_base":112.30,"shares":1500000},
        "SC008": {"fund_id":"F002","sub_fund_id":"SF004","isin":"IE0678901234","currency":"EUR","nav_base": 92.75,"shares":1800000},
        "SC009": {"fund_id":"F003","sub_fund_id":"SF005","isin":"KY0789012345","currency":"USD","nav_base": 68.90,"shares":1200000},
        "SC010": {"fund_id":"F003","sub_fund_id":"SF006","isin":"KY0890123456","currency":"USD","nav_base": 95.10,"shares": 950000},
        "SC011": {"fund_id":"F002","sub_fund_id":"SF003","isin":"IE0901234567","currency":"EUR","nav_base":103.20,"shares":2200000},
        "SC012": {"fund_id":"F003","sub_fund_id":"SF006","isin":"KY0912345678","currency":"USD","nav_base": 98.40,"shares": 700000},
    }

    dates = [(datetime.today() - timedelta(days=i)).date() for i in range(364, -1, -1)]
    nav_records = []
    nav_current = {sc_id: meta["nav_base"] for sc_id, meta in sc_meta.items()}
    for date in dates:
        for sc_id, meta in sc_meta.items():
            nav_current[sc_id] = round(max(nav_current[sc_id] + np.random.normal(0.03, 0.5), 1.0), 4)
            aum = round(nav_current[sc_id] * meta["shares"], 2)
            nav_records.append((
                sc_id, meta["fund_id"], meta["sub_fund_id"], meta["isin"],
                meta["currency"], str(date), nav_current[sc_id],
                aum, meta["shares"], "Fund Admin", True, "Gold",
            ))
    cursor.executemany(
        """INSERT OR IGNORE INTO nav(sc_id,fund_id,sub_fund_id,isin,currency,nav_date,
           nav,aum,shares_outstanding,source,validated,data_layer)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        nav_records,
    )

    # ─────────────────────────────────────────────
    # 3. PORTFOLIO POSITIONS
    # ─────────────────────────────────────────────
    portfolio = [
        ("SF001_P001","SF001","F001","US0378331005","Apple Inc",              "Equity","North America","Technology",    "USD",123000000,2.93,2500000, 49.20,"2026-03-27","Custodian","Silver"),
        ("SF001_P002","SF001","F001","US5949181045","Microsoft Corp",         "Equity","North America","Technology",    "USD",112000000,2.67,1200000, 93.33,"2026-03-27","Custodian","Silver"),
        ("SF001_P003","SF001","F001","US4592001014","International Business", "Equity","North America","Technology",    "USD", 78000000,1.86, 900000, 86.67,"2026-03-27","Custodian","Silver"),
        ("SF001_P004","SF001","F001","DE0007100000","Volkswagen AG",          "Equity","Europe",        "Automotive",   "EUR", 65000000,1.55, 750000, 86.67,"2026-03-27","Custodian","Silver"),
        ("SF001_P005","SF001","F001","FR0000131104","BNP Paribas SA",         "Equity","Europe",        "Financials",   "EUR", 58000000,1.38, 850000, 68.24,"2026-03-27","Custodian","Silver"),
        ("SF001_P006","SF001","F001","JP3633400001","Toyota Motor Corp",      "Equity","Asia Pacific",  "Automotive",   "JPY", 54000000,1.29,1200000, 45.00,"2026-03-27","Custodian","Silver"),
        ("SF001_P007","SF001","F001","GB0031348658","Rio Tinto PLC",          "Equity","Europe",        "Materials",    "GBP", 47000000,1.12, 600000, 78.33,"2026-03-27","Custodian","Silver"),
        ("SF002_P001","SF002","F001","US912810TM78","US Treasury 2.5% 2028", "Government Bond","North America","Government","USD", 89000000,2.12,  89000,1000.00,"2026-03-27","Custodian","Silver"),
        ("SF002_P002","SF002","F001","US38141GXZ42","Goldman Sachs 3% 2027",  "Corporate Bond","North America","Financials","USD", 76000000,1.81,  76000,1000.00,"2026-03-27","Custodian","Silver"),
        ("SF002_P003","SF002","F001","XS1234567890","Deutsche Bank 2.8% 2029","Corporate Bond","Europe",        "Financials","EUR", 68000000,1.62,  68000,1000.00,"2026-03-27","Custodian","Silver"),
        ("SF003_P001","SF003","F002","DE0001102473","Germany Bund 1% 2030",   "Government Bond","Europe",        "Government","EUR", 95000000,3.39,  95000,1000.00,"2026-03-27","Custodian","Silver"),
        ("SF003_P002","SF003","F002","FR0013131877","France OAT 0.5% 2029",   "Government Bond","Europe",        "Government","EUR", 84000000,3.00,  84000,1000.00,"2026-03-27","Custodian","Silver"),
        ("SF004_P001","SF004","F002","XS9876543210","Altice EUR 6% 2028",     "High Yield Bond","Europe",        "Telecom",   "EUR", 45000000,1.61,  45000,1000.00,"2026-03-27","Custodian","Silver"),
        ("SF004_P002","SF004","F002","XS8765432109","Teva Finance 5.5% 2027", "High Yield Bond","North America","Healthcare","USD", 38000000,1.36,  38000,1000.00,"2026-03-27","Custodian","Silver"),
        ("SF005_P001","SF005","F003","CNE100000296","Alibaba Group",           "Equity","Asia Pacific",  "Technology",  "HKD", 42000000,4.42, 980000, 42.86,"2026-03-27","Custodian","Silver"),
        ("SF005_P002","SF005","F003","BRVALEODBR00","Vale SA",                 "Equity","Latin America", "Materials",   "BRL", 35000000,3.68, 820000, 42.68,"2026-03-27","Custodian","Silver"),
        ("SF005_P003","SF005","F003","IN9999999999","Reliance Industries",     "Equity","Asia Pacific",  "Energy",      "INR", 31000000,3.26, 750000, 41.33,"2026-03-27","Custodian","Silver"),
        ("SF006_P001","SF006","F003","XS5555555555","Brazil 5% 2030",          "Sovereign Bond","Latin America","Government","USD", 28000000,2.95,  28000,1000.00,"2026-03-27","Custodian","Silver"),
        ("SF006_P002","SF006","F003","XS6666666666","Turkey 6.5% 2028",        "Sovereign Bond","Europe",        "Government","USD", 22000000,2.32,  22000,1000.00,"2026-03-27","Custodian","Silver"),
    ]
    cursor.executemany(
        """INSERT INTO portfolio_position(position_id,sub_fund_id,fund_id,isin,security_name,
           asset_class,geography,sector,currency,market_value,weight_pct,quantity,price,
           valuation_date,source,data_layer)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        portfolio,
    )

    # ─────────────────────────────────────────────
    # 4. TRANSACTIONS (500 synthetic records)
    # ─────────────────────────────────────────────
    tx_types   = ["Subscription","Redemption","Subscription","Subscription","Redemption"]
    tx_statuses = ["Settled","Settled","Settled","Pending","Failed","Cancelled"]
    counterparties = ["JPMorgan AM","BlackRock","UBS","Deutsche Bank","BNP Paribas","Goldman Sachs","Morgan Stanley","Allianz"]
    tx_records = []
    sc_list = list(sc_meta.keys())
    for i in range(1, 501):
        sc_id   = random.choice(sc_list)
        meta    = sc_meta[sc_id]
        tx_date = datetime.today() - timedelta(days=random.randint(0, 364))
        sett    = tx_date + timedelta(days=random.choice([2, 3]))
        tx_type = random.choice(tx_types)
        amount  = round(random.uniform(50000, 5000000), 2)
        nav_tx  = round(nav_current[sc_id] + np.random.normal(0, 0.2), 4)
        units   = round(amount / nav_tx, 4) if nav_tx > 0 else 0
        status  = random.choices(tx_statuses, weights=[50,20,10,10,7,3])[0]
        error   = status == "Failed"
        reason  = "Missing investor ID" if error else None
        tx_records.append((
            f"TX{i:04d}", sc_id, meta["fund_id"], meta["sub_fund_id"], meta["isin"],
            tx_type, tx_date.strftime("%Y-%m-%d"), sett.strftime("%Y-%m-%d"),
            amount, meta["currency"], nav_tx, units,
            random.choice(counterparties), f"INV{random.randint(1000,9999)}",
            status, "Transfer Agent", error, reason, "Silver",
        ))
    cursor.executemany(
        """INSERT INTO fund_transaction(tx_id,sc_id,fund_id,sub_fund_id,isin,tx_type,
           tx_date,settlement_date,amount,currency,nav_at_tx,units,counterparty,
           investor_id,settlement_status,source,error_flag,error_reason,data_layer)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        tx_records,
    )

    # ─────────────────────────────────────────────
    # 5. REGISTRATIONS
    # ─────────────────────────────────────────────
    jurisdictions = [
        ("Luxembourg","CSSF"),("Germany","BaFin"),("France","AMF"),
        ("United Kingdom","FCA"),("Switzerland","FINMA"),("Netherlands","AFM"),
        ("Belgium","FSMA"),("Spain","CNMV"),("Italy","Consob"),
        ("Sweden","Finansinspektionen"),("Denmark","Finanstilsynet"),("Austria","FMA"),
    ]
    reg_statuses = ["Registered","Registered","Registered","Pending","Restricted"]
    reg_records = []
    for sc in share_classes:
        sc_id, _, fund_id, _, isin = sc[0], sc[1], sc[2], sc[3], sc[4]
        sf_id = sc[1]
        for jur, regulator in jurisdictions:
            status = random.choice(reg_statuses)
            reg_date    = (datetime.today() - timedelta(days=random.randint(365, 2000))).strftime("%Y-%m-%d") if status != "Pending" else None
            expiry_date = (datetime.today() + timedelta(days=random.randint(365, 1500))).strftime("%Y-%m-%d") if status == "Registered" else None
            reg_records.append((
                sc_id, fund_id, sf_id, isin, jur, status,
                reg_date, expiry_date, regulator,
                datetime.today().strftime("%Y-%m-%d"), "Legal",
            ))
    cursor.executemany(
        """INSERT INTO registration(sc_id,fund_id,sub_fund_id,isin,jurisdiction,reg_status,
           reg_date,expiry_date,local_regulator,last_updated,source)
           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        reg_records,
    )

    # ─────────────────────────────────────────────
    # 6. STEWARDS & TEAMS
    # ─────────────────────────────────────────────
    stewards = [
        ("STW001","Clément Denorme",  "cdenorme@fundgov360.lu",    "Data Steward",      "Data Management", "+352 12345678","Luxembourg",True, "2024-01-01"),
        ("STW002","Sophie Martin",    "smartin@fundaccounting.lu", "Data Owner",        "Fund Accounting", "+352 87654321","Luxembourg",True, "2024-01-01"),
        ("STW003","James O'Brien",    "jobrien@operations.ie",     "Data Owner",        "Fund Operations", "+353 12345678","Dublin",    True, "2024-01-01"),
        ("STW004","Clara Muller",     "cmuller@compliance.lu",     "Compliance Steward","Compliance",      "+352 23456789","Luxembourg",True, "2024-01-01"),
        ("STW005","Ravi Patel",       "rpatel@risk.lu",            "Risk Steward",      "Risk Management", "+352 34567890","Luxembourg",True, "2024-03-01"),
        ("STW006","Aline Dubois",     "adubois@legal.lu",          "Legal Steward",     "Legal",           "+352 45678901","Luxembourg",True, "2024-03-01"),
        ("STW007","Marco Bianchi",    "mbianchi@operations.ie",    "Data Analyst",      "Fund Operations", "+353 23456789","Dublin",    True, "2024-06-01"),
        ("STW008","Emma Schneider",   "eschneider@data.lu",        "Data Engineer",     "Data Management", "+352 56789012","Luxembourg",False,"2023-06-01"),
    ]
    cursor.executemany(
        """INSERT INTO steward(steward_id,name,email,role,department,phone,location,active,assigned_since)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        stewards,
    )

    teams = [
        ("T01","Data Management", "Operations",     "Clément Denorme","Data governance, quality, lineage"),
        ("T02","Fund Accounting", "Finance",         "Sophie Martin",  "NAV calculation, AUM, performance"),
        ("T03","Fund Operations", "Operations",      "James O'Brien",  "Share class setup, static data, fees"),
        ("T04","Compliance",      "Compliance",      "Clara Muller",   "Regulatory reporting, SFDR, AML/KYC"),
        ("T05","Risk Management", "Risk",            "Ravi Patel",     "Risk metrics, stress testing, liquidity"),
        ("T06","Legal",           "Legal",           "Aline Dubois",   "Fund registration, legal documentation"),
    ]
    cursor.executemany(
        """INSERT INTO team(team_id,team_name,department,head,focus)
           VALUES(?,?,?,?,?)""",
        teams,
    )

    # ─────────────────────────────────────────────
    # 7. TAXONOMY
    # ─────────────────────────────────────────────
    domains = [
        ("Fund Structure & Master Data",  "Core fund entity hierarchy, legal structure and master reference data.",       "🏦"),
        ("Pricing & Valuation",           "NAV calculation, AUM, performance measurement and benchmark data.",            "💲"),
        ("Portfolio & Holdings",          "Security-level portfolio positions, exposures and risk analytics.",            "📁"),
        ("Transactions & Cash Flows",     "Fund-level subscriptions, redemptions, trades and cash movements.",           "🔄"),
        ("Regulatory & Compliance",       "Passporting, fund registration, reporting obligations and compliance.",        "⚖️"),
        ("Reference Data",                "Market, instrument and entity reference data underpinning fund operations.",   "📚"),
        ("Risk & Performance Analytics",  "Risk metrics, stress testing, liquidity analysis and performance attribution.","📊"),
    ]
    cursor.executemany(
        "INSERT INTO taxonomy_domain(domain_name,description,icon) VALUES(?,?,?)",
        domains,
    )

    domain_map = {row[1]: row[0] for row in cursor.execute("SELECT domain_id, domain_name FROM taxonomy_domain")}

    subdomains = [
        (domain_map["Fund Structure & Master Data"], "Fund Entity",              "Top-level legal fund entity attributes."),
        (domain_map["Fund Structure & Master Data"], "Sub-Fund",                 "Investment compartment within an umbrella fund structure."),
        (domain_map["Fund Structure & Master Data"], "Share Class",              "Individual tradeable class within a sub-fund."),
        (domain_map["Pricing & Valuation"],          "NAV Calculation",          "Net Asset Value computation and validation data."),
        (domain_map["Pricing & Valuation"],          "Performance Measurement",  "Return calculation and attribution."),
        (domain_map["Portfolio & Holdings"],         "Portfolio Positions",      "Security-level holdings and exposures."),
        (domain_map["Transactions & Cash Flows"],    "Investor Transactions",    "Subscription and redemption orders from investors."),
        (domain_map["Regulatory & Compliance"],      "Registration & Passporting","Jurisdiction-level fund registration status."),
        (domain_map["Reference Data"],               "Instrument Reference",     "Security master data and identifiers."),
    ]
    cursor.executemany(
        "INSERT INTO taxonomy_subdomain(domain_id,subdomain_name,description) VALUES(?,?,?)",
        subdomains,
    )

    subdomain_map = {row[1]: row[0] for row in cursor.execute("SELECT subdomain_id, subdomain_name FROM taxonomy_subdomain")}

    concepts = [
        (subdomain_map["Fund Entity"],            "Legal Identity"),
        (subdomain_map["Sub-Fund"],               "Sub-Fund Identity"),
        (subdomain_map["Share Class"],            "Share Class Identity"),
        (subdomain_map["NAV Calculation"],        "NAV Record"),
        (subdomain_map["Investor Transactions"],  "Transaction Record"),
        (subdomain_map["Registration & Passporting"], "Registration Record"),
        (subdomain_map["Portfolio Positions"],    "Position Record"),
    ]
    cursor.executemany(
        "INSERT INTO taxonomy_concept(subdomain_id,concept_name) VALUES(?,?)",
        concepts,
    )

    # ─────────────────────────────────────────────
    # 8. DATA ELEMENTS
    # ─────────────────────────────────────────────
    data_elements = [
        ("DE-001","nav",               "Net Asset Value",          domain_map["Pricing & Valuation"],          subdomain_map["NAV Calculation"],          2,"Per-share NAV calculated as (Total Assets - Liabilities) / Shares Outstanding.","float",  "######.####",              "142.3512",  False,False,"Confidential","Fund Administrator","Bloomberg",    "Gold","T02","STW002","UCITS Directive","[]","Net Asset Value (NAV)",              "Certified","Critical",None),
        ("DE-002","isin",              "ISIN",                     domain_map["Fund Structure & Master Data"], subdomain_map["Share Class"],               0,"ISO 6166 identifier uniquely identifying the share class.",                       "string", "^[A-Z]{2}[A-Z0-9]{9}[0-9]$","LU0123456789",False,False,"Public",        "Fund Administrator","Euroclear",     "Gold","T03","STW001","ISO 6166",       "[]","ISIN",                                "Certified","Critical",None),
        ("DE-003","settlement_status", "Settlement Status",        domain_map["Transactions & Cash Flows"],    subdomain_map["Investor Transactions"],    0,"Current settlement state of a transaction.",                                     "enum",   "Settled|Pending|Failed",   "Settled",   False,False,"Internal",      "Transfer Agent",    "Custodian",     "Silver","T03","STW003","CSDR",           "[]","Settlement",                          "Certified","High",    None),
        ("DE-004","jurisdiction",      "Jurisdiction",             domain_map["Regulatory & Compliance"],      subdomain_map["Registration & Passporting"],0,"Country or region in which the fund is registered for distribution.",             "string", "[A-Z]{2,50}",              "Luxembourg",False,False,"Public",        "Legal",             "CSSF",          "Gold","T06","STW004","UCITS Directive","[]","Jurisdiction",                       "Certified","High",    None),
        ("DE-005","fund_manager",      "Fund Manager",             domain_map["Fund Structure & Master Data"], subdomain_map["Fund Entity"],               0,"Name of the investment management entity responsible for the fund.",             "string", None,                       "Apex Capital",False,False,"Public",       "Fund Administrator","Bloomberg",    "Gold","T01","STW001","AIFMD",          "[]","Fund Manager",                       "Certified","Medium",  None),
        ("DE-006","aum",               "Assets Under Management",  domain_map["Pricing & Valuation"],          subdomain_map["NAV Calculation"],           0,"Total market value of assets managed by the fund.",                              "float",  "####.##",                  "1234567.89",False,False,"Confidential","Fund Administrator","Bloomberg",    "Gold","T02","STW002","UCITS Directive","[]","Assets Under Management (AUM)",       "Certified","Critical",None),
        ("DE-007","reg_status",        "Registration Status",      domain_map["Regulatory & Compliance"],      subdomain_map["Registration & Passporting"],0,"Current registration state for a given jurisdiction.",                            "enum",   "Registered|Pending|Restricted","Registered",False,False,"Internal","Legal",          "CSSF",          "Gold","T06","STW004","UCITS Directive","[]","Registration Status",                "Certified","High",    None),
        ("DE-008","nav_frequency",     "NAV Frequency",            domain_map["Pricing & Valuation"],          subdomain_map["NAV Calculation"],           0,"Frequency at which the NAV is calculated and published.",                        "enum",   "Daily|Weekly|Monthly",     "Daily",     False,False,"Public",        "Fund Administrator","Bloomberg",    "Gold","T02","STW002","UCITS Directive","[]","NAV Frequency",                      "Draft",    "Medium",  None),
        ("DE-009","error_flag",        "Error Flag",               domain_map["Transactions & Cash Flows"],    subdomain_map["Investor Transactions"],    0,"Boolean flag indicating a data quality issue on a transaction.",                 "boolean",None,                       "False",     False,False,"Internal",      "Transfer Agent",    None,            "Silver","T01","STW001",None,            "[]","Error Flag",                          "Draft",    "Low",     None),
        ("DE-010","weight_pct",        "Portfolio Weight (%)",     domain_map["Portfolio & Holdings"],         subdomain_map["Portfolio Positions"],       0,"Percentage weight of a security within the portfolio.",                          "float",  "##.##",                    "2.93",      False,False,"Internal",      "Custodian",         None,            "Silver","T02","STW002",None,            "[]","Portfolio Weight",                    "Draft",    "Medium",  None),
    ]
    cursor.executemany(
        """INSERT INTO data_element(element_id,element_name,display_name,domain_id,subdomain_id,
           concept_id,description,data_type,format,example_value,nullable,pii,classification,
           golden_source,secondary_source,data_layer,owner_team,steward,regulatory_ref,
           linked_rule_ids,glossary_term,status,criticality,last_reviewed)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        data_elements,
    )

    # ─────────────────────────────────────────────
    # 9. BUSINESS GLOSSARY
    # ─────────────────────────────────────────────
    glossary = [
        ("Net Asset Value (NAV)","NAV",domain_map["Pricing & Valuation"],subdomain_map["NAV Calculation"],
         "The per-share value of a fund, calculated daily as total assets minus liabilities divided by shares outstanding.",
         "NAV of 142.35 USD means each share is worth 142.35 USD.",
         '["AUM","Share Class","Valuation"]',"UCITS Directive Art. 85","float","STW002","Certified"),
        ("ISIN","ISIN",domain_map["Fund Structure & Master Data"],subdomain_map["Share Class"],
         "International Securities Identification Number — a 12-character alphanumeric code identifying a security.",
         "LU0123456789","[]","ISO 6166","string","STW001","Certified"),
        ("Settlement Status",None,domain_map["Transactions & Cash Flows"],subdomain_map["Investor Transactions"],
         "The state of a transaction's settlement process: Settled, Pending, Failed, or Cancelled.",
         "A subscription that has completed delivery vs payment is Settled.",
         '["Transaction","Custodian"]',"CSDR","enum","STW003","Certified"),
        ("AUM","AUM",domain_map["Pricing & Valuation"],subdomain_map["NAV Calculation"],
         "Assets Under Management — the total market value of all assets managed on behalf of clients.",
         "Fund AUM of USD 4.2B.",
         '["NAV","Fund"]',"UCITS Directive","float","STW002","Certified"),
        ("Jurisdiction",None,domain_map["Regulatory & Compliance"],subdomain_map["Registration & Passporting"],
         "A country or regulatory zone in which a fund is authorised for distribution.",
         "Luxembourg, Germany, France are typical UCITS jurisdictions.",
         '["Registration","CSSF","BaFin"]',"UCITS Directive","string","STW004","Certified"),
        ("Golden Record",None,domain_map["Fund Structure & Master Data"],subdomain_map["Fund Entity"],
         "A single, trusted, authoritative version of a data entity, reconciled from multiple source systems.",
         "The golden record for SC001 merges data from Transfer Agent and Fund Admin.",
         '["MDM","Data Quality","Conflict Resolution"]',None,"concept","STW001","Draft"),
        ("Data Steward",None,domain_map["Fund Structure & Master Data"],subdomain_map["Fund Entity"],
         "A business role responsible for the quality, governance and fitness-for-purpose of a data domain.",
         "The Data Steward for NAV data is responsible for validating daily NAV feeds.",
         '["Data Owner","Data Governance"]',None,"string","STW001","Draft"),
    ]
    cursor.executemany(
        """INSERT INTO business_glossary(term,abbreviation,domain_id,subdomain_id,definition,
           example,related_terms,regulatory_ref,data_type,owner,status)
           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        glossary,
    )

    # ─────────────────────────────────────────────
    # COMMIT & CLOSE
    # ─────────────────────────────────────────────
    conn.commit()
    conn.close()
    print(f"✅ Database created and seeded at {db_path}")
    return db_path


# ✅ Alias so app.py can call create_db()
create_db = create_database_and_seed


def get_connection(db_path: str = DB_PATH):
    """Get a live connection to the DB."""
    return sqlite3.connect(db_path)


def load_fundgov360_data(db_path: str = DB_PATH) -> dict:
    """
    Load all tables as a dict of DataFrames.
    Auto-creates the DB if it doesn't exist (Streamlit Cloud safe).
    """
    if not os.path.exists(db_path):
        create_database_and_seed(db_path)

    conn = get_connection(db_path)
    tables = {
        "fund":               pd.read_sql_query("SELECT * FROM fund", conn),
        "sub_funds":          pd.read_sql_query("SELECT * FROM sub_fund", conn),
        "share_classes":      pd.read_sql_query("SELECT * FROM share_class", conn),
        "nav":                pd.read_sql_query("SELECT * FROM nav ORDER BY nav_date ASC", conn),
        "portfolio":          pd.read_sql_query("SELECT * FROM portfolio_position ORDER BY market_value DESC", conn),
        "transactions":       pd.read_sql_query("SELECT * FROM fund_transaction ORDER BY tx_date DESC", conn),
        "registration":       pd.read_sql_query("SELECT * FROM registration", conn),
        "stewards":           pd.read_sql_query("SELECT * FROM steward", conn),
        "teams":              pd.read_sql_query("SELECT * FROM team", conn),
        "data_elements":      pd.read_sql_query("SELECT * FROM data_element", conn),
        "taxonomy_domains":   pd.read_sql_query("SELECT * FROM taxonomy_domain", conn),
        "taxonomy_subdomains":pd.read_sql_query(
            "SELECT td.domain_name, tsd.* FROM taxonomy_subdomain tsd "
            "JOIN taxonomy_domain td ON tsd.domain_id = td.domain_id", conn),
        "taxonomy_concepts":  pd.read_sql_query(
            "SELECT td.domain_name, tsd.subdomain_name, tc.* FROM taxonomy_concept tc "
            "JOIN taxonomy_subdomain tsd ON tc.subdomain_id = tsd.subdomain_id "
            "JOIN taxonomy_domain td ON tsd.domain_id = td.domain_id", conn),
        "business_glossary":  pd.read_sql_query("SELECT * FROM business_glossary", conn),
    }
    conn.close()
    return tables


if __name__ == "__main__":
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"🗑️  Removed existing {DB_PATH}")
    create_database_and_seed()
