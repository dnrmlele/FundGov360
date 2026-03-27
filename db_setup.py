# db_setup.py
# FundGov360 v5 — SQLite Database with Demo Data Generator
# Schema + Seeding + Utils for Streamlit integration

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker()
Faker.seed(42)
np.random.seed(42)

DB_PATH = "fundgov360.db"

def create_database_and_seed():
    """Create complete schema and populate with production-like demo data."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ─────────────────────────────────────────────
    # SCHEMA DEFINITION
    # Bronze/Silver/Gold + Governance Metadata
    # ─────────────────────────────────────────────

    schema_sql = """
    -- 1. CORE FUND HIERARCHY (Bronze → Gold)
    CREATE TABLE IF NOT EXISTS fund (
        fund_id TEXT PRIMARY KEY,
        fund_name TEXT NOT NULL,
        lei TEXT UNIQUE NOT NULL,
        legal_form TEXT NOT NULL,
        domicile TEXT NOT NULL,
        fund_manager TEXT NOT NULL,
        custodian TEXT NOT NULL,
        fund_admin TEXT NOT NULL,
        inception_date DATE NOT NULL,
        currency TEXT NOT NULL,
        aum_usd DECIMAL(15,2),
        status TEXT DEFAULT 'Active',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS sub_fund (
        sub_fund_id TEXT PRIMARY KEY,
        fund_id TEXT NOT NULL,
        sub_fund_name TEXT NOT NULL,
        isin_base TEXT,
        asset_class TEXT NOT NULL,
        strategy TEXT NOT NULL,
        benchmark TEXT,
        inception_date DATE NOT NULL,
        management_fee_pct DECIMAL(5,2),
        performance_fee_pct DECIMAL(5,2),
        nav_frequency TEXT DEFAULT 'Daily',
        status TEXT DEFAULT 'Active',
        data_layer TEXT DEFAULT 'Gold',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (fund_id) REFERENCES fund(fund_id)
    );

    CREATE TABLE IF NOT EXISTS share_class (
        sc_id TEXT PRIMARY KEY,
        sub_fund_id TEXT NOT NULL,
        fund_id TEXT NOT NULL,
        sc_name TEXT NOT NULL,
        isin TEXT UNIQUE NOT NULL,
        currency TEXT NOT NULL,
        investor_type TEXT NOT NULL,
        min_investment DECIMAL(12,2),
        distribution TEXT,
        hedged BOOLEAN DEFAULT FALSE,
        inception_date DATE NOT NULL,
        status TEXT DEFAULT 'Active',
        shares_outstanding DECIMAL(15,0),
        transfer_agent TEXT NOT NULL,
        data_layer TEXT DEFAULT 'Gold',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sub_fund_id) REFERENCES sub_fund(sub_fund_id),
        FOREIGN KEY (fund_id) REFERENCES fund(fund_id)
    );

    -- 2. TIME SERIES DATA (Silver → Gold)
    CREATE TABLE IF NOT EXISTS nav (
        nav_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sc_id TEXT NOT NULL,
        fund_id TEXT NOT NULL,
        sub_fund_id TEXT NOT NULL,
        isin TEXT NOT NULL,
        nav_date DATE NOT NULL,
        currency TEXT NOT NULL,
        nav DECIMAL(10,4) NOT NULL,
        aum DECIMAL(15,2) NOT NULL,
        shares_outstanding DECIMAL(15,0) NOT NULL,
        source TEXT NOT NULL,
        validated BOOLEAN DEFAULT FALSE,
        data_layer TEXT DEFAULT 'Gold',
        UNIQUE(sc_id, nav_date),
        FOREIGN KEY (sc_id) REFERENCES share_class(sc_id)
    );

    CREATE TABLE IF NOT EXISTS portfolio_position (
        position_id TEXT PRIMARY KEY,
        sub_fund_id TEXT NOT NULL,
        fund_id TEXT NOT NULL,
        isin TEXT NOT NULL,
        security_name TEXT NOT NULL,
        asset_class TEXT NOT NULL,
        geography TEXT NOT NULL,
        sector TEXT NOT NULL,
        currency TEXT NOT NULL,
        market_value DECIMAL(15,2) NOT NULL,
        weight_pct DECIMAL(5,2) NOT NULL,
        quantity DECIMAL(15,0),
        price DECIMAL(10,4),
        valuation_date DATE NOT NULL,
        source TEXT NOT NULL,
        data_layer TEXT DEFAULT 'Silver',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sub_fund_id) REFERENCES sub_fund(sub_fund_id)
    );

    CREATE TABLE IF NOT EXISTS transaction (
        tx_id TEXT PRIMARY KEY,
        sc_id TEXT NOT NULL,
        fund_id TEXT NOT NULL,
        sub_fund_id TEXT NOT NULL,
        isin TEXT NOT NULL,
        tx_type TEXT NOT NULL,
        tx_date DATE NOT NULL,
        settlement_date DATE NOT NULL,
        amount DECIMAL(15,2) NOT NULL,
        currency TEXT NOT NULL,
        nav_at_tx DECIMAL(10,4),
        units DECIMAL(15,4),
        counterparty TEXT,
        investor_id TEXT,
        settlement_status TEXT NOT NULL,
        source TEXT NOT NULL,
        error_flag BOOLEAN DEFAULT FALSE,
        error_reason TEXT,
        data_layer TEXT DEFAULT 'Silver',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sc_id) REFERENCES share_class(sc_id)
    );

    CREATE TABLE IF NOT EXISTS registration (
        reg_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sc_id TEXT NOT NULL,
        fund_id TEXT NOT NULL,
        sub_fund_id TEXT NOT NULL,
        isin TEXT NOT NULL,
        jurisdiction TEXT NOT NULL,
        reg_status TEXT NOT NULL,
        reg_date DATE,
        expiry_date DATE,
        local_regulator TEXT,
        last_updated DATE NOT NULL,
        source TEXT NOT NULL,
        data_layer TEXT DEFAULT 'Gold',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (sc_id) REFERENCES share_class(sc_id)
    );

    -- 3. GOVERNANCE METADATA (Gold)
    CREATE TABLE IF NOT EXISTS steward (
        steward_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        role TEXT NOT NULL,
        department TEXT NOT NULL,
        phone TEXT,
        location TEXT,
        active BOOLEAN DEFAULT TRUE,
        assigned_since DATE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS team (
        team_id TEXT PRIMARY KEY,
        team_name TEXT UNIQUE NOT NULL,
        department TEXT NOT NULL,
        head TEXT NOT NULL,
        focus TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS taxonomy_domain (
        domain_id INTEGER PRIMARY KEY AUTOINCREMENT,
        domain_name TEXT UNIQUE NOT NULL,
        description TEXT NOT NULL,
        icon TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS taxonomy_subdomain (
        subdomain_id INTEGER PRIMARY KEY AUTOINCREMENT,
        domain_id INTEGER NOT NULL,
        subdomain_name TEXT NOT NULL,
        description TEXT NOT NULL,
        FOREIGN KEY (domain_id) REFERENCES taxonomy_domain(domain_id)
    );

    CREATE TABLE IF NOT EXISTS taxonomy_concept (
        concept_id INTEGER PRIMARY KEY AUTOINCREMENT,
        subdomain_id INTEGER NOT NULL,
        concept_name TEXT NOT NULL,
        FOREIGN KEY (subdomain_id) REFERENCES taxonomy_subdomain(subdomain_id)
    );

    CREATE TABLE IF NOT EXISTS data_element (
        element_id TEXT PRIMARY KEY,
        element_name TEXT NOT NULL,
        display_name TEXT NOT NULL,
        domain_id INTEGER,
        subdomain_id INTEGER,
        concept_id INTEGER,
        description TEXT NOT NULL,
        data_type TEXT NOT NULL,
        format TEXT,
        example_value TEXT,
        nullable BOOLEAN DEFAULT TRUE,
        pii BOOLEAN DEFAULT FALSE,
        classification TEXT NOT NULL,
        golden_source TEXT NOT NULL,
        secondary_source TEXT,
        data_layer TEXT DEFAULT 'Gold',
        owner_team TEXT,
        steward TEXT,
        regulatory_ref TEXT,
        linked_rule_ids TEXT,  -- JSON array
        glossary_term TEXT,
        status TEXT DEFAULT 'Draft',
        criticality TEXT DEFAULT 'Medium',
        last_reviewed DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (domain_id) REFERENCES taxonomy_domain(domain_id),
        FOREIGN KEY (subdomain_id) REFERENCES taxonomy_subdomain(subdomain_id),
        FOREIGN KEY (concept_id) REFERENCES taxonomy_concept(concept_id)
    );

    CREATE TABLE IF NOT EXISTS data_element_usage (
        usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
        element_id TEXT NOT NULL,
        consuming_team TEXT NOT NULL,
        usage_purpose TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (element_id) REFERENCES data_element(element_id)
    );

    CREATE TABLE IF NOT EXISTS business_glossary (
        glossary_id INTEGER PRIMARY KEY AUTOINCREMENT,
        term TEXT UNIQUE NOT NULL,
        abbreviation TEXT,
        domain_id INTEGER,
        subdomain_id INTEGER,
        definition TEXT NOT NULL,
        example TEXT,
        related_terms TEXT,  -- JSON array
        regulatory_ref TEXT,
        data_type TEXT,
        owner TEXT NOT NULL,
        status TEXT DEFAULT 'Draft',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (domain_id) REFERENCES taxonomy_domain(domain_id),
        FOREIGN KEY (subdomain_id) REFERENCES taxonomy_subdomain(subdomain_id)
    );

    -- 4. DQ & GOVERNANCE
    CREATE TABLE IF NOT EXISTS dq_rule (
        rule_id TEXT PRIMARY KEY,
        rule_name TEXT NOT NULL,
        description TEXT,
        dataset TEXT NOT NULL,
        field TEXT NOT NULL,
        rule_type TEXT NOT NULL,
        threshold_min DECIMAL,
        threshold_max DECIMAL,
        regex_pattern TEXT,
        formula_expr TEXT,
        severity TEXT NOT NULL,
        active BOOLEAN DEFAULT TRUE,
        category TEXT,
        owner TEXT,
        sla_pass_rate DECIMAL DEFAULT 95.0,
        created_date DATE NOT NULL,
        last_modified DATE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dq_run_result (
        result_id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_id TEXT NOT NULL,
        run_at TIMESTAMP NOT NULL,
        dataset TEXT NOT NULL,
        checked INTEGER NOT NULL,
        passed INTEGER NOT NULL,
        failed INTEGER NOT NULL,
        pass_rate DECIMAL(5,2) NOT NULL,
        sla_met BOOLEAN NOT NULL,
        FOREIGN KEY (rule_id) REFERENCES dq_rule(rule_id)
    );

    CREATE TABLE IF NOT EXISTS conflict (
        conflict_id TEXT PRIMARY KEY,
        conflict_type TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        dataset TEXT NOT NULL,
        field TEXT NOT NULL,
        source_a TEXT NOT NULL,
        value_a ANY,
        source_b TEXT NOT NULL,
        value_b ANY,
        priority TEXT NOT NULL,
        status TEXT DEFAULT 'Open',
        assigned_to TEXT,
        department TEXT,
        detected_at TIMESTAMP NOT NULL,
        resolved_at TIMESTAMP,
        resolved_by TEXT,
        resolution_method TEXT,
        final_value ANY,
        resolution_comment TEXT,
        sla_hours INTEGER,
        sla_breached BOOLEAN DEFAULT FALSE,
        escalation_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS conflict_audit (
        audit_id TEXT PRIMARY KEY,
        conflict_id TEXT NOT NULL,
        action TEXT NOT NULL,
        performed_by TEXT NOT NULL,
        details TEXT,
        timestamp TIMESTAMP NOT NULL,
        FOREIGN KEY (conflict_id) REFERENCES conflict(conflict_id)
    );

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_nav_sc_date ON nav(sc_id, nav_date);
    CREATE INDEX IF NOT EXISTS idx_nav_date ON nav(nav_date);
    CREATE INDEX IF NOT EXISTS idx_tx_settlement ON transaction(settlement_status, settlement_date);
    CREATE INDEX IF NOT EXISTS idx_tx_date ON transaction(tx_date);
    CREATE INDEX IF NOT EXISTS idx_port_subfund ON portfolio_position(sub_fund_id);
    CREATE INDEX IF NOT EXISTS idx_reg_sc_jur ON registration(sc_id, jurisdiction);
    CREATE INDEX IF NOT EXISTS idx_element_domain ON data_element(domain_id);
    CREATE INDEX IF NOT EXISTS idx_element_status ON data_element(status);
    CREATE INDEX IF NOT EXISTS idx_conflict_status ON conflict(status);
    CREATE INDEX IF NOT EXISTS idx_conflict_priority ON conflict(priority);
    """
    cursor.executescript(schema_sql)

    # ─────────────────────────────────────────────
    # SEED PRODUCTION-LIKE DEMO DATA
    # ─────────────────────────────────────────────

    # Clear existing data for clean demo
    cursor.execute("DELETE FROM conflict_audit")
    cursor.execute("DELETE FROM conflict")
    cursor.execute("DELETE FROM dq_run_result")
    cursor.execute("DELETE FROM data_element_usage")
    cursor.execute("DELETE FROM business_glossary")
    cursor.execute("DELETE FROM data_element")
    cursor.execute("DELETE FROM taxonomy_concept")
    cursor.execute("DELETE FROM taxonomy_subdomain")
    cursor.execute("DELETE FROM taxonomy_domain")
    cursor.execute("DELETE FROM team")
    cursor.execute("DELETE FROM steward")
    cursor.execute("DELETE FROM registration")
    cursor.execute("DELETE FROM transaction")
    cursor.execute("DELETE FROM portfolio_position")
    cursor.execute("DELETE FROM nav")
    cursor.execute("DELETE FROM share_class")
    cursor.execute("DELETE FROM sub_fund")
    cursor.execute("DELETE FROM fund")

    # 1. FUND HIERARCHY
    funds = [
        ("F001", "Apex Global SICAV", "529900HNOAA1KXQJUQ27", "SICAV", "LU", "Apex Capital", "State Street", "Northern Trust", "2010-03-15", "USD", 4200000000, "Active"),
        ("F002", "Atlas Income ICAV",  "5493000ABC123DEF45678", "ICAV",  "IE", "Atlas AM",    "BNP Paribas", "CACEIS",      "2014-07-01", "EUR", 2800000000, "Active"),
        ("F003", "Orion EM Partners LP","529900XYZ987654321098", "LP",    "KY", "Orion Partners","Citi",        "Maples",      "2018-11-20", "USD", 950000000,  "Active"),
    ]
    cursor.executemany(
        "INSERT INTO fund VALUES(?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))",
        funds
    )

    sub_funds = [
        ("SF001", "F001", "Apex Global Equity",       "LU1234567890", "Equity",       "Long Only", "MSCI World", "2010-06-01", 0.75, 10.0, "Daily", "Active", "Gold"),
        ("SF002", "F001", "Apex Global Fixed Income", "LU0987654321", "Fixed Income", "Active",   "Bloomberg Global Agg", "2012-03-15", 0.50, 0.0,  "Daily", "Active", "Gold"),
        ("SF003", "F002", "Atlas Core Income",        "IE1111111111", "Fixed Income", "Passive",  "Bloomberg Euro Agg", "2014-09-01", 0.30, 0.0,  "Weekly","Active", "Gold"),
        ("SF004", "F002", "Atlas High Yield",         "IE2222222222", "Fixed Income", "Active",   "ICE BofA HY Index", "2016-01-10", 0.65, 15.0, "Daily", "Active", "Gold"),
        ("SF005", "F003", "Orion EM Equity",          "KY3333333333", "Equity",       "Long Only", "MSCI EM",    "2018-12-01", 0.85, 15.0, "Daily", "Active", "Gold"),
        ("SF006", "F003", "Orion EM Debt",            "KY4444444444", "Fixed Income", "Active",   "JPM EMBI",   "2020-05-20", 0.60, 10.0, "Weekly","Active", "Gold"),
    ]
    cursor.executemany(
        """INSERT INTO sub_fund VALUES(?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
        sub_funds
    )

    share_classes = [
        ("SC001", "SF001", "F001", "Apex Global Equity A-USD",   "LU0123456789", "USD", "Retail",        1000,    "Accumulating", False, "2010-06-01", "Active",  8700000, "RBC IS"),
        ("SC002", "SF001", "F001", "Apex Global Equity I-USD",   "LU0234567890", "USD", "Institutional", 1000000, "Accumulating", False, "2011-01-15", "Active",  2300000, "RBC IS"),
        ("SC003", "SF002", "F001", "Apex Global FI C-EUR H",     "LU0345678901", "EUR", "Retail",        1000,    "Distributing", True,  "2012-06-01", "Active",  1200000, "CACEIS TA"),
        ("SC004", "SF003", "F002", "Atlas Core Income D-EUR",    "IE0456789012", "EUR", "Institutional", 500000,  "Accumulating", False, "2014-10-01", "Active",  890000,  "RBC IS"),
        ("SC005", "SF004", "F002", "Atlas HY B-EUR",             "IE0567890123", "EUR", "Retail",        1000,    "Distributing", False, "2016-02-15", "Active",  3400000, "CACEIS TA"),
        ("SC006", "SF005", "F003", "Orion EM Equity A-USD",      "KY0678901234", "USD", "Retail",        1000,    "Accumulating", False, "2019-01-01", "Active",  2100000, "Maples TA"),
    ]
    cursor.executemany(
        """INSERT INTO share_class VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
        share_classes
    )

    # 2. TIME SERIES DEMO DATA (30 days)
    dates = [(datetime.today() - timedelta(days=i)).date() for i in range(30)]
    nav_records = []
    for date in dates:
        for sc_id in ["SC001", "SC002", "SC003", "SC004", "SC005", "SC006"]:
            nav_base = {"SC001":142.35, "SC002":142.35, "SC003":98.12, "SC004":98.12, "SC005":87.45, "SC006":87.45}
            nav = round(nav_base[sc_id] + np.random.normal(0, 0.5), 4)
            nav_records.append((sc_id, sc_id.split("_")[0][1:], sc_id.split("_")[0], sc_id.split("_")[1:], sc_id.split(" ")[-1].split("-")[0],
                                date, sc_id.split(" ")[-1].split("-")[0], nav, nav*random.uniform(8700000,8900000), 8700000, "Fund Admin", True, "Gold"))

    cursor.executemany(
        """INSERT INTO nav(sc_id, fund_id, sub_fund_id, isin, currency, nav_date, nav, aum, shares_outstanding, source, validated, data_layer)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        nav_records
    )

    # 3. PORTFOLIO POSITIONS (demo)
    portfolio_demo = [
        ("SF001_P001", "SF001", "F001", "US0123456789", "Apple Inc", "Equity", "North America", "Technology", "USD", 123000000, 2.93, 2500000, 49.20, "2026-03-27", "Custodian", "Silver"),
        ("SF001_P002", "SF001", "F001", "US0987654321", "Microsoft Corp", "Equity", "North America", "Technology", "USD", 112000000, 2.67, 1200000, 93.33, "2026-03-27", "Custodian", "Silver"),
        ("SF002_P001", "SF002", "F001", "US1111111111", "US Treasury 2.5% 2028", "Government Bond", "North America", "Government", "USD", 89000000, 2.12, 89000, 1000.00, "2026-03-27", "Custodian", "Silver"),
    ]
    cursor.executemany(
        """INSERT INTO portfolio_position VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        portfolio_demo
    )

    # 4. TRANSACTIONS (demo)
    tx_demo = [
        ("TX001", "SC001", "F001", "SF001", "LU0123456789", "Subscription", "2026-03-25", "2026-03-28", 2500000, "USD", 142.35, 17540.00, "Bank of America", "INV4821", "Settled", "Transfer Agent", False, None, "Silver"),
        ("TX002", "SC002", "F001", "SF001", "LU0234567890", "Redemption",  "2026-03-26", "2026-03-29", 1250000, "USD", 142.50, 8772.00,  "JPMorgan AM",   "INV1934", "Pending",  "Transfer Agent", False, None, "Silver"),
        ("TX003", "SC003", "F001", "SF002", "LU0345678901", "Subscription", "2026-03-27", "2026-03-30", 750000,  "EUR", 98.12, 7642.00,  "UBS",           "INV6723", "Failed",   "Transfer Agent", True,  "Missing investor ID", "Silver"),
    ]
    cursor.executemany(
        """INSERT INTO transaction(tx_id, sc_id, fund_id, sub_fund_id, isin, tx_type, tx_date, settlement_date, amount, currency, nav_at_tx, units, counterparty, investor_id, settlement_status, source, error_flag, error_reason, data_layer)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        tx_demo
    )

    # 5. REGISTRATIONS (demo)
    reg_demo = [
        ("SC001", "F001", "SF001", "LU0123456789", "Luxembourg", "Registered", "2010-06-01", "2028-06-01", "CSSF", "2026-03-27", "Legal"),
        ("SC001", "F001", "SF001", "LU0123456789", "Germany",    "Registered", "2011-03-15", "2027-03-15", "BaFin", "2026-03-27", "Legal"),
        ("SC001", "F001", "SF001", "LU0123456789", "France",     "Pending",    None,         None,         "AMF",   "2026-03-15", "Legal"),
        ("SC002", "F001", "SF001", "LU0234567890", "United Kingdom", "Restricted", "2012-01-10", "2026-01-10", "FCA",  "2026-03-27", "Legal"),
    ]
    cursor.executemany(
        """INSERT INTO registration(sc_id, fund_id, sub_fund_id, isin, jurisdiction, reg_status, reg_date, expiry_date, local_regulator, last_updated, source)
           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        reg_demo
    )

    # 6. STEWARDS & TEAMS
    stewards = [
        ("STW001", "Clément Denorme", "cdenorme@fundgov360.lu", "Data Steward", "Data Management", "+352 12345678", "Luxembourg", True, "2024-01-01"),
        ("STW002", "Sophie Martin",   "smartin@fundaccounting.lu","Data Owner",  "Fund Accounting", "+352 87654321", "Luxembourg", True, "2024-01-01"),
        ("STW003", "James O'Brien",   "jobrien@operations.ie",   "Data Owner",  "Fund Operations", "+353 12345678", "Dublin",     True, "2024-01-01"),
        ("STW004", "Clara Muller",    "cmuller@compliance.lu",   "Compliance Steward", "Compliance", "+352 23456789", "Luxembourg", True, "2024-01-01"),
    ]
    cursor.executemany(
        "INSERT INTO steward VALUES(?,?,?,?,?,?,?,?,datetime('now'))",
        stewards
    )

    teams = [
        ("T01", "Data Management", "Operations", "Clément Denorme", "Data governance, quality, lineage"),
        ("T02", "Fund Accounting",  "Finance",    "Sophie Martin",   "NAV calculation, AUM, performance"),
        ("T03", "Fund Operations",  "Operations", "James O'Brien",   "Share class setup, static data, fees"),
        ("T04", "Compliance",       "Compliance", "Clara Muller",    "Regulatory reporting, SFDR, AML/KYC"),
    ]
    cursor.executemany(
        "INSERT INTO team VALUES(?,?,?,?,datetime('now'))",
        teams
    )

    # 7. TAXONOMY
    domains = [
        ("Fund Structure & Master Data", "🏦", "Core fund entity hierarchy, legal structure, and master reference data."),
        ("Pricing & Valuation",         "💲", "NAV calculation, AUM, performance measurement and benchmark data."),
        ("Portfolio & Holdings",        "📁", "Security-level portfolio positions, exposures and risk analytics."),
        ("Transactions & Cash Flows",   "🔄", "Fund-level subscriptions, redemptions, trades and cash movements."),
        ("Regulatory & Compliance",     "⚖️", "Passporting, fund registration, reporting obligations and compliance monitoring."),
        ("Reference Data",              "📚", "Market, instrument and entity reference data underpinning all fund operations."),
        ("Risk & Performance Analytics","📊", "Risk metrics, stress testing, liquidity analysis and performance attribution."),
    ]
    cursor.executemany("INSERT INTO taxonomy_domain(domain_name, description, icon) VALUES(?,?,?)", domains)

    # Get domain IDs for subdomains
    domain_map = {}
    for row in cursor.execute("SELECT domain_id, domain_name FROM taxonomy_domain"):
        domain_map[row[1]] = row[0]

    subdomains = [
        (domain_map["Fund Structure & Master Data"], "Fund Entity", "Top-level legal fund entity attributes."),
        (domain_map["Fund Structure & Master Data"], "Sub-Fund",    "Investment compartment within an umbrella fund structure."),
        (domain_map["Pricing & Valuation"],          "NAV Calculation", "Net Asset Value computation and validation data."),
        (domain_map["Transactions & Cash Flows"],    "Investor Transactions", "Subscription and redemption orders from investors."),
        (domain_map["Regulatory & Compliance"],      "Registration & Passporting", "Jurisdiction-level fund registration status."),
    ]
    cursor.executemany("INSERT INTO taxonomy_subdomain(domain_id, subdomain_name, description) VALUES(?,?,?)", subdomains)

    # Concepts
    subdomain_map = {}
    for row in cursor.execute("SELECT subdomain_id, subdomain_name FROM taxonomy_subdomain"):
        subdomain_map[row[1]] = row[0]

    concepts = [
        (subdomain_map["Fund Entity"], "Legal Identity"),
        (subdomain_map["Sub-Fund"],    "Sub-Fund Identity"),
        (subdomain_map["NAV Calculation"], "NAV Record"),
        (subdomain_map["Investor Transactions"], "Transaction Record"),
        (subdomain_map["Registration & Passporting"], "Registration Record"),
    ]
    cursor.executemany("INSERT INTO taxonomy_concept(subdomain_id, concept_name) VALUES(?,?)", concepts)

    # 8. DATA ELEMENTS (sample)
    data_elements = [
        ("DE-001", "nav", "Net Asset Value", domain_map["Pricing & Valuation"], subdomain_map["NAV Calculation"], 2, "Per-share net asset value calculated as (Total Assets - Total Liabilities) / Shares Outstanding.", "float", "######.####", "142.3512", False, False, "Confidential", "Fund Administrator", "Bloomberg", "Gold", "T02", "STW002", "UCITS Directive", "[]", "Net Asset Value (NAV)", "Certified", "Critical", None),
        ("DE-002", "isin", "ISIN", domain_map["Fund Structure & Master Data"], subdomain_map["Fund Entity"], 0, "International Securities Identification Number uniquely identifying the share class (ISO 6166).", "string", "^[A-Z]{{2}}[A-Z0-9]{{9}}[0-9]$", "LU0123456789", False, False, "Public", "Fund Administrator", "Euroclear", "Gold", "T03", "STW001", "ISO 6166", "[]", "International Securities Identification Number", "Certified", "Critical", None),
        ("DE-003", "settlement_status", "Settlement Status", domain_map["Transactions & Cash Flows"], subdomain_map["Investor Transactions"], 3, "Current settlement state of a transaction.", "enum", "Settled|Pending|Failed|Cancelled", "Settled", False, False, "Internal", "Transfer Agent", "Custodian", "Silver", "T03", "STW003", "CSDR", "[]", "Settlement", "Certified", "High", None),
    ]
    cursor.executemany(
        """INSERT INTO data_element(element_id, element_name, display_name, domain_id, subdomain_id, concept_id, description, data_type, format, example_value, nullable, pii, classification, golden_source, secondary_source, data_layer, owner_team, steward, regulatory_ref, linked_rule_ids, glossary_term, status, criticality, last_reviewed)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))""",
        data_elements
    )

    # Commit and close
    conn.commit()
    conn.close()
    print(f"✅ Database created and seeded at {DB_PATH}")
    print("Tables: fund, sub_fund, share_class, nav, portfolio_position, transaction, registration")
    print("Governance: steward, team, taxonomy_*, data_element*, business_glossary, dq_*")
    return DB_PATH


def get_connection():
    """Get a connection to the DB."""
    return sqlite3.connect(DB_PATH)


def load_fundgov360_data():
    """Load all tables as a dict of DataFrames — drop-in replacement for data_generator."""
    conn = get_connection()
    
    tables = {
        "fund":              pd.read_sql_query("SELECT * FROM fund", conn),
        "sub_funds":         pd.read_sql_query("SELECT * FROM sub_fund", conn),
        "share_classes":     pd.read_sql_query("SELECT * FROM share_class", conn),
        "nav":               pd.read_sql_query("SELECT * FROM nav ORDER BY nav_date DESC", conn),
        "portfolio":         pd.read_sql_query("SELECT * FROM portfolio_position", conn),
        "transactions":      pd.read_sql_query("SELECT * FROM transaction ORDER BY tx_date DESC", conn),
        "registration":      pd.read_sql_query("SELECT * FROM registration", conn),
        "stewards":          pd.read_sql_query("SELECT * FROM steward", conn),
        "teams":             pd.read_sql_query("SELECT * FROM team", conn),
        "data_elements":     pd.read_sql_query("SELECT * FROM data_element", conn),
        "taxonomy_domains":  pd.read_sql_query("SELECT * FROM taxonomy_domain", conn),
        "taxonomy_subdomains": pd.read_sql_query("SELECT td.domain_name, tsd.* FROM taxonomy_subdomain tsd JOIN taxonomy_domain td ON tsd.domain_id = td.domain_id", conn),
        "taxonomy_concepts": pd.read_sql_query("""
            SELECT td.domain_name, tsd.subdomain_name, tc.*
            FROM taxonomy_concept tc 
            JOIN taxonomy_subdomain tsd ON tc.subdomain_id = tsd.subdomain_id 
            JOIN taxonomy_domain td ON tsd.domain_id = td.domain_id
        """, conn),
        "business_glossary": pd.read_sql_query("SELECT * FROM business_glossary", conn),
    }
    
    conn.close()
    return tables


if __name__ == "__main__":
    create_database_and_seed()
