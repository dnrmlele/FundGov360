# utils/data_catalog.py
# FundGov360 v5 — Enterprise Data Catalog with Fund-Oriented Taxonomy
# Inspired by FIBO, EDM Council DCAM, Collibra, Atlan, Alation
# Taxonomy: Domain → Subdomain → Concept → Data Element

import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta
import streamlit as st

# ─────────────────────────────────────────────
# FUND-ORIENTED TAXONOMY
# 4 levels: Domain → Subdomain → Concept → Data Element
# Inspired by FIBO (Financial Industry Business Ontology)
# and EDM Council's DCAM framework
# ─────────────────────────────────────────────

TAXONOMY = {
    "Fund Structure & Master Data": {
        "icon": "🏦",
        "description": "Core fund entity hierarchy, legal structure, and master reference data.",
        "subdomains": {
            "Fund Entity": {
                "description": "Top-level legal fund entity attributes.",
                "concepts": {
                    "Legal Identity": [
                        "fund_id", "fund_name", "lei", "legal_form", "domicile",
                        "fund_manager", "inception_date",
                    ],
                    "Service Providers": [
                        "custodian", "fund_admin", "auditor", "legal_counsel",
                        "depositary", "paying_agent",
                    ],
                    "Fund Classification": [
                        "fund_type", "investment_strategy", "asset_class_primary",
                        "ucits_flag", "aifmd_flag", "esg_flag",
                    ],
                },
            },
            "Sub-Fund": {
                "description": "Investment compartment within an umbrella fund structure.",
                "concepts": {
                    "Sub-Fund Identity": [
                        "sub_fund_id", "sub_fund_name", "fund_id", "isin_base",
                        "inception_date", "status", "currency_base",
                    ],
                    "Investment Policy": [
                        "asset_class", "strategy", "benchmark", "geographic_focus",
                        "sector_focus", "leverage_limit", "concentration_limit",
                    ],
                    "Fee Structure": [
                        "management_fee_pct", "performance_fee_pct", "entry_fee_max",
                        "exit_fee_max", "ongoing_charges", "hurdle_rate",
                    ],
                    "Operations": [
                        "nav_frequency", "dealing_frequency", "cut_off_time",
                        "settlement_days", "valuation_point",
                    ],
                },
            },
            "Share Class": {
                "description": "Individual share class within a sub-fund.",
                "concepts": {
                    "Share Class Identity": [
                        "sc_id", "sc_name", "isin", "ticker", "sedol",
                        "sub_fund_id", "inception_date", "status",
                    ],
                    "Class Characteristics": [
                        "currency", "hedged", "distribution", "investor_type",
                        "min_investment", "min_subsequent", "denomination",
                    ],
                    "Transfer Agent Data": [
                        "transfer_agent", "shares_outstanding", "registrar",
                        "nominee_flag", "trailer_fee_pct",
                    ],
                },
            },
        },
    },

    "Pricing & Valuation": {
        "icon": "💲",
        "description": "NAV calculation, AUM, performance measurement and benchmark data.",
        "subdomains": {
            "NAV Calculation": {
                "description": "Net Asset Value computation and validation data.",
                "concepts": {
                    "NAV Record": [
                        "nav", "nav_date", "nav_currency", "sc_id", "isin",
                        "source", "validated", "data_layer",
                    ],
                    "NAV Validation": [
                        "tolerance_pct", "prev_nav", "nav_change_pct",
                        "spike_flag", "override_flag", "override_reason",
                    ],
                    "Pricing Sources": [
                        "primary_source", "secondary_source", "pricing_vendor",
                        "price_type", "price_frequency",
                    ],
                },
            },
            "AUM & Flows": {
                "description": "Assets under management and net flow tracking.",
                "concepts": {
                    "AUM Record": [
                        "aum", "aum_date", "aum_currency", "shares_outstanding",
                        "nav_per_share", "fund_id", "sub_fund_id",
                    ],
                    "Net Flows": [
                        "net_subscriptions", "net_redemptions", "net_flow",
                        "flow_date", "flow_currency",
                    ],
                },
            },
            "Performance": {
                "description": "Return calculation, attribution and benchmark comparison.",
                "concepts": {
                    "Return Metrics": [
                        "ytd_pct", "mtd_pct", "1y_return", "3y_return_ann",
                        "5y_return_ann", "since_inception_ann",
                    ],
                    "Benchmark": [
                        "benchmark_name", "benchmark_isin", "benchmark_return",
                        "active_return", "tracking_error", "information_ratio",
                    ],
                    "Risk-Adjusted": [
                        "sharpe_ratio", "sortino_ratio", "max_drawdown",
                        "volatility_ann", "beta", "alpha",
                    ],
                },
            },
        },
    },

    "Portfolio & Holdings": {
        "icon": "📁",
        "description": "Security-level portfolio positions, exposures and risk analytics.",
        "subdomains": {
            "Security Master": {
                "description": "Reference data for investable instruments.",
                "concepts": {
                    "Security Identity": [
                        "isin", "sedol", "cusip", "bloomberg_ticker",
                        "security_name", "issuer_name", "country_of_risk",
                    ],
                    "Instrument Classification": [
                        "asset_class", "instrument_type", "sector", "sub_sector",
                        "geography", "rating", "rating_agency",
                    ],
                    "Fixed Income Attributes": [
                        "maturity_date", "coupon_pct", "coupon_frequency",
                        "yield_to_maturity", "duration", "convexity",
                        "seniority", "callable_flag",
                    ],
                    "Equity Attributes": [
                        "market_cap", "dividend_yield", "pe_ratio",
                        "price_to_book", "earnings_per_share",
                    ],
                },
            },
            "Position": {
                "description": "Portfolio holding records at position level.",
                "concepts": {
                    "Position Record": [
                        "position_id", "sub_fund_id", "isin", "quantity",
                        "market_value", "weight_pct", "valuation_date", "source",
                    ],
                    "Valuation": [
                        "price", "price_currency", "fx_rate", "local_market_value",
                        "base_market_value", "accrued_interest",
                    ],
                    "Exposure": [
                        "gross_exposure", "net_exposure", "leverage_contribution",
                        "var_contribution", "duration_contribution",
                    ],
                },
            },
            "ESG & Sustainability": {
                "description": "ESG scores and SFDR-related classification.",
                "concepts": {
                    "ESG Scores": [
                        "esg_score", "environmental_score", "social_score",
                        "governance_score", "esg_rating_provider",
                    ],
                    "SFDR": [
                        "sfdr_article", "sustainable_investment_pct",
                        "pai_indicator", "taxonomy_alignment_pct",
                    ],
                },
            },
        },
    },

    "Transactions & Cash Flows": {
        "icon": "🔄",
        "description": "Fund-level subscriptions, redemptions, trades and cash movements.",
        "subdomains": {
            "Investor Transactions": {
                "description": "Subscription and redemption orders from investors.",
                "concepts": {
                    "Transaction Record": [
                        "tx_id", "tx_type", "tx_date", "settlement_date",
                        "sc_id", "isin", "amount", "currency",
                    ],
                    "Settlement": [
                        "settlement_status", "settlement_method",
                        "settlement_currency", "fx_rate", "net_amount",
                    ],
                    "Investor": [
                        "investor_id", "investor_name", "investor_type",
                        "investor_country", "aml_status", "kyc_date",
                    ],
                    "Counterparty": [
                        "counterparty", "counterparty_lei", "counterparty_type",
                        "counterparty_country", "broker_code",
                    ],
                },
            },
            "Portfolio Trades": {
                "description": "Buy/sell orders at security level within the portfolio.",
                "concepts": {
                    "Trade Record": [
                        "trade_id", "trade_date", "isin", "direction",
                        "quantity", "price", "gross_amount", "net_amount",
                    ],
                    "Execution": [
                        "executing_broker", "execution_venue", "order_type",
                        "execution_price", "slippage_bps", "commission",
                    ],
                },
            },
            "Corporate Actions": {
                "description": "Dividends, splits, mergers and other corporate events.",
                "concepts": {
                    "Corporate Action": [
                        "ca_id", "ca_type", "ex_date", "payment_date",
                        "isin", "ratio", "cash_amount",
                    ],
                },
            },
        },
    },

    "Regulatory & Compliance": {
        "icon": "⚖️",
        "description": "Passporting, fund registration, reporting obligations and compliance monitoring.",
        "subdomains": {
            "Registration & Passporting": {
                "description": "Jurisdiction-level fund registration status.",
                "concepts": {
                    "Registration Record": [
                        "sc_id", "isin", "jurisdiction", "reg_status",
                        "reg_date", "expiry_date", "local_regulator",
                    ],
                    "Passporting": [
                        "passport_type", "home_regulator", "host_regulator",
                        "notification_date", "approval_date",
                    ],
                },
            },
            "UCITS & AIFMD": {
                "description": "UCITS/AIFMD compliance parameters and limits.",
                "concepts": {
                    "UCITS Compliance": [
                        "ucits_eligible_flag", "eligible_assets_pct",
                        "concentration_5_10_40_rule", "leverage_ucits",
                        "derivative_exposure", "counterparty_limit_pct",
                    ],
                    "AIFMD": [
                        "aifmd_flag", "leverage_gross", "leverage_commitment",
                        "liquidity_stress_test_date", "depositary_name",
                    ],
                },
            },
            "EMIR & MiFID II": {
                "description": "Derivatives reporting and best execution obligations.",
                "concepts": {
                    "EMIR Reporting": [
                        "uti", "trade_repository", "clearing_obligation",
                        "variation_margin", "initial_margin",
                    ],
                    "MiFID II": [
                        "lei", "best_execution_report", "rts27_flag",
                        "target_market_criteria", "cost_and_charges",
                    ],
                },
            },
            "SFDR & ESG Reporting": {
                "description": "Sustainable Finance Disclosure Regulation reporting.",
                "concepts": {
                    "SFDR Disclosure": [
                        "sfdr_article", "rts_pai_indicator", "do_no_harm_flag",
                        "sustainable_investment_objective", "taxonomy_eligible_pct",
                    ],
                },
            },
        },
    },

    "Reference Data": {
        "icon": "📚",
        "description": "Market, instrument and entity reference data underpinning all fund operations.",
        "subdomains": {
            "Market Data": {
                "description": "Prices, rates and indices from market data vendors.",
                "concepts": {
                    "Price Data": [
                        "isin", "price_date", "open_price", "close_price",
                        "bid_price", "ask_price", "volume", "source_vendor",
                    ],
                    "FX Rates": [
                        "currency_pair", "fx_rate", "fx_date",
                        "fx_source", "fixing_type",
                    ],
                    "Indices": [
                        "index_id", "index_name", "index_level",
                        "index_date", "index_provider",
                    ],
                },
            },
            "Counterparty Reference": {
                "description": "Legal entity master for brokers, custodians and counterparties.",
                "concepts": {
                    "Legal Entity": [
                        "lei", "entity_name", "entity_type", "country_of_incorporation",
                        "bic", "credit_rating", "parent_lei",
                    ],
                },
            },
            "Calendar & Time": {
                "description": "Business day calendars and settlement conventions.",
                "concepts": {
                    "Business Calendar": [
                        "calendar_id", "currency", "exchange", "holiday_date",
                        "settlement_convention", "cut_off_time",
                    ],
                },
            },
        },
    },

    "Risk & Performance Analytics": {
        "icon": "📊",
        "description": "Risk metrics, stress testing, liquidity analysis and performance attribution.",
        "subdomains": {
            "Market Risk": {
                "description": "VaR, stress testing and sensitivity analysis.",
                "concepts": {
                    "VaR Metrics": [
                        "var_1d_95", "var_1d_99", "var_10d_99",
                        "cvar_95", "historical_var_date",
                    ],
                    "Sensitivities": [
                        "dv01", "cs01", "delta", "gamma", "vega",
                        "portfolio_duration", "portfolio_convexity",
                    ],
                    "Stress Tests": [
                        "stress_scenario_id", "stress_scenario_name",
                        "stressed_nav_impact", "stressed_aum_impact",
                    ],
                },
            },
            "Liquidity Risk": {
                "description": "Liquidity classification and redemption coverage.",
                "concepts": {
                    "Liquidity Buckets": [
                        "liquidity_bucket", "days_to_liquidate",
                        "liquidity_coverage_ratio", "redemption_coverage_days",
                    ],
                    "Liquidity Stress": [
                        "stress_redemption_pct", "stressed_coverage_days",
                        "liquidity_stress_test_date", "reverse_stress_threshold",
                    ],
                },
            },
            "Performance Attribution": {
                "description": "Brinson-Hood-Beebower attribution and factor decomposition.",
                "concepts": {
                    "Attribution": [
                        "allocation_effect", "selection_effect", "interaction_effect",
                        "total_active_return", "attribution_date",
                    ],
                    "Factor Exposure": [
                        "factor_id", "factor_name", "factor_exposure",
                        "factor_return_contribution", "factor_model",
                    ],
                },
            },
        },
    },

    "Counterparty & Investor Data": {
        "icon": "👥",
        "description": "Investor registry, distributor networks and service provider data.",
        "subdomains": {
            "Investor Registry": {
                "description": "Investor-level data managed by the transfer agent.",
                "concepts": {
                    "Investor Identity": [
                        "investor_id", "investor_name", "investor_type",
                        "nationality", "tax_residency", "kyc_status", "aml_flag",
                    ],
                    "Investor Holdings": [
                        "sc_id", "isin", "units_held", "value_held",
                        "acquisition_date", "distribution_preference",
                    ],
                },
            },
            "Distribution Network": {
                "description": "Distributor and intermediary channel data.",
                "concepts": {
                    "Distributor": [
                        "distributor_id", "distributor_name", "distributor_type",
                        "distribution_agreement_date", "trailer_fee_pct",
                        "distribution_country", "retrocession_flag",
                    ],
                },
            },
        },
    },
}

# ─────────────────────────────────────────────
# BUSINESS GLOSSARY
# ─────────────────────────────────────────────

GLOSSARY = [
    {
        "term":           "Net Asset Value (NAV)",
        "abbreviation":   "NAV",
        "domain":         "Pricing & Valuation",
        "subdomain":      "NAV Calculation",
        "definition":     "The per-share value of a fund's assets minus its liabilities. Calculated as (Total Assets - Total Liabilities) / Shares Outstanding.",
        "example":        "NAV = 142.35 USD means each share is worth USD 142.35 at the valuation point.",
        "related_terms":  ["AUM", "Share Class", "Valuation Point"],
        "regulatory_ref": "UCITS Directive 2009/65/EC, Art. 85",
        "data_type":      "float",
        "owner":          "Fund Accounting",
        "status":         "Approved",
    },
    {
        "term":           "Assets Under Management",
        "abbreviation":   "AUM",
        "domain":         "Pricing & Valuation",
        "subdomain":      "AUM & Flows",
        "definition":     "Total market value of all investments managed by a fund on behalf of investors. AUM = NAV × Shares Outstanding.",
        "example":        "AUM = USD 1.24B for a sub-fund with 8.7M shares at NAV 142.35.",
        "related_terms":  ["NAV", "Net Flows", "Share Class"],
        "regulatory_ref": "AIFMD Annex IV Reporting",
        "data_type":      "float",
        "owner":          "Fund Accounting",
        "status":         "Approved",
    },
    {
        "term":           "International Securities Identification Number",
        "abbreviation":   "ISIN",
        "domain":         "Reference Data",
        "subdomain":      "Market Data",
        "definition":     "A 12-character alphanumeric code uniquely identifying a security. Format: 2-letter country code + 9-character national code + 1 check digit (ISO 6166).",
        "example":        "LU0123456789 — Luxembourg-domiciled share class.",
        "related_terms":  ["SEDOL", "CUSIP", "Bloomberg Ticker", "Share Class"],
        "regulatory_ref": "ISO 6166",
        "data_type":      "string",
        "owner":          "Data Management",
        "status":         "Approved",
    },
    {
        "term":           "Legal Entity Identifier",
        "abbreviation":   "LEI",
        "domain":         "Reference Data",
        "subdomain":      "Counterparty Reference",
        "definition":     "A 20-character alphanumeric code uniquely identifying legal entities participating in financial transactions (ISO 17442).",
        "example":        "529900HNOAA1KXQJUQ27 — a fund's LEI registered with GLEIF.",
        "related_terms":  ["Fund Entity", "MiFID II", "EMIR"],
        "regulatory_ref": "ISO 17442 / EMIR / MiFID II",
        "data_type":      "string",
        "owner":          "Compliance",
        "status":         "Approved",
    },
    {
        "term":           "Share Class",
        "abbreviation":   "SC",
        "domain":         "Fund Structure & Master Data",
        "subdomain":      "Share Class",
        "definition":     "A distinct category of shares within a sub-fund, differentiated by currency, investor type, fee structure, distribution policy or hedging.",
        "example":        "Apex Global Equity A-USD: retail, USD-denominated, accumulating.",
        "related_terms":  ["ISIN", "Sub-Fund", "NAV", "Transfer Agent"],
        "regulatory_ref": "UCITS Directive, CSSF Circular 14/592",
        "data_type":      "entity",
        "owner":          "Fund Operations",
        "status":         "Approved",
    },
    {
        "term":           "Golden Record",
        "abbreviation":   "GR",
        "domain":         "Fund Structure & Master Data",
        "subdomain":      "Fund Entity",
        "definition":     "The single authoritative version of a data entity resolved from multiple source systems through master data management processes.",
        "example":        "Golden NAV = 142.35 (Fund Admin, trust=5) overriding Bloomberg 141.90 (trust=4).",
        "related_terms":  ["MDM", "Conflict Resolution", "Source Trust"],
        "regulatory_ref": "EDM Council DCAM v2",
        "data_type":      "concept",
        "owner":          "Data Management",
        "status":         "Approved",
    },
    {
        "term":           "Valuation Point",
        "abbreviation":   "VP",
        "domain":         "Pricing & Valuation",
        "subdomain":      "NAV Calculation",
        "definition":     "The specific time at which a fund's assets are priced for NAV calculation purposes.",
        "example":        "Daily valuation point at 4:00 PM New York time for US equity funds.",
        "related_terms":  ["NAV", "Cut-Off Time", "Dealing Frequency"],
        "regulatory_ref": "UCITS Directive 2009/65/EC",
        "data_type":      "timestamp",
        "owner":          "Fund Accounting",
        "status":         "Approved",
    },
    {
        "term":           "Tracking Error",
        "abbreviation":   "TE",
        "domain":         "Risk & Performance Analytics",
        "subdomain":      "Market Risk",
        "definition":     "The standard deviation of the difference between the portfolio return and benchmark return, measuring active risk.",
        "example":        "TE = 1.8% annualised for a fund targeting TE < 3% vs MSCI World.",
        "related_terms":  ["Active Return", "Information Ratio", "Benchmark"],
        "regulatory_ref": "UCITS risk management guidelines",
        "data_type":      "float",
        "owner":          "Risk",
        "status":         "Approved",
    },
    {
        "term":           "SFDR Article",
        "abbreviation":   "SFDR",
        "domain":         "Regulatory & Compliance",
        "subdomain":      "SFDR & ESG Reporting",
        "definition":     "Classification under EU Sustainable Finance Disclosure Regulation: Article 6 (no sustainability claim), Article 8 (ESG characteristics promoted), Article 9 (sustainable investment objective).",
        "example":        "Article 8 fund must disclose how ESG characteristics are met in pre-contractual documents.",
        "related_terms":  ["ESG Score", "PAI Indicator", "Taxonomy Alignment"],
        "regulatory_ref": "EU Regulation 2019/2088 (SFDR)",
        "data_type":      "enum",
        "owner":          "Compliance",
        "status":         "Approved",
    },
    {
        "term":           "Transfer Agent",
        "abbreviation":   "TA",
        "domain":         "Counterparty & Investor Data",
        "subdomain":      "Investor Registry",
        "definition":     "The institution responsible for maintaining the shareholder register, processing subscriptions/redemptions and distributing dividends.",
        "example":        "RBC Investor Services acting as TA for Luxembourg SICAV, processing T+3 redemptions.",
        "related_terms":  ["Share Class", "Settlement", "Investor Registry"],
        "regulatory_ref": "UCITS Directive / CSSF",
        "data_type":      "entity",
        "owner":          "Operations",
        "status":         "Approved",
    },
    {
        "term":           "Medallion Architecture",
        "abbreviation":   "MDL",
        "domain":         "Reference Data",
        "subdomain":      "Market Data",
        "definition":     "A data lakehouse design pattern with three layers: Bronze (raw ingestion), Silver (cleansed & validated), Gold (aggregated, business-ready).",
        "example":        "Raw NAV file from Fund Admin → Bronze. Validated NAV record → Silver. Golden NAV record → Gold.",
        "related_terms":  ["Data Lineage", "ETL", "Data Quality"],
        "regulatory_ref": "Databricks / EDM Council best practice",
        "data_type":      "concept",
        "owner":          "Data Management",
        "status":         "Approved",
    },
    {
        "term":           "Value at Risk",
        "abbreviation":   "VaR",
        "domain":         "Risk & Performance Analytics",
        "subdomain":      "Market Risk",
        "definition":     "The maximum expected loss over a given time horizon at a specified confidence level under normal market conditions.",
        "example":        "1-day 99% VaR = USD 2.1M means there is a 1% probability of losing more than USD 2.1M in one day.",
        "related_terms":  ["CVaR", "Stress Test", "Portfolio Duration"],
        "regulatory_ref": "UCITS SRRI / AIFMD risk reporting",
        "data_type":      "float",
        "owner":          "Risk",
        "status":         "Approved",
    },
    {
        "term":           "Principal Adverse Indicator",
        "abbreviation":   "PAI",
        "domain":         "Regulatory & Compliance",
        "subdomain":      "SFDR & ESG Reporting",
        "definition":     "Mandatory sustainability indicators under SFDR RTS measuring the negative environmental and social impacts of investments.",
        "example":        "PAI 1: GHG emissions intensity of portfolio companies in tCO2e/€M invested.",
        "related_terms":  ["SFDR", "ESG Score", "Taxonomy Alignment"],
        "regulatory_ref": "SFDR Delegated Regulation (EU) 2022/1288",
        "data_type":      "float",
        "owner":          "Compliance",
        "status":         "Approved",
    },
    {
        "term":           "Prospectus",
        "abbreviation":   None,
        "domain":         "Regulatory & Compliance",
        "subdomain":      "UCITS & AIFMD",
        "definition":     "The legally binding document describing the fund's investment policy, fees, risks and operational terms. The primary source of truth for fund static data.",
        "example":        "Prospectus for Apex Global Fund dated 2024-03-15 specifying management fee of 0.75% p.a.",
        "related_terms":  ["KID/KIID", "Share Class", "Fund Entity"],
        "regulatory_ref": "UCITS Directive Art. 68-82 / PRIIPs Regulation",
        "data_type":      "document",
        "owner":          "Legal",
        "status":         "Approved",
    },
    {
        "term":           "Cut-Off Time",
        "abbreviation":   "COT",
        "domain":         "Transactions & Cash Flows",
        "subdomain":      "Investor Transactions",
        "definition":     "The deadline by which a subscription or redemption order must be received to be processed at that day's NAV.",
        "example":        "Cut-off 12:00 CET: orders received after noon are processed at next business day NAV.",
        "related_terms":  ["Dealing Frequency", "Valuation Point", "Settlement"],
        "regulatory_ref": "Fund Prospectus / TA Agreement",
        "data_type":      "time",
        "owner":          "Operations",
        "status":         "Approved",
    },
]

# ─────────────────────────────────────────────
# DATA ELEMENT REGISTRY
# ─────────────────────────────────────────────

ELEMENT_REGISTRY_BASE = [
    {
        "element_id":       "DE-001",
        "element_name":     "nav",
        "display_name":     "Net Asset Value",
        "domain":           "Pricing & Valuation",
        "subdomain":        "NAV Calculation",
        "concept":          "NAV Record",
        "description":      "Per-share net asset value calculated as (Total Assets - Total Liabilities) / Shares Outstanding.",
        "data_type":        "float",
        "format":           "######.#### (4 decimal places)",
        "example_value":    "142.3512",
        "nullable":         False,
        "pii":              False,
        "classification":   "Confidential",
        "golden_source":    "Fund Administrator",
        "secondary_source": "Bloomberg",
        "data_layer":       "Gold",
        "owner_team":       "Fund Accounting",
        "steward":          "Data Management",
        "consuming_teams":  ["Risk", "Compliance", "Distribution", "Reporting"],
        "regulatory_ref":   "UCITS Directive / CSSF Circular",
        "linked_rule_ids":  ["BR-001", "BR-002"],
        "glossary_term":    "Net Asset Value (NAV)",
        "status":           "Certified",
        "criticality":      "Critical",
        "last_reviewed":    "2025-12-01",
    },
    {
        "element_id":       "DE-002",
        "element_name":     "isin",
        "display_name":     "ISIN",
        "domain":           "Fund Structure & Master Data",
        "subdomain":        "Share Class",
        "concept":          "Share Class Identity",
        "description":      "International Securities Identification Number uniquely identifying the share class (ISO 6166).",
        "data_type":        "string",
        "format":           "^[A-Z]{2}[A-Z0-9]{9}[0-9]$ (12 chars)",
        "example_value":    "LU0123456789",
        "nullable":         False,
        "pii":              False,
        "classification":   "Public",
        "golden_source":    "Fund Administrator",
        "secondary_source": "Euroclear / Bloomberg",
        "data_layer":       "Gold",
        "owner_team":       "Fund Operations",
        "steward":          "Data Management",
        "consuming_teams":  ["Risk", "Compliance", "Portfolio", "Distribution", "Reporting", "Transfer Agent"],
        "regulatory_ref":   "ISO 6166 / MiFID II / EMIR",
        "linked_rule_ids":  ["BR-005", "SC-001"],
        "glossary_term":    "International Securities Identification Number",
        "status":           "Certified",
        "criticality":      "Critical",
        "last_reviewed":    "2025-12-01",
    },
    {
        "element_id":       "DE-003",
        "element_name":     "aum",
        "display_name":     "Assets Under Management",
        "domain":           "Pricing & Valuation",
        "subdomain":        "AUM & Flows",
        "concept":          "AUM Record",
        "description":      "Total market value of fund assets. Computed as NAV × Shares Outstanding.",
        "data_type":        "float",
        "format":           "######.## (2 decimal places, base currency)",
        "example_value":    "1240000000.00",
        "nullable":         False,
        "pii":              False,
        "classification":   "Confidential",
        "golden_source":    "Fund Administrator",
        "secondary_source": "Custodian",
        "data_layer":       "Gold",
        "owner_team":       "Fund Accounting",
        "steward":          "Data Management",
        "consuming_teams":  ["Risk", "Senior Management", "Compliance", "Distribution"],
        "regulatory_ref":   "AIFMD Annex IV",
        "linked_rule_ids":  ["BR-003"],
        "glossary_term":    "Assets Under Management",
        "status":           "Certified",
        "criticality":      "Critical",
        "last_reviewed":    "2025-12-01",
    },
    {
        "element_id":       "DE-004",
        "element_name":     "settlement_status",
        "display_name":     "Settlement Status",
        "domain":           "Transactions & Cash Flows",
        "subdomain":        "Investor Transactions",
        "concept":          "Settlement",
        "description":      "Current settlement state of a transaction. Domain: Settled | Pending | Failed | Cancelled.",
        "data_type":        "enum",
        "format":           "Settled | Pending | Failed | Cancelled",
        "example_value":    "Settled",
        "nullable":         False,
        "pii":              False,
        "classification":   "Internal",
        "golden_source":    "Transfer Agent",
        "secondary_source": "Custodian",
        "data_layer":       "Silver",
        "owner_team":       "Operations",
        "steward":          "Data Management",
        "consuming_teams":  ["Operations", "Compliance", "Finance", "Transfer Agent"],
        "regulatory_ref":   "CSDR / T2S Settlement Regulation",
        "linked_rule_ids":  ["TX-002", "TX-004"],
        "glossary_term":    "Settlement",
        "status":           "Certified",
        "criticality":      "High",
        "last_reviewed":    "2025-11-15",
    },
    {
        "element_id":       "DE-005",
        "element_name":     "investor_id",
        "display_name":     "Investor ID",
        "domain":           "Counterparty & Investor Data",
        "subdomain":        "Investor Registry",
        "concept":          "Investor Identity",
        "description":      "Unique internal identifier for an investor in the transfer agent register. Required for AML/KYC compliance.",
        "data_type":        "string",
        "format":           "INV#### (alphanumeric)",
        "example_value":    "INV4821",
        "nullable":         False,
        "pii":              True,
        "classification":   "Restricted",
        "golden_source":    "Transfer Agent",
        "secondary_source": "Internal CRM",
        "data_layer":       "Silver",
        "owner_team":       "Compliance",
        "steward":          "Compliance",
        "consuming_teams":  ["Compliance", "Operations", "Legal"],
        "regulatory_ref":   "AMLD5 / FATF Recommendations",
        "linked_rule_ids":  ["TX-003"],
        "glossary_term":    "Transfer Agent",
        "status":           "Certified",
        "criticality":      "Critical",
        "last_reviewed":    "2025-10-20",
    },
    {
        "element_id":       "DE-006",
        "element_name":     "reg_status",
        "display_name":     "Registration Status",
        "domain":           "Regulatory & Compliance",
        "subdomain":        "Registration & Passporting",
        "concept":          "Registration Record",
        "description":      "Current passporting/registration status of a share class in a given jurisdiction. Domain: Registered | Pending | Restricted | Not Registered.",
        "data_type":        "enum",
        "format":           "Registered | Pending | Restricted | Not Registered",
        "example_value":    "Registered",
        "nullable":         False,
        "pii":              False,
        "classification":   "Internal",
        "golden_source":    "Legal & Compliance",
        "secondary_source": "Internal",
        "data_layer":       "Gold",
        "owner_team":       "Compliance",
        "steward":          "Compliance",
        "consuming_teams":  ["Compliance", "Distribution", "Legal", "Operations"],
        "regulatory_ref":   "UCITS Directive / ESMA Passporting Guidelines",
        "linked_rule_ids":  ["RG-001", "RG-002"],
        "glossary_term":    "SFDR Article",
        "status":           "Certified",
        "criticality":      "Critical",
        "last_reviewed":    "2025-12-01",
    },
    {
        "element_id":       "DE-007",
        "element_name":     "weight_pct",
        "display_name":     "Position Weight (%)",
        "domain":           "Portfolio & Holdings",
        "subdomain":        "Position",
        "concept":          "Position Record",
        "description":      "Percentage weight of a security position relative to total portfolio net assets. Sum across all positions must equal ~100%.",
        "data_type":        "float",
        "format":           "###.## (0.00 to 100.00)",
        "example_value":    "4.82",
        "nullable":         False,
        "pii":              False,
        "classification":   "Confidential",
        "golden_source":    "Fund Administrator",
        "secondary_source": "Custodian",
        "data_layer":       "Silver",
        "owner_team":       "Portfolio Management",
        "steward":          "Data Management",
        "consuming_teams":  ["Portfolio Management", "Risk", "Compliance", "Reporting"],
        "regulatory_ref":   "UCITS 5/10/40 rule",
        "linked_rule_ids":  ["PF-001", "PF-002"],
        "glossary_term":    "Golden Record",
        "status":           "Certified",
        "criticality":      "High",
        "last_reviewed":    "2025-11-01",
    },
    {
        "element_id":       "DE-008",
        "element_name":     "lei",
        "display_name":     "Legal Entity Identifier",
        "domain":           "Reference Data",
        "subdomain":        "Counterparty Reference",
        "concept":          "Legal Entity",
        "description":      "20-character globally unique identifier for legal entities (ISO 17442). Mandatory for EMIR, MiFID II and AIFMD reporting.",
        "data_type":        "string",
        "format":           "^[0-9A-Z]{18}[0-9]{2}$ (20 chars)",
        "example_value":    "529900HNOAA1KXQJUQ27",
        "nullable":         False,
        "pii":              False,
        "classification":   "Public",
        "golden_source":    "GLEIF (Global LEI Foundation)",
        "secondary_source": "Bloomberg",
        "data_layer":       "Gold",
        "owner_team":       "Compliance",
        "steward":          "Data Management",
        "consuming_teams":  ["Compliance", "Risk", "Reporting", "Operations"],
        "regulatory_ref":   "ISO 17442 / EMIR / MiFID II",
        "linked_rule_ids":  [],
        "glossary_term":    "Legal Entity Identifier",
        "status":           "Certified",
        "criticality":      "Critical",
        "last_reviewed":    "2025-12-01",
    },
    {
        "element_id":       "DE-009",
        "element_name":     "sfdr_article",
        "display_name":     "SFDR Article Classification",
        "domain":           "Regulatory & Compliance",
        "subdomain":        "SFDR & ESG Reporting",
        "concept":          "SFDR Disclosure",
        "description":      "Fund classification under EU SFDR: 6 (mainstream), 8 (ESG characteristics promoted), 9 (sustainable investment objective).",
        "data_type":        "enum",
        "format":           "6 | 8 | 9",
        "example_value":    "8",
        "nullable":         True,
        "pii":              False,
        "classification":   "Public",
        "golden_source":    "Legal & Compliance",
        "secondary_source": "Fund Prospectus",
        "data_layer":       "Gold",
        "owner_team":       "Compliance",
        "steward":          "Compliance",
        "consuming_teams":  ["Compliance", "Distribution", "Marketing", "Senior Management"],
        "regulatory_ref":   "EU Regulation 2019/2088 (SFDR)",
        "linked_rule_ids":  [],
        "glossary_term":    "SFDR Article",
        "status":           "In Review",
        "criticality":      "High",
        "last_reviewed":    "2025-11-20",
    },
    {
        "element_id":       "DE-010",
        "element_name":     "management_fee_pct",
        "display_name":     "Management Fee (%)",
        "domain":           "Fund Structure & Master Data",
        "subdomain":        "Sub-Fund",
        "concept":          "Fee Structure",
        "description":      "Annual management fee charged as a percentage of AUM. Must match the fund prospectus and KID/KIID disclosures.",
        "data_type":        "float",
        "format":           "#.## (percentage)",
        "example_value":    "0.75",
        "nullable":         False,
        "pii":              False,
        "classification":   "Public",
        "golden_source":    "Fund Prospectus",
        "secondary_source": "Bloomberg / Fund Admin",
        "data_layer":       "Gold",
        "owner_team":       "Fund Operations",
        "steward":          "Data Management",
        "consuming_teams":  ["Fund Operations", "Distribution", "Compliance", "Finance"],
        "regulatory_ref":   "UCITS KID / PRIIPs Regulation",
        "linked_rule_ids":  [],
        "glossary_term":    "Prospectus",
        "status":           "Certified",
        "criticality":      "High",
        "last_reviewed":    "2025-10-01",
    },
]

# ─────────────────────────────────────────────
# TEAMS & OWNERSHIP MATRIX
# ─────────────────────────────────────────────

TEAMS = [
    {"team_id": "T01", "team_name": "Data Management",   "department": "Operations",  "head": "Clément Denorme",    "focus": "Data governance, quality, lineage"},
    {"team_id": "T02", "team_name": "Fund Accounting",    "department": "Finance",     "head": "Sophie Martin",      "focus": "NAV calculation, AUM, performance"},
    {"team_id": "T03", "team_name": "Fund Operations",    "department": "Operations",  "head": "James O'Brien",      "focus": "Share class setup, static data, fees"},
    {"team_id": "T04", "team_name": "Risk",               "department": "Risk",        "head": "Hugo Lefevre",       "focus": "VaR, stress testing, limit monitoring"},
    {"team_id": "T05", "team_name": "Compliance",         "department": "Compliance",  "head": "Clara Muller",       "focus": "Regulatory reporting, SFDR, AML/KYC"},
    {"team_id": "T06", "team_name": "Portfolio Management","department": "Investments","head": "Luca Bianchi",       "focus": "Portfolio construction, trading, attribution"},
    {"team_id": "T07", "team_name": "Distribution",       "department": "Sales",       "head": "Amara Diallo",       "focus": "Investor reporting, distributor management"},
    {"team_id": "T08", "team_name": "Reporting",          "department": "Finance",     "head": "Mei Lin",            "focus": "Regulatory, investor and management reporting"},
    {"team_id": "T09", "team_name": "Legal",              "department": "Legal",       "head": "Patrick Walsh",      "focus": "Prospectus, fund structures, contracts"},
    {"team_id": "T10", "team_name": "Transfer Agent",     "department": "Operations",  "head": "Fatima El-Amin",     "focus": "Investor register, subscriptions, redemptions"},
    {"team_id": "T11", "team_name": "Finance",            "department": "Finance",     "head": "Björn Lindqvist",    "focus": "P&L, fee billing, management accounts"},
    {"team_id": "T12", "team_name": "Senior Management",  "department": "Executive",   "head": "CEO",                "focus": "Strategic oversight, AUM reporting"},
]

# ─────────────────────────────────────────────
# SESSION STATE INIT
# ─────────────────────────────────────────────

def init_catalog_state() -> None:
    if "catalog_elements" not in st.session_state:
        st.session_state["catalog_elements"] = ELEMENT_REGISTRY_BASE.copy()
    if "catalog_glossary" not in st.session_state:
        st.session_state["catalog_glossary"] = GLOSSARY.copy()


# ─────────────────────────────────────────────
# TAXONOMY HELPERS
# ─────────────────────────────────────────────

def get_taxonomy_tree() -> dict:
    """Return full taxonomy dict."""
    return TAXONOMY


def get_all_domains() -> list[str]:
    return list(TAXONOMY.keys())


def get_subdomains(domain: str) -> list[str]:
    return list(TAXONOMY.get(domain, {}).get("subdomains", {}).keys())


def get_concepts(domain: str, subdomain: str) -> list[str]:
    return list(
        TAXONOMY.get(domain, {})
        .get("subdomains", {})
        .get(subdomain, {})
        .get("concepts", {})
        .keys()
    )


def get_elements_for_concept(domain: str, subdomain: str, concept: str) -> list[str]:
    return (
        TAXONOMY.get(domain, {})
        .get("subdomains", {})
        .get(subdomain, {})
        .get("concepts", {})
        .get(concept, [])
    )


def build_taxonomy_flat() -> pd.DataFrame:
    """Flatten taxonomy to a DataFrame for display/search."""
    rows = []
    for domain, d_val in TAXONOMY.items():
        for subdomain, sd_val in d_val.get("subdomains", {}).items():
            for concept, elements in sd_val.get("concepts", {}).items():
                for element in elements:
                    rows.append({
                        "domain":      domain,
                        "subdomain":   subdomain,
                        "concept":     concept,
                        "element":     element,
                        "domain_icon": d_val.get("icon", ""),
                    })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# DATA ELEMENT REGISTRY FUNCTIONS
# ─────────────────────────────────────────────

def get_elements_df() -> pd.DataFrame:
    init_catalog_state()
    return pd.DataFrame(st.session_state["catalog_elements"])


def get_element(element_id: str) -> dict | None:
    init_catalog_state()
    for e in st.session_state["catalog_elements"]:
        if e["element_id"] == element_id:
            return e
    return None


def add_element(element: dict) -> None:
    init_catalog_state()
    existing = {e["element_id"] for e in st.session_state["catalog_elements"]}
    if element["element_id"] in existing:
        raise ValueError(f"Element ID '{element['element_id']}' already exists.")
    st.session_state["catalog_elements"].append({
        **element,
        "last_reviewed": datetime.today().strftime("%Y-%m-%d"),
        "status": element.get("status", "Draft"),
    })


def update_element(element_id: str, updates: dict) -> None:
    init_catalog_state()
    for i, e in enumerate(st.session_state["catalog_elements"]):
        if e["element_id"] == element_id:
            st.session_state["catalog_elements"][i] = {
                **e, **updates,
                "last_reviewed": datetime.today().strftime("%Y-%m-%d"),
            }
            return
    raise ValueError(f"Element ID '{element_id}' not found.")


def search_elements(query: str) -> pd.DataFrame:
    """Full-text search across element name, description, domain, tags."""
    df = get_elements_df()
    if not query:
        return df
    q = query.lower()
    mask = (
        df["element_name"].str.lower().str.contains(q, na=False) |
        df["display_name"].str.lower().str.contains(q, na=False) |
        df["description"].str.lower().str.contains(q, na=False) |
        df["domain"].str.lower().str.contains(q, na=False) |
        df["subdomain"].str.lower().str.contains(q, na=False) |
        df["concept"].str.lower().str.contains(q, na=False) |
        df["owner_team"].str.lower().str.contains(q, na=False)
    )
    return df[mask].reset_index(drop=True)


# ─────────────────────────────────────────────
# GLOSSARY FUNCTIONS
# ─────────────────────────────────────────────

def get_glossary_df() -> pd.DataFrame:
    init_catalog_state()
    return pd.DataFrame(st.session_state["catalog_glossary"])


def search_glossary(query: str) -> pd.DataFrame:
    df = get_glossary_df()
    if not query:
        return df
    q = query.lower()
    mask = (
        df["term"].str.lower().str.contains(q, na=False) |
        df["abbreviation"].astype(str).str.lower().str.contains(q, na=False) |
        df["definition"].str.lower().str.contains(q, na=False) |
        df["domain"].str.lower().str.contains(q, na=False)
    )
    return df[mask].reset_index(drop=True)


def add_glossary_term(term: dict) -> None:
    init_catalog_state()
    st.session_state["catalog_glossary"].append(term)


# ─────────────────────────────────────────────
# OWNERSHIP & USAGE MATRIX
# ─────────────────────────────────────────────

def get_teams_df() -> pd.DataFrame:
    return pd.DataFrame(TEAMS)


def get_ownership_matrix() -> pd.DataFrame:
    """
    Build a team × domain ownership matrix showing
    how many elements each team owns vs consumes.
    """
    elements = get_elements_df()
    teams    = [t["team_name"] for t in TEAMS]
    domains  = get_all_domains()

    rows = []
    for team in teams:
        owned    = elements[elements["owner_team"] == team]
        consumed = elements[elements["consuming_teams"].apply(lambda c: team in c if isinstance(c, list) else False)]
        for domain in domains:
            owned_d    = len(owned[owned["domain"] == domain])
            consumed_d = len(consumed[consumed["domain"] == domain])
            if owned_d > 0 or consumed_d > 0:
                rows.append({
                    "team":     team,
                    "domain":   domain,
                    "owned":    owned_d,
                    "consumed": consumed_d,
                    "total":    owned_d + consumed_d,
                })
    return pd.DataFrame(rows)


def get_usage_heatmap_data() -> pd.DataFrame:
    """
    Return a pivot-ready DataFrame: teams as rows, domains as columns,
    values = number of elements consumed. Used for heatmap rendering.
    """
    elements = get_elements_df()
    teams    = [t["team_name"] for t in TEAMS]
    domains  = get_all_domains()

    matrix = {}
    for team in teams:
        matrix[team] = {}
        for domain in domains:
            count = elements[
                elements["consuming_teams"].apply(
                    lambda c: team in c if isinstance(c, list) else False
                ) & (elements["domain"] == domain)
            ].shape[0]
            matrix[team][domain] = count

    return pd.DataFrame(matrix).T  # teams as rows, domains as columns


# ─────────────────────────────────────────────
# CATALOG STATISTICS
# ─────────────────────────────────────────────

def get_catalog_stats() -> dict:
    elements = get_elements_df()
    glossary = get_glossary_df()
    taxonomy = build_taxonomy_flat()

    return {
        "total_elements":    len(elements),
        "certified":         len(elements[elements["status"] == "Certified"]),
        "in_review":         len(elements[elements["status"] == "In Review"]),
        "draft":             len(elements[elements["status"] == "Draft"]),
        "pii_elements":      elements["pii"].sum() if "pii" in elements.columns else 0,
        "critical_elements": len(elements[elements["criticality"] == "Critical"]),
        "total_glossary":    len(glossary),
        "taxonomy_domains":  len(get_all_domains()),
        "taxonomy_elements": len(taxonomy),
        "domains_covered":   elements["domain"].nunique(),
        "teams_owning":      elements["owner_team"].nunique(),
    }


def get_elements_by_domain() -> pd.DataFrame:
    df = get_elements_df()
    return df.groupby("domain").agg(
        elements=("element_id", "count"),
        certified=("status", lambda x: (x == "Certified").sum()),
        critical=("criticality", lambda x: (x == "Critical").sum()),
        pii=("pii", "sum"),
    ).reset_index()


def get_elements_by_team() -> pd.DataFrame:
    df = get_elements_df()
    return df.groupby("owner_team").agg(
        owned=("element_id", "count"),
        certified=("status", lambda x: (x == "Certified").sum()),
        critical=("criticality", lambda x: (x == "Critical").sum()),
    ).reset_index().sort_values("owned", ascending=False)


# ─────────────────────────────────────────────
# STREAMLIT PAGE RENDERER
# ─────────────────────────────────────────────

def render_data_catalog_page(
    sf_df: pd.DataFrame = None,
    sc_df: pd.DataFrame = None,
    stewards_df: pd.DataFrame = None,
) -> None:
    """
    Full Streamlit page renderer for the Data Catalog.
    Call from app.py: render_data_catalog_page(sf_df, sc_df, stewards_df)
    """
    init_catalog_state()
    st.title("📖 Enterprise Data Catalog")
    st.caption("Fund-oriented taxonomy · Business glossary · Data element registry · Ownership & usage matrix")

    tab_overview, tab_taxonomy, tab_elements, tab_glossary, tab_ownership, tab_search = st.tabs([
        "📊 Overview",
        "🌳 Taxonomy",
        "📋 Element Registry",
        "📚 Business Glossary",
        "👥 Ownership & Usage",
        "🔍 Search",
    ])

    # ─── TAB: OVERVIEW
    with tab_overview:
        stats = get_catalog_stats()
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("Registered Elements",  stats["total_elements"])
        m2.metric("Certified",            f"{stats['certified']} ✅")
        m3.metric("In Review",            stats["in_review"])
        m4.metric("Glossary Terms",       stats["total_glossary"])
        m5.metric("Taxonomy Domains",     stats["taxonomy_domains"])
        m6.metric("PII Elements",         f"🔒 {stats['pii_elements']}")

        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📁 Elements by Domain")
            by_domain = get_elements_by_domain()
            import plotly.express as px
            fig_d = px.bar(
                by_domain.sort_values("elements", ascending=True),
                x="elements", y="domain", orientation="h",
                color="certified", color_continuous_scale="Blues",
                title="Registered Elements per Domain",
            )
            fig_d.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="white", height=380, coloraxis_showscale=False,
            )
            st.plotly_chart(fig_d, use_container_width=True)

        with col2:
            st.subheader("👥 Elements by Owner Team")
            by_team = get_elements_by_team()
            fig_t = px.bar(
                by_team, x="owner_team", y="owned", color="certified",
                color_continuous_scale="Greens",
                title="Owned Elements per Team",
            )
            fig_t.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="white", height=380, coloraxis_showscale=False,
                xaxis_tickangle=-30,
            )
            st.plotly_chart(fig_t, use_container_width=True)

        # Criticality breakdown
        st.subheader("🔴 Criticality & Classification")
        crit_col, class_col, status_col = st.columns(3)
        elems = get_elements_df()
        with crit_col:
            crit = elems["criticality"].value_counts().reset_index()
            crit.columns = ["Criticality", "Count"]
            fig_cr = px.pie(crit, values="Count", names="Criticality", hole=0.45,
                            color_discrete_map={"Critical":"#ef5350","High":"#ff9800","Medium":"#fdd835","Low":"#66bb6a"})
            fig_cr.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=260)
            st.plotly_chart(fig_cr, use_container_width=True)
        with class_col:
            cls = elems["classification"].value_counts().reset_index()
            cls.columns = ["Classification", "Count"]
            fig_cls = px.pie(cls, values="Count", names="Classification", hole=0.45,
                             color_discrete_sequence=["#4fc3f7","#81c784","#ffb74d","#e57373"])
            fig_cls.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=260)
            st.plotly_chart(fig_cls, use_container_width=True)
        with status_col:
            sts = elems["status"].value_counts().reset_index()
            sts.columns = ["Status", "Count"]
            fig_sts = px.pie(sts, values="Count", names="Status", hole=0.45,
                             color_discrete_map={"Certified":"#66bb6a","In Review":"#fdd835","Draft":"#90caf9","Deprecated":"#ef5350"})
            fig_sts.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white", height=260)
            st.plotly_chart(fig_sts, use_container_width=True)

    # ─── TAB: TAXONOMY
    with tab_taxonomy:
        st.subheader("🌳 Fund Data Taxonomy — 4-Level Hierarchy")
        st.caption("Domain → Subdomain → Concept → Data Element | Based on FIBO & EDM Council DCAM")

        tax_flat = build_taxonomy_flat()
        sel_domain = st.selectbox(
            "Select Domain",
            ["(All)"] + get_all_domains(),
            format_func=lambda d: f"{TAXONOMY[d]['icon']} {d}" if d in TAXONOMY else d,
        )

        if sel_domain == "(All)":
            for domain, d_val in TAXONOMY.items():
                with st.expander(f"{d_val['icon']} **{domain}**  —  {d_val['description']}"):
                    for subdomain, sd_val in d_val["subdomains"].items():
                        st.markdown(f"**📂 {subdomain}** — _{sd_val['description']}_")
                        for concept, elements in sd_val["concepts"].items():
                            elem_badges = "  ".join([f"`{e}`" for e in elements])
                            st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;🔹 **{concept}**: {elem_badges}")
                        st.markdown("")
        else:
            d_val = TAXONOMY[sel_domain]
            st.markdown(f"### {d_val['icon']} {sel_domain}")
            st.caption(d_val["description"])
            for subdomain, sd_val in d_val["subdomains"].items():
                st.markdown(f"#### 📂 {subdomain}")
                st.caption(sd_val["description"])
                for concept, elements in sd_val["concepts"].items():
                    st.markdown(f"**🔹 {concept}**")
                    cols = st.columns(4)
                    for i, elem in enumerate(elements):
                        cols[i % 4].code(elem)

        st.markdown("---")
        st.subheader("📊 Taxonomy Coverage")
        coverage = tax_flat.groupby("domain").agg(
            subdomains=("subdomain", "nunique"),
            concepts=("concept", "nunique"),
            elements=("element", "count"),
        ).reset_index()
        coverage["domain_icon"] = coverage["domain"].map(lambda d: TAXONOMY.get(d, {}).get("icon", ""))
        st.dataframe(coverage, use_container_width=True, hide_index=True)

    # ─── TAB: ELEMENT REGISTRY
    with tab_elements:
        st.subheader("📋 Data Element Registry")
        st.caption("Registered, defined and governed data elements linked to the fund taxonomy")

        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            filter_domain = st.selectbox("Filter by Domain", ["(All)"] + get_all_domains(), key="elem_domain")
        with col_f2:
            filter_status = st.multiselect("Status", ["Certified","In Review","Draft","Deprecated"],
                                            default=["Certified","In Review","Draft"])
        with col_f3:
            filter_crit = st.multiselect("Criticality", ["Critical","High","Medium","Low"],
                                          default=["Critical","High","Medium","Low"])

        elems_df = get_elements_df()
        if filter_domain != "(All)":
            elems_df = elems_df[elems_df["domain"] == filter_domain]
        if filter_status:
            elems_df = elems_df[elems_df["status"].isin(filter_status)]
        if filter_crit:
            elems_df = elems_df[elems_df["criticality"].isin(filter_crit)]

        display_cols = [
            "element_id","display_name","element_name","domain","subdomain","concept",
            "data_type","classification","golden_source","owner_team","steward",
            "criticality","status","pii","last_reviewed",
        ]
        st.dataframe(elems_df[display_cols], use_container_width=True, hide_index=True)

        # Element detail panel
        st.markdown("---")
        st.subheader("🔍 Element Detail")
        sel_elem_id = st.selectbox("Select Element", elems_df["element_id"].tolist(),
                                    format_func=lambda x: f"{x} — {elems_df[elems_df['element_id']==x]['display_name'].values[0]}"
                                    if x in elems_df["element_id"].values else x)
        elem = get_element(sel_elem_id)
        if elem:
            dc1, dc2, dc3 = st.columns(3)
            dc1.metric("Data Type",     elem["data_type"])
            dc2.metric("Criticality",   elem["criticality"])
            dc3.metric("Status",        elem["status"])

            d1, d2 = st.columns(2)
            with d1:
                st.markdown(f"**Definition:** {elem['description']}")
                st.markdown(f"**Format:** `{elem['format']}`")
                st.markdown(f"**Example:** `{elem['example_value']}`")
                st.markdown(f"**Regulatory Ref:** {elem['regulatory_ref']}")
                st.markdown(f"**Linked DQ Rules:** {', '.join(elem['linked_rule_ids']) if elem['linked_rule_ids'] else '—'}")
                st.markdown(f"**Glossary Term:** {elem['glossary_term']}")
            with d2:
                st.markdown(f"**Golden Source:** {elem['golden_source']}")
                st.markdown(f"**Secondary Source:** {elem['secondary_source']}")
                st.markdown(f"**Data Layer:** {elem['data_layer']}")
                st.markdown(f"**Classification:** {elem['classification']}")
                st.markdown(f"**PII:** {'🔒 Yes' if elem['pii'] else 'No'}")
                consuming = elem['consuming_teams']
                if isinstance(consuming, list):
                    st.markdown(f"**Consumed by:** {', '.join(consuming)}")

        # Add new element form
        st.markdown("---")
        st.subheader("➕ Register New Data Element")
        with st.form("add_element_form"):
            ae1, ae2, ae3 = st.columns(3)
            with ae1:
                new_eid     = st.text_input("Element ID", placeholder="DE-011")
                new_ename   = st.text_input("Field Name", placeholder="e.g. ytd_pct")
                new_dname   = st.text_input("Display Name", placeholder="e.g. YTD Return (%)")
                new_domain  = st.selectbox("Domain", get_all_domains(), key="ae_domain")
            with ae2:
                subdomains  = get_subdomains(new_domain)
                new_sub     = st.selectbox("Subdomain", subdomains if subdomains else ["—"], key="ae_sub")
                new_concept = st.text_input("Concept", placeholder="e.g. Return Metrics")
                new_type    = st.selectbox("Data Type", ["float","string","int","enum","date","timestamp","boolean","document","concept"])
                new_format  = st.text_input("Format / Pattern")
            with ae3:
                new_golden  = st.selectbox("Golden Source", ["Fund Administrator","Bloomberg","Custodian","Transfer Agent","Legal & Compliance","Internal","GLEIF"])
                new_owner   = st.selectbox("Owner Team", [t["team_name"] for t in TEAMS])
                new_crit    = st.selectbox("Criticality", ["Critical","High","Medium","Low"])
                new_class   = st.selectbox("Classification", ["Public","Internal","Confidential","Restricted"])
                new_pii     = st.checkbox("PII Data")
                new_status  = st.selectbox("Status", ["Draft","In Review","Certified"])

            new_desc     = st.text_area("Definition / Description")
            new_reg      = st.text_input("Regulatory Reference")
            new_example  = st.text_input("Example Value")

            if st.form_submit_button("Register Element", type="primary"):
                try:
                    add_element({
                        "element_id":       new_eid,
                        "element_name":     new_ename,
                        "display_name":     new_dname,
                        "domain":           new_domain,
                        "subdomain":        new_sub,
                        "concept":          new_concept,
                        "description":      new_desc,
                        "data_type":        new_type,
                        "format":           new_format,
                        "example_value":    new_example,
                        "nullable":         True,
                        "pii":              new_pii,
                        "classification":   new_class,
                        "golden_source":    new_golden,
                        "secondary_source": "",
                        "data_layer":       "Gold",
                        "owner_team":       new_owner,
                        "steward":          "Data Management",
                        "consuming_teams":  [],
                        "regulatory_ref":   new_reg,
                        "linked_rule_ids":  [],
                        "glossary_term":    "",
                        "status":           new_status,
                        "criticality":      new_crit,
                    })
                    st.success(f"✅ Element {new_eid} registered.")
                except ValueError as e:
                    st.error(str(e))

    # ─── TAB: BUSINESS GLOSSARY
    with tab_glossary:
        st.subheader("📚 Business Glossary")
        st.caption("Approved fund industry definitions linked to the data taxonomy")

        gloss_search = st.text_input("🔍 Search glossary", placeholder="e.g. NAV, ISIN, SFDR...")
        gloss_df = search_glossary(gloss_search)
        sel_gdom = st.multiselect("Filter by Domain", get_all_domains(), default=[])
        if sel_gdom:
            gloss_df = gloss_df[gloss_df["domain"].isin(sel_gdom)]

        st.caption(f"Showing {len(gloss_df)} term(s)")
        for _, row in gloss_df.iterrows():
            abbrev = f" ({row['abbreviation']})" if row.get("abbreviation") and row["abbreviation"] != "None" else ""
            with st.expander(f"**{row['term']}{abbrev}** — _{row['domain']} / {row['subdomain']}_"):
                col_g1, col_g2 = st.columns([3, 1])
                with col_g1:
                    st.markdown(f"**Definition:** {row['definition']}")
                    st.markdown(f"**Example:** _{row['example']}_")
                    related = row.get("related_terms", [])
                    if isinstance(related, list) and related:
                        st.markdown(f"**Related Terms:** {', '.join(related)}")
                with col_g2:
                    st.markdown(f"**Owner:** {row['owner']}")
                    st.markdown(f"**Status:** {row['status']}")
                    st.markdown(f"**Data Type:** `{row['data_type']}`")
                    if row.get("regulatory_ref"):
                        st.markdown(f"**Reg. Ref:** {row['regulatory_ref']}")

        st.markdown("---")
        st.subheader("➕ Add Glossary Term")
        with st.form("add_gloss_form"):
            gg1, gg2 = st.columns(2)
            with gg1:
                g_term   = st.text_input("Term")
                g_abbrev = st.text_input("Abbreviation (optional)")
                g_domain = st.selectbox("Domain", get_all_domains(), key="g_domain")
                g_sub    = st.selectbox("Subdomain", get_subdomains(get_all_domains()[0]), key="g_sub")
            with gg2:
                g_owner  = st.selectbox("Owner", [t["team_name"] for t in TEAMS])
                g_type   = st.selectbox("Data Type", ["float","string","enum","entity","concept","document","timestamp"])
                g_status = st.selectbox("Status", ["Approved","Draft","Under Review","Deprecated"])
                g_reg    = st.text_input("Regulatory Reference")
            g_def  = st.text_area("Definition")
            g_ex   = st.text_input("Example")
            if st.form_submit_button("Add Term", type="primary"):
                add_glossary_term({
                    "term": g_term, "abbreviation": g_abbrev or None,
                    "domain": g_domain, "subdomain": g_sub,
                    "definition": g_def, "example": g_ex,
                    "related_terms": [], "regulatory_ref": g_reg,
                    "data_type": g_type, "owner": g_owner, "status": g_status,
                })
                st.success(f"✅ Term '{g_term}' added.")

    # ─── TAB: OWNERSHIP & USAGE
    with tab_ownership:
        st.subheader("👥 Ownership & Usage Matrix")
        st.caption("Which teams own and consume data elements across domains")

        # Teams directory
        st.markdown("#### 🏢 Team Directory")
        st.dataframe(get_teams_df(), use_container_width=True, hide_index=True)

        st.markdown("---")
        # Ownership heatmap
        st.markdown("#### 🔥 Usage Heatmap — Teams × Domains")
        import plotly.graph_objects as go
        heatmap_data = get_usage_heatmap_data()
        if not heatmap_data.empty:
            fig_heat = go.Figure(go.Heatmap(
                z     = heatmap_data.values.tolist(),
                x     = heatmap_data.columns.tolist(),
                y     = heatmap_data.index.tolist(),
                colorscale = "Blues",
                showscale  = True,
                hoverongaps = False,
            ))
            fig_heat.update_layout(
                paper_bgcolor = "rgba(0,0,0,0)",
                plot_bgcolor  = "rgba(0,0,0,0)",
                font_color    = "white",
                height        = 420,
                xaxis = dict(tickangle=-35, tickfont=dict(size=10)),
                yaxis = dict(tickfont=dict(size=10)),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        st.markdown("---")
        st.markdown("#### 📋 Ownership Details")
        ownership_df = get_ownership_matrix()
        if not ownership_df.empty:
            st.dataframe(
                ownership_df.sort_values(["team","domain"]),
                use_container_width=True, hide_index=True,
            )

    # ─── TAB: SEARCH
    with tab_search:
        st.subheader("🔍 Universal Catalog Search")
        st.caption("Search across elements, glossary terms and the taxonomy")

        query = st.text_input("Search anything...", placeholder="e.g. NAV, settlement, ISIN, AML, SFDR, weight...")

        if query:
            # Elements
            elem_results = search_elements(query)
            st.markdown(f"#### 📋 Data Elements ({len(elem_results)} result(s))")
            if not elem_results.empty:
                st.dataframe(
                    elem_results[["element_id","display_name","domain","subdomain","concept",
                                  "data_type","owner_team","status","criticality"]],
                    use_container_width=True, hide_index=True,
                )
            else:
                st.info("No matching elements.")

            # Glossary
            gloss_results = search_glossary(query)
            st.markdown(f"#### 📚 Glossary Terms ({len(gloss_results)} result(s))")
            if not gloss_results.empty:
                st.dataframe(
                    gloss_results[["term","abbreviation","domain","definition","owner","status"]],
                    use_container_width=True, hide_index=True,
                )
            else:
                st.info("No matching glossary terms.")

            # Taxonomy
            tax_flat = build_taxonomy_flat()
            q = query.lower()
            tax_results = tax_flat[
                tax_flat["domain"].str.lower().str.contains(q, na=False) |
                tax_flat["subdomain"].str.lower().str.contains(q, na=False) |
                tax_flat["concept"].str.lower().str.contains(q, na=False) |
                tax_flat["element"].str.lower().str.contains(q, na=False)
            ]
            st.markdown(f"#### 🌳 Taxonomy Hits ({len(tax_results)} result(s))")
            if not tax_results.empty:
                st.dataframe(tax_results, use_container_width=True, hide_index=True)
            else:
                st.info("No matching taxonomy nodes.")
        else:
            st.info("Type a search term above to query elements, glossary and taxonomy simultaneously.")
