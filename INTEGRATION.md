
# FundGov360 — Scenario Simulator: Integration Guide

## What's in this package

```
FundGov360_ScenarioSimulator/
├── utils/
│   └── scenario_engine.py      ← NEW: data engine for all 3 scenarios
├── scenario_page.py            ← NEW: drop-in Streamlit page (copy into app.py)
└── INTEGRATION.md              ← this file
```

---

## Step 1 — Copy scenario_engine.py

Drop `utils/scenario_engine.py` into your existing `utils/` folder.

---

## Step 2 — Update load_all_data() in app.py

Add these imports at the top of `app.py`:

```python
from utils.scenario_engine import (
    gen_share_class_events,
    gen_kid_data_packages,
    gen_deregistration_events,
    gen_event_log,
)
```

Inside `load_all_data()`, add after the existing generators:

```python
sc_events_df  = gen_share_class_events(sc_df)
kid_pkg_df    = gen_kid_data_packages(sc_df)
dereg_df      = gen_deregistration_events(sc_df)
event_log_df  = gen_event_log(sc_events_df, dereg_df)
```

Return them alongside the existing dataframes:

```python
return (
    funds_df, sub_funds_df, sc_df, nav_df, port_df,
    tx_df, reg_df, imports_df, conflicts_df, stewards_df,
    catalog_df, lineage_df, profiling_df,
    sc_events_df, kid_pkg_df, dereg_df, event_log_df   # ← ADD
)
```

---

## Step 3 — Unpack in the main app body

Find where you unpack `load_all_data()` and add the 4 new variables:

```python
(
    funds_df, sub_funds_df, sc_df, nav_df, port_df,
    tx_df, reg_df, imports_df, conflicts_df, stewards_df,
    catalog_df, lineage_df, profiling_df,
    sc_events_df, kid_pkg_df, dereg_df, event_log_df   # ← ADD
) = load_all_data()
```

---

## Step 4 — Add the page to the sidebar

In the sidebar navigation list, add:

```python
"🎬 Scenario Simulator",
```

---

## Step 5 — Add the page route

In the big `if page == ...` / `elif page == ...` routing block,
paste the full contents of `scenario_page.py` as a new `elif` block:

```python
elif page == "🎬 Scenario Simulator":
    # ... paste everything from scenario_page.py here
```

---

## Step 6 — Run

```bash
streamlit run app.py
```

The **🎬 Scenario Simulator** page will appear in the sidebar with 4 tabs:
- 📌 Scenario 1 — New Share Class Creation
- 📄 Scenario 2 — Annual KID Review
- 🗑️  Scenario 3 — De-registration Cascade
- 📋 Unified Event Log

---

## What each scenario shows

### Scenario 1 — New Share Class Creation
- Select any simulated share class creation event
- See side-by-side: which teams were notified WITHOUT vs WITH governance
- Delays, missed notifications, and issues (EPT/KID mismatch, missing ESG flag, etc.)
- Summary bar chart across all 15 simulated events

### Scenario 2 — Annual KID Review
- Per-share-class KID Data Package readiness (🟢 Ready / 🟡 Pending / 🔴 Missing)
- Root cause breakdown: what data is missing and why
- Language version gap (how many country versions are needed vs ready)
- Filterable table showing days to CSSF deadline

### Scenario 3 — De-registration Cascade
- Select any simulated de-registration event
- See which teams were notified without governance (many missed)
- See full instant cascade with governance (all 4 teams, within minutes)
- CGT notification deadline surfaced automatically
- Summary bar chart across all 12 simulated events

### Unified Event Log
- Combined timeline of all events (share class creation + de-registration)
- Shows teams notified, issues, and triggering team
- Interactive scatter timeline with hover details
