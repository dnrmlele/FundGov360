
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

random.seed(77)
np.random.seed(77)

TEAMS = ["Entity Setup", "KID / KIID", "Narratives", "EPT Reporting",
         "Registration", "Global Tax", "Client Reporting"]

TEAM_COLORS = {
    "Entity Setup":    "#86BC25",
    "KID / KIID":      "#00A3E0",
    "Narratives":      "#A3C46F",
    "EPT Reporting":   "#F0AB00",
    "Registration":    "#D0006F",
    "Global Tax":      "#FF6B35",
    "Client Reporting":"#8B5CF6",
}

COUNTRIES = ["LU","DE","FR","GB","IT","ES","NL","BE","AT","SE","DK","CH"]
COUNTRY_NAMES = {
    "LU":"Luxembourg","DE":"Germany","FR":"France","GB":"United Kingdom",
    "IT":"Italy","ES":"Spain","NL":"Netherlands","BE":"Belgium",
    "AT":"Austria","SE":"Sweden","DK":"Denmark","CH":"Switzerland"
}
LANG_MAP = {"DE":"de","FR":"fr","IT":"it","ES":"es","NL":"nl","BE":"fr",
            "AT":"de","SE":"sv","DK":"da","CH":"de","LU":"fr","GB":"en"}

# ─── SCENARIO 1: Share Class Creation Events ──────────────────────────────────

def gen_share_class_events(share_classes_df, n_events=15):
    """Simulates share class creation events with WITHOUT vs WITH governance comparison."""
    rows = []
    sc_sample = share_classes_df.sample(min(n_events, len(share_classes_df)),
                                        random_state=77).reset_index(drop=True)
    for i, sc in sc_sample.iterrows():
        ev_date = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 420))
        target_countries = random.sample(COUNTRIES, random.randint(2, 6))
        has_perf_fee = random.random() > 0.5
        has_esg = random.random() > 0.6
        has_uk = "GB" in target_countries

        # ── WITHOUT governance ────────────────────────────────────────────────
        issues = []
        team_status_without = {}
        for team in TEAMS:
            if team == "Entity Setup":
                team_status_without[team] = {"notified": True, "delay_h": 0,
                                             "status": "✅ Done", "issue": None}
            elif team == "KID / KIID":
                delay = random.randint(0, 48)
                issue = None
                if has_perf_fee and random.random() > 0.5:
                    issue = "Benchmark not specified — assumption used"
                    issues.append(issue)
                team_status_without[team] = {"notified": random.random() > 0.15,
                                             "delay_h": delay, "status": "⚠ Manual",
                                             "issue": issue}
            elif team == "Narratives":
                notified = random.random() > 0.3
                delay = random.randint(24, 96)
                issue = None
                if has_esg and not notified:
                    issue = "ESG disclosure missing — flag not communicated"
                    issues.append(issue)
                team_status_without[team] = {"notified": notified,
                                             "delay_h": delay,
                                             "status": "❌ Late" if not notified else "⚠ Late",
                                             "issue": issue}
            elif team == "EPT Reporting":
                notified = random.random() > 0.4
                issue = None
                if not notified:
                    issue = "EPT produced after distributor request — 6-day gap"
                    issues.append(issue)
                team_status_without[team] = {"notified": notified, "delay_h": random.randint(72, 168),
                                             "status": "❌ Not informed" if not notified else "⚠ Delayed",
                                             "issue": issue}
            elif team == "Registration":
                notified = random.random() > 0.2
                issue = None
                if notified and random.random() > 0.5:
                    issue = "Language versions missing for target countries"
                    issues.append(issue)
                team_status_without[team] = {"notified": notified,
                                             "delay_h": random.randint(12, 72),
                                             "status": "⚠ CC on email" if notified else "❌ Missed",
                                             "issue": issue}
            elif team == "Global Tax":
                notified = random.random() > 0.7
                issue = None
                if not notified:
                    issue = "ITR/CGT obligation configured at year-end, not at inception"
                    issues.append(issue)
                team_status_without[team] = {"notified": notified,
                                             "delay_h": random.randint(0, 8760),
                                             "status": "❌ Not informed" if not notified else "⚠ Informal",
                                             "issue": issue}
            else:  # Client Reporting
                notified = random.random() > 0.5
                team_status_without[team] = {"notified": notified,
                                             "delay_h": random.randint(0, 120),
                                             "status": "⚠ Discovered" if notified else "❌ Missing",
                                             "issue": None}

        # ── WITH governance ───────────────────────────────────────────────────
        team_status_with = {}
        for team in TEAMS:
            if team == "Entity Setup":
                team_status_with[team] = {"notified": True, "delay_h": 0,
                                          "status": "✅ Data Owner — validated record",
                                          "issue": None}
            elif team == "KID / KIID":
                team_status_with[team] = {"notified": True, "delay_h": 0,
                                          "status": "✅ Pre-populated data package received",
                                          "issue": None}
            elif team == "Narratives":
                team_status_with[team] = {"notified": True, "delay_h": 0,
                                          "status": "✅ ESG flag from taxonomy — correct template",
                                          "issue": None}
            elif team == "EPT Reporting":
                team_status_with[team] = {"notified": True, "delay_h": 0,
                                          "status": "✅ Same cost layer as KID — consistent",
                                          "issue": None}
            elif team == "Registration":
                team_status_with[team] = {"notified": True, "delay_h": 0,
                                          "status": "✅ Country list from share class record",
                                          "issue": None}
            elif team == "Global Tax":
                team_status_with[team] = {"notified": True, "delay_h": 0,
                                          "status": "✅ Obligation configured at inception",
                                          "issue": None}
            else:
                team_status_with[team] = {"notified": True, "delay_h": 0,
                                          "status": "✅ Dashboard auto-updated",
                                          "issue": None}

        rows.append({
            "event_id": f"EVT-SC-{i+1:03d}",
            "share_class_id": sc["share_class_id"],
            "fund_id": sc["fund_id"],
            "isin": sc.get("isin", "N/A"),
            "currency": sc.get("currency", "EUR"),
            "event_date": ev_date.strftime("%Y-%m-%d"),
            "target_countries": ", ".join(target_countries),
            "n_countries": len(target_countries),
            "has_perf_fee": has_perf_fee,
            "has_esg": has_esg,
            "has_uk": has_uk,
            "n_issues_without": len(issues),
            "issues_without": " | ".join(issues) if issues else "None",
            "n_teams_notified_without": sum(v["notified"] for v in team_status_without.values()),
            "n_teams_notified_with": len(TEAMS),
            "avg_delay_without_h": round(np.mean([v["delay_h"] for v in team_status_without.values()
                                                   if not v["notified"] or v["delay_h"] > 0]), 1),
            "team_status_without": team_status_without,
            "team_status_with": team_status_with,
        })

    return pd.DataFrame(rows)


# ─── SCENARIO 2: KID Data Package Readiness ───────────────────────────────────

def gen_kid_data_packages(share_classes_df):
    """Per share-class KID readiness status for annual review."""
    rows = []
    for _, sc in share_classes_df.iterrows():
        target_countries = random.sample(COUNTRIES, random.randint(2, 7))
        lang_needed = len(set(LANG_MAP.get(c, "en") for c in target_countries))
        lang_ready = random.randint(max(1, lang_needed - 2), lang_needed)

        static_ok      = random.random() > 0.15
        nav_ok         = random.random() > 0.20
        cost_ok        = random.random() > 0.25
        narrative_ok   = random.random() > 0.30
        ept_consistent = random.random() > 0.35
        reg_current    = random.random() > 0.20

        missing = []
        if not static_ok:      missing.append("Fee data outdated")
        if not nav_ok:         missing.append("NAV series gap")
        if not cost_ok:        missing.append("Cost data stale (>12m)")
        if not narrative_ok:   missing.append("Narrative pending approval")
        if not ept_consistent: missing.append("EPT/KID cost mismatch")
        if lang_ready < lang_needed: missing.append(f"{lang_needed - lang_ready} language version(s) missing")
        if not reg_current:    missing.append("Registration status stale")

        all_ok = all([static_ok, nav_ok, cost_ok, narrative_ok, ept_consistent,
                      lang_ready >= lang_needed, reg_current])
        if all_ok:
            status = "🟢 Ready"
        elif len(missing) <= 2:
            status = "🟡 Pending"
        else:
            status = "🔴 Missing Data"

        rows.append({
            "share_class_id": sc["share_class_id"],
            "fund_id": sc["fund_id"],
            "isin": sc.get("isin", "N/A"),
            "currency": sc.get("currency", "EUR"),
            "target_countries": ", ".join(target_countries),
            "lang_versions_needed": lang_needed,
            "lang_versions_ready": lang_ready,
            "static_data_ok": static_ok,
            "nav_series_ok": nav_ok,
            "cost_data_ok": cost_ok,
            "narrative_approved": narrative_ok,
            "ept_kid_consistent": ept_consistent,
            "registration_current": reg_current,
            "missing_items": " | ".join(missing) if missing else "None",
            "n_missing": len(missing),
            "kid_status": status,
            "days_to_deadline": random.randint(1, 35),
            "last_kid_date": (datetime(2025, 3, 1) - timedelta(days=random.randint(300, 370))).strftime("%Y-%m-%d"),
        })

    return pd.DataFrame(rows)


# ─── SCENARIO 3: De-registration Cascade ─────────────────────────────────────

def gen_deregistration_events(share_classes_df, n_events=12):
    """Simulated de-registration events with cascade notification status."""
    rows = []
    sc_sample = share_classes_df.sample(min(n_events, len(share_classes_df)),
                                        random_state=88).reset_index(drop=True)
    for i, sc in sc_sample.iterrows():
        country = random.choice(COUNTRIES)
        ev_date = datetime(2025, 6, 1) + timedelta(days=random.randint(0, 270))

        # Without governance: most teams miss it
        kid_upd_without      = random.random() > 0.7
        ept_upd_without      = random.random() > 0.7
        tax_notified_without = random.random() > 0.75
        cr_upd_without       = random.random() > 0.6

        missed_without = []
        if not kid_upd_without:      missed_without.append("KID still maintained for de-registered country")
        if not ept_upd_without:      missed_without.append("EPT UK fields still populated")
        if not tax_notified_without: missed_without.append("Final CGT notification not issued")
        if not cr_upd_without:       missed_without.append("Fund still on client dashboard")

        cgt_deadline = (ev_date + timedelta(days=30)).strftime("%Y-%m-%d")

        rows.append({
            "event_id": f"EVT-DREG-{i+1:03d}",
            "share_class_id": sc["share_class_id"],
            "fund_id": sc["fund_id"],
            "de_registered_country": country,
            "country_name": COUNTRY_NAMES.get(country, country),
            "event_date": ev_date.strftime("%Y-%m-%d"),
            "cgt_notification_deadline": cgt_deadline,

            # WITHOUT
            "kid_scope_updated_without": kid_upd_without,
            "ept_scope_updated_without": ept_upd_without,
            "tax_notified_without": tax_notified_without,
            "cr_updated_without": cr_upd_without,
            "cascade_complete_without": all([kid_upd_without, ept_upd_without,
                                            tax_notified_without, cr_upd_without]),
            "n_missed_without": len(missed_without),
            "missed_obligations": " | ".join(missed_without) if missed_without else "None",

            # WITH governance — always complete, always instant
            "kid_scope_updated_with": True,
            "ept_scope_updated_with": True,
            "tax_notified_with": True,
            "cr_updated_with": True,
            "cascade_complete_with": True,
            "n_missed_with": 0,
            "time_to_cascade_with_min": random.randint(1, 5),
        })

    return pd.DataFrame(rows)


# ─── Unified Event Log ────────────────────────────────────────────────────────

def gen_event_log(sc_events_df, dereg_events_df):
    """Unified governance event log across all scenarios."""
    rows = []
    for _, ev in sc_events_df.head(8).iterrows():
        rows.append({
            "event_id": ev["event_id"],
            "event_type": "Share Class Creation",
            "entity": ev["share_class_id"],
            "event_date": ev["event_date"],
            "triggered_by": "Entity Setup",
            "teams_notified_without": ev["n_teams_notified_without"],
            "teams_notified_with": ev["n_teams_notified_with"],
            "issues_without": ev["n_issues_without"],
            "governed": True,
            "icon": "🆕",
        })
    for _, ev in dereg_events_df.head(8).iterrows():
        rows.append({
            "event_id": ev["event_id"],
            "event_type": "De-registration",
            "entity": f"{ev['share_class_id']} — {ev['country_name']}",
            "event_date": ev["event_date"],
            "triggered_by": "Registration Team",
            "teams_notified_without": 4 - ev["n_missed_without"],
            "teams_notified_with": 4,
            "issues_without": ev["n_missed_without"],
            "governed": True,
            "icon": "🗑️",
        })
    df = pd.DataFrame(rows).sort_values("event_date").reset_index(drop=True)
    return df
