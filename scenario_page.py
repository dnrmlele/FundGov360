
# ══════════════════════════════════════════════════════════════════════════════
# PAGE: 🎬 SCENARIO SIMULATOR
# Drop this block into your app.py page routing (the big if/elif chain).
# Prerequisite: add to load_all_data():
#   from utils.scenario_engine import (gen_share_class_events,
#       gen_kid_data_packages, gen_deregistration_events, gen_event_log)
#   sc_events_df   = gen_share_class_events(sc_df)
#   kid_pkg_df     = gen_kid_data_packages(sc_df)
#   dereg_df       = gen_deregistration_events(sc_df)
#   event_log_df   = gen_event_log(sc_events_df, dereg_df)
# And return them from load_all_data() alongside the existing dataframes.
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🎬 Scenario Simulator":
    from utils.scenario_engine import TEAMS, TEAM_COLORS

    st.title("🎬 Scenario Simulator")
    st.markdown(
        "> **What does your data governance problem look like in practice?**  "
        "These three scenarios map directly to real operations at Deloitte Operate Luxembourg.  "
        "Use the tabs below to walk through each one."
    )

    # ── Top KPI row ─────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Share Class Events Simulated", len(sc_events_df))
    k2.metric("KID Packages — Ready 🟢",
              len(kid_pkg_df[kid_pkg_df["kid_status"] == "🟢 Ready"]))
    k3.metric("De-registration Events", len(dereg_df))
    k4.metric("Cascade Failures (No Governance)",
              int(dereg_df["n_missed_without"].sum()))

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs([
        "📌 Scenario 1 — New Share Class",
        "📄 Scenario 2 — Annual KID Review",
        "🗑️  Scenario 3 — De-registration Cascade",
        "📋 Unified Event Log",
    ])

    # ────────────────────────────────────────────────────────────────────────
    # TAB 1: NEW SHARE CLASS CREATION
    # ────────────────────────────────────────────────────────────────────────
    with tab1:
        st.subheader("Scenario 1 — New Share Class Creation")
        st.markdown(
            "Select a simulated share class creation event to see how notifications "
            "flow (or don't flow) across teams **without** and **with** governance."
        )

        event_options = sc_events_df["event_id"] + " — " + sc_events_df["share_class_id"]
        sel = st.selectbox("Choose an event", event_options, key="sc_event_sel")
        ev_row = sc_events_df[sc_events_df["event_id"] == sel.split(" — ")[0]].iloc[0]

        # Meta info
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("ISIN", ev_row["isin"])
        c2.metric("Currency", ev_row["currency"])
        c3.metric("Target Countries", ev_row["n_countries"])
        c4.metric("Event Date", ev_row["event_date"])

        flags = []
        if ev_row["has_perf_fee"]: flags.append("⚡ Performance Fee")
        if ev_row["has_esg"]:      flags.append("🌱 ESG Share Class")
        if ev_row["has_uk"]:       flags.append("🇬🇧 UK Registration")
        if flags:
            st.info("  ·  ".join(flags))

        st.markdown("---")
        col_wo, col_wi = st.columns(2)

        with col_wo:
            st.markdown("### ⚠️  Without Governance")
            st.caption(f"Teams notified: **{ev_row['n_teams_notified_without']} / {len(TEAMS)}**")
            for team in TEAMS:
                s = ev_row["team_status_without"][team]
                status_icon = "✅" if s["notified"] else "❌"
                with st.expander(f"{status_icon} **{team}**  —  {s['status']}",
                                 expanded=not s["notified"]):
                    if s["delay_h"] > 0:
                        if s["delay_h"] >= 720:
                            st.warning(f"⏱ Delay: **{round(s['delay_h']/720,1)} month(s)**")
                        elif s["delay_h"] >= 24:
                            st.warning(f"⏱ Delay: **{round(s['delay_h']/24,1)} day(s)**")
                        else:
                            st.warning(f"⏱ Delay: **{s['delay_h']}h**")
                    if s["issue"]:
                        st.error(f"🚨 Issue: {s['issue']}")
                    elif not s["notified"]:
                        st.error("Not informed at all.")

            if ev_row["n_issues_without"] > 0:
                st.error(f"**{ev_row['n_issues_without']} issue(s) identified:**

"
                         + "
".join(f"- {x}" for x in ev_row["issues_without"].split(" | ")))

        with col_wi:
            st.markdown("### ✅  With Data Governance")
            st.caption(f"Teams notified: **{ev_row['n_teams_notified_with']} / {len(TEAMS)}**  "
                       "— simultaneously, within seconds of record activation")
            for team in TEAMS:
                s = ev_row["team_status_with"][team]
                with st.expander(f"✅ **{team}**  —  {s['status']}", expanded=False):
                    st.success("Notified via governed data event. No email, no delay, no re-entry.")

            st.success(
                "**0 issues.** All teams receive the same validated, taxonomy-driven "
                "data package. EPT and KID draw from the same cost layer. "
                "Tax obligations configured at inception."
            )

        st.divider()
        st.subheader("📊 Team Notification Summary — All Events")
        import plotly.express as px
        import plotly.graph_objects as go

        summary = sc_events_df[["event_id", "n_teams_notified_without",
                                 "n_teams_notified_with"]].copy()
        summary.columns = ["Event", "Without Governance", "With Governance"]
        fig_bar = go.Figure()
        fig_bar.add_bar(x=summary["Event"], y=summary["Without Governance"],
                        name="Without Governance", marker_color="#EF4444")
        fig_bar.add_bar(x=summary["Event"], y=summary["With Governance"],
                        name="With Governance", marker_color="#86BC25")
        fig_bar.update_layout(
            barmode="group", template="plotly_dark",
            title="Teams Notified per Share Class Creation Event",
            yaxis_title="# Teams Notified", xaxis_title="Event",
            height=350, legend=dict(orientation="h", y=1.15)
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        issues_by_event = sc_events_df[["event_id", "n_issues_without"]].copy()
        total_issues = issues_by_event["n_issues_without"].sum()
        st.metric("Total Governance Issues Across All Events (Without Governance)", total_issues,
                  delta=f"-{total_issues} with governance", delta_color="normal")

    # ────────────────────────────────────────────────────────────────────────
    # TAB 2: ANNUAL KID REVIEW
    # ────────────────────────────────────────────────────────────────────────
    with tab2:
        st.subheader("Scenario 2 — Annual KID Review: Data Package Readiness")
        st.markdown(
            "Every share class requires a KID Data Package for the annual PRIIPs review. "
            "The dashboard below simulates current readiness — showing what the KID team "
            "would face if they opened the dashboard on **January 2nd**."
        )

        # Filters
        fc1, fc2 = st.columns(2)
        status_filter = fc1.multiselect(
            "Filter by KID Status",
            ["🟢 Ready", "🟡 Pending", "🔴 Missing Data"],
            default=["🟢 Ready", "🟡 Pending", "🔴 Missing Data"],
            key="kid_status_filter"
        )
        fund_filter = fc2.multiselect(
            "Filter by Fund",
            kid_pkg_df["fund_id"].unique().tolist(),
            default=kid_pkg_df["fund_id"].unique().tolist(),
            key="kid_fund_filter"
        )

        filtered_kid = kid_pkg_df[
            kid_pkg_df["kid_status"].isin(status_filter) &
            kid_pkg_df["fund_id"].isin(fund_filter)
        ]

        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Share Classes", len(filtered_kid))
        k2.metric("🟢 Ready",
                  len(filtered_kid[filtered_kid["kid_status"] == "🟢 Ready"]))
        k3.metric("🟡 Pending",
                  len(filtered_kid[filtered_kid["kid_status"] == "🟡 Pending"]))
        k4.metric("🔴 Missing Data",
                  len(filtered_kid[filtered_kid["kid_status"] == "🔴 Missing Data"]))

        # Status donut
        status_counts = filtered_kid["kid_status"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        color_map = {"🟢 Ready": "#86BC25", "🟡 Pending": "#F0AB00",
                     "🔴 Missing Data": "#EF4444"}
        fig_donut = px.pie(
            status_counts, names="Status", values="Count",
            hole=0.55, title="KID Package Status Distribution",
            color="Status", color_discrete_map=color_map,
        )
        fig_donut.update_layout(template="plotly_dark", height=320)
        st.plotly_chart(fig_donut, use_container_width=True)

        # Missing item breakdown
        st.subheader("What is Missing — Root Cause Breakdown")
        missing_fields = {
            "Fee data outdated": 0, "NAV series gap": 0, "Cost data stale (>12m)": 0,
            "Narrative pending approval": 0, "EPT/KID cost mismatch": 0,
            "Language version(s) missing": 0, "Registration status stale": 0
        }
        for _, row in filtered_kid.iterrows():
            for item in row["missing_items"].split(" | "):
                for key in missing_fields:
                    if key.split("(")[0].strip() in item:
                        missing_fields[key] += 1

        miss_df = pd.DataFrame(list(missing_fields.items()),
                               columns=["Issue", "Count"]).sort_values("Count", ascending=True)
        miss_df = miss_df[miss_df["Count"] > 0]
        if not miss_df.empty:
            fig_miss = px.bar(miss_df, x="Count", y="Issue", orientation="h",
                              title="Missing Data Items by Type",
                              color_discrete_sequence=["#EF4444"])
            fig_miss.update_layout(template="plotly_dark", height=350)
            st.plotly_chart(fig_miss, use_container_width=True)

        # Detailed table
        st.subheader("Share Class KID Readiness Table")
        display_cols = ["share_class_id", "fund_id", "isin", "kid_status",
                        "lang_versions_ready", "lang_versions_needed",
                        "days_to_deadline", "missing_items"]
        st.dataframe(
            filtered_kid[display_cols].rename(columns={
                "share_class_id": "Share Class", "fund_id": "Fund",
                "isin": "ISIN", "kid_status": "Status",
                "lang_versions_ready": "Lang Ready",
                "lang_versions_needed": "Lang Needed",
                "days_to_deadline": "Days Left",
                "missing_items": "Missing Items"
            }),
            use_container_width=True, hide_index=True
        )

        st.info(
            "💡 **With governance:** The KID Data Package is maintained year-round in the Gold layer. "
            "On January 2nd, this dashboard pre-fills from live governed data. "
            "The team reviews and signs off — they don't hunt for inputs."
        )

    # ────────────────────────────────────────────────────────────────────────
    # TAB 3: DE-REGISTRATION CASCADE
    # ────────────────────────────────────────────────────────────────────────
    with tab3:
        st.subheader("Scenario 3 — Fund De-registration: The Silent Cascade")
        st.markdown(
            "Select a simulated de-registration event to see which teams were "
            "notified (or not) **without governance**, and how the cascade works **with governance**."
        )

        dreg_options = dereg_df["event_id"] + " — " + dereg_df["share_class_id"] +                        " (" + dereg_df["country_name"] + ")"
        sel_dreg = st.selectbox("Choose a de-registration event", dreg_options, key="dreg_sel")
        dreg_row = dereg_df[dereg_df["event_id"] == sel_dreg.split(" — ")[0]].iloc[0]

        d1, d2, d3 = st.columns(3)
        d1.metric("Share Class", dreg_row["share_class_id"])
        d2.metric("De-registered Country", dreg_row["country_name"])
        d3.metric("Event Date", dreg_row["event_date"])

        st.markdown("---")
        col_a, col_b = st.columns(2)

        cascade_items = [
            ("KID / KIID Team",       "kid_scope_updated_without", "kid_scope_updated_with",
             "KID production scope updated — UK KID tasks suspended"),
            ("EPT Reporting Team",    "ept_scope_updated_without", "ept_scope_updated_with",
             "UK EPT fields (08xxx) excluded from next cycle — distributor list updated"),
            ("Global Tax Team",       "tax_notified_without",      "tax_notified_with",
             f"CGT final notification required — deadline: {dreg_row['cgt_notification_deadline']}"),
            ("Client Reporting Team", "cr_updated_without",        "cr_updated_with",
             "Dashboard template updated before next reporting cycle"),
        ]

        with col_a:
            st.markdown("### ⚠️ Without Governance")
            st.caption("Registration team updates status. Nobody else is informed automatically.")
            missed = 0
            for team, col_wo, _, action in cascade_items:
                done = dreg_row[col_wo]
                if done:
                    st.success(f"✅ **{team}** — self-discovered")
                else:
                    st.error(f"❌ **{team}** — never notified")
                    st.caption(f"Obligation missed: {action}")
                    missed += 1
            if missed > 0:
                st.error(f"**{missed} team(s) never informed.** Issues discovered through "
                         "client complaints or regulator queries — weeks later.")

        with col_b:
            st.markdown("### ✅ With Governance")
            st.caption(f"Registration team updates status → governed event published → "
                       f"all teams notified in ~{dreg_row['time_to_cascade_with_min']} minute(s).")
            for team, _, col_wi, action in cascade_items:
                with st.expander(f"✅ **{team}**", expanded=True):
                    st.success(action)
            st.success("**Full cascade complete.** Audit trail created. "
                       "CGT notification configured on time. EPT scope clean. "
                       "Client dashboard correct from next cycle.")

        st.divider()

        # Summary chart across all events
        st.subheader("📊 Cascade Failures Across All De-registration Events")
        cascade_summary = dereg_df[["event_id", "country_name", "n_missed_without",
                                     "n_missed_with"]].copy()
        fig_dreg = go.Figure()
        fig_dreg.add_bar(
            x=cascade_summary["event_id"],
            y=cascade_summary["n_missed_without"],
            name="Teams Missed (Without Governance)",
            marker_color="#EF4444",
            text=cascade_summary["country_name"],
            textposition="outside"
        )
        fig_dreg.add_bar(
            x=cascade_summary["event_id"],
            y=cascade_summary["n_missed_with"],
            name="Teams Missed (With Governance)",
            marker_color="#86BC25"
        )
        fig_dreg.update_layout(
            barmode="group", template="plotly_dark",
            title="De-registration Cascade: Teams Missed per Event",
            yaxis_title="# Teams Not Notified", xaxis_title="Event",
            height=350, legend=dict(orientation="h", y=1.15)
        )
        st.plotly_chart(fig_dreg, use_container_width=True)

        total_missed = int(dereg_df["n_missed_without"].sum())
        st.metric(
            "Total Downstream Notification Failures (Without Governance)",
            total_missed,
            delta=f"-{total_missed} with governance",
            delta_color="normal"
        )

    # ────────────────────────────────────────────────────────────────────────
    # TAB 4: UNIFIED EVENT LOG
    # ────────────────────────────────────────────────────────────────────────
    with tab4:
        st.subheader("📋 Unified Governance Event Log")
        st.markdown(
            "All data events (share class creations, de-registrations) tracked in one place. "
            "With governance, every event is auditable — who triggered it, which teams were notified, "
            "and whether any issues arose."
        )

        import plotly.express as px

        fig_timeline = px.scatter(
            event_log_df,
            x="event_date", y="event_type",
            color="event_type",
            size="teams_notified_with",
            hover_data=["entity", "triggered_by", "teams_notified_without",
                        "teams_notified_with", "issues_without"],
            title="Event Timeline — All Governance Events",
            labels={"event_date": "Date", "event_type": "Event Type"},
            color_discrete_sequence=["#86BC25", "#EF4444"]
        )
        fig_timeline.update_layout(template="plotly_dark", height=350)
        st.plotly_chart(fig_timeline, use_container_width=True)

        st.dataframe(
            event_log_df[[
                "icon", "event_id", "event_type", "entity", "event_date",
                "triggered_by", "teams_notified_without", "teams_notified_with",
                "issues_without"
            ]].rename(columns={
                "icon": "", "event_id": "ID", "event_type": "Type",
                "entity": "Entity", "event_date": "Date",
                "triggered_by": "Triggered By",
                "teams_notified_without": "Teams (No Gov.)",
                "teams_notified_with": "Teams (Gov.)",
                "issues_without": "Issues (No Gov.)"
            }),
            use_container_width=True, hide_index=True
        )
