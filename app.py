"""
CUMTA TransitPulse — Decision Support Workbench
Problem Statement 6: From Chennai One Data to Mobility Decisions
Production Decision-Support System with Unified Data Integrity & Clean Design
"""

import io
import json
import math
import os
import time
import zipfile

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from revenue import (
    PREMIUM_ROUTES,
    compute_revenue,
    compute_row_metrics,
    get_standard_fare,
    run_revenue_assertions,
)

# Theme and revenue calculation engines
from theme.plotly_theme import (
    COLOR_BORDER,
    COLOR_CONVERSION,
    COLOR_LOST,
    COLOR_MUTED,
    COLOR_SEARCHES,
    COLOR_TEXT,
    COLOR_TICKETS,
    FONT_FAMILY,
    apply_clean_layout,
)

# ==============================================================================
# 1. PAGE CONFIGURATION & DATA SOURCE AUTO-DETECTION
# ==============================================================================
st.set_page_config(
    page_title="CUMTA TransitPulse — Chennai Mobility Decision Support",
    page_icon="🚍",
    layout="wide",
    initial_sidebar_state="expanded",
)


def load_clean_css():
    css_path = os.path.join("theme", "clean.css")
    if os.path.exists(css_path):
        with open(css_path, "r", encoding="utf-8") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


load_clean_css()

# Auto-detect data directory: environment variable -> data_processed -> sample_data
DATA_DIR = os.environ.get("TRANSITPULSE_DATA_DIR")
is_sample_mode = False

if not DATA_DIR:
    if os.path.exists("data_processed/kpis.parquet"):
        DATA_DIR = "data_processed"
    elif os.path.exists("sample_data/kpis.parquet"):
        DATA_DIR = "sample_data"
        is_sample_mode = True
    else:
        DATA_DIR = "data_processed"
elif DATA_DIR == "sample_data":
    is_sample_mode = True


# ==============================================================================
# 2. CACHED DATA LOADERS (SUPPORTS LOCAL DATA_PROCESSED OR SAMPLE_DATA)
# ==============================================================================
@st.cache_data
def load_kpis(data_dir: str):
    p = os.path.join(data_dir, "kpis.parquet")
    return pd.read_parquet(p).iloc[0]


@st.cache_data
def load_city_hourly(data_dir: str):
    p = os.path.join(data_dir, "city_hourly.parquet")
    return pd.read_parquet(p)


@st.cache_data
def load_city_dow(data_dir: str):
    p = os.path.join(data_dir, "city_hour_dow.parquet")
    if os.path.exists(p):
        return pd.read_parquet(p)
    return pd.DataFrame()


@st.cache_data
def load_diagnosis(data_dir: str):
    p = os.path.join(data_dir, "diagnosis.parquet")
    return pd.read_parquet(p)


@st.cache_data
def load_actions(data_dir: str):
    p = os.path.join(data_dir, "actions.parquet")
    return pd.read_parquet(p)


@st.cache_data
def load_premium_rollup(data_dir: str):
    p = os.path.join(data_dir, "premium_rollup.parquet")
    if os.path.exists(p):
        return pd.read_parquet(p)
    return pd.DataFrame()


@st.cache_data
def load_transfers_clean(data_dir: str):
    p = os.path.join(data_dir, "transfer_pairs.parquet")
    if os.path.exists(p):
        return pd.read_parquet(p)
    return pd.DataFrame()


@st.cache_data
def load_transfers_agg(data_dir: str):
    p = os.path.join(data_dir, "transfer_pairs_aggregated.parquet")
    if os.path.exists(p):
        return pd.read_parquet(p)
    return load_transfers_clean(data_dir)


@st.cache_data
def load_hubs(data_dir: str):
    p = os.path.join(data_dir, "hubs.parquet")
    if os.path.exists(p):
        return pd.read_parquet(p)
    return pd.DataFrame()


@st.cache_data
def load_route_hourly(data_dir: str):
    p = os.path.join(data_dir, "route_hourly.parquet")
    if os.path.exists(p):
        return pd.read_parquet(p)
    return pd.DataFrame()


@st.cache_data
def load_stop_coords():
    if os.path.exists("data_reference/stops.txt"):
        try:
            df = pd.read_csv("data_reference/stops.txt")
            if "stop_name" in df.columns and "stop_lat" in df.columns and "stop_lon" in df.columns:
                return df[["stop_name", "stop_lat", "stop_lon"]].rename(
                    columns={"stop_name": "hub_name", "stop_lat": "lat", "stop_lon": "lon"}
                )
        except Exception:
            pass
    if os.path.exists("data_reference/stop_coords.csv"):
        return pd.read_csv("data_reference/stop_coords.csv")
    return pd.DataFrame(columns=["hub_name", "lat", "lon", "coord_source"])


# ==============================================================================
# 3. INITIALIZE DATA & REVENUE ENGINE
# ==============================================================================
with st.spinner("Loading Chennai transit telemetry..."):
    try:
        kpi = load_kpis(DATA_DIR)
        df_city_h = load_city_hourly(DATA_DIR)
        df_city_dow = load_city_dow(DATA_DIR)
        df_diag_all = load_diagnosis(DATA_DIR)
        df_act_all = load_actions(DATA_DIR)
        df_prem_all = load_premium_rollup(DATA_DIR)
        df_xfer_clean = load_transfers_clean(DATA_DIR)
        df_xfer_agg = load_transfers_agg(DATA_DIR)
        df_hubs = load_hubs(DATA_DIR)
        df_route_h = load_route_hourly(DATA_DIR)
        df_coords = load_stop_coords()
    except Exception as e:
        st.error(f"Error loading system data: {e}")
        st.stop()

# Setup coordinate lookup dictionary
coord_lookup = {}
for _, r in df_coords.iterrows():
    h_clean = str(r["hub_name"]).strip().upper()
    coord_lookup[h_clean] = (float(r["lat"]), float(r["lon"]))


def match_coordinates(name, hub):
    n = str(name).strip().upper()
    h = str(hub).strip().upper()
    if n in coord_lookup:
        return coord_lookup[n]
    if h in coord_lookup:
        return coord_lookup[h]
    for k, v in coord_lookup.items():
        if k == n or k == h or k in n or n in k:
            return v
    return None


# Build premium route parallel fare lookup dictionary
prem_lookup = {}
if not df_prem_all.empty and "from_station_id" in df_prem_all.columns:
    prem_lookup = df_prem_all.set_index(["from_station_id", "to_station_id"])[
        "parallel_fare"
    ].to_dict()

# Attach standard fare and row metrics to actions table
df_act_all = compute_row_metrics(df_act_all, capture_rate=0.35, days=30, prem_lookup=prem_lookup)

# Run load assertions per rule 4d
assertions_ok, assertion_msg, assertion_metrics = run_revenue_assertions(
    df_act_all, prem_lookup=prem_lookup
)
if not assertions_ok:
    st.warning(f"Revenue Assertion Warning: {assertion_msg}")

if "selected_corridor" not in st.session_state:
    top_init = df_diag_all.sort_values("lost_trips", ascending=False).iloc[0]
    st.session_state.selected_corridor = (
        f"{top_init['from_name']} → {top_init['to_name']} ({top_init['time_bucket']})"
    )


# ==============================================================================
# 4. GLOBAL SIDEBAR (NAVIGATION, FILTERS, METHODOLOGY)
# ==============================================================================
with st.sidebar:
    st.markdown("## CUMTA TransitPulse")
    st.caption("Chennai Mobility Decision Support Workbench")

    if is_sample_mode:
        st.info(
            "Live demo on a small aggregated sample of March 2026 Chennai One data. Full-data results are in the README."
        )

    workbench_views = [
        "📊 Dashboard",
        "🗺️ Chennai Map",
        "🔍 Diagnose",
        "⚡ Actions",
        "📥 Data & Replay",
    ]

    query_tab = st.query_params.get("tab", "1")
    default_tab_idx = 0
    if str(query_tab).isdigit() and 1 <= int(query_tab) <= 5:
        default_tab_idx = int(query_tab) - 1

    selected_view = st.radio(
        "Navigation",
        workbench_views,
        index=default_tab_idx,
        label_visibility="collapsed",
        key="nav_view_radio",
    )

    with st.expander("Telemetry Filters", expanded=False):
        # 1. Mode filter
        avail_modes = sorted(list(df_diag_all["mode"].dropna().unique()))
        sel_modes = st.multiselect(
            "Transit Mode",
            avail_modes,
            default=avail_modes,
            help="Filter by transit mode: direct bus, rail, or multimodal combinations.",
        )

        # 2. Time Window filter
        avail_windows = ["Morning Peak", "Midday", "Evening Peak", "Night"]
        sel_windows = st.multiselect(
            "Time Window (IST)",
            avail_windows,
            default=avail_windows,
            help="Morning Peak: 07-10 | Midday: 11-15 | Evening Peak: 16-20 | Night: 21-06",
        )

        # 3. Day Type filter
        sel_daytypes = st.multiselect(
            "Day Type",
            ["Weekday", "Weekend"],
            default=["Weekday", "Weekend"],
            help="Filter corridor activity by weekday or weekend travel patterns.",
        )

        # 4. Confidence filter
        avail_confs = ["High", "Medium", "Low"]
        sel_confs = st.multiselect(
            "Evidence Confidence",
            avail_confs,
            default=avail_confs,
            help="High: sample size >= 100 with consistent gap | Medium: 30-99 | Low: 10-29",
        )

        # 5. Diagnostic Cause filter
        avail_causes = sorted(list(df_diag_all["primary_cause"].dropna().unique()))
        sel_causes = st.multiselect(
            "Diagnostic Cause",
            avail_causes,
            default=avail_causes,
            help="Root cause: fare friction, feeder gap, frequency desert, or missing direct connection.",
        )

        # 6. Minimum Searches filter
        min_searches = st.number_input(
            "Min Searches per Corridor Window",
            min_value=10,
            max_value=5000,
            value=10,
            step=10,
            help="Minimum search volume threshold per corridor in each time window (privacy minimum: 10).",
        )

    with st.expander("Methodology & Honesty", expanded=False):
        st.markdown("""
        - **Data Source:** Exclusively Chennai One operational search and ticketing logs (March 2026, 31 days).
        - **Intent vs Travel:** Searches represent digital commuter trip intent; tickets serve as verified digital booking proxies.
        - **No Physical Counts:** Contains zero bus occupancy, GPS fleet dispatch, or physical conductor ticket records.
        - **Standard Route Tariff Rule:** Premium routes (543001, 546001, 918001, 598001) are modeled at parallel standard route tariffs (e.g. ₹60 standard instead of ₹82 premium).
        - **Scenario Capture:** Illustrative 35% conversion of lost demand across actionable corridors.
        - **Sibling Co-Search:** Corridors with >50% parallel co-searches carry ₹0 revenue upside (riders prefer standard route).
        - **Privacy Protection:** Corridors with fewer than 10 unique searchers are suppressed.
        - **Map Coordinates:** Centroid references marked `coord_source=manual_approx`.
        """)


# ==============================================================================
# 5. APPLY GLOBAL FILTERING ACROSS DATASETS
# ==============================================================================
# Direct corridors filtering (diagnosis)
diag_filtered = df_diag_all[
    (df_diag_all["corridor_type"] == "DIRECT")
    & (df_diag_all["mode"].isin(sel_modes))
    & (df_diag_all["time_bucket"].isin(sel_windows))
    & (df_diag_all["confidence"].isin(sel_confs))
    & (df_diag_all["primary_cause"].isin(sel_causes))
    & (df_diag_all["searches"] >= min_searches)
].copy()

if "unique_searchers" in diag_filtered.columns:
    diag_filtered = diag_filtered[diag_filtered["unique_searchers"] >= 10]

# Actions filtering (actions)
act_filtered = df_act_all[
    (df_act_all["corridor_type"] == "DIRECT")
    & (df_act_all["time_window"] != "All")
    & (df_act_all["mode"].isin(sel_modes))
    & (df_act_all["time_window"].isin(sel_windows))
    & (df_act_all["confidence"].isin(sel_confs))
    & (df_act_all["diagnostic_cause"].isin(sel_causes))
    & (df_act_all["searches"] >= min_searches)
].copy()


# ==============================================================================
# MAIN WORKBENCH CONTENT
# ==============================================================================
if is_sample_mode:
    st.info(
        "Live demo on a small aggregated sample of March 2026 Chennai One data. Full-data results are in the README."
    )
if "Dashboard" in selected_view:
    t_start = time.perf_counter()
    try:
        # Breadcrumb and Header
        st.markdown(
            """
        <div class="app-header">
            <div class="breadcrumb">Home / Dashboard</div>
            <h1>CUMTA TransitPulse</h1>
            <div class="app-subtitle">Evidence-based transit planning: Transforming commuter search intent into targeted supply decisions across Chennai.</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

        # Headline Funnel Counts
        tot_searches = int(kpi["total_single_searches"])
        tot_tickets = int(kpi["total_single_tickets"])
        tot_lost = int(kpi["single_lost"])
        conv_pct = float(kpi["single_conversion"]) * 100
        unmet_pct = (tot_lost / tot_searches) * 100

        # Actionable Corridors & Single Source of Truth Revenue
        act_prescriptive = act_filtered[act_filtered["is_actionable"]]
        act_investigate = act_filtered[~act_filtered["is_actionable"]]

        # Actionable High+Medium count per rule 4e
        act_hm = act_prescriptive[act_prescriptive["confidence"].isin(["High", "Medium"])]
        inv_hm = act_investigate[act_investigate["confidence"].isin(["High", "Medium"])]

        rev_high = compute_revenue(
            act_prescriptive[act_prescriptive["confidence"] == "High"], prem_lookup=prem_lookup
        )
        rev_hm = compute_revenue(act_hm, prem_lookup=prem_lookup)
        rev_all = compute_revenue(act_prescriptive, prem_lookup=prem_lookup)

        # 3 columns x 2 rows KPI cards grid
        row1_c1, row1_c2, row1_c3 = st.columns(3)
        row2_c1, row2_c2, row2_c3 = st.columns(3)

        with row1_c1:
            st.markdown(
                f"""
            <div class="kpi-card" title="Single-leg direct trip searches initiated by commuters in Chennai One.">
                <div class="kpi-caption">Searches (Intent)</div>
                <div class="kpi-value metric-accent">{tot_searches / 1e6:.2f}M</div>
                <div class="kpi-sub">Exact: {tot_searches:,} single-leg searches</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with row1_c2:
            st.markdown(
                f"""
            <div class="kpi-card" title="Completed in-app ticket bookings matched to searches via journey_id.">
                <div class="kpi-caption">Matched Tickets</div>
                <div class="kpi-value metric-good">{tot_tickets / 1e6:.2f}M</div>
                <div class="kpi-sub">Exact: {tot_tickets:,} (conversion: {conv_pct:.1f}%)</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with row1_c3:
            st.markdown(
                f"""
            <div class="kpi-card" title="Searches without ticket bookings representing unmet commuter demand or checkout friction.">
                <div class="kpi-caption">Lost Searches</div>
                <div class="kpi-value metric-problem">{tot_lost / 1e6:.2f}M</div>
                <div class="kpi-sub">Exact: {tot_lost:,} (unmet gap: {unmet_pct:.1f}%)</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with row2_c1:
            st.markdown(
                f"""
            <div class="kpi-card" title="Prioritized High and Medium confidence corridor-windows with prescriptive transit interventions.">
                <div class="kpi-caption">Actionable Corridors</div>
                <div class="kpi-value metric-primary">{len(act_hm):,}</div>
                <div class="kpi-sub">High+Med actionable ({len(inv_hm):,} needing diagnosis)</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with row2_c2:
            st.markdown(
                f"""
            <div class="kpi-card" title="Recoverable monthly revenue at 35% capture modeled at parallel standard route tariffs.">
                <div class="kpi-caption">Revenue Upside (High+Med)</div>
                <div class="kpi-value metric-good">₹{rev_hm / 1e5:.1f} L/mo</div>
                <div class="kpi-sub">High alone: ₹{rev_high / 1e5:.1f} L/mo (at standard tariff)</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with row2_c3:
            st.markdown(
                f"""
            <div class="kpi-card" title="Upper bound monthly revenue projection including Low confidence corridors at 35% capture.">
                <div class="kpi-caption">Upper Bound (All Confidence)</div>
                <div class="kpi-value metric-primary">₹{rev_all / 1e5:.1f} L/mo</div>
                <div class="kpi-sub">Includes Low confidence; 35% capture scenario</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with st.expander("📘 How to Read These Metrics", expanded=False):
            st.markdown("""
            - **Searches (Intent):** Commuters who queried a direct origin-destination path. Not proof of travel.
            - **Matched Tickets (Proxy):** Confirmed in-app ticket bookings matched to a preceding search by `journey_id`.
            - **Lost Searches:** Unfulfilled searches where commuter did not buy a ticket (unmet demand or fare friction).
            - **Actionable Corridors:** Corridors with specific bus-network prescriptions (mini-bus loops, express cut-services, fare tier reviews).
            - **Revenue Upside:** Estimated monthly revenue from converting 35% of lost searchers at parallel standard route fares.
            """)

        with st.expander("🚀 Start Here — Quick User Guide", expanded=False):
            st.markdown("""
            1. **Dashboard:** Explore diurnal system dynamics, bottleneck corridors, and hourly ticket conversion.
            2. **Chennai Map:** Visualize spatial desire lines across core interchange hubs and identify regional demand gaps.
            3. **Diagnose:** Deep-dive into specific corridor dossiers, parallel standard route co-search shares, and hourly profiles.
            4. **Actions:** Simulate revenue upside under custom capture rates and download decision-ready depot CSV action plans.
            5. **Data & Replay:** Replay historical March 2026 telemetry hour-by-hour or sanitize raw operational CSV uploads.
            """)

        st.caption(
            "Data Matching Footnote: Excludes 7,406 ticketed journeys with no preceding search record per standard transit funnel rules. "
            "Reported single-leg searches reflect 4,177,536 of 4,178,481 raw searches (945 circular/test records pruned). "
            "Multi-leg transfer searches reflect 267,941 of 267,951 raw searches (10 circular records pruned)."
        )

        st.markdown("---")

        # ----------------------------------------------------------------------
        # Hourly Diurnal Multi-Series Chart
        # ----------------------------------------------------------------------
        st.subheader("Hourly Diurnal Demand & Ticket Conversion Profile")

        h_df = df_city_h.sort_values("hour").copy()
        h_df["lost_demand"] = h_df["searches"] - h_df["tickets"]
        h_df["conversion_pct"] = h_df["conversion"] * 100.0

        fig_hourly = make_subplots(specs=[[{"secondary_y": True}]])

        # 1. Lost Searches (bars at 40% opacity)
        fig_hourly.add_trace(
            go.Bar(
                x=h_df["hour"],
                y=h_df["lost_demand"],
                name="Lost Searches (Unmet Demand)",
                marker_color="rgba(220, 38, 38, 0.40)",
                marker_line=dict(width=1, color="#DC2626"),
                hoverinfo="x+y+name",
            ),
            secondary_y=False,
        )

        # 2. Searches (solid blue line with circle markers)
        fig_hourly.add_trace(
            go.Scatter(
                x=h_df["hour"],
                y=h_df["searches"],
                name="Searches (Commuter Intent)",
                mode="lines+markers",
                line=dict(color=COLOR_SEARCHES, width=2.5),
                marker=dict(size=6, symbol="circle"),
                hoverinfo="x+y+name",
            ),
            secondary_y=False,
        )

        # 3. Tickets (solid green line with square markers)
        fig_hourly.add_trace(
            go.Scatter(
                x=h_df["hour"],
                y=h_df["tickets"],
                name="Tickets (Matched Booking Proxy)",
                mode="lines+markers",
                line=dict(color=COLOR_TICKETS, width=2.5),
                marker=dict(size=6, symbol="square"),
                hoverinfo="x+y+name",
            ),
            secondary_y=False,
        )

        # 4. Conversion Rate (dashed amber line with diamond markers on secondary y-axis)
        fig_hourly.add_trace(
            go.Scatter(
                x=h_df["hour"],
                y=h_df["conversion_pct"],
                name="Conversion Rate % (Right Axis)",
                mode="lines+markers",
                line=dict(color=COLOR_CONVERSION, width=2.0, dash="dash"),
                marker=dict(size=6, symbol="diamond"),
                hoverinfo="x+y+name",
            ),
            secondary_y=True,
        )

        # Shaded Peak Bands
        fig_hourly.add_vrect(
            x0=7,
            x1=10,
            fillcolor="rgba(217, 119, 6, 0.10)",
            layer="below",
            line_width=1,
            line_dash="dot",
            line_color="rgba(217, 119, 6, 0.40)",
            annotation_text="Morning Peak (07-10)",
            annotation_position="top left",
            annotation_font=dict(size=11, color=COLOR_MUTED, family=FONT_FAMILY),
        )

        fig_hourly.add_vrect(
            x0=16,
            x1=20,
            fillcolor="rgba(220, 38, 38, 0.10)",
            layer="below",
            line_width=1,
            line_dash="dot",
            line_color="rgba(220, 38, 38, 0.40)",
            annotation_text="Evening Peak (16-20)",
            annotation_position="top left",
            annotation_font=dict(size=11, color=COLOR_MUTED, family=FONT_FAMILY),
        )

        fig_hourly.update_xaxes(
            title_text="Hour of Day (IST)", dtick=1, range=[-0.5, 23.5], gridcolor=COLOR_BORDER
        )
        fig_hourly.update_yaxes(
            title_text="Search & Ticket Volume", gridcolor=COLOR_BORDER, secondary_y=False
        )
        fig_hourly.update_yaxes(
            title_text="Search-to-Ticket Conversion (%)",
            range=[0, 100],
            dtick=20,
            showgrid=False,
            secondary_y=True,
        )

        apply_clean_layout(fig_hourly, height=380, legend_y=-0.22)
        st.plotly_chart(fig_hourly, use_container_width=True, config={"displayModeBar": False})

        st.markdown("---")

        # ----------------------------------------------------------------------
        # Day-of-Week × Hour Heatmap & Mode Split
        # ----------------------------------------------------------------------
        col_c1, col_c2 = st.columns([1, 1])

        with col_c1:
            st.subheader("Day-of-Week vs Hour Pattern")
            heat_metric = st.selectbox(
                "Heatmap Telemetry Metric",
                ["Searches (Intent)", "Tickets (Proxy)", "Conversion Rate (%)"],
                index=0,
                key="heat_metric_selector",
                help="Select metric to display across days of week and hours of day.",
            )

            if not df_city_dow.empty:
                dow_order = [
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                    "Saturday",
                    "Sunday",
                ]
                df_dow_work = df_city_dow.copy()
                if sel_modes:
                    df_dow_work = df_dow_work[df_dow_work["mode"].isin(sel_modes)]

                dow_grp = (
                    df_dow_work.groupby(["day_of_week", "hour"])
                    .agg({"searches": "sum", "tickets": "sum"})
                    .reset_index()
                )
                dow_grp["conversion_pct"] = np.where(
                    dow_grp["searches"] > 0, (dow_grp["tickets"] / dow_grp["searches"]) * 100.0, 0.0
                )
                dow_grp["day_of_week"] = pd.Categorical(
                    dow_grp["day_of_week"], categories=dow_order, ordered=True
                )

                if "Searches" in heat_metric:
                    val_col = "searches"
                    cscale = "Blues"
                    cap_text = (
                        "Concentration of commuter digital search intent by hour and weekday."
                    )
                elif "Tickets" in heat_metric:
                    val_col = "tickets"
                    cscale = "Blues"
                    cap_text = "Concentration of ticketed bookings by hour and weekday."
                else:
                    val_col = "conversion_pct"
                    cscale = "RdYlGn"
                    cap_text = "Conversion rate percentage (%) by hour and weekday."

                heat_pivot = dow_grp.pivot(
                    index="day_of_week", columns="hour", values=val_col
                ).reindex(dow_order)

                st.caption(cap_text)
                fig_heat = px.imshow(
                    heat_pivot,
                    labels=dict(x="Hour of Day", y="Day of Week", color="Value"),
                    x=list(range(24)),
                    y=dow_order,
                    color_continuous_scale=cscale,
                    aspect="auto",
                )
                fig_heat.update_xaxes(dtick=2, tickangle=0, gridcolor=COLOR_BORDER)
                fig_heat.update_yaxes(gridcolor=COLOR_BORDER)
                apply_clean_layout(fig_heat, height=340)
                st.plotly_chart(
                    fig_heat, use_container_width=True, config={"displayModeBar": False}
                )

        with col_c2:
            st.subheader("Transit Mode Volume Distribution")
            mode_summary = (
                df_diag_all.groupby("mode")[["searches", "lost_trips"]].sum().reset_index()
            )
            mode_summary["tickets"] = mode_summary["searches"] - mode_summary["lost_trips"]
            mode_summary = mode_summary.sort_values("searches", ascending=False)

            st.caption("Volume comparison across direct and multimodal feeder legs in Millions.")
            fig_mode = go.Figure()
            fig_mode.add_trace(
                go.Bar(
                    x=mode_summary["mode"],
                    y=mode_summary["searches"] / 1e6,
                    name="Searches",
                    marker_color=COLOR_SEARCHES,
                )
            )
            fig_mode.add_trace(
                go.Bar(
                    x=mode_summary["mode"],
                    y=mode_summary["tickets"] / 1e6,
                    name="Tickets",
                    marker_color=COLOR_TICKETS,
                )
            )
            fig_mode.add_trace(
                go.Bar(
                    x=mode_summary["mode"],
                    y=mode_summary["lost_trips"] / 1e6,
                    name="Lost Searches",
                    marker_color=COLOR_LOST,
                )
            )

            fig_mode.update_layout(barmode="group")
            fig_mode.update_xaxes(title_text="Transit Mode", gridcolor=COLOR_BORDER)
            fig_mode.update_yaxes(title_text="Volume (Millions)", gridcolor=COLOR_BORDER)
            apply_clean_layout(fig_mode, height=340, legend_y=-0.22)
            st.plotly_chart(fig_mode, use_container_width=True, config={"displayModeBar": False})

        st.markdown("---")

        # ----------------------------------------------------------------------
        # Top 15 Bottleneck Corridors Table & CSV Export Button
        # ----------------------------------------------------------------------
        t_col1, t_col2 = st.columns([3, 1])
        with t_col1:
            st.subheader("Top Corridors by Unmet Demand Gap")
            st.caption(
                "Prioritized single-leg corridors sorted by total lost trips during March 2026."
            )

        top_corridors = diag_filtered.sort_values("lost_trips", ascending=False).head(15).copy()
        top_corridors["corridor"] = top_corridors["from_name"] + " → " + top_corridors["to_name"]
        top_corridors["tickets"] = top_corridors["searches"] - top_corridors["lost_trips"]
        top_corridors["conversion_pct"] = top_corridors["srcr"] * 100.0

        disp_top = top_corridors[
            [
                "corridor",
                "mode",
                "time_bucket",
                "searches",
                "tickets",
                "lost_trips",
                "conversion_pct",
                "conversion_percentile",
                "avg_price",
                "confidence",
                "primary_cause",
            ]
        ].copy()

        csv_top = disp_top.to_csv(index=False).encode("utf-8")
        with t_col2:
            st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)
            st.download_button(
                "📥 Download Table (CSV)",
                data=csv_top,
                file_name="cumta_top15_corridors.csv",
                mime="text/csv",
                help="Export this filtered table of top bottleneck corridors as CSV.",
            )

        st.dataframe(
            disp_top,
            column_config={
                "corridor": st.column_config.TextColumn("Corridor", width="large"),
                "mode": st.column_config.TextColumn("Mode", width="small"),
                "time_bucket": st.column_config.TextColumn("Time Window", width="medium"),
                "searches": st.column_config.NumberColumn("Searches", format="%,d"),
                "tickets": st.column_config.NumberColumn("Tickets", format="%,d"),
                "lost_trips": st.column_config.NumberColumn("Lost Searches", format="%,d"),
                "conversion_pct": st.column_config.NumberColumn("Conversion %", format="%.1f%%"),
                "conversion_percentile": st.column_config.NumberColumn("Peer %ile", format="%.0f"),
                "avg_price": st.column_config.NumberColumn("Fare (₹)", format="₹%.0f"),
                "confidence": st.column_config.TextColumn("Confidence", width="small"),
                "primary_cause": st.column_config.TextColumn("Diagnostic Cause", width="medium"),
            },
            use_container_width=True,
            hide_index=True,
        )

    except Exception as tab_err:
        st.error(
            "An unexpected error occurred while rendering the Dashboard. Please adjust filters or reload."
        )
        with st.expander("Technical Error Details"):
            st.code(str(tab_err))

    t_elapsed = time.perf_counter() - t_start
    print(f"[PERF] Tab 1 (Dashboard) rendered in {t_elapsed:.3f}s")


# ==============================================================================
# TAB 2: CHENNAI MAP
# ==============================================================================
elif "Chennai Map" in selected_view:
    t_start = time.perf_counter()
    try:
        st.markdown(
            """
        <div class="app-header">
            <div class="breadcrumb">Home / Chennai Map</div>
            <h1>Geospatial Desire Lines & Core Interchange Hubs</h1>
            <div class="app-subtitle">Visualizing major commuter desire lines connecting interchange hubs across the Chennai metropolitan area.</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

        col_m1, col_m2 = st.columns([1, 2])
        with col_m1:
            top_n_map = st.slider(
                "Top N Corridors by Lost Trips",
                min_value=10,
                max_value=100,
                value=30,
                step=5,
                help="Adjust the number of desire lines shown on the map.",
            )
        with col_m2:
            map_window = st.selectbox(
                "Time Window Filter",
                ["All Windows"] + avail_windows,
                index=0,
                help="Filter desire lines by specific operational time window.",
            )

        # Filter corridors for map
        map_base = df_diag_all[df_diag_all["corridor_type"] == "DIRECT"].copy()
        if map_window != "All Windows":
            map_base = map_base[map_base["time_bucket"] == map_window]
        if sel_modes:
            map_base = map_base[map_base["mode"].isin(sel_modes)]

        map_base["coord_from"] = map_base.apply(
            lambda r: match_coordinates(r["from_name"], r["from_hub"]), axis=1
        )
        map_base["coord_to"] = map_base.apply(
            lambda r: match_coordinates(r["to_name"], r["to_hub"]), axis=1
        )
        map_base["is_mapped"] = map_base["coord_from"].notnull() & map_base["coord_to"].notnull()

        mapped_df = (
            map_base[map_base["is_mapped"]].sort_values("lost_trips", ascending=False).copy()
        )
        unmapped_df = (
            map_base[~map_base["is_mapped"]].sort_values("lost_trips", ascending=False).copy()
        )

        total_s = map_base["searches"].sum()
        mapped_s = mapped_df["searches"].sum()
        cov_pct = (mapped_s / total_s * 100) if total_s > 0 else 0

        st.info(
            f"**Map Coverage:** {cov_pct:.1f}% of searches mapped ({mapped_s:,} of {total_s:,} searches). "
            "Showing desire lines for top mapped corridors based on approximate hub centroids in `data_reference/stop_coords.csv`. "
            "Unmapped corridors are safely excluded from the visual plot and listed in the reference table below."
        )

        top_map_rows = mapped_df.head(top_n_map).copy()
        if not top_map_rows.empty:
            q25 = top_map_rows["srcr"].quantile(0.25)
            q75 = top_map_rows["srcr"].quantile(0.75)
            med_searches = top_map_rows["searches"].median()

            def assign_map_color(row):
                if row["srcr"] <= q25 and row["lost_trips"] > 0:
                    return ("#DC2626", "Bottom-Quartile Conv (High Lost)")
                elif row["srcr"] >= q75:
                    return ("#16A34A", "Top-Quartile Conv (Healthy)")
                elif row["searches"] < med_searches and row["srcr"] > 0.40:
                    return ("#2563EB", "High Tickets / Low Searches")
                else:
                    return ("#D97706", "Transfer / Feeder Gap")

            top_map_rows["color_tuple"] = top_map_rows.apply(assign_map_color, axis=1)
            top_map_rows["line_color"] = top_map_rows["color_tuple"].apply(lambda x: x[0])
            top_map_rows["category"] = top_map_rows["color_tuple"].apply(lambda x: x[1])

            fig_map = go.Figure()

            # Outline trace beneath for crisp contrast
            outline_lats, outline_lons = [], []
            for _, r in top_map_rows.iterrows():
                cf, ct = r["coord_from"], r["coord_to"]
                outline_lats.extend([cf[0], ct[0], None])
                outline_lons.extend([cf[1], ct[1], None])

            trace_cls = getattr(go, "Scattermap", getattr(go, "Scattermapbox", None))

            fig_map.add_trace(
                trace_cls(
                    mode="lines",
                    lat=outline_lats,
                    lon=outline_lons,
                    line=dict(width=5.0, color="#1F2937"),
                    hoverinfo="none",
                    showlegend=False,
                )
            )

            # Colored Desire Lines
            for cat_name, cat_color in [
                ("Bottom-Quartile Conv (High Lost)", "#DC2626"),
                ("Transfer / Feeder Gap", "#D97706"),
                ("Top-Quartile Conv (Healthy)", "#16A34A"),
                ("High Tickets / Low Searches", "#2563EB"),
            ]:
                cat_rows = top_map_rows[top_map_rows["category"] == cat_name]
                if cat_rows.empty:
                    continue

                lats, lons, hover_texts = [], [], []
                for _, r in cat_rows.iterrows():
                    cf, ct = r["coord_from"], r["coord_to"]
                    lats.extend([cf[0], ct[0], None])
                    lons.extend([cf[1], ct[1], None])
                    ht = (
                        f"<b>{r['from_name']} → {r['to_name']}</b><br>"
                        f"Time Window: {r['time_bucket']}<br>"
                        f"Searches: {int(r['searches']):,} | Lost: {int(r['lost_trips']):,}<br>"
                        f"Conversion: {r['srcr'] * 100:.1f}% (Peer %ile: {r['conversion_percentile']:.0f})<br>"
                        f"Route: {r['primary_route_id']} | Cause: {r['primary_cause']}<br>"
                        f"Confidence: {r['confidence']}"
                    )
                    hover_texts.extend([ht, ht, None])

                fig_map.add_trace(
                    trace_cls(
                        mode="lines",
                        lat=lats,
                        lon=lons,
                        line=dict(width=3.2, color=cat_color),
                        name=cat_name,
                        hoverinfo="text",
                        text=hover_texts,
                    )
                )

            # Interchange Hubs: show labels only for top 12 hubs by searches
            hub_coords_used = {}
            for _, r in top_map_rows.iterrows():
                hub_coords_used[r["from_name"]] = (r["coord_from"], r["searches"])
                hub_coords_used[r["to_name"]] = (r["coord_to"], r["searches"])

            sorted_hubs = sorted(hub_coords_used.items(), key=lambda x: x[1][1], reverse=True)
            top12_hub_names = set([x[0] for x in sorted_hubs[:12]])

            h_lats = [v[0][0] for v in hub_coords_used.values()]
            h_lons = [v[0][1] for v in hub_coords_used.values()]
            h_names = list(hub_coords_used.keys())
            h_text_labels = [n if n in top12_hub_names else "" for n in h_names]
            h_sizes = [min(20, max(8, math.sqrt(v[1]) * 0.38)) for v in hub_coords_used.values()]

            fig_map.add_trace(
                trace_cls(
                    mode="markers+text",
                    lat=h_lats,
                    lon=h_lons,
                    marker=dict(size=h_sizes, color="#1F2937", opacity=0.90),
                    text=h_text_labels,
                    textposition="top center",
                    textfont=dict(size=11, family=FONT_FAMILY, color="#1F2937"),
                    name="Interchange Hubs",
                    hoverinfo="text",
                    hovertext=[f"<b>Interchange Hub:</b> {n}" for n in h_names],
                )
            )

            map_layout_args = dict(
                center=dict(lat=13.0450, lon=80.2200), zoom=10, style="carto-positron"
            )
            if hasattr(go, "Scattermap"):
                fig_map.update_layout(map=map_layout_args)
            else:
                fig_map.update_layout(mapbox=map_layout_args)

            fig_map.update_layout(
                height=620,
                margin=dict(l=0, r=0, t=10, b=50),
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.08,
                    xanchor="center",
                    x=0.5,
                    font=dict(family=FONT_FAMILY, color=COLOR_TEXT, size=11),
                    bgcolor="rgba(255,255,255,0.9)",
                    bordercolor=COLOR_BORDER,
                    borderwidth=1,
                ),
            )
            try:
                st.plotly_chart(fig_map, use_container_width=True, config={"displayModeBar": False})
            except Exception:
                st.info(
                    "ℹ️ Interactive map tiles unavailable or network restricted. Tabular corridor metrics are presented below."
                )
        else:
            st.warning("No mapped desire lines match the selected mode or time window criteria.")

        # Corridor selector to jump to Tab 3
        st.markdown("##### Inspect Corridor from Map")
        sel_corridor_opts = [
            f"{r['from_name']} → {r['to_name']} ({r['time_bucket']})"
            for _, r in top_map_rows.iterrows()
        ]
        if sel_corridor_opts:
            chosen_corr = st.selectbox(
                "Select Corridor to Inspect in Tab 3", sel_corridor_opts, key="map_corr_picker"
            )
            if st.button("Inspect in Tab 3 Diagnose"):
                st.session_state.selected_corridor = chosen_corr
                st.success(
                    f"Selected `{chosen_corr}`. Switch to Tab 3 to view full diagnostic dossier."
                )

        # Unmapped Corridors Reference Table with CSV download button
        with st.expander(f"Unmapped Corridors Reference Table ({len(unmapped_df):,} corridors)"):
            st.caption(
                "Corridors with stop identifiers not yet mapped to approximate centroids. Displayed in tabular form:"
            )
            unm_disp = unmapped_df[
                [
                    "from_name",
                    "to_name",
                    "time_bucket",
                    "mode",
                    "searches",
                    "lost_trips",
                    "srcr",
                    "primary_cause",
                ]
            ].copy()
            unm_disp["conversion_pct"] = unm_disp["srcr"] * 100.0

            csv_unm = unm_disp.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Download Unmapped Corridors (CSV)",
                data=csv_unm,
                file_name="cumta_unmapped_corridors.csv",
                mime="text/csv",
                help="Export table of unmapped corridors as CSV.",
            )

            st.dataframe(
                unm_disp[
                    [
                        "from_name",
                        "to_name",
                        "time_bucket",
                        "mode",
                        "searches",
                        "lost_trips",
                        "conversion_pct",
                        "primary_cause",
                    ]
                ].head(50),
                column_config={
                    "from_name": st.column_config.TextColumn("Origin"),
                    "to_name": st.column_config.TextColumn("Destination"),
                    "time_bucket": st.column_config.TextColumn("Time Window"),
                    "mode": st.column_config.TextColumn("Mode"),
                    "searches": st.column_config.NumberColumn("Searches", format="%,d"),
                    "lost_trips": st.column_config.NumberColumn("Lost Searches", format="%,d"),
                    "conversion_pct": st.column_config.NumberColumn(
                        "Conversion %", format="%.1f%%"
                    ),
                    "primary_cause": st.column_config.TextColumn("Diagnostic Cause"),
                },
                use_container_width=True,
                hide_index=True,
            )

    except Exception as tab_err:
        st.error(
            "An unexpected error occurred while rendering the Chennai Map. Please adjust filters or reload."
        )
        with st.expander("Technical Error Details"):
            st.code(str(tab_err))

    t_elapsed = time.perf_counter() - t_start
    print(f"[PERF] Tab 2 (Map) rendered in {t_elapsed:.3f}s")


# ==============================================================================
# TAB 3: DIAGNOSE
# ==============================================================================
elif "Diagnose" in selected_view:
    t_start = time.perf_counter()
    try:
        st.markdown(
            """
        <div class="app-header">
            <div class="breadcrumb">Home / Diagnose</div>
            <h1>Corridor Deep-Dive Diagnosis & Telemetry Evidence</h1>
            <div class="app-subtitle">Granular performance diagnosis, parallel standard service competition, and hourly route profiles.</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

        all_corridor_labels = [
            f"{r['from_name']} → {r['to_name']} ({r['time_bucket']})"
            for _, r in df_diag_all.sort_values("lost_trips", ascending=False).iterrows()
        ]

        default_c_idx = 0
        if st.session_state.selected_corridor in all_corridor_labels:
            default_c_idx = all_corridor_labels.index(st.session_state.selected_corridor)

        selected_c_label = st.selectbox(
            "Pick Corridor-Window to Diagnose",
            all_corridor_labels,
            index=default_c_idx,
            help="Select any corridor across Chennai to inspect telemetry evidence.",
        )

        c_row = df_diag_all[
            (
                df_diag_all["from_name"]
                + " → "
                + df_diag_all["to_name"]
                + " ("
                + df_diag_all["time_bucket"]
                + ")"
            )
            == selected_c_label
        ].iloc[0]

        c_act = df_act_all[
            (df_act_all["from_station_id"] == c_row["from_station_id"])
            & (df_act_all["to_station_id"] == c_row["to_station_id"])
            & (df_act_all["time_window"] == c_row["time_bucket"])
        ]
        rec_action = (
            c_act.iloc[0]["recommended_action"]
            if not c_act.empty
            else "Qualitative commuter survey recommended."
        )

        # Standard vs Premium Fare details per rule 4b
        is_prem_route = str(c_row["primary_route_id"]) in PREMIUM_ROUTES
        std_fare = get_standard_fare(c_row, prem_lookup)
        prem_fare = float(c_row.get("avg_price", 82.0)) if is_prem_route else None

        # Diagnostic Metrics Row
        d_col1, d_col2, d_col3, d_col4, d_col5 = st.columns(5)
        with d_col1:
            st.markdown(
                f"""
            <div class="kpi-card" title="Total digital trip searches initiated on this corridor window.">
                <div class="kpi-caption">Searches (Intent)</div>
                <div class="kpi-value metric-accent">{int(c_row["searches"]):,}</div>
                <div class="kpi-sub">Mode: {c_row["mode"]}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )
        with d_col2:
            st.markdown(
                f"""
            <div class="kpi-card" title="Confirmed ticket bookings for this corridor.">
                <div class="kpi-caption">Tickets Booked</div>
                <div class="kpi-value metric-good">{int(c_row["searches"] - c_row["lost_trips"]):,}</div>
                <div class="kpi-sub">Proxy for travel</div>
            </div>
            """,
                unsafe_allow_html=True,
            )
        with d_col3:
            st.markdown(
                f"""
            <div class="kpi-card" title="Searches that ended without an in-app ticket purchase.">
                <div class="kpi-caption">Lost Searches (Gap)</div>
                <div class="kpi-value metric-problem">{int(c_row["lost_trips"]):,}</div>
                <div class="kpi-sub">Unmet demand</div>
            </div>
            """,
                unsafe_allow_html=True,
            )
        with d_col4:
            st.markdown(
                f"""
            <div class="kpi-card" title="Search to ticket conversion rate relative to peer corridors.">
                <div class="kpi-caption">Conversion Rate</div>
                <div class="kpi-value metric-primary">{c_row["srcr"] * 100:.1f}%</div>
                <div class="kpi-sub">Peer %ile: {c_row["conversion_percentile"]:.0f}%</div>
            </div>
            """,
                unsafe_allow_html=True,
            )
        with d_col5:
            fare_sub = f"Standard Fare: ₹{std_fare:.0f}" + (
                f" (Premium: ₹{prem_fare:.0f})" if prem_fare else ""
            )
            st.markdown(
                f"""
            <div class="kpi-card" title="Tariff applied for service modeling and revenue projection.">
                <div class="kpi-caption">Standard Tariff</div>
                <div class="kpi-value metric-primary">₹{std_fare:.0f}</div>
                <div class="kpi-sub">{fare_sub}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        st.markdown("<br>", unsafe_allow_html=True)

        # Diagnostic Evidence Card
        cause_clean = str(c_row["primary_cause"]).replace("_", " ").title()
        sib_share = float(c_row.get("sibling_share", 0.0)) * 100

        fare_detail_str = f"Standard Route Tariff: ₹{std_fare:.0f}"
        if is_prem_route and prem_fare:
            fare_detail_str += f" | Premium Tariff: ₹{prem_fare:.0f} (Revenue modeled at standard route tariff per rule 4b)"

        st.markdown(
            f"""
        <div class="clean-card">
            <h3 style="margin-top:0;">Diagnostic Finding: {cause_clean} (Confidence: {c_row["confidence"]})</h3>
            <p style="margin-bottom:6px;"><strong>Corridor:</strong> {c_row["from_name"]} → {c_row["to_name"]} (Route {c_row["primary_route_id"]})</p>
            <p style="margin-bottom:6px;"><strong>Observed Telemetry Evidence:</strong> {c_row["evidence"]}</p>
            <p style="margin-bottom:6px;"><strong>Sibling Route Co-Search Share:</strong> {sib_share:.2f}% of riders co-searched the parallel standard route in the same minute.</p>
            <p style="margin-bottom:6px;"><strong>Tariff Structure:</strong> {fare_detail_str}</p>
            <p style="margin-bottom:0;"><strong>Recommended Intervention:</strong> {rec_action}</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

        # Route Hourly Profile
        st.subheader(f"Hourly Profile for Primary Route {c_row['primary_route_id']}")
        if not df_route_h.empty:
            r_hourly = df_route_h[
                df_route_h["route_id"] == str(c_row["primary_route_id"])
            ].sort_values("hour")
            if not r_hourly.empty:
                fig_r = go.Figure()
                fig_r.add_trace(
                    go.Bar(
                        x=r_hourly["hour"],
                        y=r_hourly["searches"],
                        name="Route Searches",
                        marker_color=COLOR_SEARCHES,
                    )
                )
                fig_r.add_trace(
                    go.Bar(
                        x=r_hourly["hour"],
                        y=r_hourly["tickets"],
                        name="Route Tickets",
                        marker_color=COLOR_TICKETS,
                    )
                )
                fig_r.update_layout(barmode="group")
                fig_r.update_xaxes(title_text="Hour of Day", dtick=1, gridcolor=COLOR_BORDER)
                fig_r.update_yaxes(title_text="Passenger Volume", gridcolor=COLOR_BORDER)
                apply_clean_layout(fig_r, height=320, legend_y=-0.25)
                st.plotly_chart(fig_r, use_container_width=True, config={"displayModeBar": False})
            else:
                st.info(
                    f"No isolated hourly route profile found for route {c_row['primary_route_id']}; corridor telemetry reflects aggregate OD traffic."
                )

    except Exception as tab_err:
        st.error(
            "An unexpected error occurred while rendering the Diagnose section. Please adjust filters or reload."
        )
        with st.expander("Technical Error Details"):
            st.code(str(tab_err))

    t_elapsed = time.perf_counter() - t_start
    print(f"[PERF] Tab 3 (Diagnose) rendered in {t_elapsed:.3f}s")


# ==============================================================================
# TAB 4: ACTIONS AND SIMULATOR
# ==============================================================================
elif "Actions" in selected_view:
    t_start = time.perf_counter()
    try:
        st.markdown(
            """
        <div class="app-header">
            <div class="breadcrumb">Home / Actions</div>
            <h1>Prioritized Depot Action Plan & Interactive Revenue Simulator</h1>
            <div class="app-subtitle">Data-driven transit supply interventions, fleet allocation recommendations, and scenario revenue modeling.</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

        col_act1, col_act2 = st.columns([1, 1])
        with col_act1:
            include_low = st.checkbox(
                "Include Low Confidence Corridors in Plan",
                value=False,
                help="Expand actionable set to include low sample-size corridors.",
            )
        with col_act2:
            filter_mode_act = st.selectbox(
                "Action Mode Filter",
                ["All Modes"] + avail_modes,
                index=0,
                help="Filter action recommendations by specific transit mode.",
            )

        # Revenue Simulator Controls
        st.markdown("#### Interactive Revenue & Rider Simulator")
        sim_col1, sim_col2 = st.columns([1, 1])
        with sim_col1:
            capture_slider = st.slider(
                "Target Capture Rate of Leaking Searches (%)",
                min_value=5,
                max_value=60,
                value=35,
                step=5,
                help="Assumed percentage of lost searchers converted into ticketed riders upon service improvement.",
            )
        with sim_col2:
            days_per_month = st.number_input(
                "Operating Days per Month",
                min_value=20,
                max_value=31,
                value=30,
                help="Operating calendar days per month used for monthly revenue projections.",
            )

        # Filter base actions table
        act_table_base = act_filtered.copy()
        if not include_low:
            act_table_base = act_table_base[act_table_base["confidence"].isin(["High", "Medium"])]
        if filter_mode_act != "All Modes":
            act_table_base = act_table_base[act_table_base["mode"] == filter_mode_act]

        # Recompute row metrics using shared single source of truth function
        sim_df = compute_row_metrics(
            act_table_base,
            capture_rate=(capture_slider / 100.0),
            days=days_per_month,
            prem_lookup=prem_lookup,
        )

        # Separate prescriptive action plan vs qualitative diagnosis per rule 4e
        prescriptive_plan = (
            sim_df[sim_df["is_actionable"]].copy().sort_values("sim_rev_mo", ascending=False)
        )
        investigate_plan = sim_df[~sim_df["is_actionable"]].copy()

        tot_sim_riders = prescriptive_plan["sim_riders_day"].sum()
        tot_sim_rev = compute_revenue(
            prescriptive_plan,
            capture_rate=(capture_slider / 100.0),
            days=days_per_month,
            prem_lookup=prem_lookup,
        )

        st.info(
            f"**Simulation Outcome:** Capturing {capture_slider}% of leaking searches across these **{len(prescriptive_plan):,}** actionable corridor-windows "
            f"converts ~**{int(tot_sim_riders):,}** daily searchers into riders and adds ~**₹{tot_sim_rev:,.0f}/month** (~₹{tot_sim_rev / 1e5:.1f} Lakhs/mo) "
            f"modeled at standard route tariffs over {days_per_month} days. "
            f"({len(investigate_plan):,} corridors requiring qualitative station-area investigation are tracked separately below)."
        )

        # Action Plan Data Table with friendly headers and no raw names
        disp_act = prescriptive_plan[
            [
                "corridor_name",
                "primary_route_id",
                "time_window",
                "diagnostic_cause",
                "recommended_action",
                "sim_riders_day",
                "standard_fare",
                "sim_rev_mo",
                "confidence",
            ]
        ].copy()

        act_t1, act_t2 = st.columns([3, 1])
        with act_t1:
            st.markdown(f"##### Recommended Transit Interventions ({len(disp_act):,} corridors)")
        with act_t2:
            csv_plan = disp_act.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Download Action Plan (CSV)",
                data=csv_plan,
                file_name="cumta_depot_action_plan.csv",
                mime="text/csv",
                help="Export prioritized prescriptive transit action plan.",
            )

        st.dataframe(
            disp_act,
            column_config={
                "corridor_name": st.column_config.TextColumn("Corridor", width="large"),
                "primary_route_id": st.column_config.TextColumn("Primary Route", width="small"),
                "time_window": st.column_config.TextColumn("Time Window", width="medium"),
                "diagnostic_cause": st.column_config.TextColumn("Diagnostic Cause", width="medium"),
                "recommended_action": st.column_config.TextColumn(
                    "Recommended Action", width="large"
                ),
                "sim_riders_day": st.column_config.NumberColumn("Est. Daily Riders", format="%.1f"),
                "standard_fare": st.column_config.NumberColumn("Standard Tariff", format="₹%.0f"),
                "sim_rev_mo": st.column_config.NumberColumn(
                    "Monthly Revenue Upside", format="₹%,.0f"
                ),
                "confidence": st.column_config.TextColumn("Confidence", width="small"),
            },
            use_container_width=True,
            hide_index=True,
        )

        with st.expander(
            f"Corridors Needing Qualitative Diagnosis ({len(investigate_plan):,} corridors)"
        ):
            st.caption(
                "Corridors flagged for station-area, frequency, or transfer investigation (excluded from prescriptive fleet actions):"
            )
            disp_inv = investigate_plan[
                [
                    "corridor_name",
                    "primary_route_id",
                    "time_window",
                    "diagnostic_cause",
                    "recommended_action",
                    "confidence",
                ]
            ].copy()
            csv_inv = disp_inv.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 Download Investigation List (CSV)",
                data=csv_inv,
                file_name="cumta_investigation_corridors.csv",
                mime="text/csv",
                help="Export list of qualitative investigation corridors.",
            )
            st.dataframe(
                disp_inv,
                column_config={
                    "corridor_name": st.column_config.TextColumn("Corridor"),
                    "primary_route_id": st.column_config.TextColumn("Primary Route"),
                    "time_window": st.column_config.TextColumn("Time Window"),
                    "diagnostic_cause": st.column_config.TextColumn("Diagnostic Cause"),
                    "recommended_action": st.column_config.TextColumn("Investigation Directive"),
                    "confidence": st.column_config.TextColumn("Confidence"),
                },
                use_container_width=True,
                hide_index=True,
            )

        # ----------------------------------------------------------------------
        # CSV & ZIP Export Center
        # ----------------------------------------------------------------------
        st.markdown("#### Decision-Ready Data Exports")

        # 1. Depot Action Plan CSV (matches on-screen table)
        depot_plan_export = disp_act.copy()
        depot_plan_export["Assumptions"] = (
            f"Assumes {capture_slider}% capture rate over {days_per_month} days at standard route tariff"
        )
        csv_depot = depot_plan_export.to_csv(index=False).encode("utf-8")

        # 2. Filtered Corridors CSV
        diag_export = diag_filtered.copy()
        if "srcr" in diag_export.columns:
            diag_export["conversion_pct"] = diag_export["srcr"] * 100.0
        csv_corridors = diag_export.to_csv(index=False).encode("utf-8")

        # 3. Hourly Table CSV
        csv_hourly = df_city_h.to_csv(index=False).encode("utf-8")

        # 4. Transfer Pairs CSV
        csv_transfers = df_xfer_agg.to_csv(index=False).encode("utf-8")

        # 5. Map Data CSV
        map_export = diag_filtered.copy()
        map_export["from_lat_lon"] = map_export.apply(
            lambda r: match_coordinates(r["from_name"], r["from_hub"]), axis=1
        )
        map_export["to_lat_lon"] = map_export.apply(
            lambda r: match_coordinates(r["to_name"], r["to_hub"]), axis=1
        )
        map_export = map_export[
            map_export["from_lat_lon"].notnull() & map_export["to_lat_lon"].notnull()
        ].copy()
        if not map_export.empty:
            map_export["from_lat"] = map_export["from_lat_lon"].apply(lambda x: x[0])
            map_export["from_lon"] = map_export["from_lat_lon"].apply(lambda x: x[1])
            map_export["to_lat"] = map_export["to_lat_lon"].apply(lambda x: x[0])
            map_export["to_lon"] = map_export["to_lat_lon"].apply(lambda x: x[1])
            map_export = map_export.drop(columns=["from_lat_lon", "to_lat_lon"])
        csv_map = map_export.to_csv(index=False).encode("utf-8")

        # ZIP Bundle
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("depot_action_plan.csv", csv_depot)
            zf.writestr("filtered_corridors.csv", csv_corridors)
            zf.writestr("hourly_demand_gap.csv", csv_hourly)
            zf.writestr("transfer_pairs_aggregated.csv", csv_transfers)
            zf.writestr("map_desire_lines.csv", csv_map)
        zip_bytes = zip_buffer.getvalue()

        b_col1, b_col2, b_col3, b_col4, b_col5 = st.columns(5)
        with b_col1:
            st.download_button(
                "📥 Depot Action Plan",
                data=csv_depot,
                file_name="cumta_depot_action_plan.csv",
                mime="text/csv",
            )
        with b_col2:
            st.download_button(
                "📥 Filtered Corridors",
                data=csv_corridors,
                file_name="cumta_filtered_corridors.csv",
                mime="text/csv",
            )
        with b_col3:
            st.download_button(
                "📥 Hourly Table",
                data=csv_hourly,
                file_name="cumta_hourly_demand_gap.csv",
                mime="text/csv",
            )
        with b_col4:
            st.download_button(
                "📥 Transfer Pairs",
                data=csv_transfers,
                file_name="cumta_transfer_pairs_aggregated.csv",
                mime="text/csv",
            )
        with b_col5:
            st.download_button(
                "📦 Download All (ZIP)",
                data=zip_bytes,
                file_name="cumta_transitpulse_complete_data.zip",
                mime="application/zip",
            )

    except Exception as tab_err:
        st.error(
            "An unexpected error occurred while rendering the Actions section. Please adjust filters or reload."
        )
        with st.expander("Technical Error Details"):
            st.code(str(tab_err))

    t_elapsed = time.perf_counter() - t_start
    print(f"[PERF] Tab 4 (Actions) rendered in {t_elapsed:.3f}s")


# ==============================================================================
# TAB 5: DATA INPUT AND REPLAY
# ==============================================================================
elif "Data & Replay" in selected_view:
    t_start = time.perf_counter()
    try:
        st.markdown(
            """
        <div class="app-header">
            <div class="breadcrumb">Home / Data & Replay</div>
            <h1>Data Ingestion Pipeline & Telemetry Replay Engine</h1>
            <div class="app-subtitle">Step through March 2026 operational logs hour-by-hour or sanitize raw transaction batch files.</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

        mode_options = [
            "Local Folder Ingestion",
            "Raw CSV / XLSX Sanitizer",
            "Replay of March 2026 Data",
        ]
        mode_param = st.query_params.get("mode", "3")
        default_mode_idx = int(mode_param) - 1 if mode_param in ["1", "2", "3"] else 2
        selected_mode = st.radio(
            "Workbench Mode", mode_options, index=default_mode_idx, horizontal=True
        )

        if selected_mode == mode_options[0]:
            st.markdown("#### Mode 1: Ingest From Local Folder")
            folder_path = st.text_input(
                "Path to Processed Parquet Directory", value="data_processed/"
            )
            if st.button("Reload Parquet Cache"):
                if os.path.exists(folder_path):
                    st.cache_data.clear()
                    st.success(f"Cache cleared and reloaded from `{folder_path}`.")
                else:
                    st.error(f"Path `{folder_path}` does not exist.")

        elif selected_mode == mode_options[1]:
            st.markdown("#### Mode 2: Raw Chennai One Ingestion & Sanitizer")
            st.caption("Upload raw operational transaction logs up to 200MB.")

            uploaded_file = st.file_uploader("Upload Raw Chennai One File", type=["csv", "xlsx"])
            if uploaded_file is not None:
                try:
                    if uploaded_file.name.endswith(".csv"):
                        raw_upload = pd.read_csv(uploaded_file, nrows=500)
                    else:
                        raw_upload = pd.read_excel(uploaded_file, nrows=500)

                    cols = list(raw_upload.columns)
                    if "route_id_1" in cols and "quantity_1" in cols:
                        file_type = "Single-Leg Searches Telemetry"
                    elif "ticket_no_1" in cols:
                        file_type = "Multi-Leg Journeys Telemetry"
                    elif "from_station_id_2" in cols:
                        file_type = "Multi-Leg Searches Telemetry"
                    elif "journey_id" in cols and "quantity_1" in cols:
                        file_type = "Single-Leg Journeys Telemetry"
                    else:
                        file_type = "Generic Telemetry Table"

                    st.info(
                        f"**Detected File Type:** `{file_type}` ({len(raw_upload):,} rows sampled)"
                    )

                    clean_upload = raw_upload.copy()
                    n_nulls_fixed = 0
                    for c in clean_upload.columns:
                        if clean_upload[c].dtype == "object":
                            n_nulls_fixed += (clean_upload[c] == r"\N").sum()
                            clean_upload[c] = clean_upload[c].replace(r"\N", np.nan)
                            clean_upload[c] = (
                                clean_upload[c].astype(str).str.strip().replace("nan", np.nan)
                            )

                    st.success(
                        f"Cleaned `{uploaded_file.name}`: converted {n_nulls_fixed:,} '\\N' strings to NULL and trimmed whitespace."
                    )
                    st.markdown("**Sanitized Data Preview (First 20 Rows):**")
                    st.dataframe(clean_upload.head(20), use_container_width=True)

                except Exception as ex:
                    st.error(f"Error parsing uploaded file: {ex}")
            else:
                st.info("Upload a file above or test demo batch sanitization below:")
                if st.button("Simulate Raw Batch Sanitization"):
                    demo_raw = pd.DataFrame(
                        {
                            "journey_id": ["j_101", "j_102", "j_103"],
                            "from_station_name_1": ["  M.G.R.KOYAMBEDU  ", r"\N", "TAMBARAM "],
                            "to_station_name_1": ["SIRUSERI I.T.PARK", "VELACHERY", r"\N"],
                            "route_id_1": ["543001", "570", r"\N"],
                        }
                    )
                    st.write("Raw input with `\\N` and uneven whitespace:")
                    st.dataframe(demo_raw, use_container_width=True)

                    demo_clean = demo_raw.replace(r"\N", np.nan)
                    for col in demo_clean.columns:
                        if demo_clean[col].dtype == "object":
                            demo_clean[col] = demo_clean[col].str.strip()
                    st.write("Sanitized output (`\\N` → NULL, trimmed):")
                    st.dataframe(demo_clean, use_container_width=True)

        else:
            st.markdown("#### Mode 3: Replay of March 2026 Telemetry (Not Live)")
            st.caption("Step through real hourly telemetry across all 24 hours of the day:")

            rep_col1, rep_col2 = st.columns([2, 1])
            with rep_col1:
                replay_hour = st.slider(
                    "Hour of Day (IST)", min_value=0, max_value=23, value=18, format="%02d:00"
                )
            with rep_col2:
                replay_speed = st.selectbox(
                    "Replay Speed", ["1x (Normal)", "2x (Fast)", "5x (Turbo)"]
                )

            h_row = df_city_h[df_city_h["hour"].astype(int) == replay_hour].iloc[0]
            h_gap = h_row["searches"] - h_row["tickets"]

            st.markdown(f"**Hourly Metrics for {replay_hour:02d}:00 IST:**")
            r_c1, r_c2, r_c3, r_c4 = st.columns(4)
            with r_c1:
                st.markdown(
                    f"""
                <div class="kpi-card" title="Total commuter searches during this hour window.">
                    <div class="kpi-caption">Searches This Hour</div>
                    <div class="kpi-value metric-accent">{int(h_row["searches"]):,}</div>
                    <div class="kpi-sub">Hourly search intent</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )
            with r_c2:
                st.markdown(
                    f"""
                <div class="kpi-card" title="Confirmed ticket bookings during this hour.">
                    <div class="kpi-caption">Tickets This Hour</div>
                    <div class="kpi-value metric-good">{int(h_row["tickets"]):,}</div>
                    <div class="kpi-sub">Proxy for travel</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )
            with r_c3:
                st.markdown(
                    f"""
                <div class="kpi-card" title="Searches minus tickets during this hour.">
                    <div class="kpi-caption">Hourly Gap</div>
                    <div class="kpi-value metric-problem">{int(h_gap):,}</div>
                    <div class="kpi-sub">Unmet demand</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )
            with r_c4:
                st.markdown(
                    f"""
                <div class="kpi-card" title="Conversion percentage during this hour.">
                    <div class="kpi-caption">Conversion Rate</div>
                    <div class="kpi-value metric-primary">{h_row["conversion"] * 100:.1f}%</div>
                    <div class="kpi-sub">Digital conversion</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

            st.markdown("<br>", unsafe_allow_html=True)

            # Top bottleneck corridors during this hour window
            hour_window = (
                "Morning Peak"
                if 7 <= replay_hour <= 10
                else (
                    "Midday"
                    if 11 <= replay_hour <= 15
                    else ("Evening Peak" if 16 <= replay_hour <= 20 else "Night")
                )
            )
            hour_corridors = (
                df_diag_all[df_diag_all["time_bucket"] == hour_window]
                .sort_values("lost_trips", ascending=False)
                .head(5)
                .copy()
            )

            # Fix rule 4f: Store conversion as 0-100 number so 45/2,336 displays as 1.9% not 0.0%
            hour_corridors["conversion_pct"] = hour_corridors["srcr"] * 100.0

            rep_t1, rep_t2 = st.columns([3, 1])
            with rep_t1:
                st.markdown(
                    f"**Top Bottleneck Corridors Active During {hour_window} ({replay_hour:02d}:00):**"
                )
            with rep_t2:
                csv_h_corridors = (
                    hour_corridors[
                        [
                            "from_name",
                            "to_name",
                            "mode",
                            "searches",
                            "lost_trips",
                            "conversion_pct",
                            "primary_cause",
                        ]
                    ]
                    .to_csv(index=False)
                    .encode("utf-8")
                )
                st.download_button(
                    "📥 Download Hourly Table (CSV)",
                    data=csv_h_corridors,
                    file_name=f"cumta_bottlenecks_{hour_window.lower().replace(' ', '_')}.csv",
                    mime="text/csv",
                    help="Export this hourly bottleneck table as CSV.",
                )

            st.dataframe(
                hour_corridors[
                    [
                        "from_name",
                        "to_name",
                        "mode",
                        "searches",
                        "lost_trips",
                        "conversion_pct",
                        "primary_cause",
                    ]
                ],
                column_config={
                    "from_name": st.column_config.TextColumn("Origin"),
                    "to_name": st.column_config.TextColumn("Destination"),
                    "mode": st.column_config.TextColumn("Mode"),
                    "searches": st.column_config.NumberColumn("Searches", format="%,d"),
                    "lost_trips": st.column_config.NumberColumn("Lost Searches", format="%,d"),
                    "conversion_pct": st.column_config.NumberColumn(
                        "Conversion Rate", format="%.1f%%"
                    ),
                    "primary_cause": st.column_config.TextColumn("Diagnostic Cause"),
                },
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("---")
            st.markdown("##### Mock Telemetry Stream: `get_stream()`")
            if st.button("Serve Sample Event Packet via get_stream()"):
                sample_records = (
                    df_diag_all[
                        ["from_name", "to_name", "mode", "searches", "lost_trips", "primary_cause"]
                    ]
                    .sample(5)
                    .to_dict(orient="records")
                )
                st.code(json.dumps(sample_records, indent=2), language="json")
                st.success(
                    "Successfully streamed 5 live records as JSON from processed Parquet tables."
                )

    except Exception as tab_err:
        st.error(
            "An unexpected error occurred while rendering the Data & Replay section. Please adjust filters or reload."
        )
        with st.expander("Technical Error Details"):
            st.code(str(tab_err))

    t_elapsed = time.perf_counter() - t_start
    print(f"[PERF] Tab 5 (Replay) rendered in {t_elapsed:.3f}s")
