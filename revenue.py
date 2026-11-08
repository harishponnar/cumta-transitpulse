"""
Unified Revenue & Fare Calculation Engine for CUMTA TransitPulse.
Single source of truth for revenue upside, standard route tariffs, and capture simulations.
"""

import numpy as np
import pandas as pd

PREMIUM_ROUTES = {"543001", "546001", "918001", "598001"}

DEFAULT_PARALLEL_FARES = {
    "543001": 60.0,
    "546001": 60.0,
    "918001": 29.0,
    "598001": 37.0,
}


def get_standard_fare(row, prem_lookup=None):
    """
    Returns the standard route fare for any corridor.
    Per rule 4b: For rows on designated premium routes, ALWAYS uses the parallel
    STANDARD route fare (e.g. Rs 60), never the premium fare (Rs 82).
    """
    route_id = str(row.get("primary_route_id", "")).strip()
    if route_id in PREMIUM_ROUTES:
        if prem_lookup is not None:
            key = (row.get("from_station_id"), row.get("to_station_id"))
            if key in prem_lookup and pd.notnull(prem_lookup[key]):
                return float(prem_lookup[key])
        return float(DEFAULT_PARALLEL_FARES.get(route_id, 60.0))

    fare = row.get("avg_fare", row.get("avg_price", 15.0))
    try:
        val = float(fare)
        return val if pd.notnull(val) and val > 0 else 15.0
    except (ValueError, TypeError):
        return 15.0


def compute_row_metrics(df, capture_rate=0.35, days=30, prem_lookup=None):
    """
    Computes daily rider gains and monthly revenue projections for each row.
    Returns DataFrame with:
      - standard_fare: Tariff used for revenue modeling (standard route tariff)
      - premium_fare: Original premium tariff if applicable
      - is_actionable: True if prescriptive action, False if investigate/diagnosis
      - sim_riders_day: Estimated daily riders gained at target capture rate
      - sim_rev_mo: Estimated monthly revenue upside
    """
    if df.empty:
        res = df.copy()
        res["standard_fare"] = 0.0
        res["is_actionable"] = False
        res["sim_riders_day"] = 0.0
        res["sim_rev_mo"] = 0.0
        return res

    res = df.copy()

    # 1. Determine standard fare for every row
    if "standard_fare" not in res.columns:
        res["standard_fare"] = res.apply(lambda r: get_standard_fare(r, prem_lookup), axis=1)

    # 2. Daily lost trips (from 31-day March baseline)
    lost_col = "lost_trips" if "lost_trips" in res.columns else "lost_daily_searches"
    if lost_col == "lost_trips":
        daily_lost = res["lost_trips"].astype(float) / 31.0
    else:
        daily_lost = res["lost_daily_searches"].astype(float)

    # 3. Prescriptive action status (Spec 4c: Exclude investigate from headline revenue)
    is_actionable = ~res["recommended_action"].astype(str).str.startswith("Investigate")
    if "diagnostic_cause" in res.columns:
        is_actionable = is_actionable & (res["diagnostic_cause"] != "NO_CLEAR_CAUSE")
    res["is_actionable"] = is_actionable

    # 4. Sibling share check: if >50% commuters co-searched standard route, demand is not lost
    sibling_share = res["sibling_share"].astype(float) if "sibling_share" in res.columns else 0.0
    not_sibling_dominant = sibling_share <= 0.5

    # 5. Daily rider gain
    rider_gain = daily_lost * capture_rate
    res["sim_riders_day"] = np.where(is_actionable, rider_gain, 0.0)

    # 6. Monthly revenue upside (standard tariff × days)
    revenue = rider_gain * res["standard_fare"] * days
    # Mask non-actionable or sibling-dominated corridors to 0.0
    res["sim_rev_mo"] = np.where(is_actionable & not_sibling_dominant, revenue, 0.0)

    return res


def compute_revenue(rows, capture_rate=0.35, days=30, prem_lookup=None):
    """
    The ONE single source of truth for revenue calculation.
    KPI cards, Actions simulator, tables, exports, and tooltips all call this function.
    Returns total projected monthly revenue in INR.
    """
    if rows is None or len(rows) == 0:
        return 0.0

    if isinstance(rows, pd.Series):
        rows = pd.DataFrame([rows])

    calc_df = compute_row_metrics(
        rows, capture_rate=capture_rate, days=days, prem_lookup=prem_lookup
    )
    return float(calc_df["sim_rev_mo"].sum())


def run_revenue_assertions(act_df, capture_rate=0.35, days=30, prem_lookup=None):
    """
    Runs startup assertions:
      1. revenue(High) <= revenue(High+Medium) <= revenue(all)
      2. Simulator total at 35% equals KPI High+Med figure.
    Returns (success: bool, msg: str, metrics: dict)
    """
    base = act_df[(act_df["corridor_type"] == "DIRECT") & (act_df["time_window"] != "All")].copy()
    if "searches" in base.columns:
        base = base[base["searches"] >= 10]

    h_rows = base[base["confidence"] == "High"]
    hm_rows = base[base["confidence"].isin(["High", "Medium"])]
    all_rows = base

    rev_high = compute_revenue(
        h_rows, capture_rate=capture_rate, days=days, prem_lookup=prem_lookup
    )
    rev_hm = compute_revenue(hm_rows, capture_rate=capture_rate, days=days, prem_lookup=prem_lookup)
    rev_all = compute_revenue(
        all_rows, capture_rate=capture_rate, days=days, prem_lookup=prem_lookup
    )

    # Simulator default table has High+Medium actionable corridors
    sim_table = hm_rows[~hm_rows["recommended_action"].astype(str).str.startswith("Investigate")]
    sim_total = compute_revenue(
        sim_table, capture_rate=capture_rate, days=days, prem_lookup=prem_lookup
    )

    cond1 = rev_high <= rev_hm <= rev_all
    cond2 = np.isclose(rev_hm, sim_total, rtol=1e-5, atol=1.0)

    success = cond1 and cond2
    metrics = {
        "rev_high": rev_high,
        "rev_hm": rev_hm,
        "rev_all": rev_all,
        "sim_total": sim_total,
        "count_actionable_hm": len(sim_table),
        "count_actionable_all": len(
            base[~base["recommended_action"].astype(str).str.startswith("Investigate")]
        ),
        "count_investigate_hm": len(
            hm_rows[hm_rows["recommended_action"].astype(str).str.startswith("Investigate")]
        ),
        "count_investigate_all": len(
            base[base["recommended_action"].astype(str).str.startswith("Investigate")]
        ),
    }

    if success:
        msg = f"Revenue assertions passed: High (₹{rev_high:,.0f}) <= High+Med (₹{rev_hm:,.0f}) <= All (₹{rev_all:,.0f}). Simulator total equals High+Med figure."
    else:
        msg = f"Revenue assertion failure! rev_high={rev_high}, rev_hm={rev_hm}, rev_all={rev_all}, sim_total={sim_total}"

    return success, msg, metrics
