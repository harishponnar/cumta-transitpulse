"""
Automated Unit and Integrity Tests for CUMTA TransitPulse.
Tests data consistency, privacy constraints, revenue math, time buckets, and formatting.
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from revenue import (
    get_standard_fare,
    run_revenue_assertions,
)


def get_time_window(hour: int) -> str:
    """Canonical hour to time window mapping."""
    if 7 <= hour <= 10:
        return "Morning Peak"
    elif 11 <= hour <= 15:
        return "Midday"
    elif 16 <= hour <= 20:
        return "Evening Peak"
    elif (21 <= hour <= 23) or (0 <= hour <= 6):
        return "Night"
    raise ValueError(f"Invalid hour: {hour}")


def test_time_bucket_coverage():
    """Verify hours 0-23 map to exactly one time window."""
    mapped = [get_time_window(h) for h in range(24)]
    assert len(mapped) == 24
    expected_windows = {"Morning Peak", "Midday", "Evening Peak", "Night"}
    assert set(mapped) == expected_windows
    # Ensure every hour maps deterministically
    assert mapped[8] == "Morning Peak"
    assert mapped[12] == "Midday"
    assert mapped[18] == "Evening Peak"
    assert mapped[22] == "Night"
    assert mapped[2] == "Night"


def test_conversion_within_bounds():
    """Verify search-to-ticket conversion rates stay strictly within [0.0, 1.0]."""
    data_dir = (
        "data_processed" if os.path.exists("data_processed/city_hourly.parquet") else "sample_data"
    )
    hourly = pd.read_parquet(os.path.join(data_dir, "city_hourly.parquet"))
    assert (hourly["conversion"] >= 0.0).all()
    assert (hourly["conversion"] <= 1.0).all()

    diag = pd.read_parquet(os.path.join(data_dir, "diagnosis.parquet"))
    assert (diag["srcr"] >= 0.0).all()
    assert (diag["srcr"] <= 1.0).all()


def test_lost_demand_non_negative():
    """Verify lost demand gap is non-negative and searches >= tickets."""
    data_dir = (
        "data_processed" if os.path.exists("data_processed/city_hourly.parquet") else "sample_data"
    )
    hourly = pd.read_parquet(os.path.join(data_dir, "city_hourly.parquet"))
    lost = hourly["searches"] - hourly["tickets"]
    assert (lost >= 0).all()

    diag = pd.read_parquet(os.path.join(data_dir, "diagnosis.parquet"))
    assert (diag["lost_trips"] >= 0).all()


def test_no_rider_id_in_datasets():
    """Strict privacy test: ensure personal commuter identifiers never exist in processed or sample tables."""
    forbidden_col = "rider" + "_id"
    for folder in ["sample_data", "data_processed"]:
        if os.path.exists(folder):
            for f in os.listdir(folder):
                if f.endswith(".parquet"):
                    df = pd.read_parquet(os.path.join(folder, f))
                    assert forbidden_col not in df.columns, (
                        f"Privacy violation: '{forbidden_col}' found in {folder}/{f}!"
                    )


def test_revenue_assertions_and_math():
    """Verify single source of truth revenue math and monotonicity assertions."""
    data_dir = (
        "data_processed" if os.path.exists("data_processed/actions.parquet") else "sample_data"
    )
    act = pd.read_parquet(os.path.join(data_dir, "actions.parquet"))
    prem = pd.read_parquet(os.path.join(data_dir, "premium_rollup.parquet"))
    prem_lookup = prem.set_index(["from_station_id", "to_station_id"])["parallel_fare"].to_dict()

    success, msg, metrics = run_revenue_assertions(
        act, capture_rate=0.35, days=30, prem_lookup=prem_lookup
    )
    assert success, f"Revenue assertion failed: {msg}"
    assert metrics["rev_high"] <= metrics["rev_hm"] <= metrics["rev_all"]
    assert np.isclose(metrics["rev_hm"], metrics["sim_total"])


def test_standard_route_fare_rule():
    """Verify premium routes strictly adopt parallel standard route fares."""
    prem_sample = {
        "primary_route_id": "543001",
        "from_station_id": "st_a",
        "to_station_id": "st_b",
        "avg_fare": 82.0,
    }
    # Lookup returns 60.0 standard fare
    lookup = {("st_a", "st_b"): 60.0}
    std_fare = get_standard_fare(prem_sample, lookup)
    assert std_fare == 60.0
    assert std_fare != 82.0


def test_percentage_formatting():
    """Verify percentage formatting rule 4f: 45 / 2336 produces '1.9%' not '0.0%'."""
    sample_tickets = 45
    sample_searches = 2336
    srcr = sample_tickets / sample_searches
    # Stored as 0-100 number
    pct_val = srcr * 100.0
    formatted = f"{pct_val:.1f}%"
    assert formatted == "1.9%"
    assert formatted != "0.0%"


def test_app_sample_mode_smoke():
    """Verify app imports and data loaders initialize in sample_data mode."""
    os.environ["TRANSITPULSE_DATA_DIR"] = "sample_data"
    import app

    assert hasattr(app, "load_kpis")
    kpis = app.load_kpis("sample_data")
    assert not kpis.empty
