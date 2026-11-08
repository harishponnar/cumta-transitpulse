import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent


def safe_exists(p: Path) -> bool:
    try:
        return p.exists()
    except (PermissionError, OSError):
        return False


# Raw data directory: reads from RAW_DIR env var, defaulting to ../transit-pulse-private/raw
raw_env = os.environ.get("RAW_DIR")
if raw_env:
    RAW_DIR = Path(raw_env).resolve()
elif safe_exists(BASE_DIR.parent / "transit-pulse-private" / "raw"):
    RAW_DIR = (BASE_DIR.parent / "transit-pulse-private" / "raw").resolve()
elif safe_exists(BASE_DIR / "raw") and any((BASE_DIR / "raw").glob("*.csv")):
    RAW_DIR = (BASE_DIR / "raw").resolve()
elif safe_exists(BASE_DIR / "Theme 6") and any((BASE_DIR / "Theme 6").glob("*.csv")):
    RAW_DIR = (BASE_DIR / "Theme 6").resolve()
else:
    RAW_DIR = (BASE_DIR.parent / "transit-pulse-private" / "raw").resolve()

# Intermediate and generated directories: configurable via env vars, defaulting to transit-pulse-private
parquet_env = os.environ.get("PARQUET_DIR")
if parquet_env:
    PARQUET_DIR = Path(parquet_env).resolve()
else:
    PARQUET_DIR = (BASE_DIR.parent / "transit-pulse-private" / "parquet").resolve()

tmp_duck_env = os.environ.get("TMP_DUCK_DIR")
if tmp_duck_env:
    TMP_DUCK_DIR = Path(tmp_duck_env).resolve()
else:
    TMP_DUCK_DIR = (BASE_DIR.parent / "transit-pulse-private" / "tmp_duck").resolve()

DATA_PROCESSED_DIR = Path(
    os.environ.get("DATA_PROCESSED_DIR", BASE_DIR / "data_processed")
).resolve()
LOGS_DIR = Path(os.environ.get("LOGS_DIR", BASE_DIR / "logs")).resolve()

# Ensure generated directories exist only within workspace
for d in [DATA_PROCESSED_DIR, LOGS_DIR]:
    try:
        d.mkdir(parents=True, exist_ok=True)
    except (PermissionError, OSError):
        pass

# CSV File Names
CSV_FILES = {
    "multi_leg_journeys": RAW_DIR / "multi_leg_journeys_202603.csv",
    "multi_leg_searches": RAW_DIR / "multi_leg_searches_202603.csv",
    "single_leg_journeys": RAW_DIR / "single_leg_journeys_202603.csv",
    "single_leg_searches": RAW_DIR / "single_leg_searches_202603.csv",
}

# 3. CONSTANTS
MIN_SEARCHES = 50
MIN_UNIQUE_SEARCHERS = 10
RELIABLE_MULTIPLIER = 5  # >= 5x thresholds = "High" confidence
CAPTURE_RATE_DEFAULT = 0.35
DAYS_PER_MONTH = 30
FREQ_GAP_MINUTES = 30
FREQ_DROP_PCT = 0.70  # tickets drop >70% vs route's own all-day hourly average
SRCR_RED = 0.25
SRCR_GREEN = 0.60
PENALTY_DIRECT = 1.0
PENALTY_DIRECT_FREQ_GAP = 1.5
PENALTY_TRANSFER = 2.5
FRUSTRATION_WINDOW_MIN = 20
FRUSTRATION_MIN_REPEATS = 3


# TIME_BUCKETS (IST hour):
# Morning Peak: 7-10 (7, 8, 9, 10)
# Midday: 11-15 (11, 12, 13, 14, 15)
# Evening Peak: 16-20 (16, 17, 18, 19, 20)
# Night: 21-23 and 0-6 (21, 22, 23, 0, 1, 2, 3, 4, 5, 6)
def get_time_bucket(hour: int) -> str:
    if 7 <= hour <= 10:
        return "Morning Peak"
    elif 11 <= hour <= 15:
        return "Midday"
    elif 16 <= hour <= 20:
        return "Evening Peak"
    elif (21 <= hour <= 23) or (0 <= hour <= 6):
        return "Night"
    else:
        raise ValueError(f"Invalid hour: {hour}")


# Verify every hour 0-23 belongs to exactly one bucket
def verify_time_buckets():
    buckets = [get_time_bucket(h) for h in range(24)]
    assert len(buckets) == 24
    assert all(b in ["Morning Peak", "Midday", "Evening Peak", "Night"] for b in buckets)
    return True


verify_time_buckets()

# SQL expression for time bucket mapping in DuckDB
TIME_BUCKET_SQL_CASE = """
CASE 
    WHEN hour BETWEEN 7 AND 10 THEN 'Morning Peak'
    WHEN hour BETWEEN 11 AND 15 THEN 'Midday'
    WHEN hour BETWEEN 16 AND 20 THEN 'Evening Peak'
    WHEN (hour BETWEEN 21 AND 23) OR (hour BETWEEN 0 AND 6) THEN 'Night'
    ELSE 'Unknown'
END
"""
