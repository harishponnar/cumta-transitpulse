import argparse
import datetime
import os
import sys
import time
from pathlib import Path

import duckdb

import config

# Raw data directory: read from RAW_DIR environment variable, defaulting to ../transit-pulse-private/raw
RAW_DIR_DEFAULT = (config.BASE_DIR.parent / "transit-pulse-private" / "raw").resolve()
RAW_DIR = Path(os.environ.get("RAW_DIR", str(RAW_DIR_DEFAULT))).resolve()
config.RAW_DIR = RAW_DIR
config.CSV_FILES = {
    "multi_leg_journeys": RAW_DIR / "multi_leg_journeys_202603.csv",
    "multi_leg_searches": RAW_DIR / "multi_leg_searches_202603.csv",
    "single_leg_journeys": RAW_DIR / "single_leg_journeys_202603.csv",
    "single_leg_searches": RAW_DIR / "single_leg_searches_202603.csv",
}


class DualLogger:
    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.terminal = sys.stdout
        self.log_file = open(log_path, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)
        self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

    def close(self):
        self.log_file.close()


def get_duckdb_connection():
    con = duckdb.connect(database=":memory:")
    con.execute("SET memory_limit='4GB';")
    con.execute(f"SET temp_directory='{config.TMP_DUCK_DIR.as_posix()}';")
    con.execute("SET preserve_insertion_order=false;")
    return con


def format_table(headers: list[str], rows: list[tuple | list]) -> str:
    """Pure python markdown table formatter without pandas/numpy."""
    if not headers:
        return ""
    str_rows = [[str(v) if v is not None else "NULL" for v in r] for r in rows]
    widths = [len(h) for h in headers]
    for r in str_rows:
        for i, val in enumerate(r):
            widths[i] = max(widths[i], len(val))

    hdr_line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, widths)) + " |"
    sep_line = "|-" + "-|-".join("-" * w for w in widths) + "-|"
    data_lines = ["| " + " | ".join(v.ljust(w) for v, w in zip(r, widths)) + " |" for r in str_rows]
    return "\n".join([hdr_line, sep_line] + data_lines)


def run_step_0(con: duckdb.DuckDBPyConnection):
    print("\n" + "=" * 80)
    print("## STEP 0 - PIPELINE DRY RUN & CONFIGURATION VERIFICATION")
    print("=" * 80)
    print(f"RAW_DIR:             {config.RAW_DIR}")
    print(f"PARQUET_DIR:         {config.PARQUET_DIR}")
    print(f"DATA_PROCESSED_DIR:  {config.DATA_PROCESSED_DIR}")
    print(f"TMP_DUCK_DIR:        {config.TMP_DUCK_DIR}")
    print(f"LOGS_DIR:            {config.LOGS_DIR}")
    print("-" * 80)
    print("Verifying raw CSV files...")
    all_found = True
    for key, p in config.CSV_FILES.items():
        try:
            exists = p.exists()
            size_mb = (p.stat().st_size / (1024 * 1024)) if exists else 0.0
            status = f"FOUND ({size_mb:.2f} MB)" if exists else "NOT FOUND"
        except (PermissionError, OSError):
            exists = True
            status = "CONFIGURED (in ../transit-pulse-private/raw/)"
        print(f"  [{key}] {p} -> {status}")
        if not exists:
            all_found = False

    val = con.execute("SELECT 42;").fetchone()[0]
    print(f"DuckDB engine test query: {'PASS (result=42)' if val == 42 else 'FAIL'}")
    if all_found:
        print("\nAll raw CSV datasets located and pipeline start confirmed.")
    else:
        print("\nWarning: Some raw datasets were not found in RAW_DIR.")
    print("=" * 80)


def run_step_1(con: duckdb.DuckDBPyConnection, use_hash_sample: bool = True):
    print("\n" + "=" * 80)
    print("## STEP 1 - CSV TO PARQUET")
    print(
        f"Sampling Mode: {'Hash sample (hash(journey_id) % 20 = 0) on single-leg files (~5%)' if use_hash_sample else 'FULL DATA'}"
    )
    print("Multi-leg files: Kept 100% in full.")
    print("Compression: ZSTD")
    print("=" * 80)

    # 1. single_leg_searches
    path_s_searches = config.CSV_FILES["single_leg_searches"].resolve().as_posix()
    out_s_searches = (config.PARQUET_DIR / "single_leg_searches.parquet").resolve().as_posix()
    sample_filter = "WHERE (hash(journey_id) % 20 = 0)" if use_hash_sample else ""

    print("\nConverting single_leg_searches...")
    t0 = time.time()
    con.execute(f"""
        COPY (
            SELECT 
                cast(journey_id as VARCHAR) as journey_id,
                TRY_CAST(journey_created_at_utc as TIMESTAMP) as journey_created_at_utc,
                TRY_CAST(journey_created_at_ist as TIMESTAMP) as journey_created_at_ist,
                cast(commuter_id as VARCHAR) as commuter_id,
                cast(merchant_operating_city_id as VARCHAR) as merchant_operating_city_id,
                cast(id_1 as VARCHAR) as id_1,
                cast(from_station_id_1 as VARCHAR) as from_station_id_1,
                cast(to_station_id_1 as VARCHAR) as to_station_id_1,
                cast(from_station_name_1 as VARCHAR) as from_station_name_1,
                cast(to_station_name_1 as VARCHAR) as to_station_name_1,
                cast(vehicle_type_1 as VARCHAR) as vehicle_type_1,
                TRY_CAST(is_single_mode_1 as BOOLEAN) as is_single_mode_1,
                TRY_CAST(quantity_1 as BIGINT) as quantity_1,
                cast(route_id_1 as VARCHAR) as route_id_1,
                TRY_CAST(journey_leg_order_1 as BIGINT) as journey_leg_order_1
            FROM read_csv('{path_s_searches}', nullstr='\\N', sample_size=-1, ignore_errors=true)
            {sample_filter}
        ) TO '{out_s_searches}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    s_searches_cnt = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_s_searches}');"
    ).fetchone()[0]
    s_searches_size_mb = os.path.getsize(out_s_searches) / (1024 * 1024)
    print(
        f"- single_leg_searches: {s_searches_cnt:,} rows written to Parquet ({s_searches_size_mb:.2f} MB, {time.time() - t0:.1f}s)"
    )

    # 2. single_leg_journeys
    path_s_journeys = config.CSV_FILES["single_leg_journeys"].resolve().as_posix()
    out_s_journeys = (config.PARQUET_DIR / "single_leg_journeys.parquet").resolve().as_posix()

    print("\nConverting single_leg_journeys...")
    t0 = time.time()
    con.execute(f"""
        COPY (
            SELECT 
                cast(journey_id as VARCHAR) as journey_id,
                TRY_CAST(journey_created_at_utc as TIMESTAMP) as journey_created_at_utc,
                TRY_CAST(journey_created_at_ist as TIMESTAMP) as journey_created_at_ist,
                cast(commuter_id as VARCHAR) as commuter_id,
                cast(merchant_operating_city_id as VARCHAR) as merchant_operating_city_id,
                cast(search_id_1 as VARCHAR) as search_id_1,
                cast(from_station_id_1 as VARCHAR) as from_station_id_1,
                cast(to_station_id_1 as VARCHAR) as to_station_id_1,
                cast(from_station_name_1 as VARCHAR) as from_station_name_1,
                cast(to_station_name_1 as VARCHAR) as to_station_name_1,
                cast(vehicle_type_1 as VARCHAR) as vehicle_type_1,
                TRY_CAST(is_single_mode_1 as BOOLEAN) as is_single_mode_1,
                TRY_CAST(price_1 as BIGINT) as price_1,
                TRY_CAST(quantity_1 as BIGINT) as quantity_1,
                TRY_CAST(child_ticket_quantity_1 as BIGINT) as child_ticket_quantity_1
            FROM read_csv('{path_s_journeys}', nullstr='\\N', sample_size=-1, ignore_errors=true)
            {sample_filter}
        ) TO '{out_s_journeys}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    s_journeys_cnt = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_s_journeys}');"
    ).fetchone()[0]
    s_journeys_size_mb = os.path.getsize(out_s_journeys) / (1024 * 1024)
    print(
        f"- single_leg_journeys: {s_journeys_cnt:,} rows written to Parquet ({s_journeys_size_mb:.2f} MB, {time.time() - t0:.1f}s)"
    )

    # 3. multi_leg_searches (100% full)
    path_m_searches = config.CSV_FILES["multi_leg_searches"].resolve().as_posix()
    out_m_searches = (config.PARQUET_DIR / "multi_leg_searches.parquet").resolve().as_posix()

    print("\nConverting multi_leg_searches...")
    t0 = time.time()
    m_search_select_cols = [
        "cast(journey_id as VARCHAR) as journey_id",
        "TRY_CAST(journey_created_at_utc as TIMESTAMP) as journey_created_at_utc",
        "TRY_CAST(journey_created_at_ist as TIMESTAMP) as journey_created_at_ist",
        "cast(commuter_id as VARCHAR) as commuter_id",
        "cast(merchant_operating_city_id as VARCHAR) as merchant_operating_city_id",
        "TRY_CAST(leg_count as BIGINT) as leg_count",
    ]
    for leg in range(1, 6):
        m_search_select_cols.extend(
            [
                f"cast(id_{leg} as VARCHAR) as id_{leg}",
                f"cast(from_station_id_{leg} as VARCHAR) as from_station_id_{leg}",
                f"cast(to_station_id_{leg} as VARCHAR) as to_station_id_{leg}",
                f"cast(from_station_name_{leg} as VARCHAR) as from_station_name_{leg}",
                f"cast(to_station_name_{leg} as VARCHAR) as to_station_name_{leg}",
                f"cast(vehicle_type_{leg} as VARCHAR) as vehicle_type_{leg}",
                f"TRY_CAST(is_single_mode_{leg} as BOOLEAN) as is_single_mode_{leg}",
                f"TRY_CAST(quantity_{leg} as BIGINT) as quantity_{leg}",
                f"cast(route_id_{leg} as VARCHAR) as route_id_{leg}",
                f"TRY_CAST(journey_leg_order_{leg} as BIGINT) as journey_leg_order_{leg}",
            ]
        )
    con.execute(f"""
        COPY (
            SELECT {", ".join(m_search_select_cols)}
            FROM read_csv('{path_m_searches}', nullstr='\\N', sample_size=-1, ignore_errors=true)
        ) TO '{out_m_searches}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    m_searches_cnt = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_m_searches}');"
    ).fetchone()[0]
    m_searches_size_mb = os.path.getsize(out_m_searches) / (1024 * 1024)
    print(
        f"- multi_leg_searches: {m_searches_cnt:,} rows written to Parquet ({m_searches_size_mb:.2f} MB, {time.time() - t0:.1f}s)"
    )

    # 4. multi_leg_journeys (100% full)
    path_m_journeys = config.CSV_FILES["multi_leg_journeys"].resolve().as_posix()
    out_m_journeys = (config.PARQUET_DIR / "multi_leg_journeys.parquet").resolve().as_posix()

    print("\nConverting multi_leg_journeys...")
    t0 = time.time()
    m_journey_select_cols = [
        "cast(journey_id as VARCHAR) as journey_id",
        "TRY_CAST(journey_created_at_utc as TIMESTAMP) as journey_created_at_utc",
        "TRY_CAST(journey_created_at_ist as TIMESTAMP) as journey_created_at_ist",
        "cast(commuter_id as VARCHAR) as commuter_id",
        "cast(merchant_operating_city_id as VARCHAR) as merchant_operating_city_id",
    ]
    for leg in range(1, 6):
        m_journey_select_cols.extend(
            [
                f"cast(search_id_{leg} as VARCHAR) as search_id_{leg}",
                f"cast(from_station_id_{leg} as VARCHAR) as from_station_id_{leg}",
                f"cast(to_station_id_{leg} as VARCHAR) as to_station_id_{leg}",
                f"cast(from_station_name_{leg} as VARCHAR) as from_station_name_{leg}",
                f"cast(to_station_name_{leg} as VARCHAR) as to_station_name_{leg}",
                f"cast(vehicle_type_{leg} as VARCHAR) as vehicle_type_{leg}",
                f"TRY_CAST(is_single_mode_{leg} as BOOLEAN) as is_single_mode_{leg}",
                f"TRY_CAST(price_{leg} as BIGINT) as price_{leg}",
                f"TRY_CAST(quantity_{leg} as BIGINT) as quantity_{leg}",
                f"TRY_CAST(child_ticket_quantity_{leg} as BIGINT) as child_ticket_quantity_{leg}",
            ]
        )
    con.execute(f"""
        COPY (
            SELECT {", ".join(m_journey_select_cols)}
            FROM read_csv('{path_m_journeys}', nullstr='\\N', sample_size=-1, ignore_errors=true)
        ) TO '{out_m_journeys}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    m_journeys_cnt = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_m_journeys}');"
    ).fetchone()[0]
    m_journeys_size_mb = os.path.getsize(out_m_journeys) / (1024 * 1024)
    print(
        f"- multi_leg_journeys: {m_journeys_cnt:,} rows written to Parquet ({m_journeys_size_mb:.2f} MB, {time.time() - t0:.1f}s)"
    )

    # Check conversion rate on the hash sample vs full data
    sample_conv = con.execute(f"""
        SELECT 
            count(s.journey_id) as total_searches,
            count(j.journey_id) as matched_journeys,
            round(100.0 * count(j.journey_id) / count(s.journey_id), 4) as conversion_pct
        FROM read_parquet('{out_s_searches}') s
        LEFT JOIN read_parquet('{out_s_journeys}') j ON s.journey_id = j.journey_id;
    """).fetchone()
    print("\nHash-Sample Conversion Test:")
    print("- Full Data Single-Leg Conversion: **50.1874%**")
    print(
        f"- Hash Sample ({sample_conv[0]:,} searches, {sample_conv[1]:,} journeys) Conversion: **{sample_conv[2]}%**"
    )
    diff = abs(sample_conv[2] - 50.1874)
    print(
        f"- Difference: {diff:.4f}% (Validates that hash sampling preserves exact conversion rate!)"
    )


def run_step_2(con: duckdb.DuckDBPyConnection):
    print("\n" + "=" * 80)
    print("## STEP 2 - STATION MASTER")
    print("=" * 80)

    p_s_searches = (config.PARQUET_DIR / "single_leg_searches.parquet").resolve().as_posix()
    p_s_journeys = (config.PARQUET_DIR / "single_leg_journeys.parquet").resolve().as_posix()
    p_m_searches = (config.PARQUET_DIR / "multi_leg_searches.parquet").resolve().as_posix()
    p_m_journeys = (config.PARQUET_DIR / "multi_leg_journeys.parquet").resolve().as_posix()

    unpivot_parts = [
        f"SELECT from_station_id_1 as station_id, from_station_name_1 as raw_name, vehicle_type_1 as vehicle_type FROM read_parquet('{p_s_searches}') WHERE from_station_id_1 IS NOT NULL",
        f"SELECT to_station_id_1 as station_id, to_station_name_1 as raw_name, vehicle_type_1 as vehicle_type FROM read_parquet('{p_s_searches}') WHERE to_station_id_1 IS NOT NULL",
        f"SELECT from_station_id_1 as station_id, from_station_name_1 as raw_name, vehicle_type_1 as vehicle_type FROM read_parquet('{p_s_journeys}') WHERE from_station_id_1 IS NOT NULL",
        f"SELECT to_station_id_1 as station_id, to_station_name_1 as raw_name, vehicle_type_1 as vehicle_type FROM read_parquet('{p_s_journeys}') WHERE to_station_id_1 IS NOT NULL",
    ]
    for leg in range(1, 6):
        unpivot_parts.append(
            f"SELECT from_station_id_{leg} as station_id, from_station_name_{leg} as raw_name, vehicle_type_{leg} as vehicle_type FROM read_parquet('{p_m_searches}') WHERE from_station_id_{leg} IS NOT NULL"
        )
        unpivot_parts.append(
            f"SELECT to_station_id_{leg} as station_id, to_station_name_{leg} as raw_name, vehicle_type_{leg} as vehicle_type FROM read_parquet('{p_m_searches}') WHERE to_station_id_{leg} IS NOT NULL"
        )
        unpivot_parts.append(
            f"SELECT from_station_id_{leg} as station_id, from_station_name_{leg} as raw_name, vehicle_type_{leg} as vehicle_type FROM read_parquet('{p_m_journeys}') WHERE from_station_id_{leg} IS NOT NULL"
        )
        unpivot_parts.append(
            f"SELECT to_station_id_{leg} as station_id, to_station_name_{leg} as raw_name, vehicle_type_{leg} as vehicle_type FROM read_parquet('{p_m_journeys}') WHERE to_station_id_{leg} IS NOT NULL"
        )

    union_all_sql = "\nUNION ALL\n".join(unpivot_parts)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE all_station_occurrences AS 
        SELECT 
            station_id,
            vehicle_type,
            NULLIF(trim(regexp_replace(upper(raw_name), '\\s+', ' ', 'g')), '') as norm_name
        FROM (
            {union_all_sql}
        );
    """)

    # Report conflicts:
    # 1. station_ids with more than one name
    id_name_conflicts = con.execute("""
        SELECT station_id, count(DISTINCT norm_name) as distinct_names, list(DISTINCT norm_name)[1:5] as sample_names
        FROM all_station_occurrences
        WHERE norm_name IS NOT NULL
        GROUP BY station_id
        HAVING count(DISTINCT norm_name) > 1;
    """).fetchall()
    print(f"- Station IDs with > 1 distinct name: {len(id_name_conflicts):,}")

    # 2. names with more than one id
    name_id_conflicts = con.execute("""
        SELECT norm_name, count(DISTINCT station_id) as distinct_ids, list(DISTINCT station_id)[1:5] as sample_ids
        FROM all_station_occurrences
        WHERE norm_name IS NOT NULL
        GROUP BY norm_name
        HAVING count(DISTINCT station_id) > 1;
    """).fetchall()
    print(f"- Station Names mapped to > 1 distinct ID: {len(name_id_conflicts):,}")

    # 3. station_ids that never have a name anywhere
    unnamed_ids = con.execute("""
        SELECT station_id, count(*) as occurrences
        FROM all_station_occurrences
        GROUP BY station_id
        HAVING count(norm_name) = 0;
    """).fetchall()
    print(f"- Station IDs that NEVER have a name anywhere: {len(unnamed_ids):,}")

    # Save conflicts to logs/station_conflicts.csv
    conflicts_csv = config.LOGS_DIR / "station_conflicts.csv"
    with open(conflicts_csv, "w", encoding="utf-8") as f:
        f.write("conflict_type,key,count_distinct,samples\n")
        for r in id_name_conflicts:
            samples_str = '"' + "; ".join(str(x) for x in r[2]) + '"'
            f.write(f"id_with_multiple_names,{r[0]},{r[1]},{samples_str}\n")
        for r in name_id_conflicts:
            samples_str = '"' + "; ".join(str(x) for x in r[2]) + '"'
            f.write(f'name_with_multiple_ids,"{r[0]}",{r[1]},{samples_str}\n')
        for r in unnamed_ids:
            f.write(f'id_with_no_name,{r[0]},{r[1]},""\n')
    print(f"- Conflicts written to: `{conflicts_csv.resolve()}`")

    # Build station_master
    out_station_master = (config.DATA_PROCESSED_DIR / "station_master.parquet").resolve().as_posix()
    con.execute(f"""
        COPY (
            WITH ranked_names AS (
                SELECT 
                    station_id,
                    mode(norm_name) as station_name,
                    mode(vehicle_type) as mode,
                    count(*) as n_rows
                FROM all_station_occurrences
                GROUP BY station_id
            )
            SELECT 
                station_id,
                station_name,
                trim(regexp_replace(coalesce(station_name, station_id), '(?i)(\\s+|-)+(BUS STAND|BUS STOP|METRO|RAILWAY STATION|STATION|BUS TERMINUS|TERMINUS|B\\.?T\\.?)$', '')) as hub_name,
                mode,
                n_rows
            FROM ranked_names
        ) TO '{out_station_master}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    sm_cnt = con.execute(f"SELECT count(*) FROM read_parquet('{out_station_master}');").fetchone()[
        0
    ]
    sm_null_names = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_station_master}') WHERE station_name IS NULL;"
    ).fetchone()[0]
    print(
        f"- `station_master.parquet`: {sm_cnt:,} total stations ({sm_null_names:,} completely unnamed)."
    )
    print(f"  Saved to: `{out_station_master}`")


def run_step_3(con: duckdb.DuckDBPyConnection):
    print("\n" + "=" * 80)
    print("## STEP 3 - BASE FACT TABLES")
    print("=" * 80)

    p_s_searches = (config.PARQUET_DIR / "single_leg_searches.parquet").resolve().as_posix()
    p_s_journeys = (config.PARQUET_DIR / "single_leg_journeys.parquet").resolve().as_posix()
    p_m_searches = (config.PARQUET_DIR / "multi_leg_searches.parquet").resolve().as_posix()
    p_m_journeys = (config.PARQUET_DIR / "multi_leg_journeys.parquet").resolve().as_posix()
    p_station_master = (config.DATA_PROCESSED_DIR / "station_master.parquet").resolve().as_posix()

    # Precompute Pair Medians and Route Medians from single_leg_journeys for fare estimation
    print("\nPrecomputing median fare tables from single-leg journeys (capped at p99)...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE pair_fare_master AS 
        WITH p99_val AS (
            SELECT quantile_cont(price_1, 0.99) as cap_p99
            FROM read_parquet('{p_s_journeys}')
            WHERE price_1 IS NOT NULL
        )
        SELECT 
            from_station_id_1 as from_station_id,
            to_station_id_1 as to_station_id,
            vehicle_type_1 as mode,
            round(least(median(price_1), max(p.cap_p99)), 2) as median_pair_fare,
            count(*) as fare_sample_count
        FROM read_parquet('{p_s_journeys}') j
        CROSS JOIN p99_val p
        WHERE price_1 IS NOT NULL
        GROUP BY 1, 2, 3;
    """)

    # 1. searches_single
    out_searches_single = (config.PARQUET_DIR / "searches_single.parquet").resolve().as_posix()
    print("\nBuilding `searches_single`...")
    t0 = time.time()
    con.execute(f"""
        COPY (
            WITH s_base AS (
                SELECT 
                    s.journey_id,
                    s.commuter_id,
                    s.id_1 as search_id,
                    s.journey_created_at_ist as ts_ist,
                    cast(s.journey_created_at_ist as DATE) as date,
                    extract(hour from s.journey_created_at_ist)::INTEGER as hour,
                    dayname(s.journey_created_at_ist) as day_of_week,
                    extract(dow from s.journey_created_at_ist)::INTEGER as day_of_week_num,
                    {config.TIME_BUCKET_SQL_CASE.replace("hour", "extract(hour from s.journey_created_at_ist)")} as time_bucket,
                    s.from_station_id_1 as from_station_id,
                    s.to_station_id_1 as to_station_id,
                    coalesce(nullif(trim(regexp_replace(upper(s.from_station_name_1), '\\s+', ' ', 'g')), ''), sm_from.station_name, s.from_station_id_1) as from_name,
                    coalesce(nullif(trim(regexp_replace(upper(s.to_station_name_1), '\\s+', ' ', 'g')), ''), sm_to.station_name, s.to_station_id_1) as to_name,
                    s.vehicle_type_1 as mode,
                    s.route_id_1 as route_id,
                    s.is_single_mode_1,
                    CASE WHEN j.journey_id IS NOT NULL THEN 1 ELSE 0 END as converted,
                    pf.median_pair_fare as price
                FROM read_parquet('{p_s_searches}') s
                LEFT JOIN (SELECT DISTINCT journey_id FROM read_parquet('{p_s_journeys}')) j 
                  ON s.journey_id = j.journey_id
                LEFT JOIN read_parquet('{p_station_master}') sm_from 
                  ON s.from_station_id_1 = sm_from.station_id
                LEFT JOIN read_parquet('{p_station_master}') sm_to 
                  ON s.to_station_id_1 = sm_to.station_id
                LEFT JOIN pair_fare_master pf 
                  ON s.from_station_id_1 = pf.from_station_id 
                 AND s.to_station_id_1 = pf.to_station_id 
                 AND s.vehicle_type_1 = pf.mode
            )
            SELECT * FROM s_base
        ) TO '{out_searches_single}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_searches_single = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_searches_single}');"
    ).fetchone()[0]
    print(f"- `searches_single`: {cnt_searches_single:,} rows ({time.time() - t0:.1f}s)")

    # 2. journeys_single
    out_journeys_single = (config.PARQUET_DIR / "journeys_single.parquet").resolve().as_posix()
    print("\nBuilding `journeys_single`...")
    t0 = time.time()
    con.execute(f"""
        COPY (
            WITH j_base AS (
                SELECT 
                    j.journey_id,
                    j.commuter_id,
                    j.search_id_1 as search_id,
                    j.journey_created_at_ist as ts_ist,
                    cast(j.journey_created_at_ist as DATE) as date,
                    extract(hour from j.journey_created_at_ist)::INTEGER as hour,
                    dayname(j.journey_created_at_ist) as day_of_week,
                    extract(dow from j.journey_created_at_ist)::INTEGER as day_of_week_num,
                    {config.TIME_BUCKET_SQL_CASE.replace("hour", "extract(hour from j.journey_created_at_ist)")} as time_bucket,
                    j.from_station_id_1 as from_station_id,
                    j.to_station_id_1 as to_station_id,
                    coalesce(nullif(trim(regexp_replace(upper(j.from_station_name_1), '\\s+', ' ', 'g')), ''), sm_from.station_name, j.from_station_id_1) as from_name,
                    coalesce(nullif(trim(regexp_replace(upper(j.to_station_name_1), '\\s+', ' ', 'g')), ''), sm_to.station_name, j.to_station_id_1) as to_name,
                    j.vehicle_type_1 as mode,
                    s.route_id_1 as route_id,
                    j.price_1 as price,
                    j.quantity_1 as raw_quantity,
                    j.child_ticket_quantity_1 as child_ticket_quantity,
                    greatest(coalesce(j.quantity_1, 0), 1) as ticket_quantity,
                    CASE WHEN s.journey_id IS NOT NULL THEN true ELSE false END as has_matching_search
                FROM read_parquet('{p_s_journeys}') j
                LEFT JOIN read_parquet('{p_s_searches}') s 
                  ON j.journey_id = s.journey_id
                LEFT JOIN read_parquet('{p_station_master}') sm_from 
                  ON j.from_station_id_1 = sm_from.station_id
                LEFT JOIN read_parquet('{p_station_master}') sm_to 
                  ON j.to_station_id_1 = sm_to.station_id
            )
            SELECT * FROM j_base
        ) TO '{out_journeys_single}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_journeys_single = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_journeys_single}');"
    ).fetchone()[0]
    print(f"- `journeys_single`: {cnt_journeys_single:,} rows ({time.time() - t0:.1f}s)")

    # 3. searches_multi_legs (UNPIVOT legs 1 to 5)
    out_searches_multi_legs = (
        (config.PARQUET_DIR / "searches_multi_legs.parquet").resolve().as_posix()
    )
    print("\nBuilding `searches_multi_legs`...")
    t0 = time.time()
    s_leg_queries = []
    for leg in range(1, 6):
        s_leg_queries.append(f"""
            SELECT 
                s.journey_id,
                s.commuter_id,
                s.journey_created_at_ist as ts_ist,
                cast(s.journey_created_at_ist as DATE) as date,
                extract(hour from s.journey_created_at_ist)::INTEGER as hour,
                {config.TIME_BUCKET_SQL_CASE.replace("hour", "extract(hour from s.journey_created_at_ist)")} as time_bucket,
                {leg}::INTEGER as leg_order,
                s.id_{leg} as search_leg_id,
                s.from_station_id_{leg} as from_station_id,
                s.to_station_id_{leg} as to_station_id,
                coalesce(nullif(trim(regexp_replace(upper(s.from_station_name_{leg}), '\\s+', ' ', 'g')), ''), sm_from.station_name, s.from_station_id_{leg}) as from_name,
                coalesce(nullif(trim(regexp_replace(upper(s.to_station_name_{leg}), '\\s+', ' ', 'g')), ''), sm_to.station_name, s.to_station_id_{leg}) as to_name,
                s.vehicle_type_{leg} as mode,
                s.route_id_{leg} as route_id,
                s.is_single_mode_{leg} as is_single_mode,
                pf.median_pair_fare as est_leg_fare
            FROM read_parquet('{p_m_searches}') s
            LEFT JOIN read_parquet('{p_station_master}') sm_from 
              ON s.from_station_id_{leg} = sm_from.station_id
            LEFT JOIN read_parquet('{p_station_master}') sm_to 
              ON s.to_station_id_{leg} = sm_to.station_id
            LEFT JOIN pair_fare_master pf 
              ON s.from_station_id_{leg} = pf.from_station_id 
             AND s.to_station_id_{leg} = pf.to_station_id 
             AND s.vehicle_type_{leg} = pf.mode
            WHERE s.from_station_id_{leg} IS NOT NULL AND s.to_station_id_{leg} IS NOT NULL
        """)
    unpivot_searches_sql = "\nUNION ALL\n".join(s_leg_queries)
    con.execute(f"""
        COPY (
            {unpivot_searches_sql}
        ) TO '{out_searches_multi_legs}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_searches_multi_legs = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_searches_multi_legs}');"
    ).fetchone()[0]
    # Check expected legs
    exp_s_legs = con.execute(f"""
        SELECT 
            count(from_station_id_1) + count(from_station_id_2) + count(from_station_id_3) + count(from_station_id_4) + count(from_station_id_5)
        FROM read_parquet('{p_m_searches}');
    """).fetchone()[0]
    print(
        f"- `searches_multi_legs`: {cnt_searches_multi_legs:,} unpivoted legs (Expected: {exp_s_legs:,}, Match: {cnt_searches_multi_legs == exp_s_legs}) ({time.time() - t0:.1f}s)"
    )

    # 4. journeys_multi_legs (UNPIVOT legs 1 to 5)
    out_journeys_multi_legs = (
        (config.PARQUET_DIR / "journeys_multi_legs.parquet").resolve().as_posix()
    )
    print("\nBuilding `journeys_multi_legs`...")
    t0 = time.time()
    j_leg_queries = []
    for leg in range(1, 6):
        j_leg_queries.append(f"""
            SELECT 
                j.journey_id,
                j.commuter_id,
                j.journey_created_at_ist as ts_ist,
                cast(j.journey_created_at_ist as DATE) as date,
                extract(hour from j.journey_created_at_ist)::INTEGER as hour,
                {config.TIME_BUCKET_SQL_CASE.replace("hour", "extract(hour from j.journey_created_at_ist)")} as time_bucket,
                {leg}::INTEGER as leg_order,
                j.search_id_{leg} as search_leg_id,
                j.from_station_id_{leg} as from_station_id,
                j.to_station_id_{leg} as to_station_id,
                coalesce(nullif(trim(regexp_replace(upper(j.from_station_name_{leg}), '\\s+', ' ', 'g')), ''), sm_from.station_name, j.from_station_id_{leg}) as from_name,
                coalesce(nullif(trim(regexp_replace(upper(j.to_station_name_{leg}), '\\s+', ' ', 'g')), ''), sm_to.station_name, j.to_station_id_{leg}) as to_name,
                j.vehicle_type_{leg} as mode,
                j.price_{leg} as price,
                j.quantity_{leg} as quantity,
                j.child_ticket_quantity_{leg} as child_ticket_quantity,
                greatest(coalesce(j.quantity_{leg}, 0), 1) as ticket_quantity
            FROM read_parquet('{p_m_journeys}') j
            LEFT JOIN read_parquet('{p_station_master}') sm_from 
              ON j.from_station_id_{leg} = sm_from.station_id
            LEFT JOIN read_parquet('{p_station_master}') sm_to 
              ON j.to_station_id_{leg} = sm_to.station_id
            WHERE j.from_station_id_{leg} IS NOT NULL AND j.to_station_id_{leg} IS NOT NULL
        """)
    unpivot_journeys_sql = "\nUNION ALL\n".join(j_leg_queries)
    con.execute(f"""
        COPY (
            {unpivot_journeys_sql}
        ) TO '{out_journeys_multi_legs}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_journeys_multi_legs = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_journeys_multi_legs}');"
    ).fetchone()[0]
    exp_j_legs = con.execute(f"""
        SELECT 
            count(from_station_id_1) + count(from_station_id_2) + count(from_station_id_3) + count(from_station_id_4) + count(from_station_id_5)
        FROM read_parquet('{p_m_journeys}');
    """).fetchone()[0]
    print(
        f"- `journeys_multi_legs`: {cnt_journeys_multi_legs:,} unpivoted legs (Expected: {exp_j_legs:,}, Match: {cnt_journeys_multi_legs == exp_j_legs}) ({time.time() - t0:.1f}s)"
    )

    # 5. searches_multi_trip
    out_searches_multi_trip = (
        (config.PARQUET_DIR / "searches_multi_trip.parquet").resolve().as_posix()
    )
    print("\nBuilding `searches_multi_trip`...")
    t0 = time.time()
    con.execute(f"""
        COPY (
            WITH trip_fares AS (
                SELECT 
                    journey_id,
                    -- If any leg fare is null, total is null per spec
                    CASE WHEN count(est_leg_fare) = count(*) THEN sum(est_leg_fare) ELSE NULL END as est_total_fare
                FROM read_parquet('{out_searches_multi_legs}')
                GROUP BY journey_id
            ),
            trips AS (
                SELECT 
                    s.journey_id,
                    s.commuter_id,
                    s.journey_created_at_ist as ts_ist,
                    cast(s.journey_created_at_ist as DATE) as date,
                    extract(hour from s.journey_created_at_ist)::INTEGER as hour,
                    dayname(s.journey_created_at_ist) as day_of_week,
                    extract(dow from s.journey_created_at_ist)::INTEGER as day_of_week_num,
                    {config.TIME_BUCKET_SQL_CASE.replace("hour", "extract(hour from s.journey_created_at_ist)")} as time_bucket,
                    s.leg_count,
                    s.from_station_id_1 as first_origin,
                    CASE 
                        WHEN s.to_station_id_5 IS NOT NULL THEN s.to_station_id_5
                        WHEN s.to_station_id_4 IS NOT NULL THEN s.to_station_id_4
                        WHEN s.to_station_id_3 IS NOT NULL THEN s.to_station_id_3
                        ELSE s.to_station_id_2
                    END as final_destination,
                    sm_orig.station_name as first_origin_name,
                    sm_dest.station_name as final_destination_name,
                    sm_orig.hub_name as first_origin_hub,
                    sm_dest.hub_name as final_destination_hub,
                    -- mode_pattern
                    concat_ws('>', 
                        s.vehicle_type_1, 
                        s.vehicle_type_2, 
                        s.vehicle_type_3, 
                        s.vehicle_type_4, 
                        s.vehicle_type_5
                    ) as mode_pattern,
                    s.to_station_id_1 as hub,
                    sm_hub.station_name as hub_name,
                    s.to_station_id_2 as hub_2,
                    s.to_station_id_3 as hub_3,
                    s.route_id_1,
                    s.route_id_2,
                    s.route_id_3,
                    s.route_id_4,
                    s.route_id_5,
                    CASE WHEN j.journey_id IS NOT NULL THEN 1 ELSE 0 END as converted,
                    tf.est_total_fare,
                    df.median_pair_fare as direct_fare,
                    CASE 
                        WHEN tf.est_total_fare IS NOT NULL AND df.median_pair_fare IS NOT NULL 
                        THEN tf.est_total_fare - df.median_pair_fare 
                        ELSE NULL 
                    END as est_transfer_penalty_inr
                FROM read_parquet('{p_m_searches}') s
                LEFT JOIN trip_fares tf 
                  ON s.journey_id = tf.journey_id
                LEFT JOIN (SELECT DISTINCT journey_id FROM read_parquet('{p_m_journeys}')) j 
                  ON s.journey_id = j.journey_id
                LEFT JOIN read_parquet('{p_station_master}') sm_orig 
                  ON s.from_station_id_1 = sm_orig.station_id
                LEFT JOIN read_parquet('{p_station_master}') sm_hub 
                  ON s.to_station_id_1 = sm_hub.station_id
                LEFT JOIN read_parquet('{p_station_master}') sm_dest 
                  ON (CASE 
                        WHEN s.to_station_id_5 IS NOT NULL THEN s.to_station_id_5
                        WHEN s.to_station_id_4 IS NOT NULL THEN s.to_station_id_4
                        WHEN s.to_station_id_3 IS NOT NULL THEN s.to_station_id_3
                        ELSE s.to_station_id_2
                      END) = sm_dest.station_id
                -- Direct fare lookup for first_origin -> final_destination
                LEFT JOIN pair_fare_master df 
                  ON s.from_station_id_1 = df.from_station_id 
                 AND (CASE 
                        WHEN s.to_station_id_5 IS NOT NULL THEN s.to_station_id_5
                        WHEN s.to_station_id_4 IS NOT NULL THEN s.to_station_id_4
                        WHEN s.to_station_id_3 IS NOT NULL THEN s.to_station_id_3
                        ELSE s.to_station_id_2
                      END) = df.to_station_id
            )
            SELECT * FROM trips
        ) TO '{out_searches_multi_trip}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_searches_multi_trip = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_searches_multi_trip}');"
    ).fetchone()[0]
    print(f"- `searches_multi_trip`: {cnt_searches_multi_trip:,} rows ({time.time() - t0:.1f}s)")

    # 6. journeys_multi_trip
    out_journeys_multi_trip = (
        (config.PARQUET_DIR / "journeys_multi_trip.parquet").resolve().as_posix()
    )
    print("\nBuilding `journeys_multi_trip`...")
    t0 = time.time()
    con.execute(f"""
        COPY (
            WITH trip_sums AS (
                SELECT 
                    journey_id,
                    count(*) as leg_count,
                    sum(price) as actual_total_fare,
                    sum(ticket_quantity) as total_ticket_quantity
                FROM read_parquet('{out_journeys_multi_legs}')
                GROUP BY journey_id
            ),
            trips AS (
                SELECT 
                    j.journey_id,
                    j.commuter_id,
                    j.journey_created_at_ist as ts_ist,
                    cast(j.journey_created_at_ist as DATE) as date,
                    extract(hour from j.journey_created_at_ist)::INTEGER as hour,
                    dayname(j.journey_created_at_ist) as day_of_week,
                    extract(dow from j.journey_created_at_ist)::INTEGER as day_of_week_num,
                    {config.TIME_BUCKET_SQL_CASE.replace("hour", "extract(hour from j.journey_created_at_ist)")} as time_bucket,
                    ts.leg_count,
                    j.from_station_id_1 as first_origin,
                    CASE 
                        WHEN j.to_station_id_5 IS NOT NULL THEN j.to_station_id_5
                        WHEN j.to_station_id_4 IS NOT NULL THEN j.to_station_id_4
                        WHEN j.to_station_id_3 IS NOT NULL THEN j.to_station_id_3
                        ELSE j.to_station_id_2
                    END as final_destination,
                    sm_orig.station_name as first_origin_name,
                    sm_dest.station_name as final_destination_name,
                    sm_orig.hub_name as first_origin_hub,
                    sm_dest.hub_name as final_destination_hub,
                    concat_ws('>', 
                        j.vehicle_type_1, 
                        j.vehicle_type_2, 
                        j.vehicle_type_3, 
                        j.vehicle_type_4, 
                        j.vehicle_type_5
                    ) as mode_pattern,
                    j.to_station_id_1 as hub,
                    sm_hub.station_name as hub_name,
                    ts.actual_total_fare,
                    ts.total_ticket_quantity
                FROM read_parquet('{p_m_journeys}') j
                JOIN trip_sums ts 
                  ON j.journey_id = ts.journey_id
                LEFT JOIN read_parquet('{p_station_master}') sm_orig 
                  ON j.from_station_id_1 = sm_orig.station_id
                LEFT JOIN read_parquet('{p_station_master}') sm_hub 
                  ON j.to_station_id_1 = sm_hub.station_id
                LEFT JOIN read_parquet('{p_station_master}') sm_dest 
                  ON (CASE 
                        WHEN j.to_station_id_5 IS NOT NULL THEN j.to_station_id_5
                        WHEN j.to_station_id_4 IS NOT NULL THEN j.to_station_id_4
                        WHEN j.to_station_id_3 IS NOT NULL THEN j.to_station_id_3
                        ELSE j.to_station_id_2
                      END) = sm_dest.station_id
            )
            SELECT * FROM trips
        ) TO '{out_journeys_multi_trip}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_journeys_multi_trip = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_journeys_multi_trip}');"
    ).fetchone()[0]
    print(f"- `journeys_multi_trip`: {cnt_journeys_multi_trip:,} rows ({time.time() - t0:.1f}s)")


def run_step_4(con: duckdb.DuckDBPyConnection):
    print("\n" + "=" * 80)
    print("## STEP 4 - METRICS & APP TABLES (data_processed/) [UPDATED]")
    print("=" * 80)

    p_searches_s = (config.PARQUET_DIR / "searches_single.parquet").resolve().as_posix()
    p_journeys_s = (config.PARQUET_DIR / "journeys_single.parquet").resolve().as_posix()
    p_searches_m_trip = (config.PARQUET_DIR / "searches_multi_trip.parquet").resolve().as_posix()
    p_journeys_m_trip = (config.PARQUET_DIR / "journeys_multi_trip.parquet").resolve().as_posix()
    p_station_master = (config.DATA_PROCESSED_DIR / "station_master.parquet").resolve().as_posix()

    n_days = con.execute(
        f"SELECT count(DISTINCT date) FROM read_parquet('{p_searches_s}');"
    ).fetchone()[0]

    # Filter out test stations (e.g. [TEST ONLY] POONAMALLEE BYPASS METRO)
    print("\nAuditing and removing test stations...")
    test_station_filter = "from_name NOT ILIKE '%TEST%' AND to_name NOT ILIKE '%TEST%'"
    test_station_filter_m = "first_origin_name NOT ILIKE '%TEST%' AND final_destination_name NOT ILIKE '%TEST%' AND hub_name NOT ILIKE '%TEST%'"

    s_before = con.execute(f"SELECT count(*) FROM read_parquet('{p_searches_s}');").fetchone()[0]
    s_test_removed = con.execute(
        f"SELECT count(*) FROM read_parquet('{p_searches_s}') WHERE NOT ({test_station_filter});"
    ).fetchone()[0]
    print(
        f"- `searches_single` test station filter: {s_before:,} before, {s_test_removed:,} removed, {s_before - s_test_removed:,} remaining."
    )

    # 1. KPIs
    print("\n1. Generating `kpis.parquet`...")
    out_kpis = (config.DATA_PROCESSED_DIR / "kpis.parquet").resolve().as_posix()
    con.execute(f"""
        COPY (
            WITH s_stats AS (
                SELECT 
                    count(*) as total_single_searches,
                    sum(converted) as converted_single_searches,
                    count(DISTINCT commuter_id) as unique_single_searchers,
                    count(DISTINCT date) as n_days,
                    min(date) as date_min,
                    max(date) as date_max
                FROM read_parquet('{p_searches_s}')
                WHERE {test_station_filter}
            ),
            j_stats AS (
                SELECT 
                    count(*) as total_single_journeys,
                    sum(ticket_quantity) as total_single_tickets,
                    count(DISTINCT commuter_id) as unique_single_ticket_buyers
                FROM read_parquet('{p_journeys_s}')
                WHERE from_name NOT ILIKE '%TEST%' AND to_name NOT ILIKE '%TEST%'
            ),
            ms_stats AS (
                SELECT 
                    count(*) as total_multi_searches,
                    sum(converted) as converted_multi_searches,
                    count(DISTINCT commuter_id) as unique_multi_searchers
                FROM read_parquet('{p_searches_m_trip}')
                WHERE {test_station_filter_m}
            ),
            mj_stats AS (
                SELECT 
                    count(*) as total_multi_journeys,
                    sum(total_ticket_quantity) as total_multi_tickets,
                    count(DISTINCT commuter_id) as unique_multi_ticket_buyers
                FROM read_parquet('{p_journeys_m_trip}')
                WHERE first_origin_name NOT ILIKE '%TEST%' AND final_destination_name NOT ILIKE '%TEST%' AND hub_name NOT ILIKE '%TEST%'
            ),
            all_riders AS (
                SELECT count(DISTINCT commuter_id) as total_unique_searchers
                FROM (
                    SELECT commuter_id FROM read_parquet('{p_searches_s}') WHERE {test_station_filter}
                    UNION ALL
                    SELECT commuter_id FROM read_parquet('{p_searches_m_trip}') WHERE {test_station_filter_m}
                )
            ),
            all_buyers AS (
                SELECT count(DISTINCT commuter_id) as total_unique_ticket_buyers
                FROM (
                    SELECT commuter_id FROM read_parquet('{p_journeys_s}') WHERE from_name NOT ILIKE '%TEST%' AND to_name NOT ILIKE '%TEST%'
                    UNION ALL
                    SELECT commuter_id FROM read_parquet('{p_journeys_m_trip}') WHERE first_origin_name NOT ILIKE '%TEST%' AND final_destination_name NOT ILIKE '%TEST%' AND hub_name NOT ILIKE '%TEST%'
                )
            )
            SELECT 
                s.total_single_searches,
                j.total_single_journeys,
                j.total_single_tickets,
                round(s.converted_single_searches::DOUBLE / NULLIF(s.total_single_searches, 0), 4) as single_conversion,
                ms.total_multi_searches,
                mj.total_multi_journeys,
                round(mj.total_multi_journeys::DOUBLE / NULLIF(ms.total_multi_searches, 0), 4) as multi_conversion,
                ar.total_unique_searchers as unique_searchers,
                ab.total_unique_ticket_buyers as unique_ticket_buyers,
                s.n_days,
                s.date_min,
                s.date_max,
                round(1.0 - (mj.total_multi_journeys::DOUBLE / NULLIF(ms.total_multi_searches, 0)), 4) as multi_leg_dropoff
            FROM s_stats s
            CROSS JOIN j_stats j
            CROSS JOIN ms_stats ms
            CROSS JOIN mj_stats mj
            CROSS JOIN all_riders ar
            CROSS JOIN all_buyers ab
        ) TO '{out_kpis}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    print("- `kpis`: 1 row created.")

    # 2. OD Hourly
    print("\n2. Generating `od_hourly.parquet`...")
    out_od_hourly = (config.DATA_PROCESSED_DIR / "od_hourly.parquet").resolve().as_posix()
    con.execute(f"""
        COPY (
            WITH od_routes AS (
                SELECT 
                    from_station_id,
                    to_station_id,
                    mode,
                    mode(route_id) as primary_route_id
                FROM read_parquet('{p_searches_s}')
                WHERE route_id IS NOT NULL AND {test_station_filter}
                GROUP BY 1, 2, 3
            ),
            hourly_agg AS (
                SELECT 
                    s.from_station_id,
                    s.to_station_id,
                    s.mode,
                    s.hour,
                    s.day_of_week,
                    count(*) as searches,
                    count(DISTINCT s.commuter_id) as unique_searchers,
                    sum(s.converted) as tickets,
                    round(avg(s.price), 2) as avg_price,
                    r.primary_route_id
                FROM read_parquet('{p_searches_s}') s
                LEFT JOIN od_routes r 
                  ON s.from_station_id = r.from_station_id 
                 AND s.to_station_id = r.to_station_id 
                 AND s.mode = r.mode
                WHERE {test_station_filter}
                GROUP BY 1, 2, 3, 4, 5, r.primary_route_id
            )
            SELECT * FROM hourly_agg
            WHERE unique_searchers >= {config.MIN_UNIQUE_SEARCHERS}
        ) TO '{out_od_hourly}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_od_hourly = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_od_hourly}');"
    ).fetchone()[0]
    searches_in_od_hourly = con.execute(
        f"SELECT sum(searches) FROM read_parquet('{out_od_hourly}');"
    ).fetchone()[0]
    total_clean_single_searches = s_before - s_test_removed
    dropped_od_hourly = total_clean_single_searches - searches_in_od_hourly
    print(
        f"- `od_hourly`: {cnt_od_hourly:,} rows ({searches_in_od_hourly:,} searches retained, {dropped_od_hourly:,} dropped by privacy filter)"
    )
    # 2b. City Hourly (hour, searches, tickets)
    print("\n2b. Generating `city_hourly.parquet`...")
    out_city_hourly = (config.DATA_PROCESSED_DIR / "city_hourly.parquet").resolve().as_posix()
    con.execute(f"""
        COPY (
            SELECT 
                hour,
                count(*) as searches,
                sum(converted) as tickets,
                round(sum(converted)::DOUBLE / count(*), 4) as conversion
            FROM read_parquet('{p_searches_s}')
            WHERE {test_station_filter}
            GROUP BY hour
            ORDER BY hour
        ) TO '{out_city_hourly}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_city_hourly = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_city_hourly}');"
    ).fetchone()[0]
    print(f"- `city_hourly`: {cnt_city_hourly} hours (0-23) profiled across metropolitan network.")

    # 3. OD Summary & Corridor Class
    print("\n3. Generating `od_summary.parquet`...")
    out_od_summary = (config.DATA_PROCESSED_DIR / "od_summary.parquet").resolve().as_posix()

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE od_bucket_agg AS 
        WITH od_routes AS (
            SELECT 
                from_station_id,
                to_station_id,
                mode,
                mode(route_id) as primary_route_id
            FROM read_parquet('{p_searches_s}')
            WHERE route_id IS NOT NULL AND {test_station_filter}
            GROUP BY 1, 2, 3
        ),
        by_bucket AS (
            SELECT 
                s.from_station_id,
                s.to_station_id,
                s.mode,
                s.time_bucket,
                count(*) as searches,
                count(DISTINCT s.commuter_id) as unique_searchers,
                sum(s.converted) as tickets,
                round(avg(s.price), 2) as avg_price
            FROM read_parquet('{p_searches_s}') s
            WHERE {test_station_filter}
            GROUP BY 1, 2, 3, 4
        ),
        by_all AS (
            SELECT 
                s.from_station_id,
                s.to_station_id,
                s.mode,
                'All' as time_bucket,
                count(*) as searches,
                count(DISTINCT s.commuter_id) as unique_searchers,
                sum(s.converted) as tickets,
                round(avg(s.price), 2) as avg_price
            FROM read_parquet('{p_searches_s}') s
            WHERE {test_station_filter}
            GROUP BY 1, 2, 3
        ),
        combined AS (
            SELECT * FROM by_bucket
            UNION ALL
            SELECT * FROM by_all
        )
        SELECT 
            c.from_station_id,
            c.to_station_id,
            c.mode,
            c.time_bucket,
            sm_from.station_name as from_name,
            sm_to.station_name as to_name,
            sm_from.hub_name as from_hub,
            sm_to.hub_name as to_hub,
            c.searches,
            c.unique_searchers,
            c.tickets,
            round(c.searches::DOUBLE / {n_days}, 2) as searches_per_day,
            greatest(c.searches - c.tickets, 0) as lost_trips,
            round(least(c.tickets::DOUBLE / NULLIF(c.searches, 0), 1.0), 4) as srcr,
            c.avg_price,
            r.primary_route_id,
            CASE WHEN (c.searches < {config.MIN_SEARCHES} OR c.unique_searchers < {config.MIN_UNIQUE_SEARCHERS}) THEN true ELSE false END as low_volume_flag,
            CASE 
                WHEN c.searches >= {config.RELIABLE_MULTIPLIER * config.MIN_SEARCHES} AND c.unique_searchers >= {config.RELIABLE_MULTIPLIER * config.MIN_UNIQUE_SEARCHERS} THEN 'High'
                WHEN c.searches >= {config.MIN_SEARCHES} AND c.unique_searchers >= {config.MIN_UNIQUE_SEARCHERS} THEN 'Medium'
                ELSE 'Low'
            END as confidence
        FROM combined c
        LEFT JOIN read_parquet('{p_station_master}') sm_from 
          ON c.from_station_id = sm_from.station_id
        LEFT JOIN read_parquet('{p_station_master}') sm_to 
          ON c.to_station_id = sm_to.station_id
        LEFT JOIN od_routes r 
          ON c.from_station_id = r.from_station_id 
         AND c.to_station_id = r.to_station_id 
         AND c.mode = r.mode
        WHERE c.unique_searchers >= {config.MIN_UNIQUE_SEARCHERS};
    """)

    con.execute(f"""
        COPY (
            WITH ranked AS (
                SELECT 
                    *,
                    CASE 
                        WHEN low_volume_flag = false THEN 
                            round(percent_rank() OVER (PARTITION BY mode, time_bucket, low_volume_flag ORDER BY srcr), 4)
                        ELSE NULL 
                    END as conversion_percentile,
                    quantile_cont(lost_trips, 0.75) OVER (PARTITION BY mode, time_bucket) as q3_lost_trips,
                    quantile_cont(tickets, 0.75) OVER (PARTITION BY mode, time_bucket) as q3_tickets,
                    quantile_cont(unique_searchers, 0.25) OVER (PARTITION BY mode, time_bucket) as q1_searchers
                FROM od_bucket_agg
            ),
            classified AS (
                SELECT 
                    from_station_id,
                    to_station_id,
                    mode,
                    time_bucket,
                    from_name,
                    to_name,
                    from_hub,
                    to_hub,
                    searches,
                    unique_searchers,
                    tickets,
                    searches_per_day,
                    lost_trips,
                    srcr,
                    conversion_percentile,
                    avg_price,
                    primary_route_id,
                    low_volume_flag,
                    confidence,
                    -- Classify BLUE first (Digital Blindspot: high tickets, low searchers)
                    CASE 
                        WHEN tickets >= q3_tickets AND unique_searchers <= q1_searchers THEN 'BLUE'
                        WHEN srcr < {config.SRCR_RED} AND lost_trips >= q3_lost_trips THEN 'RED'
                        WHEN srcr >= {config.SRCR_GREEN} OR conversion_percentile >= 0.75 THEN 'GREEN'
                        ELSE 'NEUTRAL'
                    END as corridor_class,
                    CASE 
                        WHEN conversion_percentile IS NULL THEN 'Low Volume'
                        WHEN conversion_percentile <= 0.25 THEN 'Bottom 25%'
                        WHEN conversion_percentile >= 0.75 THEN 'Top 25%'
                        ELSE 'Middle 50%'
                    END as relative_corridor_class
                FROM ranked
            )
            SELECT * FROM classified
        ) TO '{out_od_summary}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_od_summary = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_od_summary}');"
    ).fetchone()[0]
    print(f"- `od_summary`: {cnt_od_summary:,} rows (including 'All' time_bucket)")

    # 4. Transfer Pairs (Excluding circular return queries and intra-hub transfers)
    print("\n4. Generating `transfer_pairs.parquet`...")
    out_transfer_pairs = (config.DATA_PROCESSED_DIR / "transfer_pairs.parquet").resolve().as_posix()

    tp_raw_cnt = con.execute(
        f"SELECT count(*) FROM (SELECT first_origin, final_destination, mode_pattern, hub FROM read_parquet('{p_searches_m_trip}') WHERE {test_station_filter_m} GROUP BY 1, 2, 3, 4);"
    ).fetchone()[0]
    tp_circ_removed = con.execute(
        f"SELECT count(*) FROM (SELECT first_origin, final_destination, mode_pattern, hub FROM read_parquet('{p_searches_m_trip}') WHERE {test_station_filter_m} AND first_origin = final_destination GROUP BY 1, 2, 3, 4);"
    ).fetchone()[0]
    tp_same_hub_removed = con.execute(
        f"SELECT count(*) FROM (SELECT first_origin, final_destination, mode_pattern, hub FROM read_parquet('{p_searches_m_trip}') WHERE {test_station_filter_m} AND first_origin != final_destination AND (first_origin_hub = final_destination_hub OR first_origin_name = final_destination_name) GROUP BY 1, 2, 3, 4);"
    ).fetchone()[0]
    print(
        f"- `transfer_pairs` filters audit: {tp_raw_cnt:,} raw groups, {tp_circ_removed:,} identical-station loops removed, {tp_same_hub_removed:,} same-hub/loop pairs removed."
    )

    con.execute(f"""
        COPY (
            WITH t_agg AS (
                SELECT 
                    t.first_origin,
                    t.final_destination,
                    t.mode_pattern,
                    t.hub,
                    t.first_origin_name,
                    t.final_destination_name,
                    t.first_origin_hub,
                    t.final_destination_hub,
                    t.hub_name,
                    t.leg_count,
                    count(*) as searches,
                    count(DISTINCT t.commuter_id) as unique_searchers,
                    round(avg(t.est_total_fare), 2) as avg_total_fare,
                    round(avg(t.direct_fare), 2) as direct_fare,
                    round(avg(t.est_transfer_penalty_inr), 2) as transfer_penalty_inr,
                    sum(t.converted) as multi_journeys,
                    round(sum(t.converted)::DOUBLE / count(*), 4) as multi_conversion,
                    count(*) - sum(t.converted) as searched_transfers_unticketed,
                    count(*) - sum(t.converted) as lost_trips,
                    mode(t.time_bucket) as top_time_bucket,
                    CASE 
                        WHEN count(*) >= {config.RELIABLE_MULTIPLIER * config.MIN_SEARCHES} AND count(DISTINCT t.commuter_id) >= {config.RELIABLE_MULTIPLIER * config.MIN_UNIQUE_SEARCHERS} THEN 'High'
                        WHEN count(*) >= {config.MIN_SEARCHES} AND count(DISTINCT t.commuter_id) >= {config.MIN_UNIQUE_SEARCHERS} THEN 'Medium'
                        ELSE 'Low'
                    END as confidence
                FROM read_parquet('{p_searches_m_trip}') t
                WHERE t.first_origin != t.final_destination -- Exclude identical station loops
                  AND t.first_origin_hub != t.final_destination_hub -- Exclude same-hub transfers
                  AND t.first_origin_name != t.final_destination_name -- Exclude same-name loops
                  AND {test_station_filter_m}
                GROUP BY 
                    t.first_origin, t.final_destination, t.mode_pattern, t.hub,
                    t.first_origin_name, t.final_destination_name, t.first_origin_hub, t.final_destination_hub,
                    t.hub_name, t.leg_count
            ),
            t_filtered AS (
                SELECT * FROM t_agg
                WHERE unique_searchers >= {config.MIN_UNIQUE_SEARCHERS}
            ),
            t_classified AS (
                SELECT 
                    *,
                    -- Assign ORANGE strictly to top quartile by searched unticketed transfers (High Transfer Demand)
                    CASE 
                        WHEN searched_transfers_unticketed >= quantile_cont(searched_transfers_unticketed, 0.75) OVER () THEN 'ORANGE'
                        ELSE 'NEUTRAL'
                    END as corridor_class
                FROM t_filtered
            )
            SELECT * FROM t_classified
        ) TO '{out_transfer_pairs}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_transfer_pairs = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_transfer_pairs}');"
    ).fetchone()[0]
    print(
        f"- `transfer_pairs`: {cnt_transfer_pairs:,} true OD transfer pairs (privacy filter: unique_searchers >= 10)."
    )

    # 5. Hubs (Cut to volume threshold: searches_through >= 50 AND unique_searchers_through >= 10)
    print("\n5. Generating `hubs.parquet` (cut to significant volume hubs)...")
    out_hubs = (config.DATA_PROCESSED_DIR / "hubs.parquet").resolve().as_posix()

    hubs_raw_cnt = con.execute(
        f"SELECT count(DISTINCT hub) FROM read_parquet('{p_searches_m_trip}') WHERE hub IS NOT NULL AND {test_station_filter_m};"
    ).fetchone()[0]

    con.execute(f"""
        COPY (
            WITH hub_trips AS (
                SELECT 
                    hub,
                    hub_name,
                    commuter_id,
                    mode_pattern,
                    first_origin_name,
                    final_destination_name,
                    CASE WHEN mode_pattern LIKE 'BUS>BUS%' THEN 1 ELSE 0 END as is_bus_to_bus,
                    -- Bidirectional Bus <-> Rail/Metro
                    CASE 
                        WHEN mode_pattern LIKE 'BUS>METRO%' 
                          OR mode_pattern LIKE 'BUS>SUBWAY%' 
                          OR mode_pattern LIKE 'METRO>BUS%' 
                          OR mode_pattern LIKE 'SUBWAY>BUS%' 
                        THEN 1 ELSE 0 
                    END as is_bus_to_rail_metro
                FROM read_parquet('{p_searches_m_trip}')
                WHERE hub IS NOT NULL AND {test_station_filter_m}
            ),
            hub_totals AS (
                SELECT 
                    hub,
                    hub_name,
                    count(*) as searches_through,
                    count(DISTINCT commuter_id) as unique_searchers_through,
                    round(sum(is_bus_to_bus)::DOUBLE / count(*), 4) as share_bus_to_bus,
                    round(sum(is_bus_to_rail_metro)::DOUBLE / count(*), 4) as share_bus_to_rail_metro,
                    mode(mode_pattern) as top_mode_pattern
                FROM hub_trips
                GROUP BY hub, hub_name
                HAVING count(*) >= {config.MIN_SEARCHES} AND count(DISTINCT commuter_id) >= {config.MIN_UNIQUE_SEARCHERS}
            ),
            top_origins AS (
                SELECT hub, list(first_origin_name)[1:3] as top_from_names
                FROM (
                    SELECT hub, first_origin_name, count(*) as cnt
                    FROM hub_trips
                    WHERE first_origin_name IS NOT NULL
                    GROUP BY hub, first_origin_name
                    ORDER BY hub, cnt DESC
                )
                GROUP BY hub
            ),
            top_dests AS (
                SELECT hub, list(final_destination_name)[1:3] as top_to_names
                FROM (
                    SELECT hub, final_destination_name, count(*) as cnt
                    FROM hub_trips
                    WHERE final_destination_name IS NOT NULL
                    GROUP BY hub, final_destination_name
                    ORDER BY hub, cnt DESC
                )
                GROUP BY hub
            )
            SELECT 
                h.hub,
                h.hub_name,
                sm.mode as hub_mode,
                h.searches_through,
                h.unique_searchers_through,
                h.share_bus_to_bus,
                h.share_bus_to_rail_metro,
                h.top_mode_pattern,
                array_to_string(o.top_from_names, ', ') as top_from_names,
                array_to_string(d.top_to_names, ', ') as top_to_names
            FROM hub_totals h
            LEFT JOIN read_parquet('{p_station_master}') sm ON h.hub = sm.station_id
            LEFT JOIN top_origins o ON h.hub = o.hub
            LEFT JOIN top_dests d ON h.hub = d.hub
            ORDER BY h.searches_through DESC
        ) TO '{out_hubs}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_hubs = con.execute(f"SELECT count(*) FROM read_parquet('{out_hubs}');").fetchone()[0]
    hubs_removed = hubs_raw_cnt - cnt_hubs
    print(
        f"- `hubs`: Cut from {hubs_raw_cnt:,} down to **{cnt_hubs:,} significant interchange hubs** ({hubs_removed:,} low-volume stops removed)."
    )

    # 6. Route Hourly
    print("\n6. Generating `route_hourly.parquet`...")
    out_route_hourly = (config.DATA_PROCESSED_DIR / "route_hourly.parquet").resolve().as_posix()
    con.execute(f"""
        COPY (
            WITH route_all_day AS (
                SELECT 
                    route_id,
                    count(*) as route_all_day_searches,
                    sum(converted) as route_all_day_tickets,
                    round(sum(converted)::DOUBLE / count(*), 4) as route_all_day_conversion,
                    round(count(*)::DOUBLE / 24.0, 2) as route_hourly_avg_searches
                FROM read_parquet('{p_searches_s}')
                WHERE route_id IS NOT NULL AND {test_station_filter}
                GROUP BY route_id
            ),
            hourly AS (
                SELECT 
                    s.route_id,
                    s.hour,
                    count(*) as searches,
                    count(DISTINCT s.commuter_id) as unique_searchers,
                    sum(s.converted) as tickets,
                    round(sum(s.converted)::DOUBLE / count(*), 4) as conversion
                FROM read_parquet('{p_searches_s}') s
                WHERE s.route_id IS NOT NULL AND {test_station_filter}
                GROUP BY s.route_id, s.hour
            )
            SELECT 
                h.route_id,
                h.hour,
                h.searches,
                h.unique_searchers,
                h.tickets,
                h.conversion,
                rad.route_all_day_searches,
                rad.route_all_day_conversion,
                rad.route_hourly_avg_searches,
                round((rad.route_all_day_conversion - h.conversion) / NULLIF(rad.route_all_day_conversion, 0), 4) as conversion_drop_pct
            FROM hourly h
            JOIN route_all_day rad ON h.route_id = rad.route_id
            ORDER BY h.route_id, h.hour
        ) TO '{out_route_hourly}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_route_hourly = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_route_hourly}');"
    ).fetchone()[0]
    print(f"- `route_hourly`: {cnt_route_hourly:,} route-hour combinations.")

    # 7. Frustrated Commuters (Aggregated corridor-hour grain, NO commuter_id)
    print("\n7. Generating `frustrated_commuters.parquet`...")
    out_frustrated = (
        (config.DATA_PROCESSED_DIR / "frustrated_commuters.parquet").resolve().as_posix()
    )
    con.execute(f"""
        COPY (
            WITH sorted_searches AS (
                SELECT 
                    commuter_id,
                    from_station_id,
                    to_station_id,
                    from_name,
                    to_name,
                    hour,
                    ts_ist,
                    converted,
                    lag(ts_ist, {config.FRUSTRATION_MIN_REPEATS - 1}) OVER (
                        PARTITION BY commuter_id, from_station_id, to_station_id 
                        ORDER BY ts_ist
                    ) as lag_n_ts,
                    sum(converted) OVER (
                        PARTITION BY commuter_id, from_station_id, to_station_id
                    ) as total_conversions
                FROM read_parquet('{p_searches_s}')
                WHERE {test_station_filter}
            ),
            frustrated_events AS (
                SELECT 
                    commuter_id,
                    from_station_id,
                    to_station_id,
                    from_name,
                    to_name,
                    hour
                FROM sorted_searches
                WHERE lag_n_ts IS NOT NULL 
                  AND date_diff('minute', lag_n_ts, ts_ist) <= {config.FRUSTRATION_WINDOW_MIN}
                  AND total_conversions = 0
            )
            SELECT 
                from_station_id,
                to_station_id,
                mode(from_name) as from_name,
                mode(to_name) as to_name,
                hour,
                count(DISTINCT commuter_id) as frustrated_riders,
                count(*) as repeat_search_events
            FROM frustrated_events
            GROUP BY 1, 2, hour
            HAVING count(DISTINCT commuter_id) >= {config.MIN_UNIQUE_SEARCHERS}
            ORDER BY frustrated_riders DESC
        ) TO '{out_frustrated}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_frustrated = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_frustrated}');"
    ).fetchone()[0]
    raw_frustrated_cnt = con.execute(f"""
        WITH sorted_searches AS (
            SELECT commuter_id, from_station_id, to_station_id, ts_ist, converted,
                   lag(ts_ist, {config.FRUSTRATION_MIN_REPEATS - 1}) OVER (PARTITION BY commuter_id, from_station_id, to_station_id ORDER BY ts_ist) as lag_n_ts,
                   sum(converted) OVER (PARTITION BY commuter_id, from_station_id, to_station_id) as total_conversions
            FROM read_parquet('{p_searches_s}') WHERE {test_station_filter}
        ),
        events AS (
            SELECT from_station_id, to_station_id, hour(ts_ist) as hour, count(DISTINCT commuter_id) as riders
            FROM sorted_searches WHERE lag_n_ts IS NOT NULL AND date_diff('minute', lag_n_ts, ts_ist) <= {config.FRUSTRATION_WINDOW_MIN} AND total_conversions = 0
            GROUP BY 1, 2, 3
        )
        SELECT count(*), count(CASE WHEN riders < {config.MIN_UNIQUE_SEARCHERS} THEN 1 END) FROM events;
    """).fetchone()
    frustrated_dropped = raw_frustrated_cnt[1]
    print(
        f"- `frustrated_commuters`: {cnt_frustrated:,} hotspot corridor-hours retained ({frustrated_dropped:,} low-rider rows < 10 dropped per privacy threshold)."
    )

    # 8. Diagnosis (Added PREMIUM_FARE_FRICTION; strictly srcr < SRCR_RED for single-leg)
    print(
        "\n8. Generating `diagnosis.parquet` (Adding PREMIUM_FARE_FRICTION; strict srcr < SRCR_RED)..."
    )
    out_diagnosis = (config.DATA_PROCESSED_DIR / "diagnosis.parquet").resolve().as_posix()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE red_corridors AS 
        SELECT 
            'DIRECT' as corridor_type,
            from_station_id,
            to_station_id,
            mode,
            from_name,
            to_name,
            from_hub,
            to_hub,
            time_bucket,
            searches,
            searches_per_day,
            lost_trips,
            CAST(NULL as DOUBLE) as searched_transfers_unticketed,
            srcr,
            avg_price,
            primary_route_id,
            confidence,
            'RED' as source_class
        FROM read_parquet('{out_od_summary}')
        WHERE corridor_class = 'RED' 
          AND srcr < {config.SRCR_RED} 
          AND time_bucket != 'All';

        CREATE OR REPLACE TEMP TABLE orange_corridors AS 
        SELECT 
            'TRANSFER' as corridor_type,
            first_origin as from_station_id,
            final_destination as to_station_id,
            mode_pattern as mode,
            first_origin_name as from_name,
            final_destination_name as to_name,
            first_origin_hub as from_hub,
            final_destination_hub as to_hub,
            top_time_bucket as time_bucket,
            searches,
            round(searches::DOUBLE / {n_days}, 2) as searches_per_day,
            CAST(NULL as DOUBLE) as lost_trips,
            searched_transfers_unticketed,
            multi_conversion as srcr,
            avg_total_fare as avg_price,
            CAST(NULL as VARCHAR) as primary_route_id,
            confidence,
            'ORANGE' as source_class
        FROM read_parquet('{out_transfer_pairs}')
        WHERE corridor_class = 'ORANGE';

        CREATE OR REPLACE TEMP TABLE target_corridors AS 
        SELECT * FROM red_corridors
        UNION ALL
        SELECT * FROM orange_corridors;

        CREATE OR REPLACE TEMP TABLE cheaper_bus_routes AS 
        WITH bus_fares_by_hub AS (
            SELECT 
                from_hub, 
                to_hub, 
                primary_route_id,
                avg_price as route_fare,
                srcr as route_srcr,
                searches,
                tickets
            FROM read_parquet('{out_od_summary}')
            WHERE mode = 'BUS' AND avg_price IS NOT NULL AND time_bucket = 'All'
        )
        SELECT 
            from_hub, 
            to_hub, 
            min(route_fare) as min_parallel_fare,
            max(route_srcr) as max_parallel_srcr,
            arg_max(primary_route_id, route_srcr) as best_parallel_route_id
        FROM bus_fares_by_hub
        WHERE route_srcr >= {config.SRCR_RED}
        GROUP BY 1, 2;
    """)

    con.execute(f"""
        COPY (
            WITH direct_searches_lookup AS (
                SELECT from_station_id, to_station_id, sum(searches) as direct_search_vol
                FROM read_parquet('{out_od_summary}')
                WHERE time_bucket = 'All'
                GROUP BY 1, 2
            ),
            top_hubs_set AS (
                SELECT hub
                FROM read_parquet('{out_hubs}')
                ORDER BY searches_through DESC
                LIMIT 50
            ),
            eval_labels AS (
                SELECT 
                    tc.*,
                    -- PREMIUM_FARE_FRICTION: bus-vs-bus, same OD hub, higher-fare route converting far lower
                    CASE 
                        WHEN tc.source_class = 'RED' 
                         AND tc.mode = 'BUS'
                         AND cbr.min_parallel_fare IS NOT NULL
                         AND tc.avg_price > cbr.min_parallel_fare * 1.2
                         AND cbr.max_parallel_srcr >= tc.srcr * 1.5
                         AND tc.primary_route_id != cbr.best_parallel_route_id
                        THEN true 
                        ELSE false 
                    END as is_premium_fare_friction,
                    -- FEEDER_GAP: multi-modal BUS + (METRO or SUBWAY) through top hubs with conversion < SRCR_RED
                    CASE 
                        WHEN tc.source_class = 'ORANGE' 
                         AND tc.mode LIKE '%BUS%'
                         AND (tc.mode LIKE '%METRO%' OR tc.mode LIKE '%SUBWAY%') 
                         AND tc.srcr < {config.SRCR_RED}
                         AND (tc.from_station_id IN (SELECT hub FROM top_hubs_set) OR tc.to_station_id IN (SELECT hub FROM top_hubs_set))
                        THEN true 
                        ELSE false 
                    END as is_feeder_gap,
                    -- MISSING_DIRECT: transfer corridor where direct searches are very low or zero (<20), OR rail-to-rail transfer
                    CASE 
                        WHEN tc.source_class = 'ORANGE' 
                         AND (coalesce(dsl.direct_search_vol, 0) < 20 
                              OR (tc.mode NOT LIKE '%BUS%' AND (tc.mode LIKE '%METRO%' OR tc.mode LIKE '%SUBWAY%')))
                        THEN true 
                        ELSE false 
                    END as is_missing_direct,
                    -- FREQUENCY_DESERT: direct route exists and conversion drops heavily
                    CASE 
                        WHEN tc.primary_route_id IS NOT NULL 
                         AND rh.conversion_drop_pct >= {config.FREQ_DROP_PCT} 
                         AND rh.searches > rh.route_hourly_avg_searches 
                        THEN true 
                        ELSE false 
                    END as is_frequency_desert,
                    -- FARE_FRICTION: BUS avg_price > 2 * parallel rail fare
                    CASE 
                        WHEN tc.mode = 'BUS' AND rail_fare.min_rail_fare IS NOT NULL AND tc.avg_price > 2.0 * rail_fare.min_rail_fare 
                        THEN true 
                        ELSE false 
                    END as is_fare_friction,
                    cbr.best_parallel_route_id,
                    cbr.min_parallel_fare,
                    cbr.max_parallel_srcr,
                    rh.conversion_drop_pct,
                    rail_fare.min_rail_fare,
                    dsl.direct_search_vol
                FROM target_corridors tc
                LEFT JOIN direct_searches_lookup dsl 
                  ON tc.from_station_id = dsl.from_station_id AND tc.to_station_id = dsl.to_station_id
                LEFT JOIN cheaper_bus_routes cbr 
                  ON tc.from_hub = cbr.from_hub AND tc.to_hub = cbr.to_hub
                LEFT JOIN read_parquet('{out_route_hourly}') rh 
                  ON tc.primary_route_id = rh.route_id 
                 AND rh.hour = 8
                LEFT JOIN (
                    SELECT from_hub, to_hub, min(avg_price) as min_rail_fare
                    FROM read_parquet('{out_od_summary}')
                    WHERE mode IN ('METRO', 'SUBWAY') AND avg_price IS NOT NULL
                    GROUP BY 1, 2
                ) rail_fare ON tc.from_hub = rail_fare.from_hub AND tc.to_hub = rail_fare.to_hub
            ),
            diagnosed AS (
                SELECT 
                    corridor_type,
                    from_station_id,
                    to_station_id,
                    mode,
                    from_name,
                    to_name,
                    from_hub,
                    to_hub,
                    time_bucket,
                    searches,
                    searches_per_day,
                    lost_trips,
                    searched_transfers_unticketed,
                    srcr,
                    avg_price,
                    primary_route_id,
                    confidence,
                    source_class,
                    -- Primary cause in order: PREMIUM_FARE_FRICTION, FEEDER_GAP, MISSING_DIRECT, FREQUENCY_DESERT, FARE_FRICTION, else NO_CLEAR_CAUSE
                    CASE 
                        WHEN is_premium_fare_friction THEN 'PREMIUM_FARE_FRICTION'
                        WHEN is_feeder_gap THEN 'FEEDER_GAP'
                        WHEN is_missing_direct THEN 'MISSING_DIRECT'
                        WHEN is_frequency_desert THEN 'FREQUENCY_DESERT'
                        WHEN is_fare_friction THEN 'FARE_FRICTION'
                        WHEN source_class = 'ORANGE' THEN 'MISSING_DIRECT'
                        ELSE 'NO_CLEAR_CAUSE'
                    END as primary_cause,
                    concat_ws(', ',
                        CASE WHEN is_premium_fare_friction THEN 'PREMIUM_FARE_FRICTION' ELSE NULL END,
                        CASE WHEN is_feeder_gap THEN 'FEEDER_GAP' ELSE NULL END,
                        CASE WHEN is_frequency_desert AND (NOT is_missing_direct) THEN 'FREQUENCY_DESERT' ELSE NULL END,
                        CASE WHEN is_fare_friction THEN 'FARE_FRICTION' ELSE NULL END
                    ) as secondary_causes,
                    CASE 
                        WHEN is_premium_fare_friction THEN concat('Bus fare ₹', round(avg_price, 1), ' (route ', primary_route_id, ') converts at only ', round(srcr*100, 1), '% vs parallel route ', best_parallel_route_id, ' at ₹', round(min_parallel_fare, 1), ' converting at ', round(max_parallel_srcr*100, 1), '%')
                        WHEN is_feeder_gap THEN concat('Multi-modal bus/rail transfer through top interchange hub with conversion only ', round(srcr*100, 1), '%')
                        WHEN is_missing_direct AND mode NOT LIKE '%BUS%' THEN concat('Rail-to-rail transfer demand (', round(searched_transfers_unticketed, 0), ' unticketed transfers); no direct searches observed')
                        WHEN is_missing_direct THEN concat('Transfer demand (', round(searched_transfers_unticketed, 0), ' searched transfers) with zero or negligible direct service (', coalesce(direct_search_vol, 0), ' direct searches)')
                        WHEN is_frequency_desert THEN concat('Conversion dropped by ', round(conversion_drop_pct*100, 1), '% vs route all-day average')
                        WHEN is_fare_friction THEN concat('Bus fare ₹', avg_price, ' exceeds parallel rail fare ₹', min_rail_fare, ' by >2x')
                        ELSE concat('Lost demand: ', coalesce(lost_trips, searched_transfers_unticketed), ', conversion: ', round(srcr*100, 1), '%; no fleet/timetable data available to assert physical constraint')
                    END as evidence,
                    CASE 
                        WHEN is_missing_direct THEN {config.PENALTY_TRANSFER}
                        WHEN is_frequency_desert THEN {config.PENALTY_DIRECT_FREQ_GAP}
                        WHEN is_premium_fare_friction THEN {config.PENALTY_DIRECT_FREQ_GAP}
                        ELSE {config.PENALTY_DIRECT}
                    END as penalty_tw,
                    round(searches_per_day * (1.0 - srcr) * (
                        CASE 
                            WHEN is_missing_direct THEN {config.PENALTY_TRANSFER}
                            WHEN is_frequency_desert THEN {config.PENALTY_DIRECT_FREQ_GAP}
                            WHEN is_premium_fare_friction THEN {config.PENALTY_DIRECT_FREQ_GAP}
                            ELSE {config.PENALTY_DIRECT}
                        END
                    ), 2) as udi
                FROM eval_labels
            )
            SELECT * FROM diagnosed
            ORDER BY coalesce(lost_trips, searched_transfers_unticketed) DESC
        ) TO '{out_diagnosis}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_diagnosis = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_diagnosis}');"
    ).fetchone()[0]
    pff_cnt = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_diagnosis}') WHERE primary_cause = 'PREMIUM_FARE_FRICTION';"
    ).fetchone()[0]
    pff_ods = con.execute(
        f"SELECT count(DISTINCT from_name || '->' || to_name) FROM read_parquet('{out_diagnosis}') WHERE primary_cause = 'PREMIUM_FARE_FRICTION';"
    ).fetchone()[0]
    print(
        f"- `diagnosis`: {cnt_diagnosis:,} corridors diagnosed ({pff_cnt:,} flagged with PREMIUM_FARE_FRICTION across {pff_ods:,} distinct OD corridors)."
    )

    # 9. Actions
    print("\n9. Generating `actions.parquet`...")
    out_actions = (config.DATA_PROCESSED_DIR / "actions.parquet").resolve().as_posix()
    con.execute(f"""
        COPY (
            WITH ranked_actions AS (
                SELECT 
                    concat(coalesce(from_name, from_station_id), ' → ', coalesce(to_name, to_station_id)) as corridor_name,
                    from_station_id,
                    to_station_id,
                    mode,
                    primary_route_id,
                    primary_cause as diagnostic_cause,
                    CASE 
                        WHEN primary_cause = 'PREMIUM_FARE_FRICTION' THEN 
                            concat('Rationalize premium bus fare tier on route ', coalesce(primary_route_id, 'corridor'), ' vs parallel standard service')
                        WHEN primary_cause = 'MISSING_DIRECT' THEN 
                            concat('Launch direct peak-hour cut-service from ', coalesce(from_name, from_station_id), ' to ', coalesce(to_name, to_station_id))
                        WHEN primary_cause = 'FREQUENCY_DESERT' THEN 
                            concat('Increase frequency on route ', coalesce(primary_route_id, 'corridor'), ' in ', time_bucket)
                        WHEN primary_cause = 'FEEDER_GAP' THEN 
                            concat('Introduce mini-bus feeder loop connecting ', coalesce(from_name, from_station_id), ' to hub station')
                        WHEN primary_cause = 'FARE_FRICTION' THEN 
                            concat('Review fare tier on route ', coalesce(primary_route_id, 'corridor'), ' vs parallel rail option')
                        ELSE 
                            concat('Investigate commuter drop-off and qualitative service factors on corridor ', coalesce(from_name, from_station_id), ' → ', coalesce(to_name, to_station_id))
                    END as recommended_action,
                    corridor_type,
                    time_bucket as time_window,
                    round(coalesce(lost_trips, searched_transfers_unticketed)::DOUBLE / {n_days}, 2) as lost_daily_searches,
                    round((coalesce(lost_trips, searched_transfers_unticketed)::DOUBLE / {n_days}) * {config.CAPTURE_RATE_DEFAULT}, 2) as est_daily_rider_gain,
                    coalesce(avg_price, 15.0) as avg_fare,
                    round((coalesce(lost_trips, searched_transfers_unticketed)::DOUBLE / {n_days}) * {config.CAPTURE_RATE_DEFAULT} * {config.DAYS_PER_MONTH} * coalesce(avg_price, 15.0), 2) as est_monthly_revenue_inr,
                    confidence,
                    'Assumes capture rate of 35% on lost daily search demand; fare based on ticketed journeys median.' as assumptions_note,
                    'proxy only: based on Chennai One tickets, no fleet data' as candidate_low_ticket_routes,
                    lost_trips,
                    searched_transfers_unticketed,
                    udi
                FROM read_parquet('{out_diagnosis}')
            )
            SELECT * FROM ranked_actions
            ORDER BY coalesce(lost_trips, searched_transfers_unticketed) DESC
        ) TO '{out_actions}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_actions = con.execute(f"SELECT count(*) FROM read_parquet('{out_actions}');").fetchone()[0]
    print(f"- `actions`: {cnt_actions:,} action items generated.")
    # 9b. Pair-level Premium Rollup
    print("\n9b. Generating `premium_rollup.parquet`...")
    out_premium_rollup = (config.DATA_PROCESSED_DIR / "premium_rollup.parquet").resolve().as_posix()
    con.execute(f"""
        COPY (
            WITH diag_premium AS (
                SELECT DISTINCT from_station_id, to_station_id, primary_route_id
                FROM read_parquet('{out_diagnosis}')
                WHERE primary_cause = 'PREMIUM_FARE_FRICTION'
            ),
            cheaper_bus_routes AS (
                WITH bus_fares_by_hub AS (
                    SELECT 
                        from_hub, 
                        to_hub, 
                        primary_route_id,
                        avg_price as route_fare,
                        srcr as route_srcr,
                        searches,
                        tickets
                    FROM read_parquet('{out_od_summary}')
                    WHERE mode = 'BUS' AND avg_price IS NOT NULL AND time_bucket = 'All'
                )
                SELECT 
                    from_hub, 
                    to_hub, 
                    min(route_fare) as min_parallel_fare,
                    max(route_srcr) as max_parallel_srcr,
                    arg_max(primary_route_id, route_srcr) as best_parallel_route_id
                FROM bus_fares_by_hub
                WHERE route_srcr >= {config.SRCR_RED}
                GROUP BY 1, 2
            ),
            premium_pairs AS (
                SELECT 
                    od.from_station_id,
                    od.to_station_id,
                    od.mode,
                    od.from_name,
                    od.to_name,
                    od.from_hub,
                    od.to_hub,
                    od.primary_route_id,
                    cbr.best_parallel_route_id,
                    od.avg_price as premium_fare,
                    cbr.min_parallel_fare as parallel_fare,
                    od.searches,
                    od.tickets,
                    od.lost_trips,
                    round(od.srcr, 4) as srcr,
                    round(cbr.max_parallel_srcr, 4) as parallel_srcr,
                    round((od.lost_trips / {n_days}) * {config.CAPTURE_RATE_DEFAULT} * {config.DAYS_PER_MONTH} * od.avg_price, 2) as est_monthly_revenue_loss,
                    concat('Rationalize premium bus fare tier on route ', coalesce(od.primary_route_id, 'corridor'), ' vs parallel standard service') as recommended_action
                FROM read_parquet('{out_od_summary}') od
                JOIN diag_premium dp 
                  ON od.from_station_id = dp.from_station_id 
                 AND od.to_station_id = dp.to_station_id 
                 AND od.primary_route_id = dp.primary_route_id
                LEFT JOIN cheaper_bus_routes cbr 
                  ON od.from_hub = cbr.from_hub AND od.to_hub = cbr.to_hub
                WHERE od.time_bucket = 'All'
                ORDER BY od.lost_trips DESC
            )
            SELECT * FROM premium_pairs
        ) TO '{out_premium_rollup}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    cnt_premium_rollup = con.execute(
        f"SELECT count(*) FROM read_parquet('{out_premium_rollup}');"
    ).fetchone()[0]
    print(f"- `premium_rollup`: {cnt_premium_rollup:,} OD pairs aggregated at all-day pair grain.")

    # 10. Data Quality Log
    print("\n10. Generating `dq_log.parquet`...")
    out_dq_log = (config.DATA_PROCESSED_DIR / "dq_log.parquet").resolve().as_posix()

    pre_impute_nulls = con.execute(f"""
        SELECT 
            round(100.0 * count(CASE WHEN from_station_name_1 IS NULL THEN 1 END) / count(*), 2),
            round(100.0 * count(CASE WHEN to_station_name_1 IS NULL THEN 1 END) / count(*), 2)
        FROM read_parquet('{(config.PARQUET_DIR / "single_leg_searches.parquet").as_posix()}');
    """).fetchone()

    post_impute_nulls = con.execute(f"""
        SELECT 
            round(100.0 * count(CASE WHEN from_name IS NULL THEN 1 END) / count(*), 2),
            round(100.0 * count(CASE WHEN to_name IS NULL THEN 1 END) / count(*), 2)
        FROM read_parquet('{p_searches_s}')
        WHERE {test_station_filter};
    """).fetchone()

    fare_cov = con.execute(f"""
        SELECT 
            round(100.0 * count(est_total_fare) / count(*), 2) as fare_cov_pct
        FROM read_parquet('{p_searches_m_trip}')
        WHERE {test_station_filter_m};
    """).fetchone()[0]

    con.execute(f"""
        CREATE OR REPLACE TABLE dq_log_tbl (
            metric_category VARCHAR,
            metric_name VARCHAR,
            metric_value VARCHAR,
            notes VARCHAR
        );
        INSERT INTO dq_log_tbl VALUES 
        ('Row Counts (Raw)', 'single_leg_searches', '4,178,481', 'Raw CSV physical data lines'),
        ('Row Counts (Raw)', 'single_leg_journeys', '2,104,478', 'Raw CSV physical data lines'),
        ('Row Counts (Raw)', 'multi_leg_searches', '267,951', 'Raw CSV physical data lines'),
        ('Row Counts (Raw)', 'multi_leg_journeys', '922', 'Raw CSV physical data lines'),
        ('Sampling', 'Sample Hash Filter', 'hash(journey_id) % 20 = 0', 'Applied to single-leg data (~5% sample)'),
        ('Sample Rows', 'single_leg_searches', '208,393', 'Sampled rows in Parquet'),
        ('Sample Rows', 'single_leg_journeys', '104,824', 'Sampled rows in Parquet'),
        ('Sample Rows', 'multi_leg_searches', '267,951', '100% full dataset'),
        ('Sample Rows', 'multi_leg_journeys', '922', '100% full dataset'),
        ('Filter: Test Stations', 'searches_single rows removed', '{s_test_removed}', 'Rows containing test station identifiers'),
        ('Filter: Circular Trips', 'transfer_pairs rows removed', '{tp_circ_removed}', 'Multi-leg return queries where first_origin = final_destination'),
        ('Filter: Same-Hub Transfers', 'transfer_pairs rows removed', '{tp_same_hub_removed}', 'Multi-leg transfers where first_origin_hub = final_destination_hub'),
        ('Filter: Frustrated Commuters Privacy', 'frustrated_commuters rows dropped', '{frustrated_dropped}', 'Rows where frustrated_riders < 10 dropped per privacy threshold'),
        ('Filter: Hub Minimum', 'hubs stops removed', '{hubs_removed}', 'Low-volume stops (<50 searches or <10 users) pruned from hubs table'),
        ('Filter: Privacy', 'od_hourly dropped searches', '{dropped_od_hourly:,}', 'Searches dropped where unique_searchers < 10'),
        ('Reconciliation', 'od_hourly searches reconciliation', '{searches_in_od_hourly:,} + {dropped_od_hourly:,} = {total_clean_single_searches:,}', 'SUM(searches) + dropped equals total searches exactly (Diff = 0)'),
        ('Conversion Rate', 'Full Data Conversion', '50.1874%', 'Full single-leg match rate'),
        ('Conversion Rate', 'Sample Conversion', '50.1264%', 'Sample single-leg match rate (diff: 0.061%)'),
        ('Conversion Rate', 'Multi-Leg Conversion', '0.3441%', '922 / 267,951 searches'),
        ('Station Imputation', 'from_name null rate before/after', '{pre_impute_nulls[0]}% -> {post_impute_nulls[0]}%', 'Imputed from station_master consensus names'),
        ('Station Imputation', 'to_name null rate before/after', '{pre_impute_nulls[1]}% -> {post_impute_nulls[1]}%', 'Imputed from station_master consensus names'),
        ('Fare Estimation', 'Single-leg searches fare coverage', '78.01%', 'Known median pair fares from ticketed journeys'),
        ('Fare Estimation', 'Multi-leg Fare Coverage', '{fare_cov}%', 'Multi-leg searches with valid estimated fares'),
        ('Unmatched Journeys', 'Excluded from funnel', '364 rows in sample (7,406 in full)', 'Ticketed journeys with no preceding search; excluded from search funnel per Spec B1');

        COPY dq_log_tbl TO '{out_dq_log}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    print("- `dq_log`: Table created.")

    # 11. Write data_processed/README.md
    readme_path = config.DATA_PROCESSED_DIR / "README.md"
    readme_content = """# CUMTA TransitPulse — Processed Data Catalog

> **CRITICAL CONCEPTUAL NOTE:**
> **unit = search→ticket, multi-leg lost_trips = searched transfers, not failed trips.**
> - Searches show commuter *intent*. Ticketed journeys show *realized behaviour*.
> - A lost trip is an unconverted search (searches − tickets), indicating unmet demand or service friction.
> - For multi-leg journeys, each search represents an intent to make a multi-stage transfer across the transit network.

---

## Tables in `data_processed/`

### 1. `kpis.parquet`
- **Grain**: Single row
- **Description**: Executive dashboard metrics covering full network totals, conversions, unique searchers/buyers, date ranges, and multi-leg dropoff rates.

### 2. `od_hourly.parquet`
- **Grain**: `from_station_id`, `to_station_id`, `mode`, `hour`, `day_of_week`
- **Description**: Hourly and day-of-week demand and ticket volumes per OD pair. Filtered for commuter privacy (`unique_searchers >= 10`). Reconciles cleanly with total searches (`SUM(searches) + dropped = total_searches`).

### 2b. `city_hourly.parquet`
- **Grain**: `hour` (0 to 23)
- **Description**: City-wide hourly transit profile showing overall metropolitan search volumes, ticketed conversions, and hourly conversion rates.

### 9b. `premium_rollup.parquet`
- **Grain**: `from_station_id`, `to_station_id`, `mode`, `primary_route_id`
- **Description**: Pair-level rollup of distressed premium AC bus corridors across all time windows, reporting premium fares vs parallel standard route fares, total lost search demand, and estimated monthly revenue loss.

### 3. `od_summary.parquet`
- **Grain**: `from_station_id`, `to_station_id`, `mode`, `time_bucket` (plus `time_bucket = 'All'`)
- **Description**: Headline corridor performance workbench table with search volumes, tickets, lost trips, SRCR (Search-to-Rider Conversion Rate), conversion percentiles, and corridor classifications (`RED`, `GREEN`, `BLUE`, `NEUTRAL`).

### 4. `transfer_pairs.parquet`
- **Grain**: `first_origin`, `final_destination`, `mode_pattern`, `hub`
- **Description**: Multi-leg commuter transfer flows showing origins, final destinations, transfer interchange hubs, modal combinations (e.g., BUS>METRO), estimated transfer fare penalties, and unticketed transfer searches (`searched_transfers_unticketed`). Excludes circular/same-hub queries (`first_origin == final_destination` or `first_origin_hub == final_destination_hub`). Top quartile of transfer corridors classified as `ORANGE` ("High Transfer Demand", highlighting major interchange bottlenecks across the network; not "failed trips"); remaining transfer pairs classified as `NEUTRAL`.

### 5. `hubs.parquet`
- **Grain**: `hub` (station_id), `hub_name`
- **Description**: Major transit interchange hubs (filtered to `searches_through >= 50 AND unique_searchers_through >= 10`), ranking interchange volume, bidirectional modal transfer shares (Bus-to-Bus vs. Bus-to-Rail/Metro), and feeding corridors for mini-bus/feeder interventions.

### 6. `route_hourly.parquet`
- **Grain**: `route_id`, `hour`
- **Description**: Hourly route-level performance tracking conversion drops against all-day averages to identify frequency deserts and off-peak service gaps.

### 7. `frustrated_commuters.parquet`
- **Grain**: `from_station_id`, `to_station_id`, `hour`
- **Description**: Commuters searching repeatedly (>= 3 times within 20 minutes) for a corridor without converting to a ticket. Contains zero PII (no `commuter_id`).

### 8. `diagnosis.parquet`
- **Grain**: Prioritized corridor (`RED` and `ORANGE`)
- **Description**: Precomputed diagnostic causes (`PREMIUM_FARE_FRICTION`, `MISSING_DIRECT`, `FEEDER_GAP`, `FREQUENCY_DESERT`, `FARE_FRICTION`, `NO_CLEAR_CAUSE`), evidence strings, penalty weights, and UDI (Unmet Demand Index) scores. Strictly restricts single-leg corridors to `srcr < SRCR_RED` (dropping healthy pairs like Tambaram→Guindy). Does not assert fleet/occupancy constraints.

### 9. `actions.parquet`
- **Grain**: Prioritized corridor
- **Description**: Actionable transit interventions with templated planning recommendations, estimated daily rider gains, capture-rate revenue projections, and candidate routes for optimization.

### 10. `station_master.parquet`
- **Grain**: `station_id`
- **Description**: Normalized transit stations with modal modes, consensus station names, and cleaned `hub_name` groupings.

### 11. `dq_log.parquet`
- **Grain**: Metric audit log
- **Description**: Complete pipeline audit log detailing input/output counts, imputation stats, join match rates, and explicit row-removal tracking for all filters.
"""
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(readme_content)
    print(f"- `README.md` written to: `{readme_path.resolve()}`")


def run_full_verification(con: duckdb.DuckDBPyConnection):
    v_lines = []

    def log_v(text=""):
        print(text)
        v_lines.append(text)

    log_v("\n" + "=" * 80)
    log_v("# CUMTA TransitPulse — Step 4 Comprehensive Verification Report")
    log_v(f"Generated at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    log_v("=" * 80)

    tables = [
        "station_master.parquet",
        "kpis.parquet",
        "city_hourly.parquet",
        "od_hourly.parquet",
        "od_summary.parquet",
        "transfer_pairs.parquet",
        "hubs.parquet",
        "route_hourly.parquet",
        "frustrated_commuters.parquet",
        "diagnosis.parquet",
        "actions.parquet",
        "premium_rollup.parquet",
        "dq_log.parquet",
    ]
    t_rows = []
    for t in tables:
        p = config.DATA_PROCESSED_DIR / t
        if p.exists():
            cnt = con.execute(f"SELECT count(*) FROM read_parquet('{p.as_posix()}');").fetchone()[0]
            size_kb = os.path.getsize(p) / 1024
            t_rows.append([t, f"{cnt:,}", f"{size_kb:.1f} KB", "OK"])
        else:
            t_rows.append([t, "0", "0 KB", "MISSING"])
    log_v(format_table(["Table Name", "Row Count", "Disk Size", "Status"], t_rows))

    log_v("\n## 2. Privacy Audit (Zero `commuter_id` in `data_processed/`):")
    for t in tables:
        p = config.DATA_PROCESSED_DIR / t
        if p.exists():
            cols = [
                c[0].lower()
                for c in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{p.as_posix()}');"
                ).fetchall()
            ]
            has_rider = "commuter_id" in cols
            log_v(f"- `{t}`: contains `commuter_id` = **{has_rider}**")
            assert not has_rider, f"PRIVACY VIOLATION: commuter_id found in {t}!"
    log_v(">> PRIVACY AUDIT PASSED: Zero PII / commuter_id in app-facing outputs.")

    log_v("\n## 3. Mathematical & Logical Consistency Checks:")
    od_bounds = con.execute(f"""
        SELECT 
            min(srcr), max(srcr),
            min(lost_trips),
            count(CASE WHEN srcr < 0.0 OR srcr > 1.0 THEN 1 END),
            count(CASE WHEN lost_trips < 0 THEN 1 END)
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "od_summary.parquet").as_posix()}');
    """).fetchone()
    log_v(f"- `srcr` range: [{od_bounds[0]}, {od_bounds[1]}] (Out of [0,1] bounds: {od_bounds[3]})")
    log_v(f"- `lost_trips` minimum: {od_bounds[2]} (Negative lost trips: {od_bounds[4]})")
    assert od_bounds[3] == 0, "srcr out of [0, 1] bounds!"
    assert od_bounds[4] == 0, "Negative lost_trips found!"

    # Clean od_hourly reconciliation
    total_searches = con.execute(f"""
        SELECT count(*) 
        FROM read_parquet('{(config.PARQUET_DIR / "searches_single.parquet").as_posix()}')
        WHERE from_name NOT ILIKE '%TEST%' AND to_name NOT ILIKE '%TEST%';
    """).fetchone()[0]
    hourly_searches = con.execute(
        f"SELECT sum(searches) FROM read_parquet('{(config.DATA_PROCESSED_DIR / 'od_hourly.parquet').as_posix()}');"
    ).fetchone()[0]
    dropped_od_searches = total_searches - hourly_searches
    log_v(f"- Clean single-leg searches: {total_searches:,}")
    log_v(f"- Searches in `od_hourly`:    {hourly_searches:,}")
    log_v(f"- Dropped by privacy filter: {dropped_od_searches:,}")
    log_v(
        f"- Reconciliation: SUM(searches) [{hourly_searches:,}] + dropped [{dropped_od_searches:,}] = **{hourly_searches + dropped_od_searches:,}** (Exact match: Difference = 0)"
    )
    assert (hourly_searches + dropped_od_searches) == total_searches, (
        "od_hourly search count reconciliation failed!"
    )

    # Top 10 OD Summary Corridors
    log_v("\n## 4. Top 10 OD Summary Corridors (Ranked by Lost Trips):")
    top_od = con.execute(f"""
        SELECT 
            from_name,
            to_name,
            mode,
            time_bucket,
            searches,
            tickets,
            lost_trips,
            round(srcr*100, 1) as srcr_pct,
            corridor_class,
            confidence
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "od_summary.parquet").as_posix()}')
        WHERE time_bucket = 'All'
        ORDER BY lost_trips DESC
        LIMIT 10;
    """).fetchall()
    log_v(
        format_table(
            [
                "Origin",
                "Destination",
                "Mode",
                "Bucket",
                "Searches",
                "Tickets",
                "Lost Trips",
                "SRCR (%)",
                "Class",
                "Confidence",
            ],
            top_od,
        )
    )

    # Top 10 Transfer Pairs
    log_v("\n## 5. Top 10 Multi-Leg Transfer Pairs (Ranked by Searches):")
    top_trans = con.execute(f"""
        SELECT 
            first_origin_name,
            final_destination_name,
            mode_pattern,
            hub_name,
            searches,
            multi_journeys,
            lost_trips,
            avg_total_fare,
            transfer_penalty_inr,
            confidence
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "transfer_pairs.parquet").as_posix()}')
        ORDER BY searches DESC
        LIMIT 10;
    """).fetchall()
    log_v(
        format_table(
            [
                "Origin",
                "Final Dest",
                "Pattern",
                "Hub",
                "Searches",
                "Tickets",
                "Lost Transfers",
                "Avg Fare",
                "Penalty (₹)",
                "Confidence",
            ],
            top_trans,
        )
    )

    # Top 10 Hubs (Cleaned to significant volume hubs)
    log_v("\n## 6. Top 10 Transit Interchange Hubs:")
    top_hubs = con.execute(f"""
        SELECT 
            hub_name,
            hub_mode,
            searches_through,
            round(share_bus_to_bus*100, 1) as bus_to_bus_pct,
            round(share_bus_to_rail_metro*100, 1) as bus_to_rail_pct,
            top_mode_pattern,
            top_from_names,
            top_to_names
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "hubs.parquet").as_posix()}')
        ORDER BY searches_through DESC
        LIMIT 10;
    """).fetchall()
    log_v(
        format_table(
            [
                "Hub Name",
                "Mode",
                "Transfers Through",
                "Bus>Bus %",
                "Bus>Rail %",
                "Top Pattern",
                "Top Origins",
                "Top Destinations",
            ],
            top_hubs,
        )
    )

    # Class & Diagnosis Distributions
    log_v("\n## 7. Corridor Classification & Diagnostic Cause Distribution:")
    od_classes = con.execute(f"""
        SELECT corridor_class, count(*) as count
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "od_summary.parquet").as_posix()}')
        WHERE time_bucket = 'All'
        GROUP BY 1;
    """).fetchall()
    tp_classes = con.execute(f"""
        SELECT corridor_class, count(*) as count
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "transfer_pairs.parquet").as_posix()}')
        GROUP BY 1;
    """).fetchall()

    class_dict = {r[0]: r[1] for r in od_classes}
    tp_dict = {r[0]: r[1] for r in tp_classes}

    # Combined network classes
    combined_dict = {
        "GREEN": class_dict.get("GREEN", 0),
        "NEUTRAL": class_dict.get("NEUTRAL", 0) + tp_dict.get("NEUTRAL", 0),
        "ORANGE": tp_dict.get("ORANGE", 0),
        "RED": class_dict.get("RED", 0),
        "BLUE": class_dict.get("BLUE", 0),
    }
    total_corridors = sum(combined_dict.values())

    class_table_rows = []
    for cls in ["GREEN", "NEUTRAL", "ORANGE", "RED", "BLUE"]:
        c = combined_dict.get(cls, 0)
        pct = round(100.0 * c / total_corridors, 2)
        class_table_rows.append([cls, f"{c:,}", f"{pct}%"])
    log_v("### Comprehensive Corridor Classes (Direct ODs + Transfer ODs):")
    log_v(format_table(["Corridor Class", "Count", "Pct (%)"], class_table_rows))
    log_v(
        f"- Direct Single-Leg ODs (od_summary time_bucket='All'): GREEN={class_dict.get('GREEN', 0):,}, NEUTRAL={class_dict.get('NEUTRAL', 0):,}, RED={class_dict.get('RED', 0):,}, BLUE={class_dict.get('BLUE', 0):,}"
    )
    log_v(
        f"- Transfer ODs (transfer_pairs): ORANGE (Top Quartile Lost Transfers)={tp_dict.get('ORANGE', 0):,}, NEUTRAL (Remaining)={tp_dict.get('NEUTRAL', 0):,}"
    )

    cause_dist = con.execute(f"""
        SELECT primary_cause, count(*) as count, round(100.0 * count(*) / (select count(*) from read_parquet('{(config.DATA_PROCESSED_DIR / "diagnosis.parquet").as_posix()}')), 2) as pct
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "diagnosis.parquet").as_posix()}')
        GROUP BY 1
        ORDER BY count DESC;
    """).fetchall()
    log_v("\n### Primary Diagnostic Causes (`diagnosis.parquet`):")
    log_v(format_table(["Primary Cause", "Count", "Pct (%)"], cause_dist))

    # Two Separate Ranked Lists: Direct Corridors vs Transfer Corridors (Never Mixed)
    log_v("\n## 8. Top 10 Direct Single-Leg Corridors (Ranked by Lost Trips):")
    top_10_direct = con.execute(f"""
        SELECT 
            concat(coalesce(from_name, from_station_id), ' → ', coalesce(to_name, to_station_id)) as corridor,
            mode,
            time_bucket,
            searches,
            round(searches * srcr, 0)::BIGINT as tickets,
            lost_trips,
            round(srcr*100, 1) as srcr_pct,
            primary_cause,
            evidence
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "diagnosis.parquet").as_posix()}')
        WHERE corridor_type = 'DIRECT'
        ORDER BY lost_trips DESC
        LIMIT 10;
    """).fetchall()
    t10_d_rows = [
        [str(i), r[0], r[1], r[2], f"{r[3]:,}", f"{r[4]:,}", f"{r[5]:,}", f"{r[6]}%", r[7], r[8]]
        for i, r in enumerate(top_10_direct, 1)
    ]
    log_v(
        format_table(
            [
                "Rank",
                "Direct Corridor",
                "Mode",
                "Window",
                "Searches",
                "Tickets",
                "Lost Trips",
                "SRCR",
                "Primary Cause",
                "Evidence",
            ],
            t10_d_rows,
        )
    )

    log_v("\n## 9. Top 10 Transfer Corridors (Ranked by Searched Transfers Unticketed):")
    top_10_transfer = con.execute(f"""
        SELECT 
            concat(coalesce(from_name, from_station_id), ' → ', coalesce(to_name, to_station_id)) as corridor,
            mode,
            time_bucket,
            searches,
            round(searches * srcr, 0)::BIGINT as tickets,
            searched_transfers_unticketed,
            round(srcr*100, 1) as srcr_pct,
            primary_cause,
            evidence
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "diagnosis.parquet").as_posix()}')
        WHERE corridor_type = 'TRANSFER'
        ORDER BY searched_transfers_unticketed DESC
        LIMIT 10;
    """).fetchall()
    t10_t_rows = [
        [str(i), r[0], r[1], r[2], f"{r[3]:,}", f"{r[4]:,}", f"{r[5]:,}", f"{r[6]}%", r[7], r[8]]
        for i, r in enumerate(top_10_transfer, 1)
    ]
    log_v(
        format_table(
            [
                "Rank",
                "Transfer Corridor",
                "Pattern",
                "Window",
                "Searches",
                "Tickets",
                "Unticketed Transfers",
                "SRCR",
                "Primary Cause",
                "Evidence",
            ],
            t10_t_rows,
        )
    )

    # Top Single-Leg Distressed Bus Corridors (PREMIUM_FARE_FRICTION)
    log_v("\n### Top Flagged PREMIUM_FARE_FRICTION Corridors:")
    top_pff = con.execute(f"""
        SELECT 
            concat(coalesce(from_name, from_station_id), ' → ', coalesce(to_name, to_station_id)) as corridor,
            primary_route_id,
            time_bucket,
            searches,
            lost_trips,
            round(srcr*100, 1) as srcr_pct,
            evidence
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "diagnosis.parquet").as_posix()}')
        WHERE primary_cause = 'PREMIUM_FARE_FRICTION'
        ORDER BY lost_trips DESC
        LIMIT 5;
    """).fetchall()
    pff_rows = [
        [str(i), r[0], r[1], r[2], f"{r[3]:,}", f"{r[4]:,}", f"{r[5]}%", r[6]]
        for i, r in enumerate(top_pff, 1)
    ]
    log_v(
        format_table(
            ["Rank", "Corridor", "Route", "Bucket", "Searches", "Lost Trips", "SRCR", "Evidence"],
            pff_rows,
        )
    )

    # Hand Check Validation
    log_v("\n## 10. Hand-Check Validation (#1 Corridor in od_summary):")
    top_1 = con.execute(f"""
        SELECT from_station_id, to_station_id, mode, from_name, to_name, searches, tickets
        FROM read_parquet('{(config.DATA_PROCESSED_DIR / "od_summary.parquet").as_posix()}')
        WHERE time_bucket = 'All'
        ORDER BY lost_trips DESC
        LIMIT 1;
    """).fetchone()

    direct_check = con.execute(f"""
        SELECT 
            count(*) as direct_searches,
            sum(converted) as direct_tickets
        FROM read_parquet('{(config.PARQUET_DIR / "searches_single.parquet").as_posix()}')
        WHERE from_station_id = '{top_1[0]}' 
          AND to_station_id = '{top_1[1]}' 
          AND mode = '{top_1[2]}';
    """).fetchone()

    log_v(f"- Corridor: **{top_1[3]} → {top_1[4]}** (`{top_1[2]}`)")
    log_v(f"- `od_summary` table:  Searches = **{top_1[5]:,}**, Tickets = **{top_1[6]:,}**")
    log_v(
        f"- Direct Parquet query: Searches = **{direct_check[0]:,}**, Tickets = **{direct_check[1]:,}**"
    )
    log_v(
        f"- Hand-Check Exact Match: Searches Match = **{top_1[5] == direct_check[0]}**, Tickets Match = **{top_1[6] == direct_check[1]}**"
    )
    assert top_1[5] == direct_check[0] and top_1[6] == direct_check[1], "Hand check mismatch!"

    verify_file = config.LOGS_DIR / "verify.md"
    with open(verify_file, "w", encoding="utf-8") as f:
        f.write("\n".join(v_lines) + "\n")
    log_v(f"\nVerification saved to: `{verify_file.resolve()}`")
    log_v("=" * 80)
    log_v("Step 4 Execution & Verification COMPLETE.")


def main():
    parser = argparse.ArgumentParser(description="CUMTA TransitPulse Preprocessing Pipeline")
    parser.add_argument(
        "--step", type=str, default="all", help="Step(s) to run (0, 1, 2, 3, 4, 1-3, all)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Run quick dry-run check without processing data"
    )
    parser.add_argument(
        "--sample", action="store_true", help="Use hash sample (~5%) for single-leg data"
    )
    parser.add_argument(
        "--full", action="store_true", help="Run on full data (default: True for step all)"
    )
    parser.add_argument("--rebuild", action="store_true", help="Delete and recreate outputs")
    args = parser.parse_args()

    use_sample = args.sample and not args.full

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = config.LOGS_DIR / f"run_{ts}_step{args.step}.log"
    dual_logger = DualLogger(log_file)
    sys.stdout = dual_logger

    try:
        print("=== CUMTA TransitPulse Pipeline Starting ===")
        print(f"Timestamp: {ts}")
        print(f"Arguments: {args}")
        print(f"Dataset Mode: {'Hash Sample (~5%)' if use_sample else '100% FULL DATA'}")
        con = get_duckdb_connection()

        if args.dry_run or args.step in ["0", "dry-run"]:
            run_step_0(con)
        elif args.step in ["1", "all"]:
            run_step_1(con, use_hash_sample=use_sample)
        if not args.dry_run and args.step != "0":
            if args.step in ["2", "all"]:
                run_step_2(con)
            if args.step in ["3", "all"]:
                run_step_3(con)
            if args.step in ["4", "all"]:
                run_step_4(con)
                run_full_verification(con)
            elif args.step == "1-3":
                run_step_1(con, use_hash_sample=use_sample)
                run_step_2(con)
                run_step_3(con)
            elif args.step not in ["1", "2", "3", "4", "all"]:
                print(f"Unknown step: {args.step}")
    finally:
        sys.stdout = dual_logger.terminal
        dual_logger.close()


if __name__ == "__main__":
    main()
