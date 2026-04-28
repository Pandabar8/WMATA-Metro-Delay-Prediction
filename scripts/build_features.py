"""
Feature Engineering for WMATA Metro Delay Prediction
ENAI 603 — Builds a modeling-ready dataset from raw predictions + incidents.

Delay label: headway-based. A train is "delayed" if the observed headway
at (station, line, direction, hour) exceeds the median headway for that group
by more than DELAY_THRESHOLD_MINUTES. Gaps exceeding 2x the median are
treated as missed arrivals (phantom gaps) rather than true delays.

Usage:
    python scripts/build_features.py
"""

import argparse
import os
import sqlite3

import numpy as np
import pandas as pd

from config import DB_PATH, DELAY_THRESHOLD_MINUTES, DATA_DIR

# GTFS route_id → WMATA 2-letter code
GTFS_LINE_MAP = {
    "RED": "RD", "BLUE": "BL", "GREEN": "GR",
    "YELLOW": "YL", "ORANGE": "OR", "SILVER": "SV",
}

GTFS_DIR = os.path.join(DATA_DIR, "gtfs_rail")


# ═══════════════════════════════════════════════════════════════════════════
# 1. Load raw data
# ═══════════════════════════════════════════════════════════════════════════

def load_predictions(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    df = pd.read_sql("SELECT * FROM predictions", conn)
    conn.close()

    df["collected_at"] = pd.to_datetime(df["collected_at"], utc=True)

    # Parse minutes: numeric → int, ARR/BRD → 0, others → NaN
    df["minutes_num"] = pd.to_numeric(df["minutes"], errors="coerce")
    df.loc[df["minutes"].isin(["ARR", "BRD"]), "minutes_num"] = 0

    # Drop non-informative rows (---, --, empty, No)
    df = df.dropna(subset=["minutes_num"]).copy()

    # Keep only valid Metro lines
    df = df[df["line"].isin(["RD", "BL", "GR", "YL", "OR", "SV"])].copy()

    return df


def load_incidents(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    df = pd.read_sql("SELECT * FROM incidents", conn)
    conn.close()
    df["collected_at"] = pd.to_datetime(df["collected_at"], utc=True)
    df["date_updated"] = pd.to_datetime(df["date_updated"], errors="coerce")
    return df


def load_stations(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    df = pd.read_sql("SELECT * FROM stations", conn)
    conn.close()
    return df


# ═══════════════════════════════════════════════════════════════════════════
# 2. GTFS scheduled headways
# ═══════════════════════════════════════════════════════════════════════════

def load_gtfs_headways() -> pd.DataFrame:
    """Median scheduled headway per (station, line, hour, direction) from GTFS.

    Filters to a single weekday service_id to avoid mixing weekday/weekend
    schedules (which would produce nonsensical ~30-second 'headways').
    """
    stop_times = pd.read_csv(os.path.join(GTFS_DIR, "stop_times.txt"))
    trips = pd.read_csv(os.path.join(GTFS_DIR, "trips.txt"))

    # Filter to a typical weekday service. WMATA's GTFS uses 29_R for
    # Mon-Thu in the current feed; fall back to the most common service_id.
    weekday_sid = "29_R"
    if weekday_sid not in trips["service_id"].unique():
        weekday_sid = trips["service_id"].value_counts().idxmax()
        print(f"  GTFS: '29_R' not found; using most common service_id '{weekday_sid}'")
    trips = trips[trips["service_id"] == weekday_sid]

    st = stop_times.merge(trips[["trip_id", "route_id", "direction_id"]], on="trip_id")

    # Extract WMATA station code from GTFS stop_id  (PF_A01_C → A01)
    st["station_code"] = st["stop_id"].str.extract(r"_([A-Z]\d{2})_")
    st["line"] = st["route_id"].map(GTFS_LINE_MAP)
    st = st.dropna(subset=["station_code", "line"]).copy()

    # Parse arrival_time to seconds since midnight
    def time_to_seconds(t):
        h, m, s = t.split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)

    st["arr_seconds"] = st["arrival_time"].apply(time_to_seconds)
    st["hour"] = st["arr_seconds"] // 3600

    st = st.sort_values(["station_code", "line", "direction_id", "arr_seconds"])
    st["headway_sec"] = st.groupby(
        ["station_code", "line", "direction_id"]
    )["arr_seconds"].diff()

    headway = (
        st.dropna(subset=["headway_sec"])
        .groupby(["station_code", "line", "direction_id", "hour"])["headway_sec"]
        .median()
        .reset_index()
        .rename(columns={"headway_sec": "scheduled_headway_sec"})
    )
    return headway


# ═══════════════════════════════════════════════════════════════════════════
# 3. Headway-based delay labeling
# ═══════════════════════════════════════════════════════════════════════════

def compute_delay_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (station, line, direction) group:
    - Track ARR/BRD events as actual arrivals
    - Collapse consecutive ARR/BRD polls of the same physical train into one
      arrival event (a train sits in the station for ~30-90 sec, so the same
      train can show up as ARR/BRD across multiple 2-min polls)
    - Compute observed headway between consecutive arrival events
    - Label delayed = 1 if headway > median_headway + threshold
    - Propagate label to nearby prediction rows via merge_asof
    """
    df = df.sort_values(["location_code", "line", "group_num", "collected_at"]).copy()
    df["is_arrival"] = df["minutes"].isin(["ARR", "BRD"])

    # Compute headway between consecutive arrivals per (station, line, direction)
    arrivals = df[df["is_arrival"]].copy()
    arrivals = arrivals.sort_values(
        ["location_code", "line", "group_num", "collected_at"]
    )

    # Collapse polls of the same physical train into one arrival event.
    # A new event starts when the gap from the previous ARR/BRD at the same
    # (station, line, direction) exceeds SESSION_GAP_SEC. With 2-min polling,
    # a 4-min gap is the threshold beyond which it can't be the same train.
    SESSION_GAP_SEC = 240
    arrivals["_gap_prev_sec"] = (
        arrivals.groupby(["location_code", "line", "group_num"])["collected_at"]
        .diff().dt.total_seconds()
    )
    arrivals["_new_session"] = (
        arrivals["_gap_prev_sec"].isna()
        | (arrivals["_gap_prev_sec"] > SESSION_GAP_SEC)
    )
    arrivals["_session_id"] = arrivals.groupby(
        ["location_code", "line", "group_num"]
    )["_new_session"].cumsum()

    # Keep the FIRST poll per session as the canonical arrival timestamp
    arrivals = (
        arrivals.sort_values("collected_at")
        .groupby(
            ["location_code", "line", "group_num", "_session_id"],
            as_index=False,
        )
        .first()
        .sort_values(["location_code", "line", "group_num", "collected_at"])
    )

    arrivals["_hour"] = arrivals["collected_at"].dt.tz_convert("US/Eastern").dt.hour

    arrivals["prev_arrival"] = arrivals.groupby(
        ["location_code", "line", "group_num"]
    )["collected_at"].shift(1)
    arrivals["observed_headway_sec"] = (
        arrivals["collected_at"] - arrivals["prev_arrival"]
    ).dt.total_seconds()

    # Median headway per (station, line, direction, hour) so each time
    # window is compared against itself, not a global average.
    group_keys = ["location_code", "line", "group_num", "_hour"]
    group_medians = (
        arrivals.dropna(subset=["observed_headway_sec"])
        .groupby(group_keys)["observed_headway_sec"]
        .median()
        .reset_index()
        .rename(columns={"observed_headway_sec": "median_headway_sec"})
    )

    arrivals = arrivals.merge(group_medians, on=group_keys, how="left")

    # Phantom gap filter: if observed headway > 2x the median, a train
    # likely arrived and left between two polling cycles (every 2 min).
    # These are missed arrivals, not real delays.
    is_phantom = arrivals["observed_headway_sec"] > 2 * arrivals["median_headway_sec"]

    threshold_sec = DELAY_THRESHOLD_MINUTES * 60
    arrivals["is_delayed"] = (
        (arrivals["observed_headway_sec"] > arrivals["median_headway_sec"] + threshold_sec)
        & ~is_phantom
    ).astype(float).fillna(0).astype(int)

    # Propagate delay labels to all rows via merge_asof (forward-looking)
    delay_map = arrivals[
        ["location_code", "line", "group_num", "collected_at",
         "is_delayed", "observed_headway_sec", "median_headway_sec"]
    ].copy().rename(columns={"collected_at": "arrival_at"})

    df = df.sort_values("collected_at")
    delay_map = delay_map.sort_values("arrival_at")

    df = pd.merge_asof(
        df,
        delay_map,
        left_on="collected_at",
        right_on="arrival_at",
        by=["location_code", "line", "group_num"],
        direction="forward",
        tolerance=pd.Timedelta("15min"),
    )

    # Predictions with no arrival in the next 15 min are "orphans" — most are
    # actually long delays. Mark them explicitly so downstream code can
    # filter them out of metrics rather than silently treating them as on-time.
    df["is_orphan"] = df["is_delayed"].isna().astype(int)
    df["is_delayed"] = df["is_delayed"].fillna(0).astype(int)
    return df


# ═══════════════════════════════════════════════════════════════════════════
# 4. Feature engineering
# ═══════════════════════════════════════════════════════════════════════════

def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    local = df["collected_at"].dt.tz_convert("US/Eastern")
    df["hour"] = local.dt.hour
    df["day_of_week"] = local.dt.dayofweek  # 0=Mon
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["is_rush_hour"] = (
        (df["is_weekend"] == 0)
        & (((df["hour"] >= 6) & (df["hour"] < 9))
           | ((df["hour"] >= 16) & (df["hour"] < 19)))
    ).astype(int)
    df["date"] = local.dt.date
    return df


def add_station_features(df: pd.DataFrame, stations: pd.DataFrame) -> pd.DataFrame:
    line_cols = ["line_code1", "line_code2", "line_code3", "line_code4"]
    for col in line_cols:
        stations[col] = stations[col].replace("", np.nan)
    stations["num_lines"] = stations[line_cols].notna().sum(axis=1)

    terminals = {
        "A15", "B11",  # Red
        "J03", "G05",  # Blue
        "E10", "F11",  # Green
        "C15", "E06",  # Yellow
        "K08", "D13",  # Orange
        "N12",         # Silver (Ashburn)
    }
    stations["is_terminal"] = stations["station_code"].isin(terminals).astype(int)

    df = df.merge(
        stations[["station_code", "num_lines", "is_terminal", "lat", "lon"]],
        left_on="location_code",
        right_on="station_code",
        how="left",
    ).drop(columns=["station_code"])
    return df


def add_realtime_features(df: pd.DataFrame) -> pd.DataFrame:
    # The WMATA API returns up to N upcoming predictions per station per cycle.
    # Counting feed entries is NOT the same as counting trains physically present
    # at the station. Renamed for honesty.
    cycle = df.groupby(["collected_at", "location_code"])
    df["num_predictions_in_feed"] = cycle["id"].transform("count")
    df["car_num"] = pd.to_numeric(df["car"], errors="coerce")
    df["avg_cars_at_station"] = cycle["car_num"].transform("mean")
    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    # CRITICAL: groupby().rolling() returns rows ordered by (group_key, time).
    # We must sort df by the same keys before assigning .values, otherwise
    # rolling values get shuffled across rows.

    # Station-level rolling delay rate (30 min)
    df = df.sort_values(["location_code", "collected_at"]).reset_index(drop=True)
    station_roll = (
        df.set_index("collected_at")
        .groupby("location_code")["is_delayed"]
        .rolling("30min")
        .mean()
        .values
    )
    df["delay_rate_30min"] = station_roll

    # Line-level rolling delay rate (30 min) — re-sort by (line, time)
    df = df.sort_values(["line", "collected_at"]).reset_index(drop=True)
    line_roll = (
        df.set_index("collected_at")
        .groupby("line")["is_delayed"]
        .rolling("30min")
        .mean()
        .values
    )
    df["line_delay_rate_30min"] = line_roll

    return df


def add_incident_features(df: pd.DataFrame, incidents: pd.DataFrame) -> pd.DataFrame:
    inc = incidents.drop_duplicates(subset=["incident_id", "collected_at"]).copy()

    # Expand lines_affected to individual line rows
    valid_codes = {"OR", "SV", "BL", "RD", "GR", "YL"}
    inc_rows = []
    for _, row in inc.iterrows():
        if pd.isna(row["lines_affected"]):
            continue
        for code in str(row["lines_affected"]).split(";"):
            code = code.strip().upper()
            if code in valid_codes:
                inc_rows.append({
                    "incident_collected_at": row["collected_at"],
                    "incident_type": row["incident_type"],
                    "line": code,
                })

    if not inc_rows:
        df["active_incident"] = 0
        df["incident_is_delay"] = 0
        return df

    inc_exp = pd.DataFrame(inc_rows)
    inc_exp["incident_collected_at"] = pd.to_datetime(
        inc_exp["incident_collected_at"], utc=True
    )

    # merge_asof: match the most recent incident at or before the prediction
    # ('backward' avoids leaking future incidents into past predictions, which
    # would be unrealistic for a real-time delay model)
    merged = pd.merge_asof(
        df[["collected_at", "line"]].drop_duplicates().sort_values("collected_at"),
        inc_exp.sort_values("incident_collected_at"),
        left_on="collected_at",
        right_on="incident_collected_at",
        by="line",
        direction="backward",
        tolerance=pd.Timedelta("10min"),
    )
    merged["active_incident"] = merged["incident_collected_at"].notna().astype(int)
    merged["incident_is_delay"] = (merged["incident_type"] == "Delay").astype(int)

    df = df.merge(
        merged[["collected_at", "line", "active_incident", "incident_is_delay"]],
        on=["collected_at", "line"],
        how="left",
    )
    df["active_incident"] = df["active_incident"].fillna(0).astype(int)
    df["incident_is_delay"] = df["incident_is_delay"].fillna(0).astype(int)
    return df


def add_gtfs_headway_feature(df: pd.DataFrame, headway: pd.DataFrame) -> pd.DataFrame:
    # WMATA API: group_num is "1" or "2" (track/direction)
    # GTFS:      direction_id is 0 or 1
    # Mapping is not documented; pick whichever maximizes match coverage.

    headway = headway.copy()
    headway["direction_id"] = headway["direction_id"].astype(int)

    def _try_mapping(df, mapping_label, dir_map):
        df = df.copy()
        df["_dir"] = df["group_num"].map(dir_map)
        merged = df.merge(
            headway,
            left_on=["location_code", "line", "_dir", "hour"],
            right_on=["station_code", "line", "direction_id", "hour"],
            how="left",
        )
        coverage = merged["scheduled_headway_sec"].notna().mean()
        return coverage, merged

    # Try both mappings on a sample for speed
    sample = df.sample(n=min(100_000, len(df)), random_state=42)
    cov_a, _ = _try_mapping(sample, "A", {"1": 0, "2": 1})
    cov_b, _ = _try_mapping(sample, "B", {"1": 1, "2": 0})

    best_map = {"1": 0, "2": 1} if cov_a >= cov_b else {"1": 1, "2": 0}
    print(f"  GTFS direction mapping: A=(1→0,2→1) coverage={cov_a:.1%}, "
          f"B=(1→1,2→0) coverage={cov_b:.1%} → using "
          f"{'A' if cov_a >= cov_b else 'B'}")

    df["_dir"] = df["group_num"].map(best_map)
    df = df.merge(
        headway,
        left_on=["location_code", "line", "_dir", "hour"],
        right_on=["station_code", "line", "direction_id", "hour"],
        how="left",
    )
    df["scheduled_headway_min"] = df["scheduled_headway_sec"] / 60
    df.drop(
        columns=["station_code", "direction_id", "scheduled_headway_sec", "_dir"],
        errors="ignore", inplace=True,
    )
    return df


# ═══════════════════════════════════════════════════════════════════════════
# 5. Assemble final dataset
# ═══════════════════════════════════════════════════════════════════════════

FEATURE_COLS = [
    "hour", "day_of_week", "is_weekend", "is_rush_hour",
    "line", "location_code", "num_lines", "is_terminal",
    "lat", "lon",
    "minutes_num", "num_predictions_in_feed", "avg_cars_at_station",
    "delay_rate_30min", "line_delay_rate_30min",
    "active_incident", "incident_is_delay",
    "scheduled_headway_min",
]

LABEL_COL = "is_delayed"
ID_COLS = ["collected_at", "location_name", "destination_name", "date", "is_orphan"]


def build_dataset(db_path: str) -> pd.DataFrame:
    print("Loading predictions...")
    preds = load_predictions(db_path)
    print(f"  {len(preds):,} rows after cleaning")

    print("Loading incidents...")
    incidents = load_incidents(db_path)
    print(f"  {len(incidents):,} incident rows")

    print("Loading stations...")
    stations = load_stations(db_path)

    print("Computing delay labels (headway-based)...")
    df = compute_delay_labels(preds)
    print(f"  Delay rate: {df['is_delayed'].mean():.2%}")

    print("Adding temporal features...")
    df = add_temporal_features(df)

    print("Adding station features...")
    df = add_station_features(df, stations)

    print("Adding real-time features...")
    df = add_realtime_features(df)

    print("Adding rolling features...")
    df = add_rolling_features(df)

    print("Adding incident features...")
    df = add_incident_features(df, incidents)

    print("Loading GTFS scheduled headways...")
    try:
        headway = load_gtfs_headways()
        print("Adding GTFS headway features...")
        df = add_gtfs_headway_feature(df, headway)
    except FileNotFoundError:
        print("  GTFS files not found — skipping")
        df["scheduled_headway_min"] = np.nan

    keep = [c for c in ID_COLS + FEATURE_COLS + [LABEL_COL] if c in df.columns]
    df = df[keep].copy()

    print(f"\nFinal dataset: {len(df):,} rows × {len(df.columns)} cols")
    if "is_orphan" in df.columns:
        n_orphan = int(df["is_orphan"].sum())
        labeled = df[df["is_orphan"] == 0]
        print(f"  Orphan predictions (no arrival in 15 min): "
              f"{n_orphan:,} ({n_orphan/len(df):.1%}) — likely long delays, dropped from rate")
        print(f"  Delay rate (labeled rows only): {labeled['is_delayed'].mean():.2%}")
    print(f"  Delay rate (incl. orphans as on-time): {df['is_delayed'].mean():.2%}")
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()

    df = build_dataset(args.db)

    out_csv = os.path.join(DATA_DIR, "features.csv")
    df.to_csv(out_csv, index=False)
    print(f"\nSaved → {out_csv}")


if __name__ == "__main__":
    main()
