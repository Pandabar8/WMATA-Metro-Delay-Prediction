"""
Feature Engineering for WMATA Metro Delay Prediction
ENAI 603 — Builds a modeling-ready dataset from raw predictions + incidents.

Delay label: headway-based. A train is "delayed" if the observed headway
at (station, line, direction) exceeds the median headway for that group
by more than DELAY_THRESHOLD_MINUTES.

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
    """Median scheduled headway per (station, line, hour, direction) from GTFS."""
    stop_times = pd.read_csv(os.path.join(GTFS_DIR, "stop_times.txt"))
    trips = pd.read_csv(os.path.join(GTFS_DIR, "trips.txt"))

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
    - Compute observed headway between consecutive arrivals
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
    arrivals["prev_arrival"] = arrivals.groupby(
        ["location_code", "line", "group_num"]
    )["collected_at"].shift(1)
    arrivals["observed_headway_sec"] = (
        arrivals["collected_at"] - arrivals["prev_arrival"]
    ).dt.total_seconds()

    # Median headway per group
    group_medians = (
        arrivals.dropna(subset=["observed_headway_sec"])
        .groupby(["location_code", "line", "group_num"])["observed_headway_sec"]
        .median()
        .reset_index()
        .rename(columns={"observed_headway_sec": "median_headway_sec"})
    )

    arrivals = arrivals.merge(
        group_medians, on=["location_code", "line", "group_num"], how="left"
    )

    threshold_sec = DELAY_THRESHOLD_MINUTES * 60
    arrivals["is_delayed"] = (
        arrivals["observed_headway_sec"] > arrivals["median_headway_sec"] + threshold_sec
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
    cycle = df.groupby(["collected_at", "location_code"])
    df["num_trains_at_station"] = cycle["id"].transform("count")
    df["car_num"] = pd.to_numeric(df["car"], errors="coerce")
    df["avg_cars_at_station"] = cycle["car_num"].transform("mean")
    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("collected_at").copy()

    # Station-level rolling delay rate (30 min)
    df = df.set_index("collected_at")
    station_roll = (
        df.groupby("location_code")["is_delayed"]
        .rolling("30min")
        .mean()
        .reset_index()
        .rename(columns={"is_delayed": "delay_rate_30min"})
    )
    df = df.reset_index()
    df["delay_rate_30min"] = station_roll["delay_rate_30min"].values

    # Line-level rolling delay rate (30 min)
    df = df.set_index("collected_at")
    line_roll = (
        df.groupby("line")["is_delayed"]
        .rolling("30min")
        .mean()
        .reset_index()
        .rename(columns={"is_delayed": "line_delay_rate_30min"})
    )
    df = df.reset_index()
    df["line_delay_rate_30min"] = line_roll["line_delay_rate_30min"].values

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

    # merge_asof: match nearest incident within ±5 min per line
    merged = pd.merge_asof(
        df[["collected_at", "line"]].drop_duplicates().sort_values("collected_at"),
        inc_exp.sort_values("incident_collected_at"),
        left_on="collected_at",
        right_on="incident_collected_at",
        by="line",
        direction="nearest",
        tolerance=pd.Timedelta("5min"),
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
    # group_num is str ("1" or "2"), direction_id is int (0 or 1)
    headway["direction_id"] = headway["direction_id"].astype(str)
    df["_group_str"] = df["group_num"].astype(str)

    df = df.merge(
        headway,
        left_on=["location_code", "line", "_group_str", "hour"],
        right_on=["station_code", "line", "direction_id", "hour"],
        how="left",
    )
    df["scheduled_headway_min"] = df["scheduled_headway_sec"] / 60
    df.drop(columns=["station_code", "direction_id", "scheduled_headway_sec", "_group_str"],
            errors="ignore", inplace=True)
    return df


# ═══════════════════════════════════════════════════════════════════════════
# 5. Assemble final dataset
# ═══════════════════════════════════════════════════════════════════════════

FEATURE_COLS = [
    "hour", "day_of_week", "is_weekend", "is_rush_hour",
    "line", "location_code", "num_lines", "is_terminal",
    "lat", "lon",
    "minutes_num", "num_trains_at_station", "avg_cars_at_station",
    "delay_rate_30min", "line_delay_rate_30min",
    "active_incident", "incident_is_delay",
    "scheduled_headway_min",
]

LABEL_COL = "is_delayed"
ID_COLS = ["collected_at", "location_name", "destination_name", "date"]


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
    print(f"Delay rate: {df['is_delayed'].mean():.2%}")
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
