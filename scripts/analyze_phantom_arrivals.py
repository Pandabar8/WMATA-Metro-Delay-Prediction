"""
Empirical analysis: are consecutive ARR/BRD polls being miscounted as
separate arrivals?

For each (location, line, direction), we look at sequences of ARR/BRD
events and group them into "arrival sessions" (consecutive polls < 4 min
apart for the same train at the same station).

We then compare:
  - CURRENT logic: every ARR/BRD row = separate arrival
  - CORRECTED:    one arrival per session

Reports:
  - Distribution of session lengths (how many polls per arrival)
  - Distribution of "headways" under both methods
  - Resulting delay rate under both methods
"""

import sqlite3
import numpy as np
import pandas as pd

from config import DB_PATH, DELAY_THRESHOLD_MINUTES


def load_arrivals():
    print("Loading ARR/BRD rows from DB...")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    df = pd.read_sql(
        """
        SELECT collected_at, line, location_code, group_num, minutes
        FROM predictions
        WHERE minutes IN ('ARR', 'BRD')
          AND line IN ('RD', 'BL', 'GR', 'YL', 'OR', 'SV')
        """,
        conn,
    )
    conn.close()
    df["collected_at"] = pd.to_datetime(df["collected_at"], utc=True)
    print(f"  {len(df):,} ARR/BRD rows loaded")
    return df


def build_sessions(df, gap_threshold_sec=240):
    """Group consecutive ARR/BRD polls (within gap_threshold_sec) into sessions.

    A new session starts when the gap from the previous ARR/BRD at the same
    (station, line, direction) exceeds the threshold.
    """
    print(f"Building arrival sessions (gap threshold = {gap_threshold_sec}s)...")
    df = df.sort_values(["location_code", "line", "group_num", "collected_at"]).copy()

    # Time since previous ARR/BRD at same (station, line, direction)
    df["prev_arr"] = df.groupby(
        ["location_code", "line", "group_num"]
    )["collected_at"].shift(1)
    df["gap_sec"] = (df["collected_at"] - df["prev_arr"]).dt.total_seconds()

    # New session = first row OR gap > threshold
    df["new_session"] = df["gap_sec"].isna() | (df["gap_sec"] > gap_threshold_sec)
    df["session_id"] = df.groupby(
        ["location_code", "line", "group_num"]
    )["new_session"].cumsum()

    return df


def session_length_stats(df):
    sessions = df.groupby(
        ["location_code", "line", "group_num", "session_id"]
    ).size().rename("polls_per_arrival")

    print("\nDistribution of polls per arrival (how often does same train")
    print("show up as ARR/BRD across consecutive polls):")
    print(sessions.value_counts().sort_index().head(15).to_string())
    print(f"\n  Mean: {sessions.mean():.2f} polls per arrival")
    print(f"  % single-poll arrivals: {(sessions == 1).mean():.1%}")
    print(f"  % multi-poll arrivals:  {(sessions > 1).mean():.1%}")
    print(f"  Total physical arrivals: {len(sessions):,}")
    return sessions


def headway_comparison(df):
    """Compare 'headways' under current vs corrected logic."""

    # CURRENT method: gap between consecutive ARR/BRD rows
    current_headways = df["gap_sec"].dropna()
    current_headways = current_headways[current_headways > 0]

    # CORRECTED method: one timestamp per session (the FIRST poll), then
    # compute gaps between sessions
    sessions_df = (
        df.sort_values("collected_at")
        .groupby(["location_code", "line", "group_num", "session_id"], as_index=False)
        .first()[["collected_at", "location_code", "line", "group_num", "session_id"]]
    )
    sessions_df = sessions_df.sort_values(
        ["location_code", "line", "group_num", "collected_at"]
    )
    sessions_df["prev_session"] = sessions_df.groupby(
        ["location_code", "line", "group_num"]
    )["collected_at"].shift(1)
    sessions_df["headway_sec"] = (
        sessions_df["collected_at"] - sessions_df["prev_session"]
    ).dt.total_seconds()
    corrected_headways = sessions_df["headway_sec"].dropna()
    corrected_headways = corrected_headways[corrected_headways > 0]

    print("\n" + "=" * 60)
    print("HEADWAY DISTRIBUTION COMPARISON (seconds)")
    print("=" * 60)

    quantiles = [0.10, 0.25, 0.50, 0.75, 0.90]
    print(f"\n{'Quantile':<10}{'CURRENT (buggy)':<20}{'CORRECTED':<20}")
    print("-" * 50)
    for q in quantiles:
        c = current_headways.quantile(q)
        f = corrected_headways.quantile(q)
        print(f"  p{int(q*100):<8}{c:<20.0f}{f:<20.0f}")

    print(f"\nMedian headway (overall): "
          f"current={current_headways.median():.0f}s, "
          f"corrected={corrected_headways.median():.0f}s")
    print(f"Headway count:             "
          f"current={len(current_headways):,}, "
          f"corrected={len(corrected_headways):,}")

    return current_headways, corrected_headways, sessions_df


def delay_rate_comparison(df, sessions_df, threshold_sec):
    """Compute delay rate under both methods, segmented by hour."""
    print("\n" + "=" * 60)
    print("DELAY RATE COMPARISON (per-hour segmented median)")
    print("=" * 60)

    # CURRENT method: every ARR/BRD row is an arrival, with phantom filter
    df_curr = df.dropna(subset=["gap_sec"]).copy()
    df_curr = df_curr[df_curr["gap_sec"] > 0]
    df_curr["_hour"] = df_curr["collected_at"].dt.tz_convert("US/Eastern").dt.hour
    grp_keys = ["location_code", "line", "group_num", "_hour"]
    df_curr["med"] = df_curr.groupby(grp_keys)["gap_sec"].transform("median")
    is_phantom = df_curr["gap_sec"] > 2 * df_curr["med"]
    df_curr["is_delayed"] = (
        (df_curr["gap_sec"] > df_curr["med"] + threshold_sec) & ~is_phantom
    ).astype(int)
    print(f"\nCURRENT logic delay rate:   {df_curr['is_delayed'].mean():.2%}")
    print(f"  Phantom rows excluded:    {is_phantom.sum():,} ({is_phantom.mean():.1%})")

    # CORRECTED method: one row per arrival session
    sd = sessions_df.dropna(subset=["headway_sec"]).copy()
    sd = sd[sd["headway_sec"] > 0]
    sd["_hour"] = sd["collected_at"].dt.tz_convert("US/Eastern").dt.hour
    sd["med"] = sd.groupby(grp_keys)["headway_sec"].transform("median")
    is_phantom_sd = sd["headway_sec"] > 2 * sd["med"]
    sd["is_delayed"] = (
        (sd["headway_sec"] > sd["med"] + threshold_sec) & ~is_phantom_sd
    ).astype(int)
    print(f"\nCORRECTED logic delay rate: {sd['is_delayed'].mean():.2%}")
    print(f"  Phantom rows excluded:    {is_phantom_sd.sum():,} ({is_phantom_sd.mean():.1%})")

    print("\nInterpretation:")
    print("  - If CURRENT >> CORRECTED, the bug is inflating delay rate.")
    print("  - If similar, the bug exists but doesn't matter in practice.")


def main():
    df = load_arrivals()
    df = build_sessions(df, gap_threshold_sec=240)
    sessions = session_length_stats(df)
    _, _, sessions_df = headway_comparison(df)
    delay_rate_comparison(df, sessions_df, DELAY_THRESHOLD_MINUTES * 60)


if __name__ == "__main__":
    main()
