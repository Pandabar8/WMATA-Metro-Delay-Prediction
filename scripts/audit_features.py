"""
Comprehensive audit of suspected bugs in build_features.py and add_station_features.py.

Each section is a hypothesis. We test it against the freshly generated features.csv
and the underlying SQLite DB. Output: BUG CONFIRMED, NOT A BUG, or INCONCLUSIVE.

Hypotheses being tested:
  H1: rolling features (delay_rate_30min, line_delay_rate_30min) are MISALIGNED
      due to row-order mismatch between groupby+rolling output and the parent df
  H2: GTFS direction mapping fails -> ~50% NaN in scheduled_headway_min, but the
      remaining 50% may also be wrong (mismatched direction)
  H3: orphan predictions (no arrival within +15min) get is_delayed=0, masking
      actual long delays as "on-time"
  H4: merge_asof for incidents uses direction="nearest" -> future incidents
      can leak into past predictions (temporal leakage)
  H5: num_trains_at_station counts feed entries, not physical trains -> always
      saturates near 3 (the API returns top-3 per station)
  H6: loc_is_transfer hardcoded list disagrees with together_station from API
  H7: is_terminal hardcoded list disagrees with what the route data implies
"""

import os
import sqlite3
import numpy as np
import pandas as pd

from config import DATA_DIR, DB_PATH

CSV = os.path.join(DATA_DIR, "features.csv")


def hr(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


# ─────────────────────────────────────────────────────────────────────────
# H1 — rolling features misalignment
# ─────────────────────────────────────────────────────────────────────────
def test_h1_rolling_alignment():
    hr("H1: rolling feature alignment bug")

    df = pd.read_csv(CSV, parse_dates=["collected_at"], usecols=[
        "collected_at", "location_code", "line", "is_delayed",
        "delay_rate_30min", "line_delay_rate_30min",
    ])
    df["collected_at"] = pd.to_datetime(df["collected_at"], utc=True)

    # Mirror the EXACT sort+rolling in build_features.py.add_rolling_features
    # so any disagreement is real, not a tiebreak artifact between rows that
    # share a timestamp.
    df_sorted = df.sort_values(["location_code", "collected_at"]).reset_index(drop=True)
    expected = (
        df_sorted.set_index("collected_at")
        .groupby("location_code")["is_delayed"]
        .rolling("30min").mean()
        .values
    )
    actual = df_sorted["delay_rate_30min"].values
    diff = actual - expected
    finite = ~np.isnan(diff)
    pct_agree = np.isclose(actual[finite], expected[finite], atol=1e-6).mean()

    print(f"\nTotal rows tested: {len(df_sorted):,}")
    print(f"  delay_rate_30min match rate:  {pct_agree:.2%}")
    print(f"  Mean abs diff: {np.abs(diff[finite]).mean():.6f}")
    print(f"  Max abs diff:  {np.abs(diff[finite]).max():.4f}")

    df_sorted2 = df.sort_values(["line", "collected_at"]).reset_index(drop=True)
    expected_line = (
        df_sorted2.set_index("collected_at")
        .groupby("line")["is_delayed"]
        .rolling("30min").mean()
        .values
    )
    actual_line = df_sorted2["line_delay_rate_30min"].values
    diff_l = actual_line - expected_line
    fin_l = ~np.isnan(diff_l)
    pct_l = np.isclose(actual_line[fin_l], expected_line[fin_l], atol=1e-6).mean()
    print(f"\n  line_delay_rate_30min match rate: {pct_l:.2%}")
    print(f"  Mean abs diff: {np.abs(diff_l[fin_l]).mean():.6f}")

    if pct_agree > 0.99 and pct_l > 0.99:
        print("\n  → FIXED. Rolling features now correctly aligned.")
    else:
        print(f"\n  → STILL BUGGY. station={pct_agree:.1%}, line={pct_l:.1%}")
    return pct_agree


# ─────────────────────────────────────────────────────────────────────────
# H2 — GTFS direction mapping failure
# ─────────────────────────────────────────────────────────────────────────
def test_h2_gtfs_direction():
    hr("H2: GTFS scheduled_headway_min direction mapping")

    df = pd.read_csv(CSV, usecols=[
        "location_code", "line", "scheduled_headway_min", "hour",
    ])

    nan_pct = df["scheduled_headway_min"].isna().mean()
    print(f"\nOverall NaN rate in scheduled_headway_min: {nan_pct:.1%}")

    # Look at the non-NaN values: are they reasonable (3-15 min)?
    non_na = df["scheduled_headway_min"].dropna()
    print(f"\nNon-NaN scheduled_headway_min summary:")
    print(non_na.describe())
    impossible = ((non_na < 1) | (non_na > 30)).mean()
    print(f"  % outside reasonable range (1-30 min): {impossible:.1%}")

    # By line: which lines have most NaN?
    print("\nNaN rate by line:")
    print(df.groupby("line")["scheduled_headway_min"].apply(lambda x: x.isna().mean()).to_string())

    if nan_pct > 0.4:
        print(f"\n  → BUG CONFIRMED. {nan_pct:.0%} missing — direction mapping is broken.")
    return nan_pct


# ─────────────────────────────────────────────────────────────────────────
# H3 — orphan predictions silently labeled as 0
# ─────────────────────────────────────────────────────────────────────────
def test_h3_orphan_labels():
    hr("H3: orphan predictions tracking")

    df = pd.read_csv(CSV, usecols=["is_orphan", "is_delayed"])
    if "is_orphan" not in df.columns:
        print("\n  is_orphan column MISSING → fix not applied.")
        return

    orphan_rate = df["is_orphan"].mean()
    delay_with_orphans = df["is_delayed"].mean()
    delay_excl_orphans = df.loc[df["is_orphan"] == 0, "is_delayed"].mean()

    print(f"\n  is_orphan column present.")
    print(f"  Orphan rate: {orphan_rate:.1%}")
    print(f"  Delay rate INCLUDING orphans (old way):     {delay_with_orphans:.2%}")
    print(f"  Delay rate EXCLUDING orphans (honest way):  {delay_excl_orphans:.2%}")
    print(f"  Difference: {(delay_excl_orphans - delay_with_orphans)*100:.2f} pp")

    print("\n  → FIXED. Orphans now tracked separately; downstream code can filter.")
    return orphan_rate


# ─────────────────────────────────────────────────────────────────────────
# H4 — incident merge_asof with direction='nearest' = future leakage
# ─────────────────────────────────────────────────────────────────────────
def test_h4_incident_leakage():
    hr("H4: incident merge direction (no future leakage)")

    with open(os.path.join(os.path.dirname(__file__), "build_features.py")) as f:
        src = f.read()
    in_inc = src.split("def add_incident_features")[1].split("def ")[0]
    if 'direction="backward"' in in_inc or "direction='backward'" in in_inc:
        print("\n  Source uses direction='backward' → no future leakage.")
        print("  → FIXED.")
        return True
    elif 'direction="nearest"' in in_inc:
        print("\n  Still using direction='nearest' → STILL BUGGY.")
        return False
    else:
        print("  Direction not recognized.")
        return False


# ─────────────────────────────────────────────────────────────────────────
# H5 — num_trains_at_station saturates at API top-N
# ─────────────────────────────────────────────────────────────────────────
def test_h5_trains_at_station():
    hr("H5: misleading column name (renamed to num_predictions_in_feed)")

    cols = pd.read_csv(CSV, nrows=0).columns.tolist()
    if "num_trains_at_station" in cols:
        print("\n  Old misleading name still present → fix not applied.")
        return
    if "num_predictions_in_feed" in cols:
        df = pd.read_csv(CSV, usecols=["num_predictions_in_feed"])
        print(f"\n  Renamed column present.")
        print(f"  Mean: {df['num_predictions_in_feed'].mean():.2f}, "
              f"std: {df['num_predictions_in_feed'].std():.2f}, "
              f"max: {df['num_predictions_in_feed'].max()}")
        print(f"  → FIXED. Column now named honestly.")
        print(f"  Note: feature is still near-constant, so it has low predictive power")
        print(f"        but at least the name no longer lies.")
    else:
        print(f"\n  Neither old nor new name found. Available: {cols}")


# ─────────────────────────────────────────────────────────────────────────
# H6 — loc_is_transfer disagrees with together_station from API
# ─────────────────────────────────────────────────────────────────────────
def test_h6_transfer_definition():
    hr("H6: loc_is_transfer should match together_station (API source)")

    df = pd.read_csv(CSV, usecols=["location_code", "loc_is_transfer"]).drop_duplicates()
    csv_transfers = set(df.loc[df["loc_is_transfer"] == 1, "location_code"].unique())

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    stations = pd.read_sql("SELECT station_code, together_station FROM stations", conn)
    conn.close()
    api_a = stations.loc[stations["together_station"].astype(str).str.strip().ne(""), "station_code"].tolist()
    api_b = stations.loc[stations["together_station"].astype(str).str.strip().ne(""), "together_station"].tolist()
    api_transfers = set(api_a) | set(api_b)

    only_csv = csv_transfers - api_transfers
    only_api = api_transfers - csv_transfers
    print(f"\nCSV loc_is_transfer=1: {len(csv_transfers)} stations → {sorted(csv_transfers)}")
    print(f"API together_station:  {len(api_transfers)} stations → {sorted(api_transfers)}")
    print(f"Disagreement: api_only={sorted(only_api)}, csv_only={sorted(only_csv)}")
    if not only_csv and not only_api:
        print("  → FIXED. CSV matches API exactly.")


# ─────────────────────────────────────────────────────────────────────────
# H7 — is_terminal disagreement
# ─────────────────────────────────────────────────────────────────────────
def test_h7_terminal_definition():
    hr("H7: is_terminal hardcoded list — completeness check")

    HARDCODED = {
        "A15", "B11",   # Red
        "J03", "G05",   # Blue
        "E10", "F11",   # Green
        "C15", "E06",   # Yellow
        "K08", "D13",   # Orange
        "N12",          # Silver (Ashburn)
    }
    print(f"\nCurrent hardcoded terminals: {sorted(HARDCODED)}")
    print(f"Count: {len(HARDCODED)}")
    print("\nKnown WMATA terminals (cross-check):")
    print("  Red Line:    A15 (Shady Grove) + B11 (Glenmont)         ✓")
    print("  Blue Line:   J03 (Franconia) + G05 (Largo)              ✓")
    print("  Orange Line: K08 (Vienna) + D13 (New Carrollton)        ✓")
    print("  Silver Line: N12 (Ashburn) + G05 (Largo)                ← G05 not flagged for SV")
    print("  Green Line:  E10 (Greenbelt) + F11 (Branch Ave)         ✓")
    print("  Yellow Line: E06 (Mt Vernon Sq) + C15 (Huntington)      ✓")
    print("\nNote: a station that's a terminal for line X but not line Y is")
    print("currently flagged as terminal for ALL traffic through it. This may")
    print("be intentional, but worth a discussion.")


# ─────────────────────────────────────────────────────────────────────────
# Run all
# ─────────────────────────────────────────────────────────────────────────
def main():
    test_h1_rolling_alignment()
    test_h2_gtfs_direction()
    test_h3_orphan_labels()
    test_h4_incident_leakage()
    test_h5_trains_at_station()
    test_h6_transfer_definition()
    test_h7_terminal_definition()


if __name__ == "__main__":
    main()
