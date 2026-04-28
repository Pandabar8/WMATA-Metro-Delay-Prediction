"""
Add station-level features to features.csv:
- loc_has_parking
- loc_is_transfer (derived from stations.together_station — API source of truth)
- loc_conn_vre / loc_conn_amtrak / loc_conn_mark
- num_rails_conn (interaction term)

Skips the slow previous/next stop logic (notebook 20 §6) — those are not
required by the baseline model. Run after build_features.py.

Usage:
    python scripts/add_station_features.py
"""

import os
import sqlite3
import pandas as pd

from config import DATA_DIR, DB_PATH

CSV_PATH = os.path.join(DATA_DIR, "features.csv")

STATION_PARKING = {
    "N12", "N11", "N09", "N08", "N06",
    "K05", "K08", "K07", "K06",
    "A15", "A14", "A13", "A12", "A11",
    "J03", "J02", "C15",
    "F11", "F10", "F09", "F08", "F06",
    "G02", "G03", "G04", "G05",
    "D09", "D10", "D11", "D12", "D13",
    "B04", "B07", "B06", "B08", "B09", "B10", "B11",
    "E07", "E08", "E09", "E10",
}

RAIL_SYS_CONNECT = {
    "vre":    {"B03", "C13", "C09", "J03", "D03", "F03"},
    "amtrak": {"D13", "B03", "C13", "A14"},
    "mark":   {"D13", "E10", "E09", "B03", "B08", "A14"},
}


def derive_transfer_stations() -> set:
    """A station is a transfer if it has a together_station pairing (API truth)
    OR appears as the together_station of another station."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    stations = pd.read_sql(
        "SELECT station_code, together_station FROM stations", conn
    )
    conn.close()
    paired_a = stations.loc[
        stations["together_station"].astype(str).str.strip().ne(""),
        "station_code",
    ].tolist()
    paired_b = stations.loc[
        stations["together_station"].astype(str).str.strip().ne(""),
        "together_station",
    ].tolist()
    return set(paired_a) | set(paired_b)


def main():
    print(f"Loading {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)
    print(f"  {len(df):,} rows × {df.shape[1]} cols")

    print("Adding loc_has_parking...")
    df["loc_has_parking"] = df["location_code"].isin(STATION_PARKING).astype(int)

    print("Adding loc_is_transfer (from API together_station)...")
    transfer_codes = derive_transfer_stations()
    print(f"  {len(transfer_codes)} transfer stations identified: {sorted(transfer_codes)}")
    df["loc_is_transfer"] = df["location_code"].isin(transfer_codes).astype(int)

    print("Adding rail-system connection flags...")
    for name, codes in RAIL_SYS_CONNECT.items():
        df[f"loc_conn_{name}"] = df["location_code"].isin(codes).astype(int)

    df["num_rails_conn"] = (
        df["loc_conn_vre"] + df["loc_conn_amtrak"] + df["loc_conn_mark"]
    )

    print(f"\nFinal: {len(df):,} rows × {df.shape[1]} cols")
    print(f"Stations with parking:  {df['loc_has_parking'].sum():,} rows ({df['loc_has_parking'].mean():.1%})")
    print(f"Transfer stations:      {df['loc_is_transfer'].sum():,} rows ({df['loc_is_transfer'].mean():.1%})")
    print(f"Rows with rail conn≥1:  {(df['num_rails_conn'] >= 1).sum():,} ({(df['num_rails_conn'] >= 1).mean():.1%})")

    df.to_csv(CSV_PATH, index=False)
    print(f"\nSaved → {CSV_PATH}")


if __name__ == "__main__":
    main()
