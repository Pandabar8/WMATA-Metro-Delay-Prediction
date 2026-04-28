"""
Export the raw incidents table to data/incidents.csv so collaborators can
do incident-level EDA (e.g. event study) without needing the 1.9 GB
wmata.db file.
"""

import os
import sqlite3
import pandas as pd

from config import DATA_DIR, DB_PATH

OUT_PATH = os.path.join(DATA_DIR, "incidents.csv")


def main():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    df = pd.read_sql(
        "SELECT collected_at, incident_id, incident_type, description, "
        "lines_affected, date_updated FROM incidents",
        conn,
    )
    conn.close()
    df.to_csv(OUT_PATH, index=False)
    size_kb = os.path.getsize(OUT_PATH) / 1024
    print(f"Wrote {len(df):,} rows to {OUT_PATH} ({size_kb:.0f} KB)")
    print(f"Unique incidents: {df['incident_id'].nunique():,}")


if __name__ == "__main__":
    main()
