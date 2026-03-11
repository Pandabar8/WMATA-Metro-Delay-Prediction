"""
Export SQLite tables to CSV for use in Jupyter notebooks.
"""

import pandas as pd
import sqlite3
import os
from config import DB_PATH, DATA_DIR


def export_all():
    conn = sqlite3.connect(DB_PATH)

    for table in ["predictions", "incidents", "stations"]:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        path = os.path.join(DATA_DIR, f"{table}.csv")
        df.to_csv(path, index=False)
        print(f"Exported {table}: {len(df):,} rows → {path}")

    conn.close()


if __name__ == "__main__":
    export_all()
