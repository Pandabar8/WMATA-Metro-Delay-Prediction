"""
Initialize the SQLite database with tables for predictions, incidents, and stations.
"""

import sqlite3
import os
from config import DB_PATH, DATA_DIR


def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Real-time train predictions (polled every ~2 min)
    c.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collected_at TEXT NOT NULL,
            car TEXT,
            destination TEXT,
            destination_code TEXT,
            destination_name TEXT,
            group_num TEXT,
            line TEXT,
            location_code TEXT,
            location_name TEXT,
            minutes TEXT
        )
    """)

    # Index for common queries
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_predictions_collected
        ON predictions (collected_at)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_predictions_line
        ON predictions (line)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_predictions_location
        ON predictions (location_code)
    """)

    # Incidents / service alerts
    c.execute("""
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collected_at TEXT NOT NULL,
            incident_id TEXT,
            incident_type TEXT,
            description TEXT,
            lines_affected TEXT,
            date_updated TEXT
        )
    """)

    # Station metadata (one-time pull)
    c.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_code TEXT PRIMARY KEY,
            station_name TEXT,
            lat REAL,
            lon REAL,
            line_code1 TEXT,
            line_code2 TEXT,
            line_code3 TEXT,
            line_code4 TEXT,
            together_station TEXT
        )
    """)

    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")


if __name__ == "__main__":
    init_db()
