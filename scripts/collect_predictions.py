"""
Collect real-time train predictions from the WMATA API.
Each call returns predictions for ALL stations (~200-300 rows).
At 2-min intervals, this yields ~100K+ rows/day during operating hours.
"""

import sqlite3
import requests
from datetime import datetime, timezone
from config import WMATA_API_KEY, PREDICTIONS_URL, DB_PATH


def fetch_predictions():
    """Pull current predictions for all stations."""
    headers = {"api_key": WMATA_API_KEY}
    resp = requests.get(PREDICTIONS_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json().get("Trains", [])


def store_predictions(trains):
    """Insert prediction records into SQLite."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    rows = []
    for t in trains:
        rows.append((
            now,
            t.get("Car"),
            t.get("Destination"),
            t.get("DestinationCode"),
            t.get("DestinationName"),
            t.get("Group"),
            t.get("Line"),
            t.get("LocationCode"),
            t.get("LocationName"),
            t.get("Min"),
        ))

    c.executemany("""
        INSERT INTO predictions
        (collected_at, car, destination, destination_code, destination_name,
         group_num, line, location_code, location_name, minutes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()
    return len(rows)


def collect_once():
    """Single collection cycle. Returns row count or raises on error."""
    trains = fetch_predictions()
    count = store_predictions(trains)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Collected {count} predictions")
    return count


if __name__ == "__main__":
    collect_once()
