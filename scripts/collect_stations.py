"""
One-time pull of all WMATA rail station metadata.
"""

import sqlite3
import requests
from config import WMATA_API_KEY, STATIONS_URL, DB_PATH


def fetch_and_store_stations():
    headers = {"api_key": WMATA_API_KEY}
    resp = requests.get(STATIONS_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    stations = resp.json().get("Stations", [])

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    for s in stations:
        c.execute("""
            INSERT OR REPLACE INTO stations
            (station_code, station_name, lat, lon,
             line_code1, line_code2, line_code3, line_code4, together_station)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            s.get("Code"),
            s.get("Name"),
            s.get("Lat"),
            s.get("Lon"),
            s.get("LineCode1"),
            s.get("LineCode2"),
            s.get("LineCode3"),
            s.get("LineCode4"),
            s.get("StationTogether1"),
        ))

    conn.commit()
    conn.close()
    print(f"Stored {len(stations)} stations")


if __name__ == "__main__":
    fetch_and_store_stations()
