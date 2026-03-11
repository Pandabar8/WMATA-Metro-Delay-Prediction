"""
Collect current rail incidents/alerts from the WMATA API.
"""

import sqlite3
import requests
from datetime import datetime, timezone
from config import WMATA_API_KEY, INCIDENTS_URL, DB_PATH


def fetch_incidents():
    headers = {"api_key": WMATA_API_KEY}
    resp = requests.get(INCIDENTS_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json().get("Incidents", [])


def store_incidents(incidents):
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    rows = []
    for inc in incidents:
        rows.append((
            now,
            inc.get("IncidentID"),
            inc.get("IncidentType"),
            inc.get("Description"),
            inc.get("LinesAffected"),
            inc.get("DateUpdated"),
        ))

    c.executemany("""
        INSERT INTO incidents
        (collected_at, incident_id, incident_type, description, lines_affected, date_updated)
        VALUES (?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()
    return len(rows)


def collect_once():
    incidents = fetch_incidents()
    count = store_incidents(incidents)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Collected {count} incidents")
    return count


if __name__ == "__main__":
    collect_once()
