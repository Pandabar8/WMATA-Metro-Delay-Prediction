"""
Main pipeline orchestrator.
Runs continuously, collecting predictions every 2 min and incidents every 5 min.

Usage:
    python run_pipeline.py              # Run continuously
    python run_pipeline.py --once       # Single collection cycle (for testing)

Keep this running in a terminal, tmux session, or set up as a launchd/cron job.
"""

import sys
import time
import signal
import sqlite3
from datetime import datetime
from config import (
    WMATA_API_KEY, DB_PATH,
    PREDICTION_INTERVAL_SECONDS, INCIDENT_INTERVAL_SECONDS,
)
from init_db import init_db
from collect_predictions import collect_once as collect_predictions
from collect_incidents import collect_once as collect_incidents
from collect_stations import fetch_and_store_stations

running = True


def signal_handler(sig, frame):
    global running
    print("\nShutting down gracefully...")
    running = False


def check_api_key():
    if WMATA_API_KEY == "YOUR_KEY_HERE" or not WMATA_API_KEY:
        print("ERROR: Set your WMATA API key first!")
        print("  Option 1: export WMATA_API_KEY='your-key-here'")
        print("  Option 2: Edit scripts/config.py directly")
        sys.exit(1)


def print_stats():
    """Print current row counts."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    pred_count = c.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
    inc_count = c.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    conn.close()
    print(f"  Total rows — predictions: {pred_count:,} | incidents: {inc_count:,}")


def run_once():
    """Single collection cycle for testing."""
    check_api_key()
    init_db()
    print("Running single collection cycle...")
    fetch_and_store_stations()
    collect_predictions()
    collect_incidents()
    print_stats()
    print("Done!")


def run_continuous():
    """Continuous collection loop."""
    check_api_key()
    init_db()
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("=" * 60)
    print("WMATA Data Pipeline — Running")
    print(f"  Predictions every {PREDICTION_INTERVAL_SECONDS}s")
    print(f"  Incidents every {INCIDENT_INTERVAL_SECONDS}s")
    print(f"  Database: {DB_PATH}")
    print(f"  Press Ctrl+C to stop")
    print("=" * 60)

    # One-time station pull
    try:
        fetch_and_store_stations()
    except Exception as e:
        print(f"Warning: station pull failed: {e}")

    last_prediction = 0
    last_incident = 0

    while running:
        now = time.time()

        if now - last_prediction >= PREDICTION_INTERVAL_SECONDS:
            try:
                collect_predictions()
                last_prediction = now
            except Exception as e:
                print(f"  [ERROR] Predictions: {e}")

        if now - last_incident >= INCIDENT_INTERVAL_SECONDS:
            try:
                collect_incidents()
                last_incident = now
            except Exception as e:
                print(f"  [ERROR] Incidents: {e}")

        # Print stats every 10 minutes
        if int(now) % 600 < 2:
            print_stats()

        time.sleep(1)

    print("Pipeline stopped.")
    print_stats()


if __name__ == "__main__":
    if "--once" in sys.argv:
        run_once()
    else:
        run_continuous()
