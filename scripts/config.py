"""
WMATA Data Pipeline Configuration
ENAI 603 Capstone — WMATA Metro Delay Prediction
"""

import os

# ─── API Key ────────────────────────────────────────────────
# Set via environment variable or paste directly (not recommended for shared repos)
WMATA_API_KEY = os.environ.get("WMATA_API_KEY", "")

# ─── API Endpoints ──────────────────────────────────────────
BASE_URL = "https://api.wmata.com"
PREDICTIONS_URL = f"{BASE_URL}/StationPrediction.svc/json/GetPrediction/All"
INCIDENTS_URL = f"{BASE_URL}/Incidents.svc/json/Incidents"
STATIONS_URL = f"{BASE_URL}/Rail.svc/json/jStations"

# ─── Data Storage ───────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "wmata.db")

# ─── Delay Threshold ────────────────────────────────────────
# A train is labeled "delayed" if its predicted arrival exceeds
# the scheduled arrival by this many minutes.
DELAY_THRESHOLD_MINUTES = 2

# ─── Collection Settings ────────────────────────────────────
PREDICTION_INTERVAL_SECONDS = 120  # Poll every 2 minutes
INCIDENT_INTERVAL_SECONDS = 300    # Poll incidents every 5 minutes
