"""
Single collection cycle: predictions + incidents.
Designed to be called by launchd every 2 minutes.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from init_db import init_db
from collect_predictions import collect_once as collect_predictions
from collect_incidents import collect_once as collect_incidents
from config import WMATA_API_KEY

if WMATA_API_KEY == "YOUR_KEY_HERE" or not WMATA_API_KEY:
    print("ERROR: WMATA_API_KEY not set")
    sys.exit(1)

init_db()

try:
    collect_predictions()
except Exception as e:
    print(f"Prediction error: {e}")

try:
    collect_incidents()
except Exception as e:
    print(f"Incident error: {e}")
