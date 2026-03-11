"""
Download archived GTFS (General Transit Feed Specification) data from WMATA.
This serves as a FALLBACK dataset per professor's feedback, in case
the live collection window is too short.

GTFS provides scheduled timetable data — useful for comparing actual vs. scheduled
arrival times to compute delays.
"""

import os
import zipfile
import requests
from config import WMATA_API_KEY, DATA_DIR


GTFS_URL = "https://api.wmata.com/gtfs/rail-gtfs-static.zip"


def download_gtfs():
    os.makedirs(DATA_DIR, exist_ok=True)
    zip_path = os.path.join(DATA_DIR, "gtfs_rail.zip")
    extract_dir = os.path.join(DATA_DIR, "gtfs_rail")

    print("Downloading WMATA Rail GTFS feed...")
    headers = {"api_key": WMATA_API_KEY}
    resp = requests.get(GTFS_URL, headers=headers, timeout=60)
    resp.raise_for_status()

    with open(zip_path, "wb") as f:
        f.write(resp.content)
    print(f"  Saved to {zip_path} ({len(resp.content) / 1024:.0f} KB)")

    print("Extracting...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_dir)

    files = os.listdir(extract_dir)
    print(f"  Extracted {len(files)} files: {', '.join(files)}")
    print("Key files:")
    print("  - stop_times.txt  → scheduled arrival/departure times per trip")
    print("  - trips.txt       → trip-to-route mapping")
    print("  - stops.txt       → station info")
    print("  - calendar.txt    → service days")
    return extract_dir


if __name__ == "__main__":
    from config import WMATA_API_KEY
    if WMATA_API_KEY == "YOUR_KEY_HERE":
        print("Set your WMATA_API_KEY first!")
    else:
        download_gtfs()
