# WMATA Metro Delay Prediction

**ENAI 603 — Foundations of Data Science for Engineering AI | Spring 2026**
**Prof. Patrice Seyed | University of Maryland**

## Team

| Member | Role |
|--------|------|
| José Barrientos | Data collection & pipeline (API setup, extraction, cleaning, preprocessing) |
| Felipe Trujillo | Analysis & modeling (EDA, delay prediction, visualizations, interpretation) |
| Selena Solunna | Feature engineering, model evaluation, and results interpretation |

## Problem

Washington D.C.'s Metro system (WMATA) serves hundreds of thousands of daily commuters but struggles with service reliability. This project predicts Metro train delays and assesses service reliability using real-time and historical data from the WMATA API.

## Approach

1. **Data Collection** — Automated pipeline polling WMATA real-time predictions every 2 minutes and incidents every 5 minutes, stored in SQLite
2. **Delay Definition** — A train is labeled "delayed" if the gap between consecutive arrivals at a station exceeds the typical headway by more than **2 minutes**
3. **Feature Engineering** — Temporal (hour, rush hour, day of week), station/line, rolling delay rates, and active incident features
4. **Modeling** — Logistic Regression → Random Forest → XGBoost, evaluated with time-series aware cross-validation
5. **Target Metric** — AUC-ROC > 0.80

## Project Structure

```
WMATA_Delays_Project/
├── scripts/                         # Data collection & processing
│   ├── config.py                    # API key, endpoints, thresholds
│   ├── init_db.py                   # SQLite schema setup
│   ├── collect_predictions.py       # Real-time train predictions
│   ├── collect_incidents.py         # Service alerts
│   ├── collect_stations.py          # Station metadata (one-time)
│   ├── collect_all.py               # Single collection cycle (used by launchd)
│   ├── build_features.py            # Feature engineering → data/features.csv
│   ├── download_gtfs.py             # Archived GTFS schedule data (fallback)
│   ├── export_csv.py                # Export DB tables to CSV
│   └── run_pipeline.py              # Main orchestrator (continuous or --once)
├── notebooks/                       # Jupyter notebooks
│   ├── 01_data_audit.ipynb          # Data validation & quality checks
│   ├── 02_eda.ipynb                 # Exploratory data analysis
│   └── 03_baseline_model.ipynb      # Baseline models (Dummy, LR, RF)
├── data/                            # All data artifacts
│   ├── wmata.db                     # SQLite database (primary store)
│   ├── gtfs_rail/                   # Archived GTFS schedule files
│   └── *.csv                        # Exported snapshots for notebooks
├── reports/                         # Figures and write-up assets
│   └── figures/
├── requirements.txt
├── .gitignore
└── README.md
```

## Data Sources

| Source | Endpoint | Description |
|--------|----------|-------------|
| Real-Time Predictions | `StationPrediction.svc/json/GetPrediction/All` | Live arrival times per station (~555 rows/cycle) |
| Incidents & Alerts | `Incidents.svc/json/Incidents` | Active service disruptions by line |
| Station Metadata | `Rail.svc/json/jStations` | 102 stations with coordinates and line assignments |
| GTFS Static Feed | `gtfs/rail-gtfs-static.zip` | Scheduled timetables for baseline comparison |

All sourced from the [WMATA Developer API](https://developer.wmata.com).

## Database Schema

### predictions
| Column | Type | Description |
|--------|------|-------------|
| collected_at | TEXT | UTC timestamp of collection |
| car | TEXT | Number of cars (6 or 8) |
| destination | TEXT | Destination station abbreviation |
| destination_code | TEXT | Destination station code |
| destination_name | TEXT | Destination station full name |
| group_num | TEXT | Track group (1 or 2 = direction) |
| line | TEXT | Line color (RD, BL, OR, SV, GR, YL) |
| location_code | TEXT | Station code where prediction was made |
| location_name | TEXT | Station name where prediction was made |
| minutes | TEXT | Minutes to arrival: numeric, "ARR", "BRD", or "---" |

### incidents
| Column | Type | Description |
|--------|------|-------------|
| collected_at | TEXT | UTC timestamp of collection |
| incident_id | TEXT | WMATA incident identifier |
| incident_type | TEXT | Type (Delay, Alert) |
| description | TEXT | Full incident description |
| lines_affected | TEXT | Semicolon-separated line codes |
| date_updated | TEXT | Last update timestamp from WMATA |

### stations
| Column | Type | Description |
|--------|------|-------------|
| station_code | TEXT | Primary key station identifier |
| station_name | TEXT | Station name |
| lat / lon | REAL | Geographic coordinates |
| line_code1–4 | TEXT | Lines serving this station |
| together_station | TEXT | Connected station code (transfers) |

## Delay Label Construction

The `minutes` field reports **time-to-arrival**, not delay. We compute delay using a headway-based approach:

1. For each (station, line, direction), track when trains arrive ("ARR"/"BRD" transitions)
2. Compute the gap between consecutive arrivals
3. Compare to the median headway for that line and time-of-day
4. **Delayed = gap exceeds median headway + 2 minutes**

GTFS scheduled times serve as a secondary validation source.

## Running the Pipeline

```bash
# Activate virtual environment
source venv/bin/activate

# Test with a single collection cycle
cd scripts
python run_pipeline.py --once

# Run continuous collection
python run_pipeline.py

# Export data to CSV for notebooks
python export_csv.py
```

## Current Status (Mar 11)

- **85K+ prediction rows** collected (and growing — pipeline runs 24/7)
- **541 incident records**
- Feature engineering complete → `data/features.csv` (23 features)
- EDA notebook and baseline model notebook ready
- Delay rate: **~37%** (reasonable class balance)

## Key Dates

| Date | Milestone |
|------|-----------|
| Mar 10 | Pipeline operational, data collection begins |
| Mar 11 | Feature engineering, EDA, and baseline models complete |
| Mar 16–22 | Spring Break (pipeline runs unattended) |
| Apr 6 | Midterm checkpoint: 50K+ rows, EDA, baseline model |
| Apr 16 | Final submission |

## Requirements

See `requirements.txt` for Python dependencies. Requires Python 3.10+.
