# BlackoutWatch

BlackoutWatch is a Texas-focused outage data preparation project for downstream machine learning and analysis. The repository builds county-day datasets that combine power outage activity, weather observations, and severe storm events.

## What This Repo Contains

This repository currently focuses on data acquisition and preprocessing rather than model training or a deployed dashboard.

- EAGLE-I outage preprocessing at the county-day level
- GHCN-Daily weather aggregation from station-day to county-day
- NOAA Storm Events aggregation to county-day storm indicators
- County/station mapping utilities for Texas weather stations
- exploratory notebooks and exported figures

## Setup

Create an environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Data Pipeline

Run the scripts in this order:

```bash
python scripts/01_download_data.py
python scripts/02_preprocess_eagle_i.py
python scripts/03_scrape_station_data.py
python scripts/04_county_station_mapping.py
python scripts/05_preprocess_ghcn.py
python scripts/06_preprocess_storm_events.py
```

### Step Summary

1. `01_download_data.py`
   Downloads EAGLE-I outage files and supporting metadata from Figshare into `data/raw/eagle_i/`.
2. `02_preprocess_eagle_i.py`
   Filters outage records to Texas, aggregates 15-minute intervals to daily county metrics, adds customer-count metadata, and writes `data/processed/eagle_i_texas_daily.csv`.
3. `03_scrape_station_data.py`
   Uses the FCC geography API to map Texas GHCN weather stations to counties.
4. `04_county_station_mapping.py`
   Builds a fallback nearest-centroid county mapping, compares it to the FCC mapping, and writes the final station-to-county map.
5. `05_preprocess_ghcn.py`
   Aggregates station weather observations into county-day weather features and writes `data/processed/ghcn_texas_daily.csv`.
6. `06_preprocess_storm_events.py`
   Aggregates NOAA storm event records into county-day indicators and writes `data/processed/storm_events_texas_daily.csv`.

## Repository Layout

```text
BlackOutWatch/
├── data/
│   ├── raw/              # External source files, ignored by git
│   └── processed/        # Derived county-day datasets tracked in this repo
├── notebooks/            # EDA and scratch notebooks
├── reports/figures/      # Exported analysis figures
├── scripts/              # Data collection and preprocessing scripts
├── README.md
└── requirements.txt
```

## Data Sources

- EAGLE-I (ORNL / DOE): county-level outage data at 15-minute intervals
- GHCN-Daily (NOAA): daily weather observations from Texas stations
- NOAA Storm Events: severe weather event records with county or forecast-zone references
- FCC Census Block API: station-to-county lookup by latitude and longitude

## Reproducibility Notes

- `data/raw/` is intentionally ignored because the original source files are large and externally hosted.
- `data/processed/` is included so the current outputs can be inspected without rerunning the full pipeline.
- A fresh clone is not fully reproducible until the raw source files are downloaded or placed in the expected `data/raw/` subfolders.
- Some steps depend on external APIs and may take time to run.

## Current Outputs

The repository currently includes these main processed datasets:

- `data/processed/eagle_i_texas_daily.csv`
- `data/processed/ghcn_texas_daily.csv`
- `data/processed/storm_events_texas_daily.csv`
- `data/processed/station_county_mapping_final.csv`

## Notes

- This repo is already initialized as git and can be pushed to GitHub.
- The tracked processed CSVs make the repository relatively large, but each individual file is still below GitHub's 100 MB hard limit.

## Collaboration

This project was developed collaboratively. The current repository is maintained by Talha Amin, and earlier repository work includes contributions from Saurav Kanegaonkar.
