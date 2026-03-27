"""
BlackoutWatch - Preprocess NOAA Storm Events Data
Filters to Texas, maps zone events to counties, creates daily county-level storm flags.

Usage:
    python scripts/06_preprocess_storm_events.py

Input:  data/raw/storm_events/StormEvents_details-*.csv, zone_county_mapping.dbx
Output: data/processed/storm_events_texas_daily.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
import time
import re
import glob

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "storm_events"
OUT_DIR = PROJECT_ROOT / "data" / "processed"

# Event types grouped into categories relevant for power outages
EVENT_CATEGORIES = {
    'tornado': ['Tornado'],
    'thunderstorm_wind': ['Thunderstorm Wind'],
    'hail': ['Hail'],
    'winter_storm': ['Winter Storm', 'Winter Weather', 'Ice Storm', 'Blizzard', 'Heavy Snow', 'Cold/Wind Chill', 'Extreme Cold/Wind Chill', 'Frost/Freeze', 'Sleet'],
    'hurricane': ['Hurricane', 'Hurricane (Typhoon)', 'Tropical Storm', 'Tropical Depression'],
    'flood': ['Flash Flood', 'Flood', 'Coastal Flood'],
    'high_wind': ['High Wind', 'Strong Wind'],
    'heat': ['Heat', 'Excessive Heat'],
    'wildfire': ['Wildfire'],
    'lightning': ['Lightning'],
}

# Reverse lookup: event_type -> category
EVENT_TO_CATEGORY = {}
for cat, events in EVENT_CATEGORIES.items():
    for event in events:
        EVENT_TO_CATEGORY[event] = cat


def load_zone_county_mapping():
    """Load zone-to-county FIPS mapping for Texas."""
    filepath = RAW_DIR / "zone_county_mapping.dbx"
    cols = ['state', 'zone', 'cwa', 'name', 'state_zone', 'county', 'fips', 'time_zone', 'fe_area', 'lat', 'lon']
    mapping = pd.read_csv(filepath, sep='|', header=None, names=cols, dtype={'zone': str, 'fips': str})
    tx_mapping = mapping[mapping['state'] == 'TX'].copy()
    tx_mapping['zone'] = tx_mapping['zone'].str.zfill(3)
    tx_mapping['fips'] = tx_mapping['fips'].str.zfill(5)
    print(f"Zone-county mapping loaded: {len(tx_mapping)} zone-county pairs for Texas")
    print(f"  Unique zones: {tx_mapping['zone'].nunique()}")
    print(f"  Unique counties: {tx_mapping['fips'].nunique()}")
    return tx_mapping


def parse_damage(val):
    """Convert damage string like '10.00K' or '1.50M' to numeric."""
    if pd.isna(val) or val == '' or val == '0.00K':
        return 0.0
    val = str(val).strip()
    multipliers = {'K': 1_000, 'M': 1_000_000, 'B': 1_000_000_000}
    for suffix, mult in multipliers.items():
        if val.endswith(suffix):
            try:
                return float(val[:-1]) * mult
            except ValueError:
                return 0.0
    try:
        return float(val)
    except ValueError:
        return 0.0


def load_storm_files():
    """Load all storm event detail files and filter to Texas."""
    pattern = str(RAW_DIR / "StormEvents_details-ftp_v1.0_d*_c*.csv")
    files = sorted(glob.glob(pattern))
    print(f"Found {len(files)} storm event files")

    all_texas = []
    for filepath in files:
        # Extract year from filename
        match = re.search(r'_d(\d{4})_', filepath)
        if not match:
            continue
        year = int(match.group(1))
        if year < 2014 or year > 2023:
            continue

        df = pd.read_csv(
            filepath,
            dtype={'STATE_FIPS': str, 'CZ_FIPS': str, 'CZ_TYPE': str},
            low_memory=False,
        )

        # Filter to Texas
        texas = df[df['STATE'] == 'TEXAS'].copy()
        print(f"  {year}: {len(texas)} Texas events (C={len(texas[texas['CZ_TYPE']=='C'])}, Z={len(texas[texas['CZ_TYPE']=='Z'])})")
        all_texas.append(texas)

    combined = pd.concat(all_texas, ignore_index=True)
    print(f"\n  Total Texas storm events: {len(combined):,}")
    return combined


def map_events_to_counties(df, zone_mapping):
    """Map storm events to county FIPS codes."""

    # Pad FIPS codes
    df['STATE_FIPS'] = df['STATE_FIPS'].str.zfill(2)
    df['CZ_FIPS'] = df['CZ_FIPS'].str.zfill(3)

    # --- County events (CZ_TYPE = C): direct FIPS ---
    county_events = df[df['CZ_TYPE'] == 'C'].copy()
    county_events['fips_code'] = county_events['STATE_FIPS'] + county_events['CZ_FIPS']

    # --- Zone events (CZ_TYPE = Z): use zone-county mapping ---
    zone_events = df[df['CZ_TYPE'] == 'Z'].copy()
    zone_events['zone'] = zone_events['CZ_FIPS'].str.zfill(3)

    # Merge zone events with zone-county mapping (one zone can map to multiple counties)
    zone_mapped = zone_events.merge(
        zone_mapping[['zone', 'fips']].rename(columns={'fips': 'fips_code'}),
        on='zone',
        how='inner'
    )

    print(f"\n  County events mapped: {len(county_events):,}")
    print(f"  Zone events mapped: {len(zone_mapped):,} (expanded from {len(zone_events):,} events)")
    print(f"  Zone events unmatched: {len(zone_events) - zone_mapped['EVENT_ID'].nunique():,}")

    # Combine
    keep_cols = ['fips_code', 'BEGIN_DATE_TIME', 'EVENT_TYPE', 'MAGNITUDE',
                 'DAMAGE_PROPERTY', 'DAMAGE_CROPS', 'INJURIES_DIRECT', 'DEATHS_DIRECT']
    combined = pd.concat([
        county_events[keep_cols],
        zone_mapped[keep_cols]
    ], ignore_index=True)

    return combined


def aggregate_to_daily(df):
    """Aggregate events to daily per county with category flags."""

    # Parse date
    df['date'] = pd.to_datetime(df['BEGIN_DATE_TIME'], format='%d-%b-%y %H:%M:%S', errors='coerce')
    df['date'] = df['date'].dt.date

    # Map event types to categories
    df['category'] = df['EVENT_TYPE'].map(EVENT_TO_CATEGORY).fillna('other')

    # Parse damage
    df['damage_property'] = df['DAMAGE_PROPERTY'].apply(parse_damage)
    df['damage_crops'] = df['DAMAGE_CROPS'].apply(parse_damage)
    df['total_damage'] = df['damage_property'] + df['damage_crops']

    # Parse numeric fields
    df['injuries'] = pd.to_numeric(df['INJURIES_DIRECT'], errors='coerce').fillna(0)
    df['deaths'] = pd.to_numeric(df['DEATHS_DIRECT'], errors='coerce').fillna(0)

    # Aggregate: one row per county-date
    # Binary flags per category
    category_flags = (
        df.groupby(['fips_code', 'date', 'category'])
        .size()
        .unstack(fill_value=0)
        .clip(upper=1)  # binary flag
        .reset_index()
    )

    # Numeric aggregates per county-date
    numeric_agg = (
        df.groupby(['fips_code', 'date'])
        .agg(
            total_events=('EVENT_TYPE', 'count'),
            max_magnitude=('MAGNITUDE', 'max'),
            total_damage=('total_damage', 'sum'),
            total_injuries=('injuries', 'sum'),
            total_deaths=('deaths', 'sum'),
        )
        .reset_index()
    )

    # Merge flags and numeric
    daily = category_flags.merge(numeric_agg, on=['fips_code', 'date'], how='outer')

    # Fill missing category columns with 0
    for cat in EVENT_CATEGORIES.keys():
        if cat not in daily.columns:
            daily[cat] = 0

    # Add an any_storm flag
    storm_cols = list(EVENT_CATEGORIES.keys())
    daily['any_storm'] = daily[storm_cols].max(axis=1)

    return daily


def main():
    start = time.time()
    print("=" * 60)
    print("BlackoutWatch - Storm Events Preprocessing")
    print("=" * 60)

    # Load zone mapping
    print("\n[1/4] Loading zone-county mapping...")
    zone_mapping = load_zone_county_mapping()

    # Load storm event files
    print("\n[2/4] Loading storm event files...")
    texas_storms = load_storm_files()

    # Map to counties
    print("\n[3/4] Mapping events to counties...")
    mapped = map_events_to_counties(texas_storms, zone_mapping)

    # Aggregate to daily
    print("\n[4/4] Aggregating to daily per county...")
    daily = aggregate_to_daily(mapped)

    daily['date'] = pd.to_datetime(daily['date'])
    daily = daily.sort_values(['fips_code', 'date']).reset_index(drop=True)

    # Save
    outpath = OUT_DIR / "storm_events_texas_daily.csv"
    daily.to_csv(outpath, index=False)

    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.1f}s")
    print(f"Output: {outpath}")
    print(f"Shape: {daily.shape}")
    print(f"Date range: {daily['date'].min()} to {daily['date'].max()}")
    print(f"Counties with events: {daily['fips_code'].nunique()}")
    print(f"Total county-days with storms: {len(daily):,}")
    print(f"\nEvent category coverage:")
    for cat in sorted(EVENT_CATEGORIES.keys()):
        if cat in daily.columns:
            count = daily[cat].sum()
            print(f"  {cat}: {count:,} county-days")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()