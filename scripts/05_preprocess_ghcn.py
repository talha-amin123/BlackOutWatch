"""
BlackoutWatch - Preprocess GHCN-Daily Weather Data
Filters to Texas stations, pivots elements to columns, aggregates to county-daily.

Usage:
    python scripts/05_preprocess_ghcn.py

Input:  data/raw/ghcn_daily/*.csv, data/processed/station_county_mapping_final.csv
Output: data/processed/ghcn_texas_daily.csv
"""

import pandas as pd
from pathlib import Path
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "ghcn_daily"
OUT_DIR = PROJECT_ROOT / "data" / "processed"

YEARS = list(range(2014, 2024))

# Elements we care about
ELEMENTS = ['TMAX', 'TMIN', 'PRCP', 'SNOW', 'SNWD', 'AWND']

GHCN_COLS = ['station_id', 'date', 'element', 'value', 'm_flag', 'q_flag', 's_flag', 'obs_time']


def load_station_mapping():
    """Load the station-to-county mapping."""
    mapping = pd.read_csv(
        OUT_DIR / "station_county_mapping_final.csv",
        dtype={'fips_code': str}
    )
    mapping['fips_code'] = mapping['fips_code'].str.zfill(5)
    tx_station_ids = set(mapping['station_id'])
    print(f"Station mapping loaded: {len(mapping)} stations across {mapping['fips_code'].nunique()} counties")
    return mapping, tx_station_ids


def process_year(year, tx_station_ids):
    """Read one year of GHCN data, filter to Texas stations and relevant elements."""
    filepath = RAW_DIR / f"{year}.csv"
    if not filepath.exists():
        print(f"  ⚠️  {filepath.name} not found, skipping")
        return None

    print(f"  Reading {year}.csv...")
    chunks = []
    rows_read = 0
    rows_kept = 0

    for chunk in pd.read_csv(
        filepath,
        header=None,
        names=GHCN_COLS,
        dtype={'station_id': str, 'date': str, 'element': str, 'value': float},
        usecols=[0, 1, 2, 3, 5],  # station_id, date, element, value, q_flag
        chunksize=2_000_000,
    ):
        rows_read += len(chunk)

        # Filter: Texas stations + elements we want + no quality flag issues
        mask = (
            chunk['station_id'].isin(tx_station_ids) &
            chunk['element'].isin(ELEMENTS) &
            (chunk['q_flag'].isna() | (chunk['q_flag'] == ''))
        )
        filtered = chunk[mask][['station_id', 'date', 'element', 'value']].copy()
        rows_kept += len(filtered)
        chunks.append(filtered)

    if not chunks:
        return None

    df = pd.concat(chunks, ignore_index=True)
    print(f"    {rows_read:,} rows read → {rows_kept:,} Texas weather records kept")
    return df


def pivot_and_aggregate(df, mapping):
    """Pivot elements to columns and aggregate by county-date."""

    # Pivot: one row per station-date, columns for each element
    pivoted = df.pivot_table(
        index=['station_id', 'date'],
        columns='element',
        values='value',
        aggfunc='first'
    ).reset_index()

    pivoted.columns.name = None

    # Merge with county mapping
    pivoted = pivoted.merge(
        mapping[['station_id', 'fips_code']],
        on='station_id',
        how='inner'
    )

    # Convert units
    # TMAX/TMIN: tenths of °C → °C
    for col in ['TMAX', 'TMIN']:
        if col in pivoted.columns:
            pivoted[col] = pivoted[col] / 10.0

    # PRCP: tenths of mm → mm
    if 'PRCP' in pivoted.columns:
        pivoted['PRCP'] = pivoted['PRCP'] / 10.0

    # AWND: tenths of m/s → m/s
    if 'AWND' in pivoted.columns:
        pivoted['AWND'] = pivoted['AWND'] / 10.0

    # Aggregate by county-date (average across stations in each county)
    agg_dict = {}
    for elem in ELEMENTS:
        if elem in pivoted.columns:
            agg_dict[elem] = 'mean'

    county_daily = (
        pivoted.groupby(['fips_code', 'date'])
        .agg(
            station_count=('station_id', 'nunique'),
            **{elem: (elem, 'mean') for elem in ELEMENTS if elem in pivoted.columns}
        )
        .reset_index()
    )

    return county_daily


def main():
    start = time.time()
    print("=" * 60)
    print("BlackoutWatch - GHCN-Daily Preprocessing")
    print("=" * 60)

    # Load station mapping
    print("\n[1/3] Loading station-county mapping...")
    mapping, tx_station_ids = load_station_mapping()

    # Process each year
    print("\n[2/3] Processing yearly files...")
    all_years = []
    for year in YEARS:
        df = process_year(year, tx_station_ids)
        if df is not None:
            all_years.append(df)

    print(f"\n  Combining all years...")
    combined = pd.concat(all_years, ignore_index=True)
    print(f"  Total Texas weather records: {len(combined):,}")
    del all_years

    # Pivot and aggregate
    print("\n[3/3] Pivoting elements and aggregating to county-daily...")
    county_daily = pivot_and_aggregate(combined, mapping)
    del combined

    # Parse date
    county_daily['date'] = pd.to_datetime(county_daily['date'], format='%Y%m%d')

    # Rename columns to lowercase
    rename = {e: e.lower() for e in ELEMENTS if e in county_daily.columns}
    county_daily.rename(columns=rename, inplace=True)

    # Sort and save
    county_daily = county_daily.sort_values(['fips_code', 'date']).reset_index(drop=True)

    outpath = OUT_DIR / "ghcn_texas_daily.csv"
    county_daily.to_csv(outpath, index=False)

    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.1f}s")
    print(f"Output: {outpath}")
    print(f"Shape: {county_daily.shape}")
    print(f"Date range: {county_daily['date'].min()} to {county_daily['date'].max()}")
    print(f"Counties: {county_daily['fips_code'].nunique()}")
    print(f"Avg stations per county-day: {county_daily['station_count'].mean():.1f}")

    # Check coverage
    print(f"\nElement coverage (% of rows with data):")
    for col in ['tmax', 'tmin', 'prcp', 'snow', 'snwd', 'awnd']:
        if col in county_daily.columns:
            pct = county_daily[col].notna().mean() * 100
            print(f"  {col}: {pct:.1f}%")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
