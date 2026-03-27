"""
BlackoutWatch - Preprocess EAGLE-I Data
Filters to Texas, aggregates 15-min intervals to daily, creates full county-date grid,
merges MCC (total customers), and adds coverage metadata.

Usage:
    python scripts/02_preprocess_eagle_i.py

Input:  data/raw/eagle_i/eaglei_outages_*.csv, MCC.csv, coverage_history.csv
Output: data/processed/eagle_i_texas_daily.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "eagle_i"
OUT_DIR = PROJECT_ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

YEARS = list(range(2014, 2024))
TEXAS_FIPS_PREFIX = "48"


def load_and_filter_texas(year):
    """Load one year of EAGLE-I data and filter to Texas counties."""
    # Try both naming conventions (2021 has a typo in some versions)
    possible_names = [
        f"eaglei_outages_{year}.csv",
        f"eaglei_otuages_{year}.csv",  # known typo for 2021
    ]

    filepath = None
    for name in possible_names:
        candidate = RAW_DIR / name
        if candidate.exists():
            filepath = candidate
            break

    if filepath is None:
        print(f"  ⚠️  No file found for {year}, skipping")
        return None

    print(f"  Loading {filepath.name}...")
    df = pd.read_csv(
        filepath,
        dtype={"fips_code": str, "county": str, "state": str},
        parse_dates=["run_start_time"],
    )
    
    # 2023 file uses 'sum' instead of 'customers_out'
    if "sum" in df.columns and "customers_out" not in df.columns:
        df.rename(columns={"sum": "customers_out"}, inplace=True)

    # Handle NA values in customers_out, then convert to int
    df["customers_out"] = pd.to_numeric(df["customers_out"], errors="coerce").fillna(0).astype(int)

    # Filter to Texas
    texas = df[df["state"] == "Texas"].copy()
    print(f"    Total rows: {len(df):,} → Texas rows: {len(texas):,}")

    return texas


def aggregate_to_daily(df):
    """Aggregate 15-min intervals to daily per county."""
    df["date"] = df["run_start_time"].dt.date

    daily = (
        df.groupby(["fips_code", "county", "date"])
        .agg(
            max_customers_out=("customers_out", "max"),
            sum_customers_out=("customers_out", "sum"),
            outage_intervals=("customers_out", "count"),  # number of 15-min intervals with outage
        )
        .reset_index()
    )

    # Estimate outage hours: each interval = 15 min = 0.25 hours
    daily["outage_hours"] = daily["outage_intervals"] * 0.25

    return daily


def build_full_grid(daily_df, years):
    """Create a complete grid of all Texas counties × all dates so non-outage days are included."""

    # Get unique counties
    counties = daily_df[["fips_code", "county"]].drop_duplicates()
    print(f"\n  Unique Texas counties in data: {len(counties)}")

    # Build full date range across all years
    min_date = pd.Timestamp(f"{min(years)}-01-01").date()
    max_date = pd.Timestamp(f"{max(years)}-12-31").date()
    all_dates = pd.date_range(min_date, max_date, freq="D").date

    print(f"  Date range: {min_date} to {max_date} ({len(all_dates):,} days)")

    # Cross join: every county × every date
    grid = counties.merge(pd.DataFrame({"date": all_dates}), how="cross")
    print(f"  Full grid size: {len(grid):,} rows (counties × days)")

    # Merge with actual outage data
    merged = grid.merge(daily_df.drop(columns=["county"]), on=["fips_code", "date"], how="left")

    # Fill missing = no outage
    merged["max_customers_out"] = merged["max_customers_out"].fillna(0).astype(int)
    merged["sum_customers_out"] = merged["sum_customers_out"].fillna(0).astype(int)
    merged["outage_intervals"] = merged["outage_intervals"].fillna(0).astype(int)
    merged["outage_hours"] = merged["outage_hours"].fillna(0.0)
    merged["outage_flag"] = (merged["max_customers_out"] > 0).astype(int)

    return merged


def merge_mcc(df):
    """Merge MCC (modeled county customers) for outage percentage calculation."""
    mcc = pd.read_csv(RAW_DIR / "MCC.csv", dtype={"County_FIPS": str})
    mcc.rename(columns={"County_FIPS": "fips_code", "Customers": "total_customers"}, inplace=True)

    # Pad FIPS to 5 digits
    mcc["fips_code"] = mcc["fips_code"].str.zfill(5)

    before = len(df)
    df = df.merge(mcc, on="fips_code", how="left")

    matched = df["total_customers"].notna().sum()
    print(f"\n  MCC merge: {matched:,}/{before:,} rows matched ({matched/before*100:.1f}%)")

    return df

def adjust_customers_by_year(df):
    """Adjust total_customers using Texas population growth as proxy.
    MCC.csv is 2022 baseline. ~1.5% annual growth rate."""
    growth_factors = {
        2014: 0.89,
        2015: 0.90,
        2016: 0.92,
        2017: 0.93,
        2018: 0.95,
        2019: 0.97,
        2020: 0.98,
        2021: 0.99,
        2022: 1.00,
        2023: 1.02,
    }

    df["growth_factor"] = df["year"].map(growth_factors).fillna(1.0)
    df["total_customers_adj"] = (df["total_customers"] * df["growth_factor"]).round(0)

    print("\n  Growth factors applied:")
    for y, f in sorted(growth_factors.items()):
        print(f"    {y}: ×{f}")

    df["outage_pct"] = np.where(
        df["total_customers_adj"] > 0,
        df["max_customers_out"] / df["total_customers_adj"],
        0.0,
    )

    return df


def add_coverage_metadata(df):
    """Add coverage history flags for Texas."""
    coverage = pd.read_csv(RAW_DIR / "coverage_history.csv")
    coverage = coverage[coverage["state"] == "TX"].copy()
    coverage["year"] = pd.to_datetime(coverage["year"]).dt.year

    coverage_lookup = coverage.set_index("year")[
        ["min_pct_covered", "max_pct_covered"]
    ].to_dict("index")

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year

    df["coverage_min"] = df["year"].map(lambda y: coverage_lookup.get(y, {}).get("min_pct_covered", np.nan))
    df["coverage_max"] = df["year"].map(lambda y: coverage_lookup.get(y, {}).get("max_pct_covered", np.nan))
    df["coverage_available"] = df["coverage_min"].notna().astype(int)

    covered_years = [y for y in df["year"].unique() if y in coverage_lookup]
    uncovered_years = [y for y in df["year"].unique() if y not in coverage_lookup]
    print(f"\n  Coverage data available for: {sorted(covered_years)}")
    print(f"  No coverage data for: {sorted(uncovered_years)}")

    return df


def main():
    start = time.time()
    print("=" * 60)
    print("BlackoutWatch - EAGLE-I Preprocessing")
    print("=" * 60)

    # Step 1: Load and filter Texas data from all years
    print("\n[1/6] Loading and filtering Texas data...")
    all_texas = []
    for year in YEARS:
        texas = load_and_filter_texas(year)
        if texas is not None:
            all_texas.append(texas)

    texas_raw = pd.concat(all_texas, ignore_index=True)
    print(f"\n  Combined Texas rows: {len(texas_raw):,}")

    # Step 2: Aggregate to daily
    print("\n[2/6] Aggregating to daily per county...")
    daily = aggregate_to_daily(texas_raw)
    print(f"  Daily outage records: {len(daily):,}")

    # Free memory
    del texas_raw, all_texas

    # Step 3: Build full county-date grid
    print("\n[3/6] Building full county × date grid...")
    full = build_full_grid(daily, YEARS)

    # Step 4: Merge MCC
    print("\n[4/6] Merging MCC (total customers per county)...")
    full = merge_mcc(full)

    # Step 5: Add coverage metadata
    print("\n[5/6] Adding coverage metadata...")
    full = add_coverage_metadata(full)

    print("\n[6/6] Adjusting total customers by year...")
    full = adjust_customers_by_year(full)

    # Add temporal features useful later
    full["month"] = full["date"].dt.month
    full["day_of_week"] = full["date"].dt.dayofweek
    full["day_of_year"] = full["date"].dt.dayofyear

    # Sort and save
    full = full.sort_values(["fips_code", "date"]).reset_index(drop=True)

    outpath = OUT_DIR / "eagle_i_texas_daily.csv"
    full.to_csv(outpath, index=False)

    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"Done in {elapsed:.1f}s")
    print(f"Output: {outpath}")
    print(f"Shape: {full.shape}")
    print(f"Date range: {full['date'].min()} to {full['date'].max()}")
    print(f"Counties: {full['fips_code'].nunique()}")
    print(f"Outage days: {full['outage_flag'].sum():,} / {len(full):,} ({full['outage_flag'].mean()*100:.2f}%)\n")
    print(full.head(15))
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()