"""
BlackoutWatch - Build Final Station-to-County Mapping
Compares FCC API county assignments to a nearest-centroid fallback and writes a final mapping.

Usage:
    python scripts/04_county_station_mapping.py

Inputs:
    data/processed/tx_active_stations.csv
    data/processed/station_county_mapping_fcc.csv
    data/raw/us_county_latlng.csv

Outputs:
    data/processed/station_county_mapping_centroid.csv
    data/processed/station_county_mapping_final.csv
"""

import pandas as pd
from scipy.spatial import KDTree

# Load station data
stations = pd.read_csv('data/processed/tx_active_stations.csv')

# Download county centroids separately before running this script:
# curl -O https://gist.githubusercontent.com/russellsamora/12be4f9f574e92413ea3f92ce1bc58e6/raw/us_county_latlng.csv
# Save to data/raw/us_county_latlng.csv

counties = pd.read_csv('data/raw/us_county_latlng.csv')
counties['fips_code'] = counties['fips_code'].astype(str).str.zfill(5)

# Filter to Texas counties (FIPS starting with 48)
tx_counties = counties[counties['fips_code'].str.startswith('48')].copy()
print(f"Texas counties with centroids: {len(tx_counties)}")

# Build KDTree from county centroids
county_coords = tx_counties[['lat', 'lng']].values
tree = KDTree(county_coords)

# Query nearest county for each station
station_coords = stations[['lat', 'lon']].values
distances, indices = tree.query(station_coords)

# Build mapping
centroid_mapping = stations[['station_id', 'lat', 'lon']].copy()
centroid_mapping['fips_code'] = tx_counties.iloc[indices]['fips_code'].values
centroid_mapping['county_name'] = tx_counties.iloc[indices]['name'].values
centroid_mapping['distance_deg'] = distances  # in degrees, ~111km per degree

print(f"\nMapping stats:")
print(f"  Median distance to centroid: {centroid_mapping['distance_deg'].median():.4f} degrees ({centroid_mapping['distance_deg'].median() * 111:.1f} km)")
print(f"  Max distance to centroid: {centroid_mapping['distance_deg'].max():.4f} degrees ({centroid_mapping['distance_deg'].max() * 111:.1f} km)")
print(f"  Stations >0.5 degrees from centroid: {(centroid_mapping['distance_deg'] > 0.5).sum()}")

centroid_mapping.to_csv('data/processed/station_county_mapping_centroid.csv', index=False)
print(f"\nSaved to data/processed/station_county_mapping_centroid.csv")

# --- Compare the two approaches ---
fcc = pd.read_csv('data/processed/station_county_mapping_fcc.csv', dtype={'fips_code': str})
centroid = pd.read_csv('data/processed/station_county_mapping_centroid.csv', dtype={'fips_code': str})

fcc['fips_code'] = fcc['fips_code'].str.zfill(5)
centroid['fips_code'] = centroid['fips_code'].str.zfill(5)

merged = fcc.merge(centroid, on='station_id', suffixes=('_fcc', '_centroid'))
agreement = (merged['fips_code_fcc'] == merged['fips_code_centroid']).mean()
print(f"\nAgreement between FCC and nearest-centroid: {agreement*100:.1f}%")

disagree = merged[merged['fips_code_fcc'] != merged['fips_code_centroid']]
print(f"Disagreements: {len(disagree)}")
print(disagree[['station_id', 'fips_code_fcc', 'fips_code_centroid', 'distance_deg']].head(10))

# --- Build final mapping: FCC primary, centroid as fallback ---
fcc_stations = set(fcc['station_id'])
centroid_only = centroid[~centroid['station_id'].isin(fcc_stations)].copy()
centroid_only['source'] = 'centroid'
print(f"\nFilling {len(centroid_only)} failed FCC stations with centroid mapping")

fcc['source'] = 'fcc'
final = pd.concat([fcc, centroid_only[['station_id', 'lat', 'lon', 'fips_code', 'county_name', 'source']]], ignore_index=True)

print(f"\nFinal mapping: {len(final)} stations")
print(f"  FCC: {(final['source'] == 'fcc').sum()}")
print(f"  Centroid fallback: {(final['source'] == 'centroid').sum()}")
print(f"  Unique counties covered: {final['fips_code'].nunique()}")

final.to_csv('data/processed/station_county_mapping_final.csv', index=False)
print("Saved to data/processed/station_county_mapping_final.csv")
