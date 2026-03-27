"""
BlackoutWatch - Map Texas GHCN Stations to Counties
Uses the FCC Census Block API to map active Texas weather stations to county FIPS codes.

Usage:
    python scripts/03_scrape_station_data.py

Input:  data/processed/tx_active_stations.csv
Output: data/processed/station_county_mapping_fcc.csv
"""

import pandas as pd
import requests
import time

# tx_stations = set()
# with open('data/raw/ghcn_daily/ghcnd-stations.txt') as f:
#     for line in f:
#         if line[38:40].strip() == 'TX':
#             tx_stations.add(line[:11].strip())

# cols = ['station_id','date','element','value','m_flag','q_flag','s_flag','obs_time']
# all_active = set()

# for year in range(2014, 2024):
#     chunks = pd.read_csv(f'data/raw/ghcn_daily/{year}.csv', header=None, names=cols, usecols=[0], chunksize=1_000_000)
#     year_active = set()
#     for chunk in chunks:
#         matches = chunk[chunk['station_id'].isin(tx_stations)]
#         year_active.update(matches['station_id'].unique())
#     all_active.update(year_active)
#     print(f"{year}: {len(year_active)} active stations")

# print(f"\nTotal unique TX stations active across 2014-2023: {len(all_active)}")

# # Get lat/lon for active stations
# tx_station_info = {}
# with open('data/raw/ghcn_daily/ghcnd-stations.txt') as f:
#     for line in f:
#         sid = line[:11].strip()
#         if sid in all_active:
#             lat = float(line[12:20].strip())
#             lon = float(line[21:30].strip())
#             tx_station_info[sid] = {'lat': lat, 'lon': lon}

# print(f"Stations with coordinates: {len(tx_station_info)}")

# # Save for later use
# pd.DataFrame([
#     {'station_id': sid, 'lat': info['lat'], 'lon': info['lon']}
#     for sid, info in tx_station_info.items()
# ]).to_csv('data/processed/tx_active_stations.csv', index=False)
# print("Saved to data/processed/tx_active_stations.csv")

stations = pd.read_csv('data/processed/tx_active_stations.csv')
print(f"Mapping {len(stations)} stations to counties via FCC API...")

FCC_URL = "https://geo.fcc.gov/api/census/block/find?format=json&latitude={lat}&longitude={lon}"

results = []
failed = []

for i, row in stations.iterrows():
    try:
        resp = requests.get(
            FCC_URL.format(lat=row['lat'], lon=row['lon']),
            timeout=10
        )
        data = resp.json()
        county_fips = data['County']['FIPS']  # 5-digit county FIPS
        county_name = data['County']['name']
        results.append({
            'station_id': row['station_id'],
            'lat': row['lat'],
            'lon': row['lon'],
            'fips_code': county_fips,
            'county_name': county_name,
        })
    except Exception as e:
        failed.append({'station_id': row['station_id'], 'error': str(e)})

    # Progress update every 50 stations
    if (i + 1) % 50 == 0:
        print(f"  {i + 1}/{len(stations)} done ({len(failed)} failed)")

    time.sleep(0.2)  # ~5 requests/sec, be nice to the API

mapping = pd.DataFrame(results)
mapping.to_csv('data/processed/station_county_mapping_fcc.csv', index=False)

print(f"\nDone! Mapped: {len(results)}, Failed: {len(failed)}")
if failed:
    print(f"Failed stations: {failed[:5]}...")
