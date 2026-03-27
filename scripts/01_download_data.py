"""
BlackoutWatch - Step 01: Download EAGLE-I Data
Downloads EAGLE-I outage CSVs from Figshare and filters to Texas (FIPS starting with 48).
Also downloads supplementary files (MCC.csv, coverage_history.csv, DQI.csv).

Usage:
    python scripts/01_download_data.py

Requirements:
    pip install requests tqdm
"""

import requests
from pathlib import Path
from tqdm import tqdm

# --- Configuration ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "eagle_i"
FIGSHARE_API = "https://api.figshare.com/v2/articles/24237376/files"

# Years to download (2014-2022 for best coverage; 2023-2024 also available)
YEARS = list(range(2014, 2023))  # Adjust if you want 2023/2024 too

# Supplementary files we also want
SUPPLEMENTARY_FILES = ["MCC.csv", "coverage_history.csv", "DQI.csv"]


def get_file_list():
    """Fetch the list of files from Figshare API."""
    print("Fetching file list from Figshare...")
    resp = requests.get(FIGSHARE_API)
    resp.raise_for_status()
    files = resp.json()
    print(f"Found {len(files)} files in the dataset")
    return files


def download_file(url, dest_path, name):
    """Download a file with progress bar."""
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))

    with open(dest_path, "wb") as f:
        with tqdm(total=total, unit="B", unit_scale=True, desc=name) as pbar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))


def main():
    # Create output directory
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Get file listing from Figshare
    files = get_file_list()

    # Build list of files we want
    # Outage files: eaglei_outages_YYYY.csv
    # Supplementary: MCC.csv, coverage_history.csv, DQI.csv
    target_names = [f"eaglei_outages_{year}.csv" for year in YEARS]
    target_names.extend(SUPPLEMENTARY_FILES)

    files_to_download = []
    for f in files:
        if f["name"] in target_names:
            files_to_download.append(f)

    print(f"\nWill download {len(files_to_download)} files:")
    for f in files_to_download:
        size_mb = f["size"] / (1024 * 1024)
        print(f"  {f['name']} ({size_mb:.1f} MB)")

    total_size = sum(f["size"] for f in files_to_download) / (1024 * 1024)
    print(f"\nTotal download size: {total_size:.1f} MB")

    # Check for files we expected but didn't find
    found_names = {f["name"] for f in files_to_download}
    missing = set(target_names) - found_names
    if missing:
        print(f"\n⚠️  Files not found on Figshare: {missing}")
        print("   They may have different names. Check the Figshare page manually.")

    # Download each file
    print("\n--- Starting downloads ---\n")
    for f in files_to_download:
        dest = RAW_DIR / f["name"]

        # Skip if already downloaded
        if dest.exists() and dest.stat().st_size == f["size"]:
            print(f"✓ {f['name']} already exists, skipping")
            continue

        print(f"Downloading {f['name']}...")
        download_file(f["download_url"], dest, f["name"])
        print(f"✓ Saved to {dest}\n")

    print("\n--- All downloads complete ---")
    print(f"Files saved to: {RAW_DIR}")
    print(f"\nNext step: Run 02_preprocess_eagle_i.py to build the Texas county-day outage dataset")


if __name__ == "__main__":
    main()
