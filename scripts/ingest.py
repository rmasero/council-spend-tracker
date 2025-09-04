# scripts/ingest.py

import os
import pandas as pd
from datetime import datetime
from src.scraper import CouncilScraper

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "councils")
LOG_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "last_run.log")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    scraper = CouncilScraper()

    councils = scraper.get_known_councils()
    print(f"Found {len(councils)} councils from register")

    total_files = 0
    for council in councils:
        files = scraper.find_spend_files_for_council(council)
        if not files:
            continue
        # Try first available file for now
        df = scraper.download_dataframe(files[0])
        if df is None or df.empty:
            continue
        safe_name = council.replace(" ", "_").replace("/", "_")
        outpath = os.path.join(DATA_DIR, f"{safe_name}.csv")
        try:
            df.to_csv(outpath, index=False)
            print(f"Saved {outpath} ({len(df)} rows)")
            total_files += 1
        except Exception as e:
            print(f"Failed to save {council}: {e}")

    # Write log
    with open(LOG_FILE, "w") as f:
        f.write(f"Run completed {datetime.utcnow().isoformat()} UTC\n")
        f.write(f"Councils discovered: {len(councils)}\n")
        f.write(f"Councils ingested: {total_files}\n")

    print(f"✅ Ingest complete: {total_files}/{len(councils)} councils saved")

if __name__ == "__main__":
    main()
