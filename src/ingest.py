#!/usr/bin/env python3
import sys
from pathlib import Path
from datetime import datetime
import json

ROOT = Path(__file__).resolve().parents[0]  # repo root
SRC_DIR = ROOT / "src"
DATA_DIR = ROOT / "data"
COUNCILS_DIR = DATA_DIR / "councils"
LOG_FILE = DATA_DIR / "last_run.log"
LAST_UPDATED = DATA_DIR / "last_updated.txt"

sys.path.insert(0, str(SRC_DIR))

try:
    from scraper import CouncilScraper
except Exception as e:
    print(f"[ERROR] Cannot import scraper: {e}")
    sys.exit(1)

def save_df_safe(df, outpath):
    try:
        df.to_csv(outpath, index=False)
        return True
    except Exception:
        try:
            df.astype(str).to_csv(outpath, index=False)
            return True
        except Exception as e:
            print(f"[ERROR] Failed to save dataframe {outpath}: {e}")
            return False

def main():
    COUNCILS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    scraper = CouncilScraper()
    councils = scraper.get_known_councils()
    print(f"[INFO] Discovered {len(councils)} councils")

    total_ingested = 0
    details = []

    for council in councils:
        files = scraper.find_spend_files_for_council(council)
        if not files:
            print(f"[INFO] No spending files found for {council}")
            details.append({"council": council, "found": 0})
            continue

        ingested = 0
        for f in files:
            df = scraper.download_dataframe(f)
            if df is not None and not df.empty:
                safe_name = council.replace(" ", "_").replace("/", "_").replace("'", "")
                outpath = COUNCILS_DIR / f"{safe_name}.csv"
                if save_df_safe(df, outpath):
                    ingested += 1
                    total_ingested += 1
                    break  # only first successful file per council

        details.append({"council": council, "found": len(files), "ingested": ingested})

    summary = {
        "run_at": datetime.utcnow().isoformat() + "Z",
        "councils_total": len(councils),
        "total_ingested": total_ingested,
        "details": details
    }

    with open(LOG_FILE, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(summary, indent=2))

    with open(LAST_UPDATED, "w", encoding="utf-8") as fh:
        fh.write(datetime.utcnow().isoformat() + "Z")

    print(f"[DONE] Ingest complete: {total_ingested}/{len(councils)} councils saved")
    print(f"[DEBUG LOG] Saved to {LOG_FILE}")

if __name__ == "__main__":
    main()
