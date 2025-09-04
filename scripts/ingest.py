#!/usr/bin/env python3
"""
scripts/ingest.py

Robust ingestion script for GitHub Actions.

- Ensures repo root and src/ are on sys.path so `from src.scraper import CouncilScraper` works.
- Fetches councils via the scraper, downloads one spending file per council (first usable),
  saves per-council CSVs to data/councils/, and writes run metadata to data/last_run.log.
"""

import sys
import importlib.util
import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]  # repository root (one level above scripts/)
SRC_DIR = ROOT / "src"
DATA_DIR = ROOT / "data"
COUNCILS_DIR = DATA_DIR / "councils"
LOG_FILE = DATA_DIR / "last_run.log"
LAST_UPDATED = DATA_DIR / "last_updated.txt"

# Ensure repo root and src directory are on sys.path (helps when repo dir name/casing differs)
sys.path.insert(0, str(ROOT))
if SRC_DIR.exists():
    sys.path.insert(0, str(SRC_DIR))

# Try normal import first; if it fails, load scraper.py directly by path
try:
    from src.scraper import CouncilScraper
except Exception as exc:
    spec = importlib.util.spec_from_file_location("scraper", str(SRC_DIR / "scraper.py"))
    if spec is None or spec.loader is None:
        raise ImportError("Could not import src.scraper and failed to load scraper.py by path.") from exc
    scraper_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(scraper_mod)
    CouncilScraper = getattr(scraper_mod, "CouncilScraper")

# utilities
def save_df_safe(df, outpath: Path) -> bool:
    """Attempt to save dataframe to CSV; fall back to string coercion if needed."""
    try:
        df.to_csv(outpath, index=False)
        return True
    except Exception:
        try:
            df.astype(str).to_csv(outpath, index=False)
            return True
        except Exception as e:
            print(f"[ERROR] Failed to save dataframe to {outpath}: {e}")
            return False

def ensure_dirs():
    COUNCILS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def main():
    ensure_dirs()
    scraper = CouncilScraper()

    try:
        councils = scraper.get_known_councils() or []
    except Exception as e:
        print(f"[ERROR] scraper.get_known_councils() failed: {e}")
        councils = []

    print(f"[INFO] Councils discovered: {len(councils)}")

    total_ingested = 0
    details = []

    for council in councils:
        try:
            files = scraper.find_spend_files_for_council(council) or []
        except Exception as e:
            print(f"[WARN] find_spend_files_for_council failed for {council}: {e}")
            details.append({"council": council, "found": 0, "error": str(e)})
            continue

        if not files:
            print(f"[INFO] No candidate files for council: {council}")
            details.append({"council": council, "found": 0})
            continue

        ingested_for_council = 0
        # Try files in order; save the first successfully downloaded DataFrame
        for fmeta in files:
            try:
                print(f"[INFO] Downloading {fmeta.get('url')} for {council}")
                df = scraper.download_dataframe(fmeta)
                if df is None or getattr(df, "empty", False):
                    print(f"[INFO] No table returned for {fmeta.get('url')}")
                    continue
                safe_name = council.replace(" ", "_").replace("/", "_").replace("'", "")
                outpath = COUNCILS_DIR / f"{safe_name}.csv"
                if save_df_safe(df, outpath):
                    ingested_for_council += 1
                    total_ingested += 1
                    # break after first successful file per council (change if you want multiple per council)
                    break
            except Exception as e:
                print(f"[WARN] Error downloading/saving file for {council}: {e}")
                continue

        details.append({
            "council": council,
            "found": len(files),
            "ingested": ingested_for_council
        })

    # Write summary log
    summary = {
        "run_at": datetime.utcnow().isoformat() + "Z",
        "councils_total": len(councils),
        "total_ingested": total_ingested,
        "details": details
    }
    with open(LOG_FILE, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(summary, indent=2))

    # Write last updated timestamp
    with open(LAST_UPDATED, "w", encoding="utf-8") as fh:
        fh.write(datetime.utcnow().isoformat() + "Z")

    print(f"[DONE] Ingest complete: {total_ingested} councils saved ({len(councils)} discovered).")
    print(f"[INFO] Summary written to {LOG_FILE}")

if __name__ == "__main__":
    main()
