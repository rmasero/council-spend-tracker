"""Entry point for ingestion pipeline. Run by GitHub Actions on schedule.

This script:
- Loads a list of councils (from scripts/councils_list.csv if present, otherwise tries to fetch).
- Runs scraper to discover candidate spending files (CSV/XLSX).
- Normalises found files and appends to a processed dataframe.
- Runs anomaly detection.
- Writes data/data files and commits are done by the GitHub Action.
"""
import os
from pathlib import Path
from scraper import CouncilScraper
from normalizer import Normalizer
from anomaly import AnomalyDetector
import pandas as pd
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / 'data'
DATA_DIR.mkdir(exist_ok=True)
COUNCILS_CSV = ROOT / 'scripts' / 'councils_list.csv'

scraper = CouncilScraper()
normalizer = Normalizer()
anomaly = AnomalyDetector()

councils = []
if COUNCILS_CSV.exists():
    councils = pd.read_csv(COUNCILS_CSV)['council'].dropna().tolist()
else:
    # fallback: scraper can attempt to fetch major council list
    councils = scraper.get_known_councils()

all_rows = []
for c in councils:
    print(f"Processing council: {c}")
    try:
        files = scraper.find_spend_files_for_council(c)
        for fmeta in files:
            df = scraper.download_dataframe(fmeta)
            if df is None or df.empty:
                continue
            std = normalizer.normalize(df, source_meta=fmeta, council=c)
            all_rows.append(std)
    except Exception as e:
        print(f"Error processing {c}: {e}")

if all_rows:
    processed = pd.concat(all_rows, ignore_index=True)
else:
    processed = pd.DataFrame(columns=['council','date','supplier','amount','description','source_url','anomaly_type'])

processed['date'] = pd.to_datetime(processed['date'], errors='coerce')
processed['amount'] = pd.to_numeric(processed['amount'], errors='coerce').fillna(0.0)

# run anomaly detection
processed = anomaly.run_all(processed)

# write outputs
processed_file = DATA_DIR / 'processed.parquet'
processed.to_parquet(processed_file, index=False)

last_updated = DATA_DIR / 'last_updated.txt'
last_updated.write_text(datetime.utcnow().isoformat() + 'Z')

# also dump per-council CSVs
councils_dir = DATA_DIR / 'councils'
councils_dir.mkdir(exist_ok=True)
for cname, g in processed.groupby('council'):
    safe = cname.replace('/','_').replace(' ','_')
    (councils_dir / f"{safe}.csv").to_csv(index=False, path_or_buf=(councils_dir / f"{safe}.csv"))
print('Ingestion complete.')
