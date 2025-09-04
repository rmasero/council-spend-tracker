"""Generates placeholder CSV files for councils. This script will create a basic CSV for a list of councils.
You should replace scripts/councils_list.csv with a full authoritative list. This script can be used
to generate per-council placeholder files before the first real ingestion run.
"""
import os
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / 'data' / 'councils'
DATA_DIR.mkdir(parents=True, exist_ok=True)

councils_csv = ROOT / 'scripts' / 'councils_list.csv'
if councils_csv.exists():
    councils = pd.read_csv(councils_csv)['council'].dropna().tolist()
else:
    councils = [
        'Birmingham City Council',
        'Manchester City Council',
        'Leeds City Council',
        'Glasgow City Council',
        'Westminster City Council'
    ]

for c in councils:
    fname = c.replace('/','_').replace(' ','_') + '.csv'
    path = DATA_DIR / fname
    if path.exists():
        continue
    df = pd.DataFrame(columns=['council','date','supplier','amount','description','source_url','anomaly_type'])
    df.to_csv(path, index=False)
print(f'Placeholders created in {DATA_DIR}')
