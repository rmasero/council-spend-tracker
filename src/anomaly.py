import pandas as pd
import numpy as np
from scipy import stats

class AnomalyDetector:
    def __init__(self):
        pass
    def run_all(self, df):
        df = df.copy()
        df['anomaly_type'] = None
        # numeric outliers by Z-score per council
        for cname, g in df.groupby('council'):
            if g['amount'].dropna().empty:
                continue
            amounts = g['amount'].fillna(0.0)
            z = np.abs(stats.zscore(amounts.replace([np.inf, -np.inf], np.nan).fillna(0.0)))
            mask = z > 3
            df.loc[g.index[mask], 'anomaly_type'] = df.loc[g.index[mask], 'anomaly_type'].fillna('high_zscore')
        # duplicate invoices exact match
        dup_mask = df.duplicated(subset=['council','date','supplier','amount'], keep=False)
        df.loc[dup_mask, 'anomaly_type'] = df.loc[dup_mask, 'anomaly_type'].fillna('possible_duplicate')
        # round-number anomalies (amounts divisible by 1000)
        round_mask = df['amount'].notna() & (df['amount'] % 1000 == 0)
        df.loc[round_mask, 'anomaly_type'] = df.loc[round_mask, 'anomaly_type'].fillna('round_number')
        # splitting payments: repeated payments to same supplier with similar small amounts
        grp = df.groupby(['council','supplier'])['amount'].transform('count')
        split_mask = grp > 5
        df.loc[split_mask, 'anomaly_type'] = df.loc[split_mask, 'anomaly_type'].fillna('repeated_small_payments')
        return df
