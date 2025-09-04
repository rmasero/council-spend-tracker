import pandas as pd
from dateutil import parser
import numpy as np

class Normalizer:
    """Normalises varied council spending tables to a common schema:
       ['council','date','supplier','amount','description','source_url','raw']
    """
    def __init__(self):
        self.candidate_date_cols = ['date','transaction_date','payment_date','paid_date']
        self.candidate_supplier_cols = ['supplier','payee','recipient','name','supplier_name']
        self.candidate_amount_cols = ['amount','value','payment_amount','gross_amount','net_amount']
    def normalize(self, df, source_meta=None, council=None):
        df = df.copy()
        df.columns = [c.strip().lower().replace(' ','_') for c in df.columns]
        out = pd.DataFrame()
        out['council'] = council
        # date
        date_col = next((c for c in df.columns if c in self.candidate_date_cols), None)
        if date_col:
            out['date'] = pd.to_datetime(df[date_col], errors='coerce')
        else:
            # try to find a date-like column
            for c in df.columns:
                if 'date' in c:
                    out['date'] = pd.to_datetime(df[c], errors='coerce')
                    date_col = c
                    break
            if 'date' not in out:
                out['date'] = pd.NaT
        # supplier
        supplier_col = next((c for c in df.columns if c in self.candidate_supplier_cols), None)
        if supplier_col:
            out['supplier'] = df[supplier_col].astype(str).fillna('')
        else:
            # fallback to first text column
            text_cols = [c for c in df.columns if df[c].dtype==object]
            out['supplier'] = df[text_cols[0]].astype(str).fillna('') if text_cols else ''
        # amount
        amount_col = next((c for c in df.columns if c in self.candidate_amount_cols), None)
        if amount_col:
            out['amount'] = pd.to_numeric(df[amount_col].replace('[£,]','',regex=True), errors='coerce')
        else:
            # try numeric columns
            num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            out['amount'] = df[num_cols[0]] if num_cols else 0.0
        # description
        desc_cols = [c for c in df.columns if any(k in c for k in ['description','details','desc','purpose','narrative'])]
        out['description'] = df[desc_cols[0]].astype(str) if desc_cols else ''
        out['source_url'] = source_meta.get('url') if source_meta else ''
        out['raw'] = df.to_dict(orient='records')
        out['anomaly_type'] = None
        return out
