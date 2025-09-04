import streamlit as st
import pandas as pd
import os
from st_aggrid import AgGrid, GridOptionsBuilder
from pathlib import Path
from datetime import datetime

st.set_page_config(page_title='Council Spend Tracker', layout='wide')

DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
PROCESSED_FILE = DATA_DIR / 'processed.parquet'
LAST_UPDATED_FILE = DATA_DIR / 'last_updated.txt'

def load_data():
    if PROCESSED_FILE.exists():
        df = pd.read_parquet(PROCESSED_FILE)
    else:
        # fallback: load any CSVs in data/councils
        csvs = list((DATA_DIR / 'councils').glob('*.csv')) if (DATA_DIR / 'councils').exists() else []
        if csvs:
            df = pd.concat([pd.read_csv(p) for p in csvs], ignore_index=True)
        else:
            df = pd.DataFrame(columns=['council','date','supplier','amount','description','source_url','anomaly_type'])
    return df

@st.cache_data(ttl=3600)
def get_df():
    return load_data()

df = get_df()

st.sidebar.title("Council Spend Tracker")
st.sidebar.write("Slick, simple interface to explore council spending and flagged anomalies.")

last_updated = None
if LAST_UPDATED_FILE.exists():
    last_updated = LAST_UPDATED_FILE.read_text().strip()
st.sidebar.markdown(f"**Data last updated:** {last_updated or 'unknown'}")

councils = sorted(df['council'].dropna().unique().tolist())
council = st.sidebar.selectbox("Select council", ["All"] + councils)

anomaly_types = sorted(df['anomaly_type'].dropna().unique().tolist())
sel_anoms = st.sidebar.multiselect("Filter Anomaly Types", options=anomaly_types)

min_amt, max_amt = None, None
if 'amount' in df.columns:
    min_amt = float(df['amount'].min()) if not df['amount'].empty else 0.0
    max_amt = float(df['amount'].max()) if not df['amount'].empty else 0.0

st.title("Council Spend Tracker")
st.write("Browse, filter, and download council spending. Anomalies are highlighted.")

if council != "All":
    view = df[df['council']==council].copy()
else:
    view = df.copy()

if sel_anoms:
    view = view[view['anomaly_type'].isin(sel_anoms)]

st.markdown(f"**Rows:** {len(view)}")

tabs = st.tabs(["Overview","Anomalies","All Spend","Charts"])
with tabs[0]:
    st.header("Overview")
    total = view['amount'].sum() if 'amount' in view.columns else 0
    st.metric("Total spend in view", f"£{total:,.2f}")
    st.write("Top suppliers")
    top = view.groupby('supplier')['amount'].sum().reset_index().sort_values('amount', ascending=False).head(10)
    st.dataframe(top)

with tabs[1]:
    st.header("Anomalies")
    anom = view[view['anomaly_type'].notna()].copy()
    if anom.empty:
        st.info("No anomalies in the current view.")
    else:
        gb = GridOptionsBuilder.from_dataframe(anom)
        gb.configure_default_column(editable=False, filter=True, sortable=True)
        gb.configure_selection(selection_mode='single')
        gridOptions = gb.build()
        AgGrid(anom, gridOptions=gridOptions, fit_columns_on_grid_load=True)

with tabs[2]:
    st.header("All Spend")
    gb = GridOptionsBuilder.from_dataframe(view)
    gb.configure_default_column(filter=True, sortable=True, resizable=True)
    gridOptions = gb.build()
    AgGrid(view, gridOptions=gridOptions, fit_columns_on_grid_load=True)
    csv = view.to_csv(index=False)
    st.download_button("Download CSV of filtered data", data=csv, file_name="council_spend_filtered.csv", mime="text/csv")

with tabs[3]:
    st.header("Charts")
    st.write("Charts and visualisations coming soon. This space can show trends, anomaly heatmaps and supplier concentration.")
