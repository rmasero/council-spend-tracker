import streamlit as st
import pandas as pd
from pathlib import Path
import json

DATA_DIR = Path("data")
COUNCILS_DIR = DATA_DIR / "councils"
LOG_FILE = DATA_DIR / "last_run.log"

st.title("Council Spend Tracker")

# Show last run log
if LOG_FILE.exists():
    with open(LOG_FILE) as f:
        log = json.load(f)
    st.subheader("Last ingestion summary")
    st.json(log)
else:
    st.info("No ingestion log found. Data may not have been downloaded yet.")

# List councils
council_files = sorted(COUNCILS_DIR.glob("*.csv"))
if council_files:
    council_name = st.selectbox("Select Council", [f.stem for f in council_files])
    df = pd.read_csv(COUNCILS_DIR / f"{council_name}.csv")
    st.dataframe(df)
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", csv, file_name=f"{council_name}.csv")
else:
    st.info("No council data available. Check ingestion logs for errors.")
