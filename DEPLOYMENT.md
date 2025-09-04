# Deployment Instructions

1. Create a new **public** GitHub repository and push this repo.
2. In the repository settings -> Secrets -> Actions, add:
   - `GH_PAT` : a Personal Access Token with `repo` scope so the workflow can commit data updates.
3. Enable GitHub Actions (it is on by default).
4. Deploy `app/streamlit_app.py` to Streamlit Community Cloud:
   - Sign in to Streamlit Community Cloud, click "New app", connect to the GitHub repo and select `app/streamlit_app.py`.
5. The workflow `.github/workflows/scrape.yml` runs weekly and will:
   - Run the ingestion pipeline (`src/ingest.py`).
   - Commit `data/processed.parquet` and per-council CSV files back to the repo.
6. First run may take longer; monitor Actions logs for errors and adjust scraper heuristics in `src/scraper.py`.

