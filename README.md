# Council Spend Tracker

**Council Spend Tracker** is a Streamlit app that automatically collects publicly available UK council spending data, normalises it, detects anomalies, and exposes a searchable, downloadable interface.

This repository contains:
- Streamlit app: `app/streamlit_app.py`
- Scraper & ingestion pipeline: `src/`
- GitHub Actions workflow for weekly ingestion: `.github/workflows/scrape.yml`
- Placeholder generator for council data: `scripts/generate_placeholders.py`
- Requirements and deployment instructions.

**Important notes before deployment**
1. You must create a GitHub repository named `CouncilSpendTracker` (or any name) and push the contents of this folder to it.
2. Add a GitHub Personal Access Token (with repo permissions) as secret `GH_PAT` for the Actions workflow to commit data files.
3. Deploy the Streamlit app on Streamlit Community Cloud and connect to this GitHub repo.
4. The placeholder files are not real data. On first Actions run the scraper will fetch real data from council sites and replace placeholders where possible.

See `DEPLOYMENT.md` for detailed steps.
