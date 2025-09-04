import requests
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[0]

class CouncilScraper:
    """
    Scraper for UK councils and their published spending datasets.
    - Downloads and combines datasets for all councils
    """

    def get_all_councils(self):
        # Replace with actual list of councils
        return ["Rochdale Borough Council", "East Sussex County Council", "Another Council"]

    def download_spending_data(self, council_name):
        search_url = f"https://data.gov.uk/search?q={council_name}+spending+over+500"
        print(f"[DEBUG] Searching for: {search_url}")
        try:
            r = requests.get(search_url, timeout=30)
            r.raise_for_status()
            # Parse the response to find the dataset URL
            # This is a placeholder; actual implementation will depend on the structure of the response
            dataset_url = self.extract_dataset_url(r.text)
            if dataset_url:
                print(f"[DEBUG] Found dataset for {council_name}: {dataset_url}")
                return self.download_dataset(dataset_url)
            else:
                print(f"[WARN] No dataset found for {council_name}")
                return None
        except Exception as e:
            print(f"[ERROR] Failed to fetch data for {council_name}: {e}")
            return None

    def extract_dataset_url(self, html):
        # Implement logic to extract dataset URL from HTML response
        return None

    def download_dataset(self, url):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            # Determine file format and read accordingly
            if url.endswith('.csv'):
                return pd.read_csv(r.text)
            elif url.endswith('.xls') or url.endswith('.xlsx'):
                return pd.read_excel(r.content)
            else:
                print(f"[WARN] Unsupported file format: {url}")
                return None
        except Exception as e:
            print(f"[ERROR] Failed to download dataset: {e}")
            return None

    def combine_datasets(self, datasets):
        combined = pd.concat(datasets, ignore_index=True)
        combined.to_csv(ROOT / "combined_spending_data.csv", index=False)
        print(f"[DEBUG] Combined dataset saved to {ROOT / 'combined_spending_data.csv'}")
