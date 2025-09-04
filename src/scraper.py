import requests
import pandas as pd
from io import StringIO, BytesIO
from pathlib import Path
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[0]

class CouncilScraper:
    """
    Scraper for UK councils and their published spending datasets.
    - Primary source: mySociety CSV
    - Fallback: gov.uk "Find your local council" page
    - Full debug logging and raw CSV capture
    """

    MYSOCIETY_URL = "https://raw.githubusercontent.com/mysociety/uk-councils/master/data/councils.csv"
    GOV_UK_URL = "https://www.gov.uk/find-local-council"
    CKAN_SEARCH_URL = "https://data.gov.uk/api/3/action/package_search?q="

    def get_known_councils(self):
        councils = []
        # Attempt 1: mySociety CSV
        print("[DEBUG] Fetching council list from mySociety CSV")
        try:
            r = requests.get(self.MYSOCIETY_URL, timeout=30)
            r.raise_for_status()

            data_dir = ROOT.parent / "data"
            data_dir.mkdir(exist_ok=True)
            raw_csv_path = data_dir / "raw_councils.csv"
            with open(raw_csv_path, "w", encoding="utf-8") as f:
                f.write(r.text)
            print(f"[DEBUG] Saved raw council CSV to {raw_csv_path}")

            df = pd.read_csv(StringIO(r.text))
            print(f"[DEBUG] Columns in CSV: {df.columns.tolist()}")
            if 'name' in df.columns:
                councils = df['name'].tolist()
            elif 'title' in df.columns:
                councils = df['title'].tolist()
            else:
                print("[WARN] Could not find column for council names in CSV")
        except Exception as e:
            print(f"[ERROR] Failed to fetch mySociety CSV: {e}")

        # Fallback: gov.uk
        if not councils:
            print("[DEBUG] Falling back to scraping gov.uk")
            try:
                r = requests.get(self.GOV_UK_URL, timeout=30)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
                # Select links to individual councils
                links = soup.select("a[href*='/local-council/']")
                councils = [link.get_text(strip=True) for link in links if link.get_text(strip=True)]
                # Remove generic page titles
                councils = [c for c in councils if c.lower() not in ("local councils and services", "find your local council")]
                councils = list(set(councils))  # deduplicate
                print(f"[DEBUG] Retrieved {len(councils)} councils from gov.uk fallback")
            except Exception as e:
                print(f"[ERROR] Failed gov.uk fallback: {e}")
                councils = []

        print(f"[DEBUG] Total councils retrieved: {len(councils)}")
        return councils

    def find_spend_files_for_council(self, council_name):
        print(f"[DEBUG] Searching datasets for council: {council_name}")
        try:
            q = f"{council_name} expenditure OR spend"
            r = requests.get(self.CKAN_SEARCH_URL + q, timeout=30)
            r.raise_for_status()
            data = r.json()
            results = data.get('result', {}).get('results', [])
            print(f"[DEBUG] CKAN returned {len(results)} datasets for {council_name}")
            files = []
            for res in results:
                for resource in res.get('resources', []):
                    if resource.get('format', '').lower() in ('csv', 'xls', 'xlsx'):
                        files.append({'url': resource['url'], 'title': resource.get('name', '')})
            print(f"[DEBUG] Found {len(files)} spend files for {council_name}")
            return files
        except Exception as e:
            print(f"[ERROR] CKAN search failed for {council_name}: {e}")
            return []

    def download_dataframe(self, resource):
        url = resource.get('url')
        print(f"[DEBUG] Downloading resource: {url}")
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            if url.endswith('.csv'):
                df = pd.read_csv(StringIO(r.text))
            elif url.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(BytesIO(r.content))
            else:
                print(f"[WARN] Unsupported file format for {url}")
                return None
            print(f"[DEBUG] Downloaded {len(df)} rows from {url}")
            return df
        except Exception as e:
            print(f"[ERROR] Failed to download {url}: {e}")
            return None
