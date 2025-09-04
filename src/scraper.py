import requests
import pandas as pd
from io import StringIO

class CouncilScraper:
    """
    Scraper for UK councils and their published spending datasets.
    Includes full debug logging.
    """

    MYSOCIETY_URL = "https://raw.githubusercontent.com/mysociety/uk-councils/master/data/councils.csv"
    CKAN_SEARCH_URL = "https://data.gov.uk/api/3/action/package_search?q="

    def get_known_councils(self):
        """
        Fetch the list of councils from mySociety.
        Returns a list of council names.
        """
        print("[DEBUG] Fetching council list from mySociety")
        try:
            r = requests.get(self.MYSOCIETY_URL, timeout=30)
            r.raise_for_status()
            df = pd.read_csv(StringIO(r.text))
            # Inspect columns to confirm
            print(f"[DEBUG] Columns in CSV: {df.columns.tolist()}")
            if 'name' in df.columns:
                councils = df['name'].tolist()
            elif 'title' in df.columns:
                councils = df['title'].tolist()
            else:
                print("[ERROR] Could not find a column for council names")
                councils = []
            print(f"[DEBUG] Retrieved {len(councils)} councils")
            return councils
        except Exception as e:
            print(f"[ERROR] Failed to fetch councils: {e}")
            return []

    def find_spend_files_for_council(self, council_name):
        """
        Search data.gov.uk for council spending CSVs.
        Returns a list of dicts with keys: url, title.
        """
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
        """
        Download a CSV/XLS/XLSX resource into a DataFrame.
        """
        url = resource.get('url')
        print(f"[DEBUG] Downloading resource: {url}")
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            if url.endswith('.csv'):
                df = pd.read_csv(StringIO(r.text))
            elif url.endswith(('.xls', '.xlsx')):
                from io import BytesIO
                df = pd.read_excel(BytesIO(r.content))
            else:
                print(f"[WARN] Unsupported file format for {url}")
                return None
            print(f"[DEBUG] Downloaded {len(df)} rows from {url}")
            return df
        except Exception as e:
            print(f"[ERROR] Failed to download {url}: {e}")
            return None
