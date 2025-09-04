import requests
import pandas as pd
from pathlib import Path
from io import StringIO, BytesIO
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[0]

class CouncilScraper:
    DATA_DIR = ROOT.parent / "data"
    DATA_DIR.mkdir(exist_ok=True)

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
    }

    GOV_UK_COUNCILS_URL = "https://www.gov.uk/find-local-council"
    CKAN_SEARCH_URL = "https://data.gov.uk/api/3/action/package_search?q="

    def get_known_councils(self):
        """Fixed fallback to get all councils"""
        try:
            r = requests.get(self.GOV_UK_COUNCILS_URL, headers=self.HEADERS, timeout=30)
            r.raise_for_status()

            html_path = self.DATA_DIR / "raw_govuk.html"
            html_path.write_text(r.text, encoding="utf-8")
            print(f"[DEBUG] Saved gov.uk HTML to {html_path}")

            soup = BeautifulSoup(r.text, "html.parser")
            links = soup.select("a[href*='/local-council/']")
            councils = [link.get_text(strip=True) for link in links if link.get_text(strip=True)]
            councils = [c for c in councils if c.lower() not in ("local councils and services", "find your local council")]
            councils = list(set(councils))
            print(f"[DEBUG] Retrieved {len(councils)} councils from gov.uk")
            return councils
        except Exception as e:
            print(f"[ERROR] Failed to fetch councils: {e}")
            return []

    def find_spend_files_for_council(self, council_name):
        try:
            query = f"{council_name} spending over 500"
            r = requests.get(self.CKAN_SEARCH_URL + query, headers=self.HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
            results = data.get("result", {}).get("results", [])
            files = []
            for res in results:
                for resource in res.get("resources", []):
                    if resource.get("format", "").lower() in ("csv", "xls", "xlsx"):
                        files.append({"url": resource["url"], "title": resource.get("name", "")})
            return files
        except Exception as e:
            print(f"[ERROR] CKAN search failed for {council_name}: {e}")
            return []

    def download_dataset(self, resource):
        url = resource.get("url")
        try:
            r = requests.get(url, headers=self.HEADERS, timeout=30)
            r.raise_for_status()
            if url.endswith(".csv"):
                df = pd.read_csv(StringIO(r.text))
            elif url.endswith((".xls", ".xlsx")):
                df = pd.read_excel(BytesIO(r.content))
            else:
                print(f"[WARN] Unsupported file format: {url}")
                return None
            return df
        except Exception as e:
            print(f"[ERROR] Failed to download {url}: {e}")
            return None

    def ingest_all_councils(self):
        """Minimal changes to integrate with existing app"""
        councils = self.get_known_councils()
        all_data = []
        for council in councils:
            files = self.find_spend_files_for_council(council)
            for f in files:
                df = self.download_dataset(f)
                if df is not None:
                    df["Council"] = council
                    all_data.append(df)
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            combined_file = self.DATA_DIR / "all_council_spending.csv"
            combined.to_csv(combined_file, index=False)
            print(f"[DEBUG] Combined dataset saved: {combined_file}")
            return combined_file
        else:
            print("[WARN] No data ingested")
            return None
