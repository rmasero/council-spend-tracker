import requests, io
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re

class CouncilScraper:
    """Scraper to retrieve council spending data using authoritative sources first,
    then heuristics as a fallback."""

    DATA_GOV_API = 'https://ckan.publishing.service.gov.uk/api/3/action/'
    MYSOCIETY_URL = 'https://pages.mysociety.org/uk_local_authority_names_and_codes/datasets/uk_la_past_current/latest/download.csv'

    def __init__(self):
        self.session = requests.Session()
        self.common_extensions = ['.csv','.xls','.xlsx','.ods']

    def get_known_councils(self):
        """Fetch the authoritative list of councils from mySociety dataset."""
        try:
            df = pd.read_csv(self.MYSOCIETY_URL)
            councils = df[df['current_authority']==True]['official_name'].dropna().unique().tolist()
            return councils
        except Exception as e:
            print(f"Failed to fetch council register: {e}")
            return []

    def find_spend_files_for_council(self, council_name):
        """Look for council spend datasets via data.gov.uk API, fallback to heuristics."""
        results = []
        try:
            params = {
                'fq': f'title:"{council_name}"',
                'rows': 100,
                'q': 'spend OR expenditure OR payments'
            }
            r = self.session.get(self.DATA_GOV_API + 'package_search', params=params, timeout=20)
            if r.status_code == 200:
                js = r.json()
                for res in js.get('result', {}).get('results', []):
                    for resource in res.get('resources', []):
                        fmt = resource.get('format','').lower()
                        name = resource.get('name','').lower()
                        if fmt in ['csv','xls','xlsx'] or 'spend' in name or 'payment' in name:
                            results.append({
                                'url': resource.get('url'),
                                'source_page': res.get('title'),
                                'anchor': resource.get('name') or res.get('title')
                            })
        except Exception as e:
            print(f"data.gov.uk API lookup failed for {council_name}: {e}")

        if not results:
            # fallback heuristics
            results = self.heuristic_find(council_name)
        return results

    def heuristic_find(self, council_name):
        """Heuristic: guess common council transparency URLs and scrape for links."""
        guesses = [
            '/transparency/expenditure',
            '/transparency/payments',
            '/open-data/spend',
            '/open-data',
            '/downloads',
            '/your-council/transparency'
        ]
        base = self.guess_council_base(council_name)
        results = []
        for g in guesses:
            url = urljoin(base, g)
            try:
                r = self.session.get(url, timeout=15)
                if r.status_code!=200:
                    continue
                soup = BeautifulSoup(r.text, 'lxml')
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if any(href.lower().endswith(ext) for ext in self.common_extensions) or 'spend' in href.lower() or 'payments' in href.lower():
                        full = urljoin(url, href)
                        results.append({'url': full, 'source_page': url, 'anchor': a.get_text(strip=True)})
            except Exception:
                continue
        # de-duplicate
        seen, uniq = set(), []
        for r in results:
            if r['url'] in seen: continue
            seen.add(r['url']); uniq.append(r)
        return uniq

    def guess_council_base(self, council_name):
        name = council_name.lower().replace('city','').replace('council','').strip()
        parts = name.split()
        candidate = ''.join(parts)
        return f'https://{candidate}.gov.uk'

    def download_dataframe(self, filemeta):
        url = filemeta.get('url')
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            content_type = r.headers.get('content-type','').lower()
            if url.lower().endswith('.csv') or 'text/csv' in content_type:
                return pd.read_csv(io.StringIO(r.text))
            elif url.lower().endswith('.xls') or url.lower().endswith('.xlsx') or 'excel' in content_type:
                return pd.read_excel(io.BytesIO(r.content))
            else:
                dfs = pd.read_html(r.text)
                if dfs:
                    return dfs[0]
            return None
        except Exception as e:
            print(f"Failed to download {url}: {e}")
            return None
