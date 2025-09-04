from src.scraper import CouncilScraper
from pathlib import Path
import subprocess

def main():
    scraper = CouncilScraper()
    combined_file = scraper.ingest_all_councils()
    if combined_file:
        try:
            subprocess.run(["git", "config", "user.name", "council-spend-bot"], check=True)
            subprocess.run(["git", "config", "user.email", "council-spend-bot@users.noreply.github.com"], check=True)
            subprocess.run(["git", "add", str(combined_file)], check=True)
            subprocess.run(["git", "commit", "-m", "Automated data refresh [skip ci]"], check=True)
            subprocess.run(["git", "push"], check=True)
        except Exception as e:
            print(f"[ERROR] Git push failed: {e}")

if __name__ == "__main__":
    main()
