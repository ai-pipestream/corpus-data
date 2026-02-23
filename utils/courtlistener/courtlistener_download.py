#!/usr/bin/env python3
"""Download NY court opinions from CourtListener API"""
import requests
import json
import time
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir

BASE_URL = "https://www.courtlistener.com/api/rest/v4"
OUTPUT_DIR = storage_dir("courtlistener") / "api"
API_TOKEN = os.getenv("COURTLISTENER_TOKEN", "")

# NY Court identifiers from CourtListener
NY_COURTS = [
    "ny",           # NY Court of Appeals
    "nyappdiv",     # NY Appellate Division
    "nysupct",      # NY Supreme Court
    "nycrimct",     # NY Criminal Court
    "nycivct",      # NY Civil Court
    "nyfamct",      # NY Family Court
]

def download_opinions(court_id, output_dir):
    """Download all opinions for a court"""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not API_TOKEN:
        print("ERROR: Set COURTLISTENER_TOKEN environment variable")
        return 0
    
    url = f"{BASE_URL}/opinions/"
    params = {
        "court": court_id,
        "order_by": "date_filed",
        "page_size": 100,
    }
    headers = {"Authorization": f"Token {API_TOKEN}"}
    
    page = 1
    total = 0
    
    while url:
        print(f"\n{court_id} - Page {page}")
        
        try:
            resp = requests.get(url, params=params if page == 1 else None, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            results = data.get("results", [])
            print(f"  Got {len(results)} opinions")
            
            for opinion in results:
                opinion_id = opinion.get("id")
                if opinion_id:
                    filename = output_dir / f"{court_id}_{opinion_id}.json"
                    with open(filename, "w") as f:
                        json.dump(opinion, f, indent=2)
                    total += 1
            
            url = data.get("next")
            page += 1
            time.sleep(1)  # Rate limiting
            
        except Exception as e:
            print(f"  Error: {e}")
            break
    
    print(f"\nTotal downloaded for {court_id}: {total}")
    return total

def main():
    print("CourtListener NY Courts Downloader")
    print("=" * 50)
    
    grand_total = 0
    for court in NY_COURTS:
        print(f"\nDownloading {court}...")
        output_dir = OUTPUT_DIR / court
        count = download_opinions(court, output_dir)
        grand_total += count
    
    print(f"\n{'=' * 50}")
    print(f"Grand total: {grand_total} opinions")

if __name__ == "__main__":
    main()
