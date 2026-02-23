#!/usr/bin/env python3
"""NY Courts Search API Crawler - Uses the official search form"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pathlib import Path
import time
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir

SEARCH_URL = "https://iapps.courts.state.ny.us/lawReporting/Search"

COURTS = {
    "Court of Appeals": "court_of_appeals",
    "App Div, 1st Dept": "appellate_division_1st",
    "App Div, 2d Dept": "appellate_division_2nd",
    "App Div, 3d Dept": "appellate_division_3rd",
    "App Div, 4th Dept": "appellate_division_4th",
}

def search_by_date_range(court, start_date, end_date):
    """Search for opinions by court and date range"""
    data = {
        'rbOpinionMotion': 'opinion',
        'Pty': '',
        'and_or': 'and',
        'dtStartDate': start_date,  # mm/dd/yyyy
        'dtEndDate': end_date,
        'court': court,
        'docket': '',
        'judge': '',
        'slipYear': '',
        'slipNo': '',
        'OffRepCit': '',
        'fullText': '',
        'and_or2': 'and',
        'orderBy': 'Party Name',
        'Submit': 'Find'
    }
    
    r = requests.post(SEARCH_URL, data=data, timeout=30)
    return r.text

def extract_opinion_links(html):
    """Extract opinion URLs from search results"""
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        if 'lawReporting/Opinion' in a['href']:
            links.append(a['href'])
    return links

def download_opinion(url, output_path):
    """Download opinion text"""
    r = requests.get(url, timeout=30)
    soup = BeautifulSoup(r.text, 'html.parser')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(f"URL: {url}\n{'='*80}\n\n{soup.get_text()}")

def crawl_court_year(court_name, court_code, year, output_dir):
    """Crawl all opinions for a court in a given year"""
    print(f"\n{court_name} - {year}")
    
    start = f"01/01/{year}"
    end = f"12/31/{year}"
    
    html = search_by_date_range(court_name, start, end)
    links = extract_opinion_links(html)
    
    print(f"  Found {len(links)} opinions")
    
    for i, link in enumerate(links):
        full_url = f"https://iapps.courts.state.ny.us{link}" if link.startswith('/') else link
        filename = link.split('/')[-1] + '.txt'
        output_path = Path(output_dir) / court_code / str(year) / filename
        
        if output_path.exists():
            continue
        
        print(f"  [{i+1}/{len(links)}]", end='\r')
        download_opinion(full_url, output_path)
        time.sleep(0.5)
    
    print(f"  Downloaded: {len(links)}")
    return len(links)

def main():
    output_dir = str(storage_dir("nycourts"))
    
    # Crawl 2003-2026
    total = 0
    for year in range(2003, 2027):
        for court_name, court_code in COURTS.items():
            count = crawl_court_year(court_name, court_code, year, output_dir)
            total += count
    
    print(f"\nTOTAL: {total} opinions")

if __name__ == "__main__":
    main()
