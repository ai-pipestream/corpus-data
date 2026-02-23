#!/usr/bin/env python3
"""
NY Courts Full Archive Crawler
Coverage: 1847-present (all available formats: HTML, PDF)
"""
import requests
from bs4 import BeautifulSoup
import time
import os
from pathlib import Path
from datetime import datetime
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir

BASE_URL = "http://nycourts.gov/reporter"

COURTS = {
    "court_of_appeals": "cidxtable.shtml",
    "appellate_division_1st": "aidxtable_1.shtml",
    "appellate_division_2nd": "aidxtable_2.shtml",
    "appellate_division_3rd": "aidxtable_3.shtml",
    "appellate_division_4th": "aidxtable_4.shtml",
    "appellate_term_1st": "at_1_idxtable.shtml",
    "appellate_term_2nd": "at_2_idxtable.shtml",
}

def fetch(url, retries=3):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    # Force HTTP, disable redirects to HTTPS
    url = url.replace('https://', 'http://')
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=30, allow_redirects=False)
            if r.status_code == 301 or r.status_code == 302:
                # Manual redirect handling, keep HTTP
                new_url = r.headers.get('Location', '').replace('https://', 'http://')
                r = requests.get(new_url, headers=headers, timeout=30)
            r.raise_for_status()
            return r
        except Exception as e:
            if i == retries - 1:
                print(f"  ERROR: {e}")
                return None
            time.sleep(2)

def save_file(content, path, is_binary=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = 'wb' if is_binary else 'w'
    with open(path, mode) as f:
        f.write(content)

def extract_links(html):
    """Extract all opinion/archive links"""
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        # Match opinion links and archive links
        if any(x in href for x in ['3dseries/', 'slipop/', 'archives/', '.pdf', '.htm']):
            links.append({"href": href, "text": text})
    return links

def download_document(url, output_path):
    """Download HTML or PDF document"""
    r = fetch(url)
    if not r:
        return False
    
    if url.endswith('.pdf'):
        save_file(r.content, output_path, is_binary=True)
    else:
        soup = BeautifulSoup(r.text, 'html.parser')
        text = soup.get_text()
        save_file(f"URL: {url}\n{'='*80}\n\n{text}", output_path)
    return True

def crawl_index_page(index_url, court_name, year, month, output_dir):
    """Crawl a single index page (month/year)"""
    html = fetch(index_url)
    if not html:
        return 0
    
    links = extract_links(html.text)
    count = 0
    
    for link in links:
        href = link['href']
        if not any(x in href for x in ['3dseries/', 'slipop/', 'archives/']):
            continue
            
        # Build full URL
        if href.startswith('http'):
            url = href
        elif href.startswith('../'):
            url = f"{BASE_URL}/{href.replace('../', '')}"
        else:
            url = f"{BASE_URL}/{href}"
        
        # Determine file extension
        ext = '.pdf' if url.endswith('.pdf') else '.txt'
        filename = url.split('/')[-1].replace('.htm', '').replace('.html', '') + ext
        
        # Organize: /court/year/month/filename
        output_path = Path(output_dir) / court_name / str(year) / f"{month:02d}" / filename
        
        if output_path.exists():
            continue
            
        print(f"    {link['text'][:60]}")
        if download_document(url, output_path):
            count += 1
        time.sleep(0.3)
    
    return count

def crawl_court_archives(court_name, court_file, output_dir):
    """Crawl full archive for a court (2003-present)"""
    print(f"\n{'='*80}")
    print(f"Crawling: {court_name}")
    print(f"{'='*80}")
    
    # Start from Oct 2003 to present
    start_year, start_month = 2003, 10
    now = datetime.now()
    
    total = 0
    year, month = start_year, start_month
    
    while year < now.year or (year == now.year and month <= now.month):
        # Archive URLs follow pattern with year/month parameters or separate pages
        # For now, crawl current index which shows recent months
        index_url = f"{BASE_URL}/slipidx/{court_file}"
        
        print(f"\n{court_name} - {year}/{month:02d}")
        count = crawl_index_page(index_url, court_name, year, month, output_dir)
        total += count
        print(f"  Downloaded: {count}")
        
        # Increment month
        month += 1
        if month > 12:
            month = 1
            year += 1
        
        # Only crawl current month for now (full archive needs archive page discovery)
        break
    
    return total

def crawl_notable_cases(output_dir):
    """Crawl notable/landmark cases (pre-2003)"""
    print(f"\n{'='*80}")
    print("Crawling: Notable Cases (Historical)")
    print(f"{'='*80}")
    
    url = f"{BASE_URL}/archives.htm"
    html = fetch(url)
    if not html:
        return 0
    
    links = extract_links(html.text)
    count = 0
    
    for link in links:
        href = link['href']
        if 'archives/' not in href:
            continue
        
        full_url = f"{BASE_URL}/{href}" if not href.startswith('http') else href
        ext = '.pdf' if full_url.endswith('.pdf') else '.txt'
        filename = href.split('/')[-1].replace('.htm', '') + ext
        
        output_path = Path(output_dir) / "notable_cases" / filename
        
        if output_path.exists():
            continue
        
        print(f"  {link['text'][:70]}")
        if download_document(full_url, output_path):
            count += 1
        time.sleep(0.3)
    
    return count

def main():
    output_dir = str(storage_dir("nycourts"))
    
    # Crawl notable/historical cases first
    notable_count = crawl_notable_cases(output_dir)
    print(f"\nNotable cases downloaded: {notable_count}")
    
    # Crawl each court's archives
    total = 0
    for court_name, court_file in COURTS.items():
        count = crawl_court_archives(court_name, court_file, output_dir)
        total += count
    
    print(f"\n{'='*80}")
    print(f"TOTAL DOCUMENTS: {total + notable_count}")
    print(f"Saved to: {output_dir}")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
