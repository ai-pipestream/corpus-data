#!/usr/bin/env python3
"""
SEC EDGAR Filings Downloader

Downloads SEC filings (10-K, 10-Q, 8-K, S-1, etc.) from EDGAR bulk archives.
Uses the full-index to enumerate filings, then downloads filing archives.

SEC requires: User-Agent header with name and email.
Rate limit: 10 requests/second.

Usage:
    python stage.py --list-years                    # Show available years
    python stage.py --year 2024                     # Download all filings for a year
    python stage.py --year 2024 --quarter 1         # Download Q1 2024 only
    python stage.py --year 2024 --types 10-K 10-Q   # Only specific filing types
    python stage.py --index-only                    # Download indexes only
    python stage.py --company-facts                 # Download XBRL companyfacts.zip
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir, log_dir

EDGAR_BASE = "https://www.sec.gov/Archives/edgar/"
FULL_INDEX_BASE = EDGAR_BASE + "full-index/"
STORAGE_DIR = storage_dir("edgar")
INDEX_DIR = STORAGE_DIR / "indexes"
FILINGS_DIR = STORAGE_DIR / "filings"
LOG_DIR = log_dir()

# SEC requires identification
EDGAR_IDENTITY = os.environ.get("EDGAR_IDENTITY", "CorpusData corpus@example.com")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "edgar_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def _sec_request(url: str) -> bytes:
    """Make a rate-limited request to SEC with proper User-Agent."""
    import gzip as gzmod
    req = urllib.request.Request(url, headers={
        "User-Agent": EDGAR_IDENTITY,
        "Accept-Encoding": "gzip, deflate",
    })
    time.sleep(0.11)  # ~9 req/sec to stay under 10/sec limit
    with urllib.request.urlopen(req) as resp:
        data = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip" or data[:2] == b'\x1f\x8b':
            data = gzmod.decompress(data)
        return data


def list_available_years() -> list[int]:
    """List years available in the EDGAR full-index."""
    log.info("Fetching available years from EDGAR full-index")
    import re
    data = _sec_request(FULL_INDEX_BASE).decode("utf-8")
    years = sorted(set(int(y) for y in re.findall(r'href="(\d{4})/"', data)))
    return years


def download_index(year: int, quarter: int) -> Path | None:
    """Download the master.idx for a given year/quarter."""
    dest = INDEX_DIR / str(year) / f"QTR{quarter}" / "master.idx"
    if dest.exists():
        log.info("Index exists: %s/%d/QTR%d", year, year, quarter)
        return dest

    url = f"{FULL_INDEX_BASE}{year}/QTR{quarter}/master.idx"
    log.info("Downloading index: %s", url)
    try:
        data = _sec_request(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        log.info("Saved index: %s (%s)", dest, _human_size(len(data)))
        return dest
    except Exception as e:
        log.warning("No index for %d/QTR%d: %s", year, quarter, e)
        return None


def parse_index(index_path: Path, filing_types: list[str] | None = None) -> list[dict]:
    """Parse a master.idx file into filing records."""
    records = []
    lines = index_path.read_text(errors="replace").split("\n")
    # Skip header lines (first 11 lines typically)
    data_started = False
    for line in lines:
        if line.startswith("---"):
            data_started = True
            continue
        if not data_started:
            continue
        parts = line.split("|")
        if len(parts) < 5:
            continue
        cik, company, form_type, date_filed, filename = [p.strip() for p in parts[:5]]
        if filing_types and form_type not in filing_types:
            continue
        records.append({
            "cik": cik,
            "company": company,
            "form_type": form_type,
            "date_filed": date_filed,
            "filename": filename,
        })
    return records


def download_filing(filename: str) -> Path | None:
    """Download a single filing from EDGAR archives."""
    # filename is like "edgar/data/1234567/0001234567-24-000001.txt"
    dest = FILINGS_DIR / filename
    if dest.exists():
        return dest

    url = f"https://www.sec.gov/Archives/{filename}"
    try:
        data = _sec_request(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        return dest
    except Exception as e:
        log.warning("Failed to download %s: %s", filename, e)
        return None


def download_company_facts() -> Path | None:
    """Download the XBRL companyfacts.zip (~1GB)."""
    dest = STORAGE_DIR / "companyfacts.zip"
    if dest.exists():
        log.info("companyfacts.zip already exists")
        return dest

    url = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
    log.info("Downloading companyfacts.zip (~1GB)")
    result = subprocess.run(
        ["curl", "-L", "-o", str(dest), "--progress-bar",
         "-H", f"User-Agent: {EDGAR_IDENTITY}", url],
        check=False,
    )
    if result.returncode != 0:
        log.error("Failed to download companyfacts.zip")
        dest.unlink(missing_ok=True)
        return None
    log.info("Downloaded companyfacts.zip (%s)", _human_size(dest.stat().st_size))
    return dest


def download_daily_feed(year: int, quarter: int) -> list[Path]:
    """Download daily feed tar.gz archives for a quarter (most efficient bulk method)."""
    feed_base = f"https://www.sec.gov/Archives/edgar/Feed/{year}/QTR{quarter}/"
    log.info("Fetching daily feed listing for %d/QTR%d", year, quarter)

    import re
    try:
        data = _sec_request(feed_base).decode("utf-8")
    except Exception as e:
        log.warning("No daily feed for %d/QTR%d: %s", year, quarter, e)
        return []

    archives = sorted(set(re.findall(r'href="(\d+\.nc\.tar\.gz)"', data)))
    log.info("Found %d daily feed archives for %d/QTR%d", len(archives), year, quarter)

    downloaded = []
    feed_dir = STORAGE_DIR / "feed" / str(year) / f"QTR{quarter}"
    feed_dir.mkdir(parents=True, exist_ok=True)

    for archive in archives:
        dest = feed_dir / archive
        if dest.exists():
            downloaded.append(dest)
            continue

        url = feed_base + archive
        log.info("Downloading feed: %s", archive)
        result = subprocess.run(
            ["curl", "-L", "-o", str(dest), "--progress-bar",
             "-H", f"User-Agent: {EDGAR_IDENTITY}", url],
            check=False,
        )
        if result.returncode == 0:
            downloaded.append(dest)
        else:
            dest.unlink(missing_ok=True)

    return downloaded


def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def main():
    parser = argparse.ArgumentParser(description="SEC EDGAR filings downloader")
    parser.add_argument("--list-years", action="store_true", help="List available years")
    parser.add_argument("--year", type=int, help="Download filings for this year")
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], help="Specific quarter")
    parser.add_argument("--types", nargs="+", help="Filter by filing type (e.g. 10-K 10-Q 8-K)")
    parser.add_argument("--index-only", action="store_true", help="Download indexes only")
    parser.add_argument("--feed", action="store_true", help="Download daily feed archives (bulk)")
    parser.add_argument("--company-facts", action="store_true", help="Download XBRL companyfacts.zip")
    parser.add_argument("--limit", type=int, help="Limit number of filings downloaded")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    FILINGS_DIR.mkdir(parents=True, exist_ok=True)

    if EDGAR_IDENTITY == "CorpusData corpus@example.com":
        log.warning("Set EDGAR_IDENTITY env var to 'YourName your@email.com' (SEC requirement)")

    if args.list_years:
        years = list_available_years()
        for y in years:
            print(y)
        print(f"\nTotal: {len(years)} years ({years[0]}-{years[-1]})")
        return

    if args.company_facts:
        download_company_facts()
        return

    if not args.year:
        parser.error("Specify --year, --list-years, or --company-facts")

    quarters = [args.quarter] if args.quarter else [1, 2, 3, 4]

    for q in quarters:
        if args.feed:
            feeds = download_daily_feed(args.year, q)
            log.info("Downloaded %d feed archives for %d/QTR%d", len(feeds), args.year, q)
            continue

        idx = download_index(args.year, q)
        if not idx:
            continue

        if args.index_only:
            continue

        records = parse_index(idx, filing_types=args.types)
        log.info("Found %d filings in %d/QTR%d", len(records), args.year, q)

        if args.limit:
            records = records[:args.limit]

        if args.dry_run:
            for r in records[:20]:
                print(f"  {r['form_type']:10s} {r['date_filed']}  {r['company'][:40]}")
            if len(records) > 20:
                print(f"  ... and {len(records) - 20} more")
            continue

        downloaded = 0
        for r in records:
            if download_filing(r["filename"]):
                downloaded += 1
        log.info("Downloaded %d/%d filings for %d/QTR%d", downloaded, len(records), args.year, q)

    log.info("Done.")


if __name__ == "__main__":
    main()
