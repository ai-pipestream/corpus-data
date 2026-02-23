#!/usr/bin/env python3
"""
Common Crawl News (CC-NEWS) WARC Downloader

CC-News publishes daily WARC files of news articles since 2016.
S3 ListObjects is blocked — we must fetch the monthly warc.paths.gz index
files first, then download individual WARCs by key.

Each WARC is ~1GB compressed. ~600 WARCs per month.

Usage:
    python stage.py --list-months              # Show available year/months
    python stage.py --months 2024/01           # Download one month
    python stage.py --months 2024/01 2024/02   # Download multiple months
    python stage.py --year 2024                # Download full year
    python stage.py --index-only               # Download path indexes only
    python stage.py --limit 5                  # Download only N WARCs per month
"""

import argparse
import gzip
import logging
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir, log_dir

CC_BASE = "https://data.commoncrawl.org/"
CC_NEWS_BASE = CC_BASE + "crawl-data/CC-NEWS/"
STORAGE_DIR = storage_dir("ccnews")
INDEX_DIR = STORAGE_DIR / "indexes"
WARC_DIR = STORAGE_DIR / "warcs"
LOG_DIR = log_dir()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "ccnews_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def discover_months() -> list[str]:
    """Discover available year/month combos by probing warc.paths.gz existence.

    CC-News HTML index pages don't reliably list subdirectories,
    and S3 ListObjects is blocked. We probe each year/month directly.
    CC-News runs from 2016/08 to present.
    """
    import datetime

    log.info("Probing available months (2016/08 to present)...")
    months = []
    now = datetime.date.today()

    for year in range(2016, now.year + 1):
        start_month = 8 if year == 2016 else 1
        end_month = now.month if year == now.year else 12
        for month in range(start_month, end_month + 1):
            ym = f"{year}/{month:02d}"
            url = CC_NEWS_BASE + ym + "/warc.paths.gz"
            try:
                req = urllib.request.Request(url, method="HEAD",
                                             headers={"User-Agent": "corpus-data-stager/1.0"})
                with urllib.request.urlopen(req) as resp:
                    if resp.status == 200:
                        months.append(ym)
            except Exception:
                pass
    log.info("Found %d available months", len(months))
    return months


def fetch_warc_paths(year_month: str) -> list[str]:
    """Download and parse the warc.paths.gz index for a given month."""
    index_file = INDEX_DIR / year_month.replace("/", "-") / "warc.paths"
    if index_file.exists():
        return index_file.read_text().strip().split("\n")

    url = CC_NEWS_BASE + year_month + "/warc.paths.gz"
    log.info("Fetching WARC index: %s", url)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "corpus-data-stager/1.0"})
        with urllib.request.urlopen(req) as resp:
            data = gzip.decompress(resp.read()).decode("utf-8").strip()
        index_file.parent.mkdir(parents=True, exist_ok=True)
        index_file.write_text(data)
        paths = data.split("\n")
        log.info("Found %d WARCs for %s", len(paths), year_month)
        return paths
    except Exception as e:
        log.error("Failed to fetch index for %s: %s", year_month, e)
        return []


def download_warc(warc_path: str) -> Path | None:
    """Download a single WARC file. warc_path is relative to CC_BASE."""
    filename = Path(warc_path).name
    # Organize by year/month
    parts = warc_path.split("/")  # crawl-data/CC-NEWS/yyyy/mm/filename
    year_month = f"{parts[2]}/{parts[3]}" if len(parts) >= 5 else "unknown"
    dest_dir = WARC_DIR / year_month
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename

    if dest.exists():
        log.info("Already exists, skipping: %s", filename)
        return dest

    url = CC_BASE + warc_path
    log.info("Downloading %s", filename)
    result = subprocess.run(
        ["curl", "-L", "-o", str(dest), "--progress-bar", url],
        check=False,
    )
    if result.returncode != 0:
        log.error("Failed to download %s", filename)
        dest.unlink(missing_ok=True)
        return None

    log.info("Downloaded %s (%s)", filename, _human_size(dest.stat().st_size))
    return dest


def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def main():
    parser = argparse.ArgumentParser(description="CC-NEWS WARC downloader")
    parser.add_argument("--list-months", action="store_true", help="List available year/months")
    parser.add_argument("--months", nargs="+", help="Download specific months (e.g. 2024/01)")
    parser.add_argument("--year", help="Download all months for a year (e.g. 2024)")
    parser.add_argument("--index-only", action="store_true", help="Only download warc.paths indexes")
    parser.add_argument("--limit", type=int, help="Limit WARCs downloaded per month")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    WARC_DIR.mkdir(parents=True, exist_ok=True)

    if args.list_months:
        months = discover_months()
        for m in months:
            print(m)
        print(f"\nTotal: {len(months)} months available")
        return

    # Determine which months to process
    target_months = []
    if args.year:
        all_months = discover_months()
        target_months = [m for m in all_months if m.startswith(args.year)]
    elif args.months:
        target_months = args.months
    else:
        parser.error("Specify --months, --year, or --list-months")

    log.info("Processing %d month(s): %s", len(target_months), target_months)

    total_warcs = 0
    total_downloaded = 0
    for month in target_months:
        paths = fetch_warc_paths(month)
        total_warcs += len(paths)

        if args.index_only:
            log.info("Index saved for %s (%d WARCs)", month, len(paths))
            continue

        download_paths = paths[:args.limit] if args.limit else paths

        if args.dry_run:
            for p in download_paths:
                print(f"Would download: {Path(p).name}")
            continue

        for warc_path in download_paths:
            if download_warc(warc_path):
                total_downloaded += 1

    log.info("Done. Total WARCs indexed: %d, Downloaded: %d", total_warcs, total_downloaded)


if __name__ == "__main__":
    main()
