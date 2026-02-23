#!/usr/bin/env python3
"""
GovInfo Bulk Data Downloader

Downloads US government documents from govinfo.gov/bulkdata:
  - FR: Federal Register (daily journal, 1994-present)
  - CREC: Congressional Record (proceedings of Congress)
  - CFR: Code of Federal Regulations
  - BILLS: Congressional bills
  - PLAW: Public laws
  - STATUTE: Statutes at large

All US Government public domain.

Usage:
    python stage.py --list                              # List available collections
    python stage.py --collection FR                     # Download Federal Register
    python stage.py --collection FR --year 2024         # Download FR for one year
    python stage.py --collection CFR                    # Download Code of Fed Regulations
    python stage.py --collection BILLS --year 2024      # Download bills for a year
    python stage.py --all                               # Download everything
"""

import argparse
import logging
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir, log_dir

BULKDATA_BASE = "https://www.govinfo.gov/bulkdata/"
STORAGE_DIR = storage_dir("govinfo")
LOG_DIR = log_dir()

COLLECTIONS = {
    "FR": {
        "description": "Federal Register (daily journal of US government, 1994-present)",
        "url": BULKDATA_BASE + "FR/",
        "pattern": r'href="(\d{4})/"',
        "file_pattern": r'href="(FR-\d{4}-\d{2}-\d{2}\.xml)"',
    },
    "CREC": {
        "description": "Congressional Record (proceedings of Congress)",
        "url": BULKDATA_BASE + "CREC/",
        "pattern": r'href="(\d{4})/"',
    },
    "CFR": {
        "description": "Code of Federal Regulations",
        "url": BULKDATA_BASE + "CFR/",
        "pattern": r'href="(\d{4})/"',
    },
    "BILLS": {
        "description": "Congressional bills",
        "url": BULKDATA_BASE + "BILLS/",
        "pattern": r'href="(\d+)/"',  # Congress number
    },
    "PLAW": {
        "description": "Public laws",
        "url": BULKDATA_BASE + "PLAW/",
        "pattern": r'href="(\d+)/"',
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "govinfo_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def _fetch(url: str) -> str:
    """Fetch a URL with proper User-Agent."""
    req = urllib.request.Request(url, headers={"User-Agent": "corpus-data-stager/1.0"})
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode("utf-8")


def list_subdivisions(collection: str) -> list[str]:
    """List available years/congress numbers for a collection via the GovInfo API."""
    # The bulk data HTML pages may block scraping; use the sitemaps API instead
    api_url = f"https://api.govinfo.gov/collections/{collection}?offset=0&pageSize=1&api_key=DEMO_KEY"
    try:
        html = _fetch(api_url)
        # If API works, we know the collection exists
    except Exception:
        pass

    # For FR/CREC/CFR, generate known year ranges
    import datetime
    year = datetime.date.today().year
    if collection in ("FR", "CREC"):
        return [str(y) for y in range(1994, year + 1)]
    elif collection == "CFR":
        return [str(y) for y in range(1996, year + 1)]
    elif collection in ("BILLS", "PLAW"):
        # Congress numbers (103rd onwards = 1993+)
        return [str(c) for c in range(103, 119 + 1)]
    else:
        info = COLLECTIONS[collection]
        html = _fetch(info["url"])
        return sorted(set(re.findall(info["pattern"], html)))


def download_recursive(url: str, dest_dir: Path, dry_run: bool = False) -> int:
    """Recursively download all XML/ZIP files under a URL using wget."""
    dest_dir.mkdir(parents=True, exist_ok=True)

    if dry_run:
        log.info("DRY RUN: would wget -r %s", url)
        return 0

    log.info("Downloading recursively: %s -> %s", url, dest_dir)
    result = subprocess.run(
        ["wget", "-r", "-np", "-nH", "--cut-dirs=2",
         "-A", "*.xml,*.zip",
         "-P", str(dest_dir),
         "--wait=0.2", "--random-wait",
         "-e", "robots=off",
         url],
        capture_output=True,
        text=True,
    )
    if result.returncode not in (0, 8):  # 8 = some files not retrieved (normal)
        log.warning("wget returned %d for %s", result.returncode, url)

    # Count downloaded files
    count = sum(1 for _ in dest_dir.rglob("*.xml"))
    count += sum(1 for _ in dest_dir.rglob("*.zip"))
    return count


def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def main():
    parser = argparse.ArgumentParser(description="GovInfo bulk data downloader")
    parser.add_argument("--list", action="store_true", help="List available collections")
    parser.add_argument("--collection", choices=list(COLLECTIONS.keys()),
                        help="Download a specific collection")
    parser.add_argument("--year", help="Only download a specific year/congress number")
    parser.add_argument("--all", action="store_true", help="Download all collections")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    if args.list:
        for name, info in COLLECTIONS.items():
            try:
                subs = list_subdivisions(name)
                range_str = f"{subs[0]}-{subs[-1]} ({len(subs)} entries)" if subs else "none found"
            except Exception as e:
                range_str = f"error: {e}"
            print(f"  {name:8s}  {info['description']}")
            print(f"           Available: {range_str}")
            print()
        return

    targets = list(COLLECTIONS.keys()) if args.all else ([args.collection] if args.collection else [])
    if not targets:
        parser.error("Specify --collection, --all, or --list")

    for collection in targets:
        info = COLLECTIONS[collection]
        log.info("=== %s: %s ===", collection, info["description"])

        if args.year:
            subdivisions = [args.year]
        else:
            subdivisions = list_subdivisions(collection)

        log.info("Processing %d subdivisions for %s", len(subdivisions), collection)
        dest_dir = STORAGE_DIR / collection

        total_files = 0
        for sub in subdivisions:
            url = info["url"] + sub + "/"
            sub_dir = dest_dir / sub
            count = download_recursive(url, sub_dir, dry_run=args.dry_run)
            total_files += count
            log.info("%s/%s: %d files", collection, sub, count)

        log.info("Done with %s: %d total files", collection, total_files)


if __name__ == "__main__":
    main()
