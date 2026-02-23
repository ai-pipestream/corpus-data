#!/usr/bin/env python3
"""
StackExchange Data Dump Downloader

Downloads all StackExchange site data dumps from archive.org.
Each site is a .7z archive containing XML files (Posts, Comments, Users, etc.).

Usage:
    python stage.py                    # Download all sites
    python stage.py --list             # List available sites
    python stage.py --sites aviation   # Download specific site(s)
    python stage.py --extract          # Extract after downloading
    python stage.py --skip-meta        # Skip .meta. sites
"""

import argparse
import logging
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir, log_dir

ARCHIVE_URL = "https://archive.org/download/stackexchange/"
STORAGE_DIR = storage_dir("stackexchange")
LOG_DIR = log_dir()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "stackexchange_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def list_available_sites() -> list[str]:
    """Scrape archive.org listing for all .7z files."""
    log.info("Fetching site list from %s", ARCHIVE_URL)
    req = urllib.request.Request(ARCHIVE_URL, headers={"User-Agent": "corpus-data-stager/1.0"})
    with urllib.request.urlopen(req) as resp:
        html = resp.read().decode("utf-8")
    files = re.findall(r'href="([^"]+\.7z)"', html)
    return sorted(files)


def download_file(filename: str, dest_dir: Path) -> Path:
    """Download a single .7z file from archive.org using curl."""
    dest = dest_dir / filename
    if dest.exists():
        log.info("Already exists, skipping: %s", dest)
        return dest

    url = ARCHIVE_URL + filename
    log.info("Downloading %s", url)
    result = subprocess.run(
        ["curl", "-L", "-o", str(dest), "--progress-bar", url],
        check=False,
    )
    if result.returncode != 0:
        log.error("Failed to download %s (exit code %d)", filename, result.returncode)
        dest.unlink(missing_ok=True)
        return None
    log.info("Downloaded %s (%s)", filename, _human_size(dest.stat().st_size))
    return dest


def extract_file(archive: Path, dest_dir: Path) -> bool:
    """Extract a .7z archive into a named subdirectory."""
    site_name = archive.stem  # e.g. aviation.stackexchange.com
    extract_to = dest_dir / site_name
    if extract_to.exists() and any(extract_to.iterdir()):
        log.info("Already extracted, skipping: %s", extract_to)
        return True

    extract_to.mkdir(parents=True, exist_ok=True)
    log.info("Extracting %s -> %s", archive.name, extract_to)
    result = subprocess.run(
        ["7z", "x", "-y", f"-o{extract_to}", str(archive)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log.error("Extraction failed for %s: %s", archive.name, result.stderr)
        return False
    return True


def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def main():
    parser = argparse.ArgumentParser(description="StackExchange data dump downloader")
    parser.add_argument("--list", action="store_true", help="List available sites and exit")
    parser.add_argument("--sites", nargs="+", help="Download only these sites (partial match)")
    parser.add_argument("--extract", action="store_true", help="Extract .7z files after download")
    parser.add_argument("--skip-meta", action="store_true", help="Skip .meta. sites")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    all_files = list_available_sites()
    log.info("Found %d archives on archive.org", len(all_files))

    if args.skip_meta:
        all_files = [f for f in all_files if ".meta." not in f]
        log.info("After filtering meta sites: %d archives", len(all_files))

    if args.sites:
        filtered = []
        for pattern in args.sites:
            matches = [f for f in all_files if pattern.lower() in f.lower()]
            filtered.extend(matches)
        all_files = sorted(set(filtered))
        log.info("Filtered to %d archives matching: %s", len(all_files), args.sites)

    if args.list:
        for f in all_files:
            print(f)
        print(f"\nTotal: {len(all_files)} archives")
        return

    if args.dry_run:
        for f in all_files:
            print(f"Would download: {f}")
        print(f"\nTotal: {len(all_files)} archives")
        return

    downloaded = 0
    failed = 0
    for filename in all_files:
        dest = download_file(filename, STORAGE_DIR)
        if dest:
            downloaded += 1
            if args.extract:
                extract_file(dest, STORAGE_DIR)
        else:
            failed += 1

    log.info("Done. Downloaded: %d, Failed: %d, Total: %d", downloaded, failed, len(all_files))


if __name__ == "__main__":
    main()
