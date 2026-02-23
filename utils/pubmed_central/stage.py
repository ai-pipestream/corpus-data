#!/usr/bin/env python3
"""
PubMed Central (PMC) Open Access Full Text Downloader

Downloads full-text biomedical articles from NCBI's PMC Open Access subset.
Three tiers by license:
  - oa_comm: Commercial-use OK (CC-BY, CC0)
  - oa_noncomm: Non-commercial only (CC-BY-NC)
  - oa_other: Other licenses

Each tier has multiple tar.gz packages containing JATS XML full text.

Usage:
    python stage.py --list                           # List available packages
    python stage.py --tier oa_comm                   # Download commercial-use tier
    python stage.py --tier oa_noncomm                # Download non-commercial tier
    python stage.py --all                            # Download all tiers
    python stage.py --file-list                      # Download the file list CSV
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

PMC_BASE = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/"
TIERS = ["oa_comm", "oa_noncomm", "oa_other"]
STORAGE_DIR = storage_dir("pubmed_central")
LOG_DIR = log_dir()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "pubmed_central_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def list_packages(tier: str) -> list[tuple[str, str]]:
    """List available tar.gz packages for a tier. Returns (filename, url) pairs."""
    url = f"{PMC_BASE}{tier}/xml/"
    log.info("Fetching package list from %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "corpus-data-stager/1.0"})
    with urllib.request.urlopen(req) as resp:
        html = resp.read().decode("utf-8")
    files = sorted(set(re.findall(r'href="([^"]+\.tar\.gz)"', html)))
    return [(f, url + f) for f in files]


def download_package(filename: str, url: str, dest_dir: Path) -> Path | None:
    """Download a single tar.gz package."""
    dest = dest_dir / filename
    if dest.exists():
        log.info("Already exists, skipping: %s", filename)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
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


def download_file_list() -> Path | None:
    """Download the OA file list CSV (maps PMCIDs to files and licenses)."""
    url = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.csv"
    dest = STORAGE_DIR / "oa_file_list.csv"
    if dest.exists():
        log.info("File list already exists: %s", dest)
        return dest

    log.info("Downloading OA file list CSV")
    result = subprocess.run(
        ["curl", "-L", "-o", str(dest), "--progress-bar", url],
        check=False,
    )
    if result.returncode != 0:
        log.error("Failed to download file list")
        dest.unlink(missing_ok=True)
        return None
    return dest


def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def main():
    parser = argparse.ArgumentParser(description="PubMed Central OA full text downloader")
    parser.add_argument("--list", action="store_true", help="List available packages per tier")
    parser.add_argument("--tier", choices=TIERS, help="Download a specific tier")
    parser.add_argument("--all", action="store_true", help="Download all tiers")
    parser.add_argument("--file-list", action="store_true", help="Download the OA file list CSV")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    if args.file_list:
        download_file_list()
        return

    if args.list:
        for tier in TIERS:
            packages = list_packages(tier)
            print(f"  {tier}: {len(packages)} packages")
            for fname, _ in packages[:3]:
                print(f"    {fname}")
            if len(packages) > 3:
                print(f"    ... and {len(packages) - 3} more")
            print()
        return

    tiers = TIERS if args.all else ([args.tier] if args.tier else [])
    if not tiers:
        parser.error("Specify --tier, --all, or --list")

    for tier in tiers:
        packages = list_packages(tier)
        log.info("=== %s: %d packages ===", tier, len(packages))

        dest_dir = STORAGE_DIR / tier

        if args.dry_run:
            for fname, _ in packages:
                print(f"  Would download: {tier}/{fname}")
            continue

        downloaded = 0
        for fname, url in packages:
            if download_package(fname, url, dest_dir):
                downloaded += 1
        log.info("Downloaded %d/%d packages for %s", downloaded, len(packages), tier)

    log.info("Done.")


if __name__ == "__main__":
    main()
