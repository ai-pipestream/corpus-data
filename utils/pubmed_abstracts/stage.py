#!/usr/bin/env python3
"""
PubMed/MEDLINE Abstracts Downloader

Downloads the annual baseline and daily update files from NCBI's PubMed FTP.
Each file is a gzipped XML containing ~30,000 citation records with abstracts.

Baseline: ~1,334 files, ~20MB each = ~25GB compressed, ~100GB+ uncompressed
Updates: daily incremental files

Usage:
    python stage.py                        # Download full baseline
    python stage.py --list                 # List available files
    python stage.py --updates              # Download daily update files
    python stage.py --verify               # Verify MD5 checksums
    python stage.py --range 1 10           # Download files 1-10 only
"""

import argparse
import hashlib
import logging
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir, log_dir

BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
UPDATE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
STORAGE_DIR = storage_dir("pubmed_abstracts")
LOG_DIR = log_dir()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "pubmed_abstracts_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def list_files(url: str) -> list[str]:
    """Scrape NCBI FTP listing for .xml.gz files."""
    log.info("Fetching file list from %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "corpus-data-stager/1.0"})
    with urllib.request.urlopen(req) as resp:
        html = resp.read().decode("utf-8")
    files = sorted(set(re.findall(r'(pubmed\d+n\d+\.xml\.gz)(?!\.md5)', html)))
    return files


def download_file(filename: str, url_base: str, dest_dir: Path) -> Path:
    """Download a single file using curl, skip if exists."""
    dest = dest_dir / filename
    if dest.exists():
        log.info("Already exists, skipping: %s", filename)
        return dest

    url = url_base + filename
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


def download_md5(filename: str, url_base: str, dest_dir: Path) -> str | None:
    """Download and parse the MD5 checksum file."""
    md5_file = filename + ".md5"
    url = url_base + md5_file
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "corpus-data-stager/1.0"})
        with urllib.request.urlopen(req) as resp:
            content = resp.read().decode("utf-8").strip()
        # Format: MD5(filename)= <hash>
        match = re.search(r'=\s*([a-f0-9]{32})', content)
        if match:
            return match.group(1)
    except Exception as e:
        log.warning("Could not fetch MD5 for %s: %s", filename, e)
    return None


def verify_file(filepath: Path, expected_md5: str) -> bool:
    """Verify file MD5 checksum."""
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    actual = md5.hexdigest()
    if actual == expected_md5:
        log.info("MD5 OK: %s", filepath.name)
        return True
    else:
        log.error("MD5 MISMATCH: %s (expected %s, got %s)", filepath.name, expected_md5, actual)
        return False


def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def _file_number(filename: str) -> int:
    """Extract the file number from pubmed26n0001.xml.gz -> 1."""
    match = re.search(r'n(\d+)\.xml\.gz', filename)
    return int(match.group(1)) if match else 0


def main():
    parser = argparse.ArgumentParser(description="PubMed/MEDLINE abstracts downloader")
    parser.add_argument("--list", action="store_true", help="List available files and exit")
    parser.add_argument("--updates", action="store_true", help="Download update files instead of baseline")
    parser.add_argument("--verify", action="store_true", help="Verify MD5 checksums of downloaded files")
    parser.add_argument("--range", nargs=2, type=int, metavar=("START", "END"),
                        help="Download only file numbers in this range (inclusive)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    url_base = UPDATE_URL if args.updates else BASE_URL
    label = "update" if args.updates else "baseline"

    all_files = list_files(url_base)
    log.info("Found %d %s files", len(all_files), label)

    if args.range:
        start, end = args.range
        all_files = [f for f in all_files if start <= _file_number(f) <= end]
        log.info("Filtered to %d files in range %d-%d", len(all_files), start, end)

    if args.list:
        for f in all_files:
            print(f)
        print(f"\nTotal: {len(all_files)} {label} files")
        return

    if args.dry_run:
        for f in all_files:
            print(f"Would download: {f}")
        print(f"\nTotal: {len(all_files)} files")
        return

    downloaded = 0
    failed = 0
    verified = 0
    for filename in all_files:
        dest = download_file(filename, url_base, STORAGE_DIR)
        if dest:
            downloaded += 1
            if args.verify:
                expected_md5 = download_md5(filename, url_base, STORAGE_DIR)
                if expected_md5 and verify_file(dest, expected_md5):
                    verified += 1
                elif expected_md5:
                    log.error("Checksum failed, removing: %s", filename)
                    dest.unlink(missing_ok=True)
                    failed += 1
                    downloaded -= 1
        else:
            failed += 1

    log.info("Done. Downloaded: %d, Failed: %d, Verified: %d, Total: %d",
             downloaded, failed, verified, len(all_files))


if __name__ == "__main__":
    main()
