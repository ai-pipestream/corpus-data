#!/usr/bin/env python3
"""
Semantic Scholar Academic Graph Downloader

Downloads academic paper metadata, abstracts, and citation graphs from
Semantic Scholar's Datasets API (225M+ papers).

Requires: S2_API_KEY environment variable (free, request at semanticscholar.org)

Usage:
    python stage.py --list                     # List available dataset types
    python stage.py --dataset papers            # Download papers dataset
    python stage.py --dataset abstracts         # Download abstracts
    python stage.py --dataset citations         # Download citation edges
    python stage.py --all                       # Download everything
    python stage.py --release latest            # Use specific release
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir, log_dir

S2_API_BASE = "https://api.semanticscholar.org/datasets/v1"
STORAGE_DIR = storage_dir("semantic_scholar")
LOG_DIR = log_dir()

DATASET_TYPES = [
    "papers", "abstracts", "authors", "citations",
    "embeddings-specter_v1", "embeddings-specter_v2",
    "paper-ids", "publication-venues", "s2orc", "tldrs",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "semantic_scholar_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def _s2_request(path: str) -> dict:
    """Make an authenticated request to the S2 Datasets API."""
    api_key = os.environ.get("S2_API_KEY", "")
    url = f"{S2_API_BASE}/{path}"
    headers = {"User-Agent": "corpus-data-stager/1.0"}
    if api_key:
        headers["x-api-key"] = api_key

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_latest_release() -> str:
    """Get the latest release ID."""
    data = _s2_request("release/latest")
    return data.get("release_id", "latest")


def list_datasets(release: str = "latest") -> list[str]:
    """List available dataset types in a release."""
    data = _s2_request(f"release/{release}")
    return data.get("datasets", [])


def get_download_links(release: str, dataset: str) -> list[str]:
    """Get presigned S3 download URLs for a dataset."""
    data = _s2_request(f"release/{release}/dataset/{dataset}")
    return data.get("files", [])


def download_dataset(release: str, dataset: str, dest_dir: Path) -> int:
    """Download all files for a dataset."""
    links = get_download_links(release, dataset)
    log.info("Found %d files for %s/%s", len(links), release, dataset)

    dataset_dir = dest_dir / release / dataset
    dataset_dir.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    for i, url in enumerate(links):
        # Extract filename from URL (before query params)
        filename = url.split("?")[0].split("/")[-1]
        if not filename:
            filename = f"part_{i:04d}.jsonl.gz"

        dest = dataset_dir / filename
        if dest.exists():
            log.info("[%d/%d] Already exists: %s", i + 1, len(links), filename)
            downloaded += 1
            continue

        log.info("[%d/%d] Downloading %s", i + 1, len(links), filename)
        result = subprocess.run(
            ["curl", "-L", "-o", str(dest), "--progress-bar", url],
            check=False,
        )
        if result.returncode == 0:
            downloaded += 1
        else:
            log.error("Failed to download %s", filename)
            dest.unlink(missing_ok=True)

    return downloaded


def main():
    parser = argparse.ArgumentParser(description="Semantic Scholar datasets downloader")
    parser.add_argument("--list", action="store_true", help="List available datasets")
    parser.add_argument("--dataset", help="Download a specific dataset type")
    parser.add_argument("--all", action="store_true", help="Download all datasets")
    parser.add_argument("--release", default="latest", help="Release ID (default: latest)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    if not os.environ.get("S2_API_KEY"):
        log.warning("S2_API_KEY not set. Request a free key at semanticscholar.org")
        log.warning("Some endpoints may be rate-limited without a key.")

    release = args.release
    if release == "latest":
        try:
            release = get_latest_release()
            log.info("Latest release: %s", release)
        except Exception as e:
            log.error("Could not determine latest release: %s", e)
            sys.exit(1)

    if args.list:
        datasets = list_datasets(release)
        print(f"Release: {release}")
        print(f"Available datasets ({len(datasets)}):")
        for d in datasets:
            print(f"  {d}")
        return

    targets = DATASET_TYPES if args.all else ([args.dataset] if args.dataset else [])
    if not targets:
        parser.error("Specify --dataset, --all, or --list")

    for dataset in targets:
        log.info("=== %s (release: %s) ===", dataset, release)

        if args.dry_run:
            links = get_download_links(release, dataset)
            log.info("Would download %d files for %s", len(links), dataset)
            continue

        count = download_dataset(release, dataset, STORAGE_DIR)
        log.info("Downloaded %d files for %s", count, dataset)

    log.info("Done.")


if __name__ == "__main__":
    main()
