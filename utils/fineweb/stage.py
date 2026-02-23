#!/usr/bin/env python3
"""
FineWeb Dataset Downloader

Downloads HuggingFace's FineWeb dataset (cleaned English web text from Common Crawl).
The full dataset is 15T tokens. We use the sample-100BT subset (~500GB-1TB).

Requires: huggingface_hub
Auth: HF_TOKEN environment variable

Usage:
    python stage.py --list                  # List available subsets
    python stage.py --subset sample-10BT    # Download 10B token sample (~50GB)
    python stage.py --subset sample-100BT   # Download 100B token sample (~500GB)
    python stage.py --subset sample-350BT   # Download 350B token sample
"""

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir, log_dir

REPO_ID = "HuggingFaceFW/fineweb"
STORAGE_DIR = storage_dir("fineweb")
LOG_DIR = log_dir()

SUBSETS = {
    "sample-10BT": {
        "description": "10 billion token sample (~50GB compressed)",
        "size_estimate": "~50 GB",
    },
    "sample-100BT": {
        "description": "100 billion token sample (~250GB compressed)",
        "size_estimate": "~250 GB",
    },
    "sample-350BT": {
        "description": "350 billion token sample (~900GB compressed)",
        "size_estimate": "~900 GB",
    },
    "default": {
        "description": "Full dataset, 15 trillion tokens (VERY LARGE)",
        "size_estimate": "~12 TB",
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "fineweb_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def download_subset(subset: str, dest_dir: Path) -> int:
    """Download a FineWeb subset using huggingface_hub snapshot_download."""
    from huggingface_hub import HfApi, snapshot_download

    token = os.environ.get("HF_TOKEN")
    api = HfApi(token=token)

    # FineWeb organizes subsets as directories: sample/10BT/, sample/100BT/, etc.
    if subset == "default":
        include_pattern = "data/**/*.parquet"
    elif subset == "sample-10BT":
        include_pattern = "sample/10BT/**/*.parquet"
    elif subset == "sample-100BT":
        include_pattern = "sample/100BT/**/*.parquet"
    elif subset == "sample-350BT":
        include_pattern = "sample/350BT/**/*.parquet"
    else:
        log.error("Unknown subset: %s", subset)
        return 0

    log.info("Downloading %s (pattern: %s)", subset, include_pattern)
    log.info("Destination: %s", dest_dir)

    local_dir = snapshot_download(
        repo_id=REPO_ID,
        repo_type="dataset",
        local_dir=str(dest_dir),
        allow_patterns=[include_pattern],
        token=token,
    )

    # Count downloaded files
    count = sum(1 for _ in Path(local_dir).rglob("*.parquet"))
    return count


def list_remote_files(subset: str) -> list[tuple[str, int]]:
    """List parquet files for a subset without downloading."""
    from huggingface_hub import HfApi

    token = os.environ.get("HF_TOKEN")
    api = HfApi(token=token)

    if subset == "sample-10BT":
        prefix = "sample/10BT/"
    elif subset == "sample-100BT":
        prefix = "sample/100BT/"
    elif subset == "sample-350BT":
        prefix = "sample/350BT/"
    else:
        prefix = "data/"

    files = []
    for f in api.list_repo_tree(REPO_ID, repo_type="dataset", recursive=True,
                                 path_in_repo=prefix):
        size = getattr(f, "size", None)
        if size and f.path.endswith(".parquet"):
            files.append((f.path, size))
    return files


def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def main():
    parser = argparse.ArgumentParser(description="FineWeb dataset downloader")
    parser.add_argument("--list", action="store_true", help="List available subsets")
    parser.add_argument("--subset", choices=list(SUBSETS.keys()),
                        help="Which subset to download")
    parser.add_argument("--count-files", action="store_true",
                        help="Count remote files for the subset (slow for large subsets)")
    parser.add_argument("--dry-run", action="store_true", help="Show info without downloading")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    if args.list:
        for name, info in SUBSETS.items():
            print(f"  {name:16s}  {info['size_estimate']:>10s}  {info['description']}")
        return

    if not args.subset:
        parser.error("Specify --subset or --list")

    if not os.environ.get("HF_TOKEN"):
        log.error("HF_TOKEN environment variable not set")
        sys.exit(1)

    info = SUBSETS[args.subset]
    log.info("=== FineWeb %s: %s ===", args.subset, info["description"])

    if args.count_files or args.dry_run:
        log.info("Counting remote files (this may take a moment)...")
        files = list_remote_files(args.subset)
        total_size = sum(s for _, s in files)
        log.info("Found %d parquet files, total: %s", len(files), _human_size(total_size))
        if args.dry_run:
            return

    dest_dir = STORAGE_DIR / args.subset.replace("-", "_")
    count = download_subset(args.subset, dest_dir)
    log.info("Done. Downloaded %d parquet files to %s", count, dest_dir)


if __name__ == "__main__":
    main()
