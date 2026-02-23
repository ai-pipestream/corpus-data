#!/usr/bin/env python3
"""
OpenAlex Dataset Downloader

Downloads the OpenAlex academic graph snapshot from S3.
250M+ scholarly works, authors, institutions, citations.

Public S3 bucket: s3://openalex (--no-sign-request)

This is primarily for NAS staging. The s3-connector can also crawl
this bucket directly for the pipeline.

Usage:
    python stage.py --list                  # List available entity types
    python stage.py --entity works          # Download works (largest, ~200GB)
    python stage.py --entity authors        # Download authors
    python stage.py --all                   # Download everything (~300GB)
    python stage.py --sync                  # Sync (only download new/changed)
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir, log_dir

S3_BUCKET = "s3://openalex"
STORAGE_DIR = storage_dir("openalex")
LOG_DIR = log_dir()

ENTITIES = [
    "works", "authors", "institutions", "sources",
    "publishers", "funders", "topics", "concepts",
    "fields", "subfields", "domains",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "openalex_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def list_entity_sizes() -> dict[str, str]:
    """List each entity directory and its approximate size on S3."""
    sizes = {}
    for entity in ENTITIES:
        result = subprocess.run(
            ["aws", "s3", "ls", f"{S3_BUCKET}/data/{entity}/",
             "--no-sign-request", "--summarize", "--human-readable", "--recursive"],
            capture_output=True, text=True,
        )
        # Parse summary line: "Total Size: X.X GiB"
        for line in result.stdout.split("\n"):
            if "Total Size" in line:
                sizes[entity] = line.strip()
                break
    return sizes


def sync_entity(entity: str, dest_dir: Path) -> bool:
    """Sync an entity directory from S3 to local storage."""
    src = f"{S3_BUCKET}/data/{entity}/"
    dest = dest_dir / entity
    dest.mkdir(parents=True, exist_ok=True)

    log.info("Syncing %s -> %s", src, dest)
    result = subprocess.run(
        ["aws", "s3", "sync", src, str(dest), "--no-sign-request"],
        check=False,
    )
    if result.returncode != 0:
        log.error("Failed to sync %s", entity)
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="OpenAlex dataset downloader (S3)")
    parser.add_argument("--list", action="store_true", help="List entity types and sizes")
    parser.add_argument("--entity", choices=ENTITIES, help="Download a specific entity")
    parser.add_argument("--all", action="store_true", help="Download all entities")
    parser.add_argument("--sync", action="store_true",
                        help="Use sync mode (skip existing, download only changes)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    if args.list:
        log.info("Checking entity sizes on S3 (this takes a moment)...")
        sizes = list_entity_sizes()
        for entity in ENTITIES:
            size_str = sizes.get(entity, "unknown")
            print(f"  {entity:16s}  {size_str}")
        return

    targets = ENTITIES if args.all else ([args.entity] if args.entity else [])
    if not targets:
        parser.error("Specify --entity, --all, or --list")

    for entity in targets:
        log.info("=== %s ===", entity)

        if args.dry_run:
            log.info("Would sync %s/data/%s/ -> %s/%s/",
                     S3_BUCKET, entity, STORAGE_DIR, entity)
            continue

        if sync_entity(entity, STORAGE_DIR):
            log.info("Synced %s successfully", entity)
        else:
            log.error("Failed to sync %s", entity)

    log.info("Done.")


if __name__ == "__main__":
    main()
