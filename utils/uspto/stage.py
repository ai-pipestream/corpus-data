#!/usr/bin/env python3
"""
USPTO Patent Bulk Data Downloader (ODP API)

Downloads bulk patent data via the USPTO Open Data Portal API.
The old bulkdata.uspto.gov has been retired in favor of data.uspto.gov / api.uspto.gov.

Products available (use --list-products to see all):
  PTGRXML  - Patent Grant Full-Text (no images) XML, 2002-present (~116 GB)
  APPXML   - Patent Application Full-Text (no images) XML (~150 GB)
  PTGRDT   - Patent Grant Full-Text with embedded TIFF images (~2.6 TB)
  ... and 38 more

The XML files contain image references (<img file="...TIF">) with dimensions,
even in the "no images" products. Cross-reference with PTGRDT/PTGRMP2 for actual images.

Requires: USPTO_API_KEY env var (get from https://data.uspto.gov/myodp/landing)

Usage:
    python stage.py --list-products                    # Show all 41 bulk data products
    python stage.py --product PTGRXML --list-files     # List files in a product
    python stage.py --product PTGRXML                  # Download all files for a product
    python stage.py --product PTGRXML --limit 5        # Download latest 5 files
    python stage.py --product PTGRXML --year 2024      # Download files from a specific year
    python stage.py --product APPXML                   # Download patent applications
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

STORAGE_DIR = storage_dir("uspto")
LOG_DIR = log_dir()

API_BASE = "https://api.uspto.gov/api/v1/datasets/products"
API_KEY = os.environ.get("USPTO_API_KEY", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def _api_request(url: str) -> dict:
    """Make an authenticated request to the USPTO ODP API."""
    if not API_KEY:
        log.error("USPTO_API_KEY not set. Get one at https://data.uspto.gov/myodp/landing")
        sys.exit(1)
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": API_KEY,
        "User-Agent": "corpus-data-stager/1.0",
    })
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def list_products() -> list[dict]:
    """List all available bulk data products."""
    data = _api_request(f"{API_BASE}/search?latest=true&limit=50")
    return data.get("bulkDataProductBag", [])


def get_product_files(product_id: str, offset: int = 0, limit: int = 100) -> dict:
    """Get file listing for a specific product."""
    data = _api_request(f"{API_BASE}/{product_id}?offset={offset}&limit={limit}")
    products = data.get("bulkDataProductBag", [])
    if not products:
        return {}
    return products[0]


def download_file(file_info: dict, dest_dir: Path) -> Path | None:
    """Download a single file from the ODP API."""
    filename = file_info["fileName"]
    uri = file_info["fileDownloadURI"]
    size = file_info.get("fileSize", 0)

    dest = dest_dir / filename
    if dest.exists() and dest.stat().st_size == size:
        log.info("Already exists, skipping: %s", filename)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    log.info("Downloading %s (%s)", filename, _human_size(size))
    result = subprocess.run(
        ["curl", "-L", "-o", str(dest), "--progress-bar",
         "-H", f"x-api-key: {API_KEY}", uri],
        check=False,
    )
    if result.returncode != 0:
        log.error("Failed to download %s", filename)
        dest.unlink(missing_ok=True)
        return None

    actual_size = dest.stat().st_size
    if size and actual_size != size:
        log.warning("Size mismatch for %s: expected %d, got %d", filename, size, actual_size)
    log.info("Downloaded %s (%s)", filename, _human_size(actual_size))
    return dest


def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def main():
    parser = argparse.ArgumentParser(description="USPTO ODP bulk data downloader")
    parser.add_argument("--list-products", action="store_true",
                        help="List all available bulk data products")
    parser.add_argument("--product", type=str,
                        help="Product ID to download (e.g. PTGRXML, APPXML)")
    parser.add_argument("--list-files", action="store_true",
                        help="List files for the specified product (don't download)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit number of files to download (0 = all)")
    parser.add_argument("--year", type=int,
                        help="Filter files by year (based on file date)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    # Add file handler now that log dir exists
    fh = logging.FileHandler(LOG_DIR / "uspto_stage.log")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(fh)

    if args.list_products:
        products = list_products()
        print(f"{'ID':12s} | {'Files':>6s} | {'Size':>10s} | Title")
        print("-" * 80)
        for p in products:
            size_gb = p.get("productTotalFileSize", 0) / (1024 ** 3)
            print(f"{p['productIdentifier']:12s} | {p['productFileTotalQuantity']:6d} | "
                  f"{size_gb:8.1f} GB | {p['productTitleText']}")
        print(f"\nTotal: {len(products)} products")
        return

    if not args.product:
        parser.error("Specify --product PRODUCT_ID or --list-products")

    product_id = args.product.upper()
    dest_dir = STORAGE_DIR / product_id.lower()

    # Fetch all files with pagination
    all_files = []
    offset = 0
    page_size = 100
    while True:
        product = get_product_files(product_id, offset=offset, limit=page_size)
        if not product:
            log.error("Product %s not found", product_id)
            sys.exit(1)

        bag = product.get("productFileBag", {})
        files = bag.get("fileDataBag", [])
        total = bag.get("count", 0)
        all_files.extend(files)

        if offset == 0:
            log.info("Product: %s - %s", product_id, product.get("productTitleText", ""))
            log.info("Total files: %d, Total size: %s",
                     product.get("productFileTotalQuantity", 0),
                     _human_size(product.get("productTotalFileSize", 0)))

        if len(all_files) >= total or not files:
            break
        offset += page_size

    # Filter by year if specified
    if args.year:
        year_str = str(args.year)
        all_files = [f for f in all_files
                     if f.get("fileDataFromDate", "").startswith(year_str)]
        log.info("Filtered to %d files for year %d", len(all_files), args.year)

    # Apply limit
    if args.limit > 0:
        all_files = all_files[:args.limit]

    if args.list_files or args.dry_run:
        for f in all_files:
            size_mb = f.get("fileSize", 0) / (1024 ** 2)
            date = f.get("fileDataFromDate", "?")
            print(f"  {f['fileName']:50s} {size_mb:8.1f} MB  {date}")
        print(f"\n{len(all_files)} files")
        return

    # Download
    total_downloaded = 0
    total_failed = 0
    for f in all_files:
        if download_file(f, dest_dir):
            total_downloaded += 1
        else:
            total_failed += 1

    log.info("Done. Downloaded: %d, Failed: %d", total_downloaded, total_failed)


if __name__ == "__main__":
    main()
