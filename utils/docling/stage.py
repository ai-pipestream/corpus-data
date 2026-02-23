#!/usr/bin/env python3
"""
Docling PDF Dataset Downloader

Downloads PDF documents from Docling's HuggingFace datasets:
  - DocLayNet-v1.2: 80,863 single-page PDFs (39.8 GB parquet, 6 doc categories)
  - DP-Bench: 200 multi-page PDFs (263 MB parquet, benchmark set)

Requires: huggingface_hub, pyarrow
Auth: HF_TOKEN environment variable

Usage:
    python stage.py --list                  # Show available datasets
    python stage.py --dataset dpbench       # Download DP-Bench (small, quick test)
    python stage.py --dataset doclaynet     # Download DocLayNet-v1.2 (40GB)
    python stage.py --dataset all           # Download everything
    python stage.py --dataset doclaynet --split test  # Only test split
    python stage.py --extract-pdfs          # Extract individual PDF files from parquet
"""

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir, log_dir

LOG_DIR = log_dir()
STORAGE_DIR = storage_dir("docling")

DATASETS = {
    "dpbench": {
        "repo_id": "docling-project/docling-dpbench",
        "description": "200 multi-page PDFs (benchmark set)",
        "pdf_column": "BinaryDocument",
        "id_column": "document_id",
        "size_gb": 0.26,
        "splits": ["test"],
    },
    "doclaynet": {
        "repo_id": "docling-project/DocLayNet-v1.2",
        "description": "80,863 single-page PDFs across 6 document categories",
        "pdf_column": "pdf",
        "id_column": None,  # use metadata.page_hash
        "size_gb": 39.8,
        "splits": ["train", "validation", "test"],
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "docling_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def download_parquets(repo_id: str, dest_dir: Path, split: str | None = None) -> list[Path]:
    """Download parquet files from a HuggingFace dataset repo."""
    from huggingface_hub import HfApi, hf_hub_download

    token = os.environ.get("HF_TOKEN")
    api = HfApi(token=token)

    log.info("Listing files in %s", repo_id)
    files = api.list_repo_tree(repo_id, repo_type="dataset", recursive=True)

    parquets = []
    for f in files:
        if not hasattr(f, "size") or not f.path.endswith(".parquet"):
            continue
        if split:
            basename = Path(f.path).name
            if not basename.startswith(f"{split}-") and f"/{split}/" not in f.path:
                continue
        parquets.append(f.path)

    log.info("Found %d parquet files to download", len(parquets))

    downloaded = []
    for i, pq_path in enumerate(parquets):
        dest = dest_dir / pq_path
        if dest.exists():
            log.info("[%d/%d] Already exists: %s", i + 1, len(parquets), pq_path)
            downloaded.append(dest)
            continue

        log.info("[%d/%d] Downloading %s", i + 1, len(parquets), pq_path)
        local = hf_hub_download(
            repo_id=repo_id,
            filename=pq_path,
            repo_type="dataset",
            token=token,
            local_dir=str(dest_dir),
        )
        downloaded.append(Path(local))

    return downloaded


def extract_pdfs_dpbench(parquet_files: list[Path], output_dir: Path) -> int:
    """Extract PDFs from DP-Bench parquet (BinaryDocument column)."""
    import pyarrow.parquet as pq

    output_dir.mkdir(parents=True, exist_ok=True)
    count = 0

    for pf in parquet_files:
        log.info("Extracting PDFs from %s", pf.name)
        table = pq.read_table(str(pf), columns=["BinaryDocument", "document_id"])

        for i in range(len(table)):
            pdf_bytes = table["BinaryDocument"][i].as_py()
            doc_id = table["document_id"][i].as_py()

            # Clean up doc_id for filename
            safe_name = doc_id.replace("/", "_").replace("\\", "_")
            if not safe_name.endswith(".pdf"):
                safe_name += ".pdf"

            dest = output_dir / safe_name
            if dest.exists():
                continue

            with open(dest, "wb") as f:
                f.write(pdf_bytes)
            count += 1

        log.info("Extracted %d PDFs so far", count)

    return count


def extract_pdfs_doclaynet(parquet_files: list[Path], output_dir: Path) -> int:
    """Extract PDFs from DocLayNet-v1.2 parquet (pdf column + metadata)."""
    import pyarrow.parquet as pq

    output_dir.mkdir(parents=True, exist_ok=True)
    count = 0

    for pf in parquet_files:
        log.info("Extracting PDFs from %s", pf.name)
        table = pq.read_table(str(pf), columns=["pdf", "metadata"])

        for i in range(len(table)):
            pdf_bytes = table["pdf"][i].as_py()
            if not pdf_bytes:
                continue

            meta = table["metadata"][i].as_py()
            page_hash = meta.get("page_hash", f"unknown_{count:06d}")
            doc_cat = meta.get("doc_category", "unknown")
            orig_file = meta.get("original_filename", "")
            page_no = meta.get("page_no", 0)

            # Organize by category
            cat_dir = output_dir / doc_cat
            cat_dir.mkdir(parents=True, exist_ok=True)

            filename = f"{page_hash}.pdf"
            dest = cat_dir / filename
            if dest.exists():
                continue

            with open(dest, "wb") as f:
                f.write(pdf_bytes)
            count += 1

            if count % 5000 == 0:
                log.info("Extracted %d PDFs so far", count)

    return count


def main():
    parser = argparse.ArgumentParser(description="Docling PDF dataset downloader")
    parser.add_argument("--list", action="store_true", help="List available datasets")
    parser.add_argument("--dataset", choices=["dpbench", "doclaynet", "all"],
                        help="Which dataset to download")
    parser.add_argument("--split", help="Only download a specific split (train/validation/test)")
    parser.add_argument("--extract-pdfs", action="store_true",
                        help="Extract individual PDF files from parquet")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    if args.list:
        for name, info in DATASETS.items():
            print(f"  {name:12s}  {info['size_gb']:6.1f} GB  {info['description']}")
            print(f"               repo: {info['repo_id']}")
            print(f"               splits: {', '.join(info['splits'])}")
            print()
        return

    if not args.dataset:
        parser.error("Specify --dataset or --list")

    targets = list(DATASETS.keys()) if args.dataset == "all" else [args.dataset]

    if not os.environ.get("HF_TOKEN"):
        log.error("HF_TOKEN environment variable not set")
        sys.exit(1)

    for name in targets:
        info = DATASETS[name]
        log.info("=== %s: %s ===", name, info["description"])
        log.info("Repo: %s (%.1f GB)", info["repo_id"], info["size_gb"])

        if args.dry_run:
            log.info("DRY RUN: would download %s", info["repo_id"])
            continue

        dest_dir = STORAGE_DIR / name
        dest_dir.mkdir(parents=True, exist_ok=True)

        parquets = download_parquets(info["repo_id"], dest_dir, split=args.split)
        log.info("Downloaded %d parquet files to %s", len(parquets), dest_dir)

        if args.extract_pdfs:
            pdf_dir = dest_dir / "pdfs"
            if name == "dpbench":
                count = extract_pdfs_dpbench(parquets, pdf_dir)
            elif name == "doclaynet":
                count = extract_pdfs_doclaynet(parquets, pdf_dir)
            else:
                log.warning("No PDF extractor for %s", name)
                continue
            log.info("Extracted %d PDFs to %s", count, pdf_dir)

    log.info("Done.")


if __name__ == "__main__":
    main()
