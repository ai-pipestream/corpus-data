#!/usr/bin/env python3
"""
EUR-Lex EU Legislation Downloader

Downloads EU legal documents from the EUR-Lex CELLAR SPARQL endpoint
and REST API. Covers treaties, regulations, directives, decisions in 24 languages.

The bulk data dump at datadump.publications.europa.eu requires an EU Login account.
This script uses the public REST API as an alternative (no account needed).

Usage:
    python stage.py --list-types                    # List document types
    python stage.py --type regulation --lang en     # Download EN regulations
    python stage.py --type directive --lang en      # Download EN directives
    python stage.py --all --lang en                 # Download all types in English
    python stage.py --cellar-query                  # Run a CELLAR SPARQL query
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir, log_dir

EURLEX_SEARCH = "https://eur-lex.europa.eu/search.html"
EURLEX_REST = "https://eur-lex.europa.eu/eurlex-ws/rest"
CELLAR_SPARQL = "https://publications.europa.eu/webapi/rdf/sparql"
STORAGE_DIR = storage_dir("eurlex")
LOG_DIR = log_dir()

DOC_TYPES = {
    "regulation": "reg",
    "directive": "dir",
    "decision": "dec",
    "treaty": "treaty",
    "recommendation": "reco",
    "opinion": "opin",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "eurlex_stage.log"),
    ],
)
log = logging.getLogger(__name__)


def cellar_query(sparql: str, limit: int = 1000) -> list[dict]:
    """Execute a SPARQL query against the CELLAR endpoint."""
    full_query = sparql if "LIMIT" in sparql.upper() else f"{sparql} LIMIT {limit}"
    params = urllib.parse.urlencode({"query": full_query})
    url = f"{CELLAR_SPARQL}?{params}"

    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "User-Agent": "corpus-data-stager/1.0",
    })
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    results = []
    bindings = data.get("results", {}).get("bindings", [])
    for b in bindings:
        row = {}
        for key, val in b.items():
            row[key] = val.get("value", "")
        results.append(row)
    return results


def list_celex_ids(doc_type: str, year_start: int = 2000, year_end: int = 2026) -> list[str]:
    """Query CELLAR for CELEX identifiers of a document type."""
    # CELEX sector 3 = secondary legislation
    type_code = DOC_TYPES.get(doc_type, doc_type)

    sparql = f"""
    PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
    SELECT DISTINCT ?celex WHERE {{
        ?work cdm:resource_legal_id_celex ?celex .
        FILTER(CONTAINS(STR(?celex), "3"))
    }}
    ORDER BY ?celex
    """
    log.info("Querying CELLAR for %s documents", doc_type)
    results = cellar_query(sparql, limit=10000)
    return [r["celex"] for r in results]


def download_celex_document(celex_id: str, lang: str, dest_dir: Path) -> Path | None:
    """Download a document by CELEX ID in HTML format."""
    safe_id = celex_id.replace(":", "_")
    dest = dest_dir / f"{safe_id}_{lang}.html"
    if dest.exists():
        return dest

    # EUR-Lex REST endpoint for document content
    url = f"https://eur-lex.europa.eu/legal-content/{lang.upper()}/TXT/HTML/?uri=CELEX:{celex_id}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "corpus-data-stager/1.0"})
        time.sleep(0.5)  # Be polite
        with urllib.request.urlopen(req) as resp:
            content = resp.read()

        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        return dest
    except Exception as e:
        log.warning("Failed to download CELEX %s: %s", celex_id, e)
        return None


def main():
    parser = argparse.ArgumentParser(description="EUR-Lex EU legislation downloader")
    parser.add_argument("--list-types", action="store_true", help="List document types")
    parser.add_argument("--type", choices=list(DOC_TYPES.keys()), help="Document type")
    parser.add_argument("--all", action="store_true", help="Download all document types")
    parser.add_argument("--lang", default="en", help="Language code (default: en)")
    parser.add_argument("--cellar-query", action="store_true",
                        help="Test CELLAR SPARQL endpoint")
    parser.add_argument("--limit", type=int, default=100, help="Max documents to download")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    if args.list_types:
        for name, code in DOC_TYPES.items():
            print(f"  {name:16s}  (CELEX code: {code})")
        return

    if args.cellar_query:
        log.info("Testing CELLAR SPARQL endpoint...")
        results = cellar_query("""
            PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
            SELECT (COUNT(?work) as ?count) WHERE {
                ?work a cdm:legislation_secondary .
            }
        """)
        print(f"Secondary legislation count: {results}")
        return

    types = list(DOC_TYPES.keys()) if args.all else ([args.type] if args.type else [])
    if not types:
        parser.error("Specify --type, --all, or --list-types")

    for doc_type in types:
        log.info("=== %s (%s) ===", doc_type, args.lang)

        celex_ids = list_celex_ids(doc_type)
        log.info("Found %d CELEX IDs for %s", len(celex_ids), doc_type)

        if args.limit:
            celex_ids = celex_ids[:args.limit]

        if args.dry_run:
            for cid in celex_ids[:10]:
                print(f"  Would download: {cid}")
            if len(celex_ids) > 10:
                print(f"  ... and {len(celex_ids) - 10} more")
            continue

        dest_dir = STORAGE_DIR / doc_type / args.lang
        downloaded = 0
        for cid in celex_ids:
            if download_celex_document(cid, args.lang, dest_dir):
                downloaded += 1
        log.info("Downloaded %d/%d %s documents", downloaded, len(celex_ids), doc_type)

    log.info("Done.")


if __name__ == "__main__":
    main()
