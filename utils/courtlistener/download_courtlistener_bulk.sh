#!/bin/bash
# Download CourtListener bulk data (no auth required for bulk files)

DATE="2024-12-31"
BUCKET="s3://com-courtlistener-storage/bulk-data"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT="$REPO_ROOT/storage/courtlistener/bulk"

mkdir -p "$OUTPUT"
cd "$OUTPUT"

echo "Downloading CourtListener bulk data from $DATE..."

# Core tables needed
aws s3 cp "$BUCKET/courts-$DATE.csv.bz2" . --no-sign-request
aws s3 cp "$BUCKET/dockets-$DATE.csv.bz2" . --no-sign-request
aws s3 cp "$BUCKET/opinion-clusters-$DATE.csv.bz2" . --no-sign-request
aws s3 cp "$BUCKET/opinions-$DATE.csv.bz2" . --no-sign-request
aws s3 cp "$BUCKET/citation-map-$DATE.csv.bz2" . --no-sign-request

echo "Download complete. Decompressing..."
bunzip2 -v *.bz2

echo "Done! Files in $OUTPUT"
du -sh "$OUTPUT"
