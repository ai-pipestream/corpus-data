#!/bin/bash
# Import CourtListener data using psql COPY command

DATA_DIR="/data/bulk"
DB="courtlistener"
USER="cluser"

echo "Starting import at $(date)"

# Function to import a CSV file
import_csv() {
    local table=$1
    local file=$2
    
    echo ""
    echo "========================================"
    echo "Importing $file into $table"
    echo "File size: $(du -h $DATA_DIR/$file | cut -f1)"
    echo "========================================"
    
    psql -U $USER -d $DB -c "\COPY $table FROM '$DATA_DIR/$file' WITH (FORMAT CSV, HEADER TRUE, QUOTE '"'`'"', NULL '')"
    
    if [ $? -eq 0 ]; then
        count=$(psql -U $USER -d $DB -t -c "SELECT COUNT(*) FROM $table")
        echo "✓ Imported $count rows into $table"
    else
        echo "✗ Failed to import $file"
        exit 1
    fi
}

# Import in order (respecting foreign keys)
import_csv "courts" "courts-2024-12-31.csv"
import_csv "dockets" "dockets-2024-12-31.csv"
import_csv "opinion_clusters" "opinion-clusters-2024-12-31.csv"
import_csv "opinions" "opinions-2024-12-31.csv"
import_csv "citation_map" "citation-map-2024-12-31.csv"

echo ""
echo "========================================"
echo "✓ All imports completed at $(date)"
echo "========================================"
