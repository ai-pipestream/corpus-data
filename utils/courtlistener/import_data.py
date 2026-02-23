#!/usr/bin/env python3
"""Import CourtListener bulk CSV data into PostgreSQL."""

import csv
import psycopg2
from psycopg2 import sql
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import postgres_config, storage_dir

DB_CONFIG = postgres_config()
DATA_DIR = storage_dir("courtlistener") / "bulk"

FILES = {
    'courts': 'courts-2024-12-31.csv',
    'dockets': 'dockets-2024-12-31.csv',
    'opinion_clusters': 'opinion-clusters-2024-12-31.csv',
    'opinions': 'opinions-2024-12-31.csv',
    'citation_map': 'citation-map-2024-12-31.csv'
}

def import_csv(conn, table_name, csv_file, batch_size=10000):
    """Import CSV file into PostgreSQL table using COPY."""
    print(f"\nImporting {csv_file} into {table_name}...")
    
    csv_path = DATA_DIR / csv_file
    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        return
    
    cur = conn.cursor()
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            # Use PostgreSQL's COPY command with backtick quotes
            cur.copy_expert(
                sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '`', NULL '')").format(
                    sql.Identifier(table_name)
                ),
                f
            )
        conn.commit()
        
        # Get row count
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
        count = cur.fetchone()[0]
        print(f"Imported {count:,} rows into {table_name}")
        
    except Exception as e:
        conn.rollback()
        print(f"Error importing {csv_file}: {e}")
        raise
    finally:
        cur.close()

def main():
    """Import all CSV files."""
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Import in order to respect foreign key constraints
        for table_name, csv_file in FILES.items():
            import_csv(conn, table_name, csv_file)
        
        print("\n✓ All data imported successfully!")
        
    finally:
        conn.close()

if __name__ == '__main__':
    main()
