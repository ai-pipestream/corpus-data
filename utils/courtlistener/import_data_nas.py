#!/usr/bin/env python3
"""Import with progress tracking."""
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

def import_csv(conn, table_name, csv_file):
    """Import CSV with progress."""
    csv_path = DATA_DIR / csv_file
    if not csv_path.exists():
        print(f"✗ File not found: {csv_path}")
        return
    
    file_size = csv_path.stat().st_size
    print(f"\n{'='*60}")
    print(f"Importing {csv_file} ({file_size / 1e9:.1f} GB)")
    print(f"Into table: {table_name}")
    print('='*60)
    
    cur = conn.cursor()
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            cur.copy_expert(
                sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '`', NULL '')").format(
                    sql.Identifier(table_name)
                ),
                f
            )
        conn.commit()
        
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
        count = cur.fetchone()[0]
        print(f"✓ Imported {count:,} rows")
        
    except Exception as e:
        conn.rollback()
        print(f"✗ Error: {e}")
        raise
    finally:
        cur.close()

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        for table_name, csv_file in FILES.items():
            import_csv(conn, table_name, csv_file)
        
        print("\n" + "="*60)
        print("✓ All data imported successfully!")
        print("="*60)
        
    finally:
        conn.close()

if __name__ == '__main__':
    main()
