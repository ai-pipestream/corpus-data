#!/usr/bin/env python3
"""Verify database schema and show table information."""

import psycopg2
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import postgres_config

DB_CONFIG = postgres_config()

def verify_schema():
    """Verify the schema and show table info."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print("✓ Connected to database successfully\n")
        
        # List all tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        """)
        tables = cur.fetchall()
        
        if tables:
            print(f"Found {len(tables)} tables:")
            for table in tables:
                table_name = table[0]
                
                # Get row count
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cur.fetchone()[0]
                
                # Get column count
                cur.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                """)
                col_count = cur.fetchone()[0]
                
                print(f"  - {table_name}: {count:,} rows, {col_count} columns")
        else:
            print("No tables found. Run create_schema.py first.")
        
        cur.close()
        conn.close()
        
    except psycopg2.OperationalError as e:
        print(f"✗ Connection failed: {e}")
    except Exception as e:
        print(f"✗ Error: {e}")

if __name__ == '__main__':
    verify_schema()
