#!/usr/bin/env python3
import psycopg2
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import postgres_config

try:
    conn = psycopg2.connect(**postgres_config())
    print("✓ Connection successful!")
    
    cur = conn.cursor()
    cur.execute("SELECT version()")
    print(f"✓ PostgreSQL version: {cur.fetchone()[0]}")
    
    cur.execute("SELECT current_database()")
    print(f"✓ Current database: {cur.fetchone()[0]}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"✗ Connection failed: {e}")
