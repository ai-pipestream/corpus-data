#!/usr/bin/env python3
import psycopg2
import sys

host = input("Host [localhost]: ") or "localhost"
port = input("Port [5432]: ") or "5432"
user = input("Username [postgres]: ") or "postgres"
password = input("Password: ")
database = input("Database [postgres]: ") or "postgres"

try:
    conn = psycopg2.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=database,
        sslmode='require'
    )
    print("\n✓ Connection successful!")
    
    cur = conn.cursor()
    cur.execute("SELECT version()")
    print(f"✓ PostgreSQL: {cur.fetchone()[0][:50]}...")
    
    cur.execute("SELECT current_database()")
    print(f"✓ Database: {cur.fetchone()[0]}")
    
    cur.execute("SELECT current_user")
    print(f"✓ User: {cur.fetchone()[0]}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"\n✗ Failed: {e}")
    sys.exit(1)
