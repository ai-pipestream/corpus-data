#!/usr/bin/env python3
"""Sample queries for CourtListener database."""

import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import postgres_config

DB_CONFIG = postgres_config()

def run_query(conn, title, query, limit=10):
    """Run a query and display results."""
    print(f"\n{'='*60}")
    print(f"{title}")
    print('='*60)
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    
    rows = cur.fetchmany(limit)
    if not rows:
        print("No results found.")
        return
    
    for i, row in enumerate(rows, 1):
        print(f"\n{i}.")
        for key, value in row.items():
            if value and len(str(value)) > 100:
                value = str(value)[:100] + "..."
            print(f"  {key}: {value}")
    
    cur.close()

def main():
    """Run sample queries."""
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Query 1: Court statistics
        run_query(conn, "Court Statistics", """
            SELECT 
                c.short_name,
                c.jurisdiction,
                COUNT(DISTINCT d.id) as docket_count,
                COUNT(DISTINCT oc.id) as cluster_count
            FROM courts c
            LEFT JOIN dockets d ON c.id = d.court_id
            LEFT JOIN opinion_clusters oc ON d.id = oc.docket_id
            WHERE c.in_use = true
            GROUP BY c.id, c.short_name, c.jurisdiction
            ORDER BY docket_count DESC
            LIMIT 10
        """)
        
        # Query 2: Recent cases
        run_query(conn, "Most Recent Cases (2024)", """
            SELECT 
                d.case_name,
                c.short_name as court,
                oc.date_filed,
                oc.precedential_status
            FROM opinion_clusters oc
            JOIN dockets d ON oc.docket_id = d.id
            JOIN courts c ON d.court_id = c.id
            WHERE oc.date_filed >= '2024-01-01'
            ORDER BY oc.date_filed DESC
            LIMIT 10
        """)
        
        # Query 3: Opinion types distribution
        run_query(conn, "Opinion Types Distribution", """
            SELECT 
                type,
                COUNT(*) as count
            FROM opinions
            GROUP BY type
            ORDER BY count DESC
        """)
        
        # Query 4: Most cited opinions
        run_query(conn, "Most Cited Opinions", """
            SELECT 
                o.id,
                oc.case_name,
                c.short_name as court,
                oc.date_filed,
                COUNT(cm.id) as citation_count
            FROM opinions o
            JOIN opinion_clusters oc ON o.cluster_id = oc.id
            JOIN dockets d ON oc.docket_id = d.id
            JOIN courts c ON d.court_id = c.id
            JOIN citation_map cm ON o.id = cm.cited_opinion_id
            GROUP BY o.id, oc.case_name, c.short_name, oc.date_filed
            ORDER BY citation_count DESC
            LIMIT 10
        """)
        
        # Query 5: Database size summary
        run_query(conn, "Database Summary", """
            SELECT 
                'courts' as table_name,
                COUNT(*) as row_count
            FROM courts
            UNION ALL
            SELECT 'dockets', COUNT(*) FROM dockets
            UNION ALL
            SELECT 'opinion_clusters', COUNT(*) FROM opinion_clusters
            UNION ALL
            SELECT 'opinions', COUNT(*) FROM opinions
            UNION ALL
            SELECT 'citation_map', COUNT(*) FROM citation_map
        """, limit=100)
        
    finally:
        conn.close()

if __name__ == '__main__':
    main()
