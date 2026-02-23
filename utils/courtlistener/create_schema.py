#!/usr/bin/env python3
"""Generate PostgreSQL schema for CourtListener bulk data."""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import postgres_config

DB_CONFIG = postgres_config()

SCHEMA_SQL = """
-- Courts table
CREATE TABLE IF NOT EXISTS courts (
    id VARCHAR(15) PRIMARY KEY,
    pacer_court_id VARCHAR(50),
    pacer_has_rss_feed BOOLEAN,
    pacer_rss_entry_types TEXT,
    date_last_pacer_contact TIMESTAMP,
    fjc_court_id VARCHAR(50),
    date_modified TIMESTAMP,
    in_use BOOLEAN,
    has_opinion_scraper BOOLEAN,
    has_oral_argument_scraper BOOLEAN,
    position NUMERIC,
    citation_string VARCHAR(100),
    short_name VARCHAR(100),
    full_name VARCHAR(200),
    url VARCHAR(500),
    start_date DATE,
    end_date DATE,
    jurisdiction VARCHAR(10),
    notes TEXT,
    parent_court_id VARCHAR(15) REFERENCES courts(id)
);

-- Dockets table
CREATE TABLE IF NOT EXISTS dockets (
    id BIGINT PRIMARY KEY,
    date_created TIMESTAMP,
    date_modified TIMESTAMP,
    source SMALLINT,
    appeal_from_str TEXT,
    assigned_to_str TEXT,
    referred_to_str TEXT,
    panel_str TEXT,
    date_last_index TIMESTAMP,
    date_cert_granted DATE,
    date_cert_denied DATE,
    date_argued DATE,
    date_reargued DATE,
    date_reargument_denied DATE,
    date_filed DATE,
    date_terminated DATE,
    date_last_filing DATE,
    case_name_short TEXT,
    case_name TEXT,
    case_name_full TEXT,
    slug VARCHAR(75),
    docket_number TEXT,
    docket_number_core TEXT,
    pacer_case_id VARCHAR(100),
    cause TEXT,
    nature_of_suit TEXT,
    jury_demand TEXT,
    jurisdiction_type TEXT,
    appellate_fee_status TEXT,
    appellate_case_type_information TEXT,
    mdl_status TEXT,
    filepath_local TEXT,
    filepath_ia TEXT,
    filepath_ia_json TEXT,
    ia_upload_failure_count INTEGER,
    ia_needs_upload BOOLEAN,
    ia_date_first_change TIMESTAMP,
    view_count INTEGER,
    date_blocked DATE,
    blocked BOOLEAN,
    appeal_from_id VARCHAR(15) REFERENCES courts(id),
    assigned_to_id BIGINT,
    court_id VARCHAR(15) REFERENCES courts(id),
    idb_data_id BIGINT,
    originating_court_information_id BIGINT,
    referred_to_id BIGINT,
    federal_dn_case_type VARCHAR(50),
    federal_dn_office_code VARCHAR(50),
    federal_dn_judge_initials_assigned VARCHAR(50),
    federal_dn_judge_initials_referred VARCHAR(50),
    federal_defendant_number INTEGER,
    parent_docket_id BIGINT REFERENCES dockets(id)
);

-- Opinion Clusters table
CREATE TABLE IF NOT EXISTS opinion_clusters (
    id BIGINT PRIMARY KEY,
    date_created TIMESTAMP,
    date_modified TIMESTAMP,
    judges TEXT,
    date_filed DATE,
    date_filed_is_approximate BOOLEAN,
    slug VARCHAR(75),
    case_name_short TEXT,
    case_name TEXT,
    case_name_full TEXT,
    scdb_id VARCHAR(10),
    scdb_decision_direction SMALLINT,
    scdb_votes_majority INTEGER,
    scdb_votes_minority INTEGER,
    source VARCHAR(10),
    procedural_history TEXT,
    attorneys TEXT,
    nature_of_suit TEXT,
    posture TEXT,
    syllabus TEXT,
    headnotes TEXT,
    summary TEXT,
    disposition TEXT,
    history TEXT,
    other_dates TEXT,
    cross_reference TEXT,
    correction TEXT,
    citation_count INTEGER,
    precedential_status VARCHAR(50),
    date_blocked DATE,
    blocked BOOLEAN,
    filepath_json_harvard TEXT,
    filepath_pdf_harvard TEXT,
    docket_id BIGINT REFERENCES dockets(id),
    arguments TEXT,
    headmatter TEXT
);

-- Opinions table
CREATE TABLE IF NOT EXISTS opinions (
    id BIGINT PRIMARY KEY,
    date_created TIMESTAMP,
    date_modified TIMESTAMP,
    author_str TEXT,
    per_curiam BOOLEAN,
    joined_by_str TEXT,
    type VARCHAR(20),
    sha1 VARCHAR(40),
    page_count INTEGER,
    download_url VARCHAR(500),
    local_path TEXT,
    plain_text TEXT,
    html TEXT,
    html_lawbox TEXT,
    html_columbia TEXT,
    html_anon_2020 TEXT,
    xml_harvard TEXT,
    html_with_citations TEXT,
    extracted_by_ocr BOOLEAN,
    author_id BIGINT,
    cluster_id BIGINT REFERENCES opinion_clusters(id)
);

-- Citation map table
CREATE TABLE IF NOT EXISTS citation_map (
    id BIGINT PRIMARY KEY,
    depth INTEGER,
    cited_opinion_id BIGINT REFERENCES opinions(id),
    citing_opinion_id BIGINT REFERENCES opinions(id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_dockets_court ON dockets(court_id);
CREATE INDEX IF NOT EXISTS idx_dockets_date_filed ON dockets(date_filed);
CREATE INDEX IF NOT EXISTS idx_dockets_case_name ON dockets(case_name);
CREATE INDEX IF NOT EXISTS idx_opinion_clusters_docket ON opinion_clusters(docket_id);
CREATE INDEX IF NOT EXISTS idx_opinion_clusters_date_filed ON opinion_clusters(date_filed);
CREATE INDEX IF NOT EXISTS idx_opinions_cluster ON opinions(cluster_id);
CREATE INDEX IF NOT EXISTS idx_opinions_sha1 ON opinions(sha1);
CREATE INDEX IF NOT EXISTS idx_citation_map_cited ON citation_map(cited_opinion_id);
CREATE INDEX IF NOT EXISTS idx_citation_map_citing ON citation_map(citing_opinion_id);
"""

def create_database():
    """Create the database if it doesn't exist."""
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database='postgres',  # Connect to default postgres db first
        sslmode=DB_CONFIG['sslmode']
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    try:
        cur.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
        print(f"Database '{DB_CONFIG['database']}' created successfully")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{DB_CONFIG['database']}' already exists")
    finally:
        cur.close()
        conn.close()

def create_schema():
    """Create the schema in the database."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        cur.execute(SCHEMA_SQL)
        conn.commit()
        print("Schema created successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error creating schema: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    print("Creating database...")
    create_database()
    print("\nCreating schema...")
    create_schema()
    print("\nDone!")
