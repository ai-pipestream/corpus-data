/*
================================================================================
COURTLISTENER DATA IMPORT - FINALIZATION SCRIPT
================================================================================

This script re-adds Primary Keys, Foreign Keys, and Indexes that were removed
to speed up the bulk import.

INSTRUCTIONS:
1. Wait for the import log to show "All imports completed".
   Check log: ssh nas "docker exec courtlistener_db cat /tmp/import.log"

2. Run this script using the following command from krick:
   ssh <NAS_HOST> "docker exec -i -e PGPASSWORD='<POSTGRES_PASSWORD>' courtlistener_db psql -U cluser -d courtlistener" < utils/courtlistener/schema/finish_import.sql

3. This process may take 1-2 hours because it has to scan all 300GB of data
   to build the indexes.

4. Once finished, you can re-enable standard Postgres settings in Portainer
   by removing the custom "command" overrides if you wish, or keep them for
   better performance.
================================================================================
*/

-- 1. Add Primary Keys (if not already there - though we kept PKs in the load schema)
-- These should already exist from the load_schema.sql, but we ensure integrity here.

-- 2. Add Foreign Keys (This validates relationships)
ALTER TABLE courts ADD CONSTRAINT fk_parent_court FOREIGN KEY (parent_court_id) REFERENCES courts(id);
ALTER TABLE dockets ADD CONSTRAINT fk_dockets_court FOREIGN KEY (court_id) REFERENCES courts(id);
ALTER TABLE dockets ADD CONSTRAINT fk_dockets_appeal_from FOREIGN KEY (appeal_from_id) REFERENCES courts(id);
ALTER TABLE dockets ADD CONSTRAINT fk_dockets_parent FOREIGN KEY (parent_docket_id) REFERENCES dockets(id);
ALTER TABLE opinion_clusters ADD CONSTRAINT fk_clusters_docket FOREIGN KEY (docket_id) REFERENCES dockets(id);
ALTER TABLE opinions ADD CONSTRAINT fk_opinions_cluster FOREIGN KEY (cluster_id) REFERENCES opinion_clusters(id);
ALTER TABLE citation_map ADD CONSTRAINT fk_citations_cited FOREIGN KEY (cited_opinion_id) REFERENCES opinions(id);
ALTER TABLE citation_map ADD CONSTRAINT fk_citations_citing FOREIGN KEY (citing_opinion_id) REFERENCES opinions(id);

-- 3. Create Performance Indexes
CREATE INDEX IF NOT EXISTS idx_dockets_court ON dockets(court_id);
CREATE INDEX IF NOT EXISTS idx_dockets_date_filed ON dockets(date_filed);
CREATE INDEX IF NOT EXISTS idx_dockets_case_name ON dockets(case_name);
CREATE INDEX IF NOT EXISTS idx_opinion_clusters_docket ON opinion_clusters(docket_id);
CREATE INDEX IF NOT EXISTS idx_opinion_clusters_date_filed ON opinion_clusters(date_filed);
CREATE INDEX IF NOT EXISTS idx_opinions_cluster ON opinions(cluster_id);
CREATE INDEX IF NOT EXISTS idx_opinions_sha1 ON opinions(sha1);
CREATE INDEX IF NOT EXISTS idx_citation_map_cited ON citation_map(cited_opinion_id);
CREATE INDEX IF NOT EXISTS idx_citation_map_citing ON citation_map(citing_opinion_id);

-- 4. Final Cleanup & Optimization
ANALYZE; -- Updates statistics for the query planner
