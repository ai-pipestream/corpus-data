# CourtListener Database Schema

This directory contains the CourtListener bulk data download and PostgreSQL schema.

## Data Source

Downloaded from CourtListener's public S3 bucket:
- **Bucket**: `s3://com-courtlistener-storage/bulk-data`
- **Date**: 2024-12-31
- **Files**: courts, dockets, opinion-clusters, opinions, citation-map

## Database Schema

### Tables

#### 1. **courts**
Court metadata including jurisdiction, names, and dates.
- Primary Key: `id` (VARCHAR)
- Key fields: `short_name`, `full_name`, `jurisdiction`, `citation_string`

#### 2. **dockets**
Case dockets with filing information and metadata.
- Primary Key: `id` (BIGINT)
- Foreign Keys: `court_id` → courts, `appeal_from_id` → courts
- Key fields: `case_name`, `docket_number`, `date_filed`, `date_terminated`

#### 3. **opinion_clusters**
Groups of related opinions for a single case decision.
- Primary Key: `id` (BIGINT)
- Foreign Key: `docket_id` → dockets
- Key fields: `case_name`, `date_filed`, `precedential_status`, `judges`

#### 4. **opinions**
Individual opinion documents (majority, dissent, concurrence, etc.).
- Primary Key: `id` (BIGINT)
- Foreign Key: `cluster_id` → opinion_clusters
- Key fields: `type`, `plain_text`, `html`, `author_str`, `sha1`

#### 5. **citation_map**
Citation relationships between opinions.
- Primary Key: `id` (BIGINT)
- Foreign Keys: `cited_opinion_id` → opinions, `citing_opinion_id` → opinions
- Key fields: `depth` (citation depth)

## Relationships

```
courts
  ↓
dockets
  ↓
opinion_clusters
  ↓
opinions ←→ citation_map (self-referential)
```

## Setup Instructions

### 1. Install Dependencies

```bash
uv pip install psycopg2-binary
```

### 2. Create Database and Schema

```bash
python create_schema.py
```

This will:
- Create the `courtlistener` database (if it doesn't exist)
- Create all tables with proper foreign keys
- Create indexes for common queries

### 3. Import Data

```bash
python import_data.py
```

This will import all CSV files in order:
1. courts (no dependencies)
2. dockets (depends on courts)
3. opinion_clusters (depends on dockets)
4. opinions (depends on opinion_clusters)
5. citation_map (depends on opinions)

**Note**: Import may take several hours due to the large data size (especially opinions.csv at ~267GB).

## Data Statistics

- **courts**: ~600 rows
- **dockets**: ~27GB (~millions of rows)
- **opinion_clusters**: ~12GB (~millions of rows)
- **opinions**: ~267GB (~millions of rows)
- **citation_map**: ~1GB (~millions of rows)

## Common Queries

### Get all opinions for a case
```sql
SELECT o.*, oc.case_name, oc.date_filed
FROM opinions o
JOIN opinion_clusters oc ON o.cluster_id = oc.id
JOIN dockets d ON oc.docket_id = d.id
WHERE d.docket_number = '12-92114';
```

### Find citations between opinions
```sql
SELECT 
    citing.id as citing_opinion_id,
    cited.id as cited_opinion_id,
    cm.depth
FROM citation_map cm
JOIN opinions citing ON cm.citing_opinion_id = citing.id
JOIN opinions cited ON cm.cited_opinion_id = cited.id
WHERE citing.id = 3447253;
```

### Search by court and date range
```sql
SELECT d.case_name, oc.date_filed, c.short_name
FROM dockets d
JOIN courts c ON d.court_id = c.id
JOIN opinion_clusters oc ON oc.docket_id = d.id
WHERE c.id = 'scotus'
  AND oc.date_filed BETWEEN '2020-01-01' AND '2024-12-31'
ORDER BY oc.date_filed DESC;
```

## Schema Source

Schema reverse-engineered from:
- CourtListener Django models (cl/search/models.py)
- CSV headers from bulk data files
- CourtListener documentation

## Connection Details

- **Host**: <POSTGRES_HOST>
- **Port**: 5432 (SSL required)
- **Database**: courtlistener
- **User**: postgres

## Notes

- All text fields support full Unicode
- Timestamps are stored in UTC
- Boolean fields use PostgreSQL's native BOOLEAN type
- Foreign keys enforce referential integrity
- Indexes created on commonly queried fields
