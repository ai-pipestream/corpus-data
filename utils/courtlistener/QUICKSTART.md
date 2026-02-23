# CourtListener Schema Setup - Quick Start

## What You Have

You've downloaded CourtListener bulk data (2024-12-31) containing:
- **courts.csv** - 600+ court records
- **dockets.csv** - 27GB of case dockets
- **opinion-clusters.csv** - 12GB of opinion groupings
- **opinions.csv** - 267GB of actual opinion text
- **citation-map.csv** - 1GB of citation relationships

## Schema Overview

```
courts (court metadata)
  ↓ court_id
dockets (case filings)
  ↓ docket_id
opinion_clusters (decision groupings)
  ↓ cluster_id
opinions (individual opinions)
  ↔ citation_map (who cites whom)
```

## Quick Setup

### 1. Create the database and schema:
```bash
cd utils/courtlistener
python create_schema.py
```

### 2. Verify it worked:
```bash
python verify_schema.py
```

### 3. Import the data:
```bash
python import_data.py
```
⚠️ **Warning**: This will take several hours due to the 267GB opinions file.

## Key Files Created

- **create_schema.py** - Creates database and all tables
- **import_data.py** - Imports CSV files into PostgreSQL
- **verify_schema.py** - Checks database status
- **README.md** - Full documentation with example queries

## Database Connection

```python
import psycopg2

conn = psycopg2.connect(
    host='<POSTGRES_HOST from config.ini>',
    port=5432,
    database='courtlistener',
    user='postgres',
    password='<POSTGRES_PASSWORD from .env>',
    sslmode='require'
)
```

## Example Query

```python
# Find all Supreme Court cases from 2024
cur = conn.cursor()
cur.execute("""
    SELECT d.case_name, oc.date_filed
    FROM dockets d
    JOIN courts c ON d.court_id = c.id
    JOIN opinion_clusters oc ON oc.docket_id = d.id
    WHERE c.id = 'scotus'
      AND oc.date_filed >= '2024-01-01'
    ORDER BY oc.date_filed DESC
""")
for row in cur.fetchall():
    print(row)
```

## Next Steps

1. Run `create_schema.py` to set up the database
2. Run `verify_schema.py` to confirm it worked
3. Run `import_data.py` to load the data (be patient!)
4. Start querying!

## Schema Details

See **README.md** for:
- Complete table schemas
- All field descriptions
- More example queries
- Performance tips
