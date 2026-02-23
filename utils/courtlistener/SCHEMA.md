# CourtListener PostgreSQL Schema - Complete Setup

## 📋 Summary

I've reverse-engineered the CourtListener database schema from the Django models in `the CourtListener source code` and created a complete PostgreSQL setup for your downloaded bulk data.

## 🗂️ What's Included

### Documentation
- **QUICKSTART.md** - Get started in 5 minutes
- **README.md** - Complete documentation with examples
- **SCHEMA.md** - This file

### Scripts
1. **create_schema.py** - Creates database and all tables with proper relationships
2. **import_data.py** - Imports all CSV files in correct order
3. **verify_schema.py** - Verifies database connection and shows table stats
4. **sample_queries.py** - Demonstrates useful queries

## 📊 Database Schema

### Tables Created

```
courts (600+ rows)
├── id (PK)
├── short_name, full_name
├── jurisdiction
└── citation_string

dockets (~27GB)
├── id (PK)
├── court_id (FK → courts)
├── case_name, docket_number
├── date_filed, date_terminated
└── assigned_to_str, referred_to_str

opinion_clusters (~12GB)
├── id (PK)
├── docket_id (FK → dockets)
├── case_name, date_filed
├── precedential_status
└── judges, attorneys

opinions (~267GB)
├── id (PK)
├── cluster_id (FK → opinion_clusters)
├── type (lead, dissent, concurrence, etc.)
├── plain_text, html
├── author_str, per_curiam
└── sha1, page_count

citation_map (~1GB)
├── id (PK)
├── citing_opinion_id (FK → opinions)
├── cited_opinion_id (FK → opinions)
└── depth
```

## 🚀 Quick Start

```bash
# 1. Create the schema
python create_schema.py

# 2. Verify it worked
python verify_schema.py

# 3. Import data (takes hours!)
python import_data.py

# 4. Run sample queries
python sample_queries.py
```

## 🔍 Key Features

### Foreign Key Relationships
- All tables properly linked with foreign keys
- Referential integrity enforced
- Cascading deletes where appropriate

### Indexes Created
- Primary keys on all tables
- Foreign key indexes for joins
- Date indexes for temporal queries
- Text indexes for case name searches
- SHA1 index for document deduplication

### Data Types
- Proper PostgreSQL types (BIGINT, VARCHAR, TEXT, DATE, TIMESTAMP, BOOLEAN)
- NULL handling for optional fields
- Text fields support full Unicode

## 📈 Data Volume

| Table | Size | Estimated Rows |
|-------|------|----------------|
| courts | 764 KB | ~600 |
| dockets | 27 GB | ~10M |
| opinion_clusters | 12 GB | ~5M |
| opinions | 267 GB | ~15M |
| citation_map | 1 GB | ~50M |

## 🔗 Connection Info

```python
import psycopg2

conn = psycopg2.connect(
    host='<POSTGRES_HOST>',
    port=5432,
    database='courtlistener',
    user='postgres',
    sslmode='require'
)
```

## 💡 Example Queries

### Find Supreme Court cases
```sql
SELECT d.case_name, oc.date_filed
FROM dockets d
JOIN courts c ON d.court_id = c.id
JOIN opinion_clusters oc ON oc.docket_id = d.id
WHERE c.id = 'scotus'
ORDER BY oc.date_filed DESC;
```

### Get full opinion text
```sql
SELECT o.plain_text, o.html
FROM opinions o
JOIN opinion_clusters oc ON o.cluster_id = oc.id
WHERE oc.case_name ILIKE '%Brown v. Board%';
```

### Find citation network
```sql
SELECT 
    citing_oc.case_name as citing_case,
    cited_oc.case_name as cited_case,
    cm.depth
FROM citation_map cm
JOIN opinions citing_o ON cm.citing_opinion_id = citing_o.id
JOIN opinions cited_o ON cm.cited_opinion_id = cited_o.id
JOIN opinion_clusters citing_oc ON citing_o.cluster_id = citing_oc.id
JOIN opinion_clusters cited_oc ON cited_o.cluster_id = cited_oc.id
WHERE citing_oc.case_name ILIKE '%Roe v. Wade%';
```

## ⚠️ Important Notes

1. **Import Time**: The opinions.csv file is 267GB - expect several hours for import
2. **Disk Space**: Ensure you have at least 400GB free (data + indexes)
3. **Memory**: PostgreSQL will benefit from increased shared_buffers for large imports
4. **SSL**: Connection requires SSL (sslmode='require')

## 🛠️ Troubleshooting

### Connection fails
```bash
# Test connection
psql "host=<POSTGRES_HOST> port=5432 dbname=courtlistener user=postgres sslmode=require"
```

### Import is slow
```sql
-- Temporarily disable indexes during import
DROP INDEX idx_opinions_cluster;
-- ... import data ...
CREATE INDEX idx_opinions_cluster ON opinions(cluster_id);
```

### Out of memory
```python
# Use smaller batch sizes in import_data.py
import_csv(conn, table_name, csv_file, batch_size=1000)
```

## 📚 Source Information

Schema derived from:
- CourtListener Django models: `the CourtListener source code/cl/search/models.py`
- CSV headers from bulk data files
- CourtListener API documentation

## ✅ Next Steps

1. Run `create_schema.py` to set up the database
2. Run `verify_schema.py` to confirm success
3. Run `import_data.py` to load the data
4. Explore with `sample_queries.py`
5. Build your own queries!

---

**Created**: 2026-02-17  
**Data Date**: 2024-12-31  
**Source**: CourtListener Bulk Data (s3://com-courtlistener-storage/bulk-data)
