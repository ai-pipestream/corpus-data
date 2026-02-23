# CourtListener Data Import - Complete Instructions

## What We Have

- PostgreSQL container: `courtlistener_db` running on NAS
- Database: `courtlistener` 
- User: `cluser`
- Password: `<POSTGRES_PASSWORD from .env>`
- Data location: `storage/courtlistener/bulk/`
- Data mounted in container at: `/data/bulk`

## Schema Status

✓ Database created
✓ 5 tables created (courts, dockets, opinion_clusters, opinions, citation_map)
✓ All tables empty, ready for import

## Import Method

Use psql inside the container - it's already there, has direct access to data, no network overhead.

## Step-by-Step Instructions

### 1. Copy import script to NAS
```bash
scp utils/courtlistener/import_in_container.sh <NAS_HOST>:/tmp/
ssh nas "chmod +x /tmp/import_in_container.sh"
```

### 2. Run import in background
```bash
ssh nas "docker exec courtlistener_db bash /tmp/import_in_container.sh > /tmp/import.log 2>&1 &"
```

### 3. Watch progress
```bash
ssh nas "tail -f /tmp/import.log"
```

### 4. Check if still running
```bash
ssh nas "docker exec courtlistener_db ps aux | grep psql"
```

## Import Order (respects foreign keys)

1. courts (764 KB) - ~1 second
2. dockets (27 GB) - ~30-60 minutes
3. opinion_clusters (12 GB) - ~15-30 minutes  
4. opinions (267 GB) - ~4-6 hours
5. citation_map (1 GB) - ~5-10 minutes

**Total time: ~5-7 hours**

## After Import

Verify:
```bash
cd storage/courtlistener/
uv run verify_schema.py
```

Run sample queries:
```bash
uv run sample_queries.py
```

## Files Reference

- `create_schema.py` - Creates tables (ALREADY RUN)
- `import_in_container.sh` - Import script for container
- `verify_schema.py` - Check table counts
- `sample_queries.py` - Example queries
- `README.md` - Full documentation
- `SCHEMA.md` - Schema details

## Connection Info

From local machine:
```python
psycopg2.connect(
    host='<POSTGRES_HOST from config.ini>',
    port=5432,
    database='courtlistener',
    user='cluser',
    password='<POSTGRES_PASSWORD from .env>',
    sslmode='require'
)
```

From NAS:
```bash
docker exec -it courtlistener_db psql -U cluser -d courtlistener
```
