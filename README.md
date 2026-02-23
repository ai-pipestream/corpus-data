# Corpus Data Utilities

Download and staging scripts for building a diverse 3-5 TB text corpus across 17 data sources. Designed for AI search indexing pipelines.

## Quick Start

```bash
# 1. Clone
git clone https://github.com/ai-pipestream/corpus-data.git
cd corpus-data

# 2. Configure paths
cp config.sample.ini config.ini
# Edit config.ini — set storage_dir and log_dir for your environment

# 3. Add secrets
cat > .env <<'EOF'
USPTO_API_KEY=your_key_here
HF_TOKEN=your_token_here
EDGAR_IDENTITY=YourName your@email.com
S2_API_KEY=your_key_here
POSTGRES_PASSWORD=your_password_here
COURTLISTENER_TOKEN=your_token_here
EOF

# 4. Install Python deps (most scripts only need stdlib)
pip install huggingface_hub pyarrow requests beautifulsoup4 psycopg2-binary

# 5. Run a downloader
python utils/stackexchange/stage.py --list
python utils/stackexchange/stage.py --limit 5
```

## Configuration

All scripts share a single config system via `utils/config.py`:

| File | Purpose | Tracked |
|------|---------|---------|
| `config.sample.ini` | Example config showing all options | Yes |
| `config.ini` | Your environment-specific paths | No (gitignored) |
| `.env` | API keys, passwords, tokens | No (gitignored) |

If `config.ini` doesn't exist, scripts default to `./storage/` and `./logs/` relative to the repo root.

### Environment Variables (.env)

| Variable | Required By | How to Get |
|----------|-------------|------------|
| `USPTO_API_KEY` | USPTO | https://data.uspto.gov/myodp/landing |
| `HF_TOKEN` | FineWeb, Docling | https://huggingface.co/settings/tokens |
| `EDGAR_IDENTITY` | EDGAR | Any "Name email" string (SEC requirement) |
| `S2_API_KEY` | Semantic Scholar | https://www.semanticscholar.org/product/api |
| `COURTLISTENER_TOKEN` | CourtListener API | https://www.courtlistener.com/help/api/ |
| `POSTGRES_PASSWORD` | CourtListener DB import | Your PostgreSQL password |

## Data Sources

### Public S3 Buckets

| Source | Bucket | Size | Format | License |
|--------|--------|------|--------|---------|
| OpenAlex | `s3://openalex` | ~300 GB | jsonl.gz | CC0 |
| CC-News | `s3://commoncrawl` | ~500 GB | warc.gz | Common Crawl ToU |
| CourtListener | `s3://com-courtlistener-storage` | ~50 GB | csv.bz2 | Public Domain |

### Download Sources

| Source | Script | Size | Format | License |
|--------|--------|------|--------|---------|
| USPTO Patents | `utils/uspto/stage.py` | ~266 GB | XML | US Gov Public Domain |
| SEC EDGAR | `utils/edgar/stage.py` | ~400 GB | HTML/XML/XBRL | US Gov Public Domain |
| PubMed Central | `utils/pubmed_central/stage.py` | ~300 GB | JATS XML | Mixed CC |
| PubMed Abstracts | `utils/pubmed_abstracts/stage.py` | ~100 GB | XML.gz | NLM ToU |
| StackExchange | `utils/stackexchange/stage.py` | ~250 GB | 7z (XML) | CC-BY-SA 4.0 |
| Semantic Scholar | `utils/semantic_scholar/stage.py` | ~300 GB | jsonl.gz | ODC-By 1.0 |
| OpenAlex | `utils/openalex/stage.py` | ~300 GB | jsonl.gz | CC0 |
| FineWeb | `utils/fineweb/stage.py` | ~500 GB | Parquet | ODC-By 1.0 |
| GovInfo | `utils/govinfo/stage.py` | ~75 GB | XML | US Gov Public Domain |
| EUR-Lex | `utils/eurlex/stage.py` | ~15 GB | HTML/XML | EU Decision 2011/833 |
| CC-News | `utils/ccnews/stage.py` | ~500 GB | WARC.gz | Common Crawl ToU |
| Docling | `utils/docling/stage.py` | ~30 GB | Parquet (PDFs) | CDLA-Permissive 1.0 |
| Wikipedia | `utils/wikipedia/stage.py` | ~20 GB | HF Dataset | CC-BY-SA 3.0 |
| ArXiv | `utils/arxiv/stage.py` | ~10 GB | JSONL | arXiv ToU |
| Gutenberg | `utils/gutenberg/stage.py` | ~5 GB | JSONL | Public Domain |
| NY Courts | `utils/nycourts/stage.py` | varies | HTML/PDF | Public Domain |
| CourtListener | `utils/courtlistener/` | ~50 GB | CSV | Public Domain |

## Project Structure

```
corpus-data/
  config.sample.ini      # Example configuration
  s3_buckets.txt          # Public S3 buckets for mirror/cache setup
  utils/
    config.py             # Shared config loader (all scripts import this)
    arxiv/stage.py
    ccnews/stage.py
    courtlistener/        # Download, schema, and import pipeline
    docling/stage.py
    edgar/stage.py
    eurlex/stage.py
    fineweb/stage.py
    govinfo/stage.py
    gutenberg/stage.py
    nycourts/stage.py
    openalex/stage.py
    pubmed_abstracts/stage.py
    pubmed_central/stage.py
    semantic_scholar/stage.py
    stackexchange/stage.py
    uspto/stage.py
    wikipedia/stage.py
  storage/                # Downloaded data (gitignored)
  logs/                   # Execution logs (gitignored)
```

## Script Conventions

Every `stage.py` follows the same pattern:

- **`--list`** or **`--list-*`**: Show what's available without downloading
- **`--limit N`**: Download only N items (for testing)
- **`--dry-run`**: Show what would happen
- All paths come from `config.ini` (or defaults to `./storage/<source>/`)
- All secrets come from `.env`
- Logs go to `logs/<source>_stage.log`
- Downloads are resumable (existing files are skipped by size check)

## License

MIT
