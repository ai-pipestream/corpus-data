# AI Pipestream - Corpus Data Utilities

A collection of lightweight, distinct data-grabbing utilities designed to stage large-scale datasets (3-5 TB target) for AI research and search indexing.

## Philosophy
- **Simple**: Focused on data acquisition and staging.
- **Independent**: Each utility in `utils/` is self-contained.
- **Storage-Separated**: Logic lives in `utils/`, data lives in `storage/`.
- **S3-Ready**: Sources on public S3 buckets are cataloged in `s3_sources.yml` for the s3-connector to crawl directly. Non-S3 sources download to `/storage` on NAS.

## Structure
- `/storage`: Large-scale data files (CSVs, Arrow, JSONL, PDFs). *Not tracked in git.*
- `/utils`: Data grabbing, staging, and updating scripts.
- `/logs`: Centralized execution logs. *Not tracked in git.*
- `s3_sources.yml`: S3 bucket catalog and non-S3 source metadata.

## Supported Sources

### Existing (downloaded)
- **ArXiv**: Abstracts and articles via HuggingFace.
- **CourtListener**: Full US Federal and State caselaw corpus (S3).
- **NY Courts**: Targeted crawler for New York state opinions.
- **Gutenberg**: Public domain books.
- **Wikipedia**: Cleaned English Wikipedia corpus.

### S3 Sources (direct connector crawl)
- **OpenAlex**: 250M+ scholarly works, authors, citations (`s3://openalex`).
- **CC-News**: Worldwide news articles 2016-present (`s3://commoncrawl/crawl-data/CC-NEWS/`).

### NAS Download Sources
- **FineWeb**: Cleaned English web text, 15T tokens (HuggingFace, sample-100BT).
- **PubMed Central**: 4M+ full-text biomedical articles (FTP).
- **PubMed Abstracts**: 37M+ biomedical citation abstracts (FTP).
- **SEC EDGAR**: All US public company filings — 10-K, 10-Q, 8-K, etc.
- **USPTO Patents**: Full text of US patent grants 1976-present.
- **StackExchange**: 170+ Q&A sites including Stack Overflow.
- **GovInfo**: Federal Register, Congressional Record, CFR, Bills.
- **EUR-Lex**: EU legislation in 24 languages.
- **Semantic Scholar**: 225M+ academic papers with citation graphs.
- **Docling**: DocLayNet (80K annotated pages) + DP-Bench (200 PDFs).

## License
MIT
