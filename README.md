# reviewAnalyzer
Searches, highlights and analyzes reviews for an app online

## Quickstart
1. Create and activate a virtual environment.
2. Install dependencies: `pip install -r requirements.txt`.
3. Copy `.env.example` to `.env` and fill in values.
4. Initialize the database: `python scripts/init_db.py`.

## Reddit refresh job

Run the 30-day Reddit ingestion job:

```bash
python -m app.jobs.refresh_reddit
```


## Enrichment job

Run incremental enrichment for documents that do not yet have enrichment rows:

```bash
python -m app.jobs.enrich_new_docs
```

Useful environment variables:

- `ENRICHMENT_MAX_DOCS_PER_RUN` (default `100`)
- `ENRICHMENT_BATCH_SIZE` (default `3`)
- `ENRICHMENT_MAX_TEXT_CHARS` (default `3500`)
- `ENRICHMENT_MIN_TEXT_CHARS` (default `20`)
- `ENRICHMENT_MAX_RETRIES` (default `3`)
- `ENRICHMENT_MODEL` (default `gpt-4.1-mini`)
