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
