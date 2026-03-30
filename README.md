# reviewAnalyzer
Searches, highlights and analyzes reviews for an app online.

## Local run
1. Create and activate a virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file in the repo root and add:
   ```bash
   OPENAI_API_KEY=...
   REDDIT_CLIENT_ID=...
   REDDIT_CLIENT_SECRET=...
   REDDIT_USER_AGENT=reviewAnalyzer/0.1 by u/<reddit_username>
   ```
4. Initialize the SQLite database:
   ```bash
   python scripts/init_db.py
   ```
5. Start Streamlit:
   ```bash
   streamlit run app/ui/streamlit_app.py
   ```

## Streamlit Community Cloud deploy
1. Push this repo to GitHub.
2. In Streamlit Community Cloud, create a new app from this repo.
3. Set **Main file path** to `app/ui/streamlit_app.py`.
4. Optionally keep the default app theme/server behavior from `.streamlit/config.toml`.
5. Add required secrets (see below), then deploy.

## Secrets setup
### Local `.env`
Use a local `.env` file for development. Required variables:
- `OPENAI_API_KEY`
- `REDDIT_CLIENT_ID`
- `REDDIT_CLIENT_SECRET`
- `REDDIT_USER_AGENT`

Optional logging settings:
- `LOG_LEVEL` (default `INFO`)
- `LOG_FILE_PATH` (default `logs/review_analyzer.log`)

### Streamlit Cloud secrets
In Streamlit Cloud: **App settings → Secrets**, add TOML values:

```toml
OPENAI_API_KEY = "..."
REDDIT_CLIENT_ID = "..."
REDDIT_CLIENT_SECRET = "..."
REDDIT_USER_AGENT = "reviewAnalyzer/0.1 by u/<reddit_username>"
```

## Ingestion/enrichment runbook
### 1) Create Reddit API credentials
1. Sign in to Reddit and open: `https://www.reddit.com/prefs/apps`.
2. Click **create another app**.
3. Pick **script** type.
4. Save and copy:
   - client ID (under app name)
   - client secret
5. Build a user-agent string with app name/version and your Reddit username.

### 2) Run ingestion (Reddit refresh)
```bash
python -m app.jobs.refresh_reddit
```
What it does:
- fetches the last 30 days from configured subreddits
- applies keyword matching
- ingests posts/comments into `documents`
- stores run metadata in `ingestion_runs`

### 3) Run enrichment
```bash
python -m app.jobs.enrich_new_docs
```
What it does:
- selects docs without enrichment
- enriches in batches with OpenAI
- writes enrichment rows to `enrichments`
- stores run metadata in `enrichment_runs`

### 4) Review admin status
In the Streamlit **Admin** page:
- run ingestion/enrichment jobs manually
- inspect latest ingestion and enrichment run tables with status/errors

### 5) Recommended refresh cadence
- Manual refresh is recommended **1–2 times per week**.
- Increase to daily during launches/incidents.

## Troubleshooting
### API/auth errors
- `401`/auth failures: verify all required secrets are present and not wrapped in stray quotes.
- Reddit auth errors: verify `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, and `REDDIT_USER_AGENT`.
- OpenAI auth errors: verify `OPENAI_API_KEY` and account/project access.

### Rate limits / transient failures
- Reddit or OpenAI `429`/timeouts: retry later; enrichment retries transient errors automatically.
- Reduce enrichment pressure by lowering `ENRICHMENT_BATCH_SIZE` and/or `ENRICHMENT_MAX_DOCS_PER_RUN`.

### Empty or low ingestion volume
- Check `REDDIT_SUBREDDITS` / `REDDIT_KEYWORDS` values.
- Ensure target communities have recent activity in the last 30 days.

### Useful enrichment environment variables
- `ENRICHMENT_MAX_DOCS_PER_RUN` (default `100`)
- `ENRICHMENT_BATCH_SIZE` (default `3`)
- `ENRICHMENT_MAX_TEXT_CHARS` (default `3500`)
- `ENRICHMENT_MIN_TEXT_CHARS` (default `20`)
- `ENRICHMENT_MAX_RETRIES` (default `3`)
- `ENRICHMENT_MODEL` (default `gpt-4.1-mini`)
