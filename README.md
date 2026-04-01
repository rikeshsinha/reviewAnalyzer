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
3. Set **Main file path** to `streamlit_app.py`.
4. Optionally keep the default app theme/server behavior from `.streamlit/config.toml`.
5. Add required secrets (see below), then deploy.

### Database bootstrap & persistence notes
- Streamlit/other cloud containers can start with an empty `data/app.db` on first boot (or after a cold restart on ephemeral instances).
- The app now auto-bootstraps the SQLite schema during startup, so a missing/empty DB file is initialized automatically.
- If startup bootstrap is disabled in your deployment, use one of these fallbacks:
  - Run the Admin page refresh action to trigger initialization paths.
  - Or run `python scripts/init_db.py` as a pre-deploy (or release) step.
- Free-tier deployments commonly use ephemeral filesystem storage: DB files may be reset when the container is rebuilt/restarted unless you attach persistent storage.

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
python -m app.jobs.refresh_sources
```
What it does:
- fetches the last 30 days from configured subreddits
- applies keyword matching
- ingests posts/comments into `documents`
- stores run metadata in `ingestion_runs`

### 2a) Reddit ingestion backends: PRAW vs Pushshift
You can choose which Reddit fetch backend to use via environment variables.

Example `.env` settings:
```bash
REDDIT_FETCH_BACKEND=pushshift
PUSHSHIFT_BASE_URL=...
```

Run command (unchanged):
```bash
python -m app.jobs.refresh_reddit
```

Behavior note:
- After refresh completes, ingested Reddit data appears in the same database tables and Streamlit UI views immediately (no separate migration/sync step required).

Caveats:
- Pushshift availability and historical completeness may vary over time.
- Pushshift schema/endpoint behavior can change without notice.
- Always comply with platform terms of service and rate-limit guidance.

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
- edit Reddit list fields (communities/keywords) and click **Save config** to write runtime overrides in `data/runtime_source_config.yaml`

### 5) Source config refresh flow (exact)
1. Edit list fields in the **Admin** page.
2. Click **Save config** (writes runtime overrides to `data/runtime_source_config.yaml`).
3. Run refresh (`python -m app.jobs.refresh_sources` or Admin refresh button). Each refresh run merges:
   - base config: `app/config/source_config.yaml`
   - runtime overrides: `data/runtime_source_config.yaml` (if present)
   with list replacement + scalar override semantics.

### 6) Recommended refresh cadence
- Manual refresh is recommended **1–2 times per week**.
- Increase to daily during launches/incidents.
- For Google Play specifically, prefer **daily/weekly batch ingestion** over aggressive near-real-time polling.

### 7) Google Play connector caveats
- The Google Play connector may rely on unofficial endpoints/libraries and can break without notice if upstream behavior changes.
- Respect Google Play rate limits and avoid aggressive polling loops.
- If the Google Play connector fails during a multi-source refresh, the refresh continues for other enabled sources by default (unless `INGESTION_FAIL_FAST=true`), so the app remains available while the failed source is skipped for that run.
- Keep Reddit (or another stable source) enabled as a fallback ingestion source when possible.

## Demo checklist
1. **Seed DB**
   ```bash
   python scripts/init_db.py
   ```
2. **Run enrichment pipeline inputs**
   ```bash
   python -m app.jobs.refresh_sources
   python -m app.jobs.enrich_new_docs
   ```
3. **Run app**
   ```bash
   streamlit run app/ui/streamlit_app.py
   ```
4. **Sample demo questions**
   - "What are the top complaints this month for the selected subreddit?"
   - "Which features are most requested in the last 14 days?"
   - "Summarize negative sentiment themes and cite evidence links."
   - "What changed week-over-week in complaint volume?"

## Troubleshooting
### API/auth errors
- `401`/auth failures: verify all required secrets are present and not wrapped in stray quotes.
- Reddit auth errors: verify `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, and `REDDIT_USER_AGENT`.
- OpenAI auth errors: verify `OPENAI_API_KEY` and account/project access.

### Rate limits / transient failures
- Reddit or OpenAI `429`/timeouts: retry later; enrichment retries transient errors automatically.
- Reduce enrichment pressure by lowering `ENRICHMENT_BATCH_SIZE` and/or `ENRICHMENT_MAX_DOCS_PER_RUN`.
- Google Play throttling/timeouts: switch to daily/weekly batches, reduce per-run scope (`apps`, `countries`, and `max_reviews_per_app`), and avoid frequent refresh triggers.

### Empty or low ingestion volume
- Check `REDDIT_SUBREDDITS` / `REDDIT_KEYWORDS` values.
- Ensure target communities have recent activity in the last 30 days.

### Google Play connector failures
- Symptom: refresh logs show `google_play` run failures while other platforms continue.
- Expected fallback behavior: the failed source is skipped for that run, successful sources still ingest, and the app stays available with existing data.
- Quick disable (base config): edit `app/config/source_config.yaml` and set:
  ```yaml
  platforms:
    google_play:
      enabled: false
  ```
- Quick disable (runtime override via Admin flow): in `data/runtime_source_config.yaml`, set the same override:
  ```yaml
  platforms:
    google_play:
      enabled: false
  ```
  Then re-run refresh (`python -m app.jobs.refresh_sources`).

### Useful enrichment environment variables
- `ENRICHMENT_MAX_DOCS_PER_RUN` (default `100`)
- `ENRICHMENT_BATCH_SIZE` (default `3`)
- `ENRICHMENT_MAX_TEXT_CHARS` (default `3500`)
- `ENRICHMENT_MIN_TEXT_CHARS` (default `20`)
- `ENRICHMENT_MAX_RETRIES` (default `3`)
- `ENRICHMENT_MODEL` (default `gpt-4.1-mini`)
