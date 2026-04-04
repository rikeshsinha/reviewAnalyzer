# reviewAnalyzer

A Streamlit + SQLite application for ingesting Reddit product feedback, enriching it with LLM analysis, and querying insights for product teams. This guide is written so a new developer can copy/paste commands and run the Pushshift-backed Reddit ingestion flow end to end.

## 1) Project overview

`reviewAnalyzer` helps you:

- ingest Reddit posts/comments that match your target subreddits and keywords,
- normalize and store them in SQLite (`documents` + run metadata tables),
- enrich new records with OpenAI-powered sentiment/topic/feature-request analysis,
- explore and query results in a Streamlit UI.

### Samsung Health / Galaxy Watch focus

The default source config is already geared toward Samsung Health and Galaxy Watch feedback (for example communities like `GalaxyWatch` and keywords like `Samsung Health`, `Galaxy Watch`, `sleep`, and `workout`). This makes it straightforward to track wearable pain points and feature demand in one pipeline.

---

## 2) Architecture summary

High-level flow:

1. **Ingestion jobs** collect Reddit data (PRAW by default, or Pushshift/Public JSON/RSS in no-key environments).
2. **Normalization/storage** writes canonical document rows and raw payload snapshots into SQLite.
3. **Enrichment jobs** call OpenAI to generate structured analysis.
4. **Streamlit UI** provides search, dashboards, Q&A, and admin controls over the same DB.

Core stack:

- **UI:** Streamlit
- **Database:** SQLite (`data/app.db`)
- **Ingestion:** `app.jobs.refresh_reddit` / `app.jobs.refresh_sources`
- **Enrichment/LLM:** OpenAI API via `app.jobs.enrich_new_docs`
- **Reddit backend options:**
  - **PRAW** (official Reddit OAuth)
  - **Pushshift** (set `REDDIT_FETCH_BACKEND=pushshift`)
  - **Public JSON** (set `REDDIT_FETCH_BACKEND=public_json`)
  - **RSS/Atom** (set `REDDIT_FETCH_BACKEND=rss`)

---

## 3) Installation

From the repo root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> On Windows PowerShell, activate with: `\.venv\Scripts\Activate.ps1`

---

## 4) Environment setup

Create a `.env` file in the project root.

### Required

```bash
OPENAI_API_KEY=your_openai_key
```

### Reddit OAuth vars (used by PRAW backend and kept as fallback)

```bash
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USERNAME=your_reddit_username
REDDIT_PASSWORD=your_reddit_password
REDDIT_USER_AGENT=reviewAnalyzer/0.1 by u/your_reddit_username
```

### Pushshift vars (optional)

```bash
REDDIT_FETCH_BACKEND=public_json
PUSHSHIFT_BASE_URL=https://api.pushshift.io/reddit/search/submission/
PUSHSHIFT_PAGE_SIZE=100
PUSHSHIFT_MAX_PAGES=20
```


### Public Reddit JSON vars (no Reddit app keys required)

```bash
REDDIT_FETCH_BACKEND=public_json
PUBLIC_REDDIT_BASE_URL=https://www.reddit.com
PUBLIC_REDDIT_USER_AGENT=reviewAnalyzer/0.1 (public-json-ingestion)
PUBLIC_REDDIT_PAGE_SIZE=100
PUBLIC_REDDIT_MAX_PAGES=5
PUBLIC_REDDIT_DELAY_SECONDS=1.0
```

### Reddit RSS vars (no Reddit app keys required)

```bash
REDDIT_FETCH_BACKEND=rss
REDDIT_RSS_MAX_PAGES=3
REDDIT_RSS_DELAY_SECONDS=1.0
```

### Optional list overrides

If your environment/launcher supports source-list overrides, set:

```bash
REDDIT_SUBREDDITS=["GalaxyWatch","samsung","wearables"]
REDDIT_KEYWORDS=["Samsung Health","Galaxy Watch","sleep tracking"]
```

If you are not using env list overrides, edit `app/config/source_config.yaml` (or runtime overrides in `data/runtime_source_config.yaml`) for communities/keywords.

---

## 5) Database initialization

Initialize schema before first run:

```bash
python scripts/init_db.py
```

This creates required tables such as `sources`, `documents`, ingestion/enrichment run metadata, and related indexes.

---

## 6) Ingestion (Reddit backends + failover)

Run Reddit ingestion job (works for `public_json`, `pushshift`, and adapter-backed modes like `praw`):

```bash
python -m app.jobs.refresh_reddit
```

Optional one-off date override (used by Admin refresh button and can also be set for CLI runs):

```bash
REDDIT_INGEST_DATE_FROM=2026-03-01 REDDIT_INGEST_DATE_TO=2026-03-07 python -m app.jobs.refresh_reddit
```

What it does in non-PRAW modes (`public_json` / `pushshift`):

- Reads configured subreddit list + keyword list from source config.
- Resolves a UTC date window from `REDDIT_INGEST_DATE_FROM`/`REDDIT_INGEST_DATE_TO` when both are set; otherwise falls back to `days_back` (e.g., last 30 days).
- Queries each backend by `(subreddit × keyword)` combinations.
- Normalizes each record into internal document shape.
- Deduplicates by external ID and DB dedupe keys.
- Preserves raw source payload in `documents.raw_json` (`raw_payload`) for traceability/debugging.

Behavior notes:

- If keywords are empty, ingestion still runs with subreddit-only queries.
- Failover chains are deterministic:
  - `pushshift -> public_json`
  - `public_json -> rss`
- Any other backend value (for example `praw`) uses the configured adapter path.
- In `public_json` mode, each `(subreddit, keyword)` pair is handled independently; if one pair returns a 403/error, that pair is skipped and the batch continues with remaining pairs.
- If all attempted backends fail or return zero docs, ingestion run status is marked `failed` with details in `ingestion_runs.error_message`.
- Results are inserted with `INSERT OR IGNORE`, so reruns do not duplicate existing docs.
- Admin page date override only affects that specific **Refresh Reddit ingestion** job execution; it does **not** change global/sidebar analysis filters.

Quick syntax regression check for critical ingestion/retrieval modules:

```bash
python -m py_compile app/jobs/refresh_reddit.py app/ingestion/public_reddit_client.py app/services/retrieval_service.py
```

---

## 7) Enrichment

After ingestion, enrich newly ingested docs:

```bash
python -m app.jobs.enrich_new_docs
```

This job selects unenriched docs, calls OpenAI, and writes structured outputs for sentiment/themes/feature insights.

---

## 8) Run UI

Start Streamlit from repo root:

```bash
streamlit run streamlit_app.py
```

(Equivalent launcher exists at `app/ui/streamlit_app.py` if needed.)

---

## 9) Example Samsung Health workflow

1. Set backend and credentials in `.env`:
   - `REDDIT_FETCH_BACKEND=pushshift`
   - `OPENAI_API_KEY=...`
   - Reddit OAuth vars (recommended fallback path)
2. Configure target communities/keywords for Samsung Health + Galaxy Watch.
3. Initialize DB:
   ```bash
   python scripts/init_db.py
   ```
4. Ingest Reddit feedback:
   ```bash
   python -m app.jobs.refresh_reddit
   ```
5. Enrich new docs:
   ```bash
   python -m app.jobs.enrich_new_docs
   ```
6. Launch UI:
   ```bash
   streamlit run streamlit_app.py
   ```
7. In the app, query for themes like battery drain, sleep tracking accuracy, workout sync gaps, and feature requests.

---

## 10) Deployment notes (Streamlit Cloud)

- **Main file path:** `streamlit_app.py`
- **Required secrets:**
  - `OPENAI_API_KEY`
  - `REDDIT_CLIENT_ID`
  - `REDDIT_CLIENT_SECRET`
  - `REDDIT_USERNAME` (optional)
  - `REDDIT_PASSWORD` (optional)
  - `REDDIT_USER_AGENT`
  - `REDDIT_FETCH_BACKEND` (set to `pushshift` if desired)
  - `PUSHSHIFT_BASE_URL` (if using Pushshift)
  - `PUSHSHIFT_PAGE_SIZE` / `PUSHSHIFT_MAX_PAGES` (if your deployment uses these tunables)

Example secrets TOML:

```toml
OPENAI_API_KEY = "..."
REDDIT_CLIENT_ID = "..."
REDDIT_CLIENT_SECRET = "..."
REDDIT_USERNAME = "..."
REDDIT_PASSWORD = "..."
REDDIT_USER_AGENT = "reviewAnalyzer/0.1 by u/your_reddit_username"
REDDIT_FETCH_BACKEND = "pushshift"
PUSHSHIFT_BASE_URL = "https://api.pushshift.io/reddit/search/submission/"
PUSHSHIFT_PAGE_SIZE = "100"
PUSHSHIFT_MAX_PAGES = "20"
```

---

## 11) Troubleshooting

### A) Missing module / import path errors

- Always run commands from repo root.
- Ensure venv is activated and dependencies are installed:
  ```bash
  pip install -r requirements.txt
  ```

### B) Missing DB tables / schema issues

- Initialize (or reinitialize) schema:
  ```bash
  python scripts/init_db.py
  ```

### C) Zero ingestion results

- Verify subreddit + keyword config actually has recent activity.
- Increase `days_back` in source config.
- Temporarily broaden keywords.
- Confirm Pushshift endpoint is reachable and returning data.

### D) Auth/key failures

- `OPENAI_API_KEY` missing/invalid → enrichment fails.
- Reddit OAuth vars missing/invalid → PRAW fallback fails.
- Fix `.env`, then rerun ingestion/enrichment jobs.

---

## 12) Pushshift caveats

When running with Pushshift:

- **Coverage can vary** by subreddit/time period.
- **Endpoints/schemas can change** unexpectedly.
- **Rate limits and ToS still apply** — use conservative polling and comply with platform terms.
- Keep **PRAW fallback configured** so ingestion can continue when Pushshift quality/availability dips.

---

## Quick start (copy/paste)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cat > .env <<'ENVVARS'
OPENAI_API_KEY=your_openai_key
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USERNAME=your_reddit_username
REDDIT_PASSWORD=your_reddit_password
REDDIT_USER_AGENT=reviewAnalyzer/0.1 by u/your_reddit_username
REDDIT_FETCH_BACKEND=public_json
PUSHSHIFT_BASE_URL=https://api.pushshift.io/reddit/search/submission/
PUSHSHIFT_PAGE_SIZE=100
PUSHSHIFT_MAX_PAGES=20
ENVVARS

python scripts/init_db.py
python -m app.jobs.refresh_reddit
python -m app.jobs.enrich_new_docs
streamlit run streamlit_app.py
```
