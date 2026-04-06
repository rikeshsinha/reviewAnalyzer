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

## 8) Web Reviews ingestion

Use this section when you want to ingest editorial/product-review articles from the open web (`web_reviews` platform).

### Supported sites list

Default starter sites in `app/config/source_config.yaml`:

- `trustpilot.com`
- `g2.com`
- `cnet.com`
- `pcmag.com`
- `theverge.com`
- `androidauthority.com`
- `tomsguide.com`
- `techradar.com`
- `amazon.com`
- `bestbuy.com`

These defaults are examples, not a guaranteed-coverage list. Keep your own curated list based on crawlability and policy constraints.

### Refresh command

CLI refresh:

```bash
python -m app.jobs.refresh_web_reviews
```

Explicit date window (inclusive, UTC day boundaries):

```bash
python -m app.jobs.refresh_web_reviews --date-from 2026-03-01 --date-to 2026-03-07
```

Behavior:

- Uses merged source config (`app/config/source_config.yaml` + `data/runtime_source_config.yaml` if present).
- Requires `web_reviews.enabled: true` and a non-empty `sites` list.
- Crawls homepage/category pages, discovers likely editorial URLs, fetches article HTML, normalizes text, and inserts deduplicated docs.
- Logs run metrics to `ingestion_runs` (`records_fetched`, `records_inserted`, status/error details).

### Admin config workflow (recommended for new users)

1. Start the app:
   ```bash
   streamlit run streamlit_app.py
   ```
2. Open **Admin** page.
3. In **Source configuration**:
   - Edit **Web review sites** (one domain per line).
   - Optionally edit **Web review keywords** (used when keyword prioritization is enabled).
   - Tune **Max pages per site** and **Min content length (chars)**.
   - Click **Save config** (writes `data/runtime_source_config.yaml`).
4. In `app/config/source_config.yaml`, make sure `platforms.web_reviews.enabled: true` (runtime editor currently manages lists/tunables; enable flag is set in base config).
5. In **Web ingestion date range**, select start/end dates.
6. Click **Refresh Web Reviews**.
7. Verify status in **Recent ingestion runs** and **Ingestion run metrics by platform**.

### Date range usage

- CLI supports `--date-from` and `--date-to` in `YYYY-MM-DD`, inclusive.
- Admin **Web ingestion date range** passes the same arguments to the job.
- If no explicit dates are provided, the job falls back to `days_back` for the configured platform.
- Date filtering is applied against normalized `created_at`/published timestamps; records outside the window are skipped.

### Robots compliance behavior

`WebReviewsClient` is intentionally conservative:

- Reads and checks `robots.txt` per site before fetch (`can_fetch`).
- Skips URLs disallowed by robots rules.
- Enforces request pacing (default delay: 1.5s between requests).
- Uses a dedicated crawler user-agent (`reviewAnalyzer/0.1 (editorial-web-crawler)`).

If `robots.txt` cannot be read, crawling proceeds (warning logged), so you should still validate site policies manually before production runs.

### Blocked site warnings

Pages are treated as blocked and skipped when:

- HTTP status is `403` or `429`, or
- response body contains anti-bot markers (for example captcha/access-denied/cloudflare indicators).

The crawler logs warnings and continues processing other pages/sites, so partial success is normal when a subset of targets blocks automated requests.

### Caveats and operational safety expectations

- **Site structure drift:** HTML layouts evolve; extractor accuracy can degrade without code changes.
- **Extraction fragility:** Metadata/content heuristics can miss author/date/body on non-standard templates; tune `min_content_chars`, keywords, and site list accordingly.
- **Legal/ToS compliance:** You are responsible for honoring each site's Terms of Service, robots directives, and jurisdictional requirements. Do not use this pipeline to bypass access controls, login walls, or explicit crawl prohibitions.

### Example workflow (end-to-end)

1. Refresh web reviews:
   ```bash
   python -m app.jobs.refresh_web_reviews --date-from 2026-03-01 --date-to 2026-03-07
   ```
2. Enrich new docs:
   ```bash
   python -m app.jobs.enrich_new_docs
   ```
3. Open dashboard:
   ```bash
   streamlit run streamlit_app.py
   ```
4. In Dashboard/Insights, filter date range and query themes/complaints/feature requests from combined sources.

---

## 9) Run UI

Start Streamlit from repo root:

```bash
streamlit run streamlit_app.py
```

(Equivalent launcher exists at `app/ui/streamlit_app.py` if needed.)

Quick import smoke test for startup-module wiring:

```bash
python -c "from app.ui.streamlit_app import main; print('ok')"
```

Date-range defaults in the UI:

- **Global sidebar Date range** defaults to the **last 30 days** (`today - 30 days` to `today`) and is fully user-editable without DB min/max clamping.
- **Admin → Ingestion date range** also defaults to the **last 30 days**, remains independent from global analysis filters, and is passed to `refresh_reddit` via `REDDIT_INGEST_DATE_FROM` / `REDDIT_INGEST_DATE_TO` for that run.

---

## 10) Example Samsung Health workflow

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

## 11) Deployment notes (Streamlit Cloud)

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

## 12) Troubleshooting

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

## 13) Pushshift caveats

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
