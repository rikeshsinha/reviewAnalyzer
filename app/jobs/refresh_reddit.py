"""Job entry point for running a single platform ingestion refresh."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from sqlalchemy import text

from app.db.repositories import IngestionRunRepository
from app.db.session import SessionLocal
from app.ingestion.normalizers import normalize_pushshift_submission
from app.ingestion.public_reddit_client import PublicRedditError
from app.ingestion.pushshift_client import PushshiftError, search_submissions
from app.ingestion.public_reddit_client import search_submissions as search_public_json_submissions
from app.ingestion.reddit_rss_client import RedditRssError
from app.ingestion.reddit_rss_client import search_submissions as search_rss_submissions
from app.ingestion.registry import get_adapter_class
from app.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


def _ensure_source(session: Any, platform: str) -> int:
    session.execute(
        text(
            """
            INSERT OR IGNORE INTO sources (platform, external_id, name, metadata_json)
            VALUES (:platform, :platform, :name, '{}')
            """
        ),
        {"platform": platform, "name": platform.replace("_", " ").title()},
    )
    row = session.execute(
        text("SELECT id FROM sources WHERE platform=:platform AND external_id=:platform LIMIT 1"),
        {"platform": platform},
    ).first()
    if row is None:
        raise RuntimeError(f"Unable to resolve source row for platform '{platform}'")
    return int(row.id)


def _ensure_reddit_source(session: Any) -> int:
    return _ensure_source(session, "reddit")


def _ensure_dedupe_constraints(session: Any) -> None:
    session.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_source_external "
            "ON documents(source_id, external_id)"
        )
    )
    session.execute(text("ALTER TABLE documents ADD COLUMN dedupe_key TEXT"))


def _safe_ensure_dedupe_constraints(session: Any) -> None:
    try:
        _ensure_dedupe_constraints(session)
    except Exception:
        session.rollback()
        session.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_source_external "
                "ON documents(source_id, external_id)"
            )
        )
    session.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_dedupe_key ON documents(dedupe_key)"))
    session.commit()


def _insert_documents(session: Any, source_id: int, docs: list[dict[str, Any]]) -> int:
    inserted = 0
    for doc in docs:
        row = {
            "source_id": source_id,
            "external_id": doc.get("external_id"),
            "title": doc.get("title"),
            "body": doc.get("content"),
            "author": doc.get("author"),
            "url": doc.get("url"),
            "published_at": doc.get("created_at"),
            "raw_json": json.dumps(
                {
                    "platform": doc.get("platform") or doc.get("source"),
                    "rating": doc.get("rating"),
                    "entity_type": doc.get("entity_type") or doc.get("doc_type"),
                    "community_or_channel": doc.get("community_or_channel") or doc.get("subreddit"),
                    "platform_metadata": doc.get("platform_metadata")
                    or {
                        "subreddit": doc.get("subreddit"),
                        "parent_external_id": doc.get("parent_external_id"),
                    },
                    "ingestion_ts": doc.get("ingestion_ts"),
                    "dedupe_key": doc.get("dedupe_key"),
                    "raw_payload": doc.get("raw_payload"),
                },
                default=str,
            ),
            "dedupe_key": doc.get("dedupe_key"),
        }

        result = session.execute(
            text(
                """
                INSERT OR IGNORE INTO documents
                (source_id, external_id, title, body, author, url, published_at, raw_json, dedupe_key)
                VALUES
                (:source_id, :external_id, :title, :body, :author, :url, :published_at, :raw_json, :dedupe_key)
                """
            ),
            row,
        )
        if result.rowcount and result.rowcount > 0:
            inserted += 1

    session.commit()
    return inserted


def _run_pushshift_ingestion(config: dict[str, Any], *, days_back: int) -> tuple[list[dict[str, Any]], int]:
    subreddits = [item for item in config.get("subreddits", []) if isinstance(item, str) and item.strip()]
    keywords = [item for item in config.get("keywords", []) if isinstance(item, str) and item.strip()]
    post_limit = int(config.get("post_limit", 200))

    now_utc = datetime.now(tz=timezone.utc)
    after = int((now_utc - timedelta(days=max(days_back, 0))).timestamp())
    before = int(now_utc.timestamp())

    base_url = os.getenv("REDDIT_PUSHSHIFT_BASE_URL") or None

    seen_ids: set[str] = set()
    docs: list[dict[str, Any]] = []
    query_terms = keywords or [""]

    for subreddit in subreddits:
        for query in query_terms:
            submissions = search_submissions(
                subreddit=subreddit,
                query=query,
                after=after,
                before=before,
                size=post_limit,
                base_url=base_url,
            )
            for raw_submission in submissions:
                normalized = normalize_pushshift_submission(raw_submission)
                external_id = normalized.get("external_id")
                if isinstance(external_id, str) and external_id in seen_ids:
                    continue
                if isinstance(external_id, str):
                    seen_ids.add(external_id)
                docs.append(normalized)

    return docs, len(docs)


def _run_public_json_ingestion(config: dict[str, Any], *, days_back: int) -> tuple[list[dict[str, Any]], int]:
    subreddits = [item for item in config.get("subreddits", []) if isinstance(item, str) and item.strip()]
    keywords = [item for item in config.get("keywords", []) if isinstance(item, str) and item.strip()]

    now_utc = datetime.now(tz=timezone.utc)
    after_iso = (now_utc - timedelta(days=max(days_back, 0))).isoformat()
    before_iso = now_utc.isoformat()

    page_size = int(os.getenv("PUBLIC_REDDIT_PAGE_SIZE", "100"))
    max_pages = int(os.getenv("PUBLIC_REDDIT_MAX_PAGES", "5"))
    delay_seconds = float(os.getenv("PUBLIC_REDDIT_DELAY_SECONDS", "1.0"))
    base_url = os.getenv("PUBLIC_REDDIT_BASE_URL", "https://www.reddit.com")
    user_agent = os.getenv("PUBLIC_REDDIT_USER_AGENT") or os.getenv("REDDIT_USER_AGENT") or "reviewAnalyzer/0.1 (public-json-ingestion)"

    seen_ids: set[str] = set()
    docs: list[dict[str, Any]] = []
    query_terms = keywords or [""]

    for subreddit in subreddits:
        for query in query_terms:
            try:
                submissions = search_public_json_submissions(
                    subreddit=subreddit,
                    query=query,
                    after_iso=after_iso,
                    before_iso=before_iso,
                    page_size=page_size,
                    max_pages=max_pages,
                    base_url=base_url,
                    user_agent=user_agent,
                    request_delay_seconds=delay_seconds,
                )
            except (PublicRedditError, requests.RequestException) as exc:
                logger.warning(
                    "Skipping failed public_json pair and continuing batch",
                    extra={"subreddit": subreddit, "query": query, "error": str(exc)},
                )
                continue
            for raw_submission in submissions:
                normalized = normalize_pushshift_submission(raw_submission)
                external_id = normalized.get("external_id")
                if isinstance(external_id, str) and external_id in seen_ids:
                    continue
                if isinstance(external_id, str):
                    seen_ids.add(external_id)
                docs.append(normalized)

    return docs, len(docs)


def _run_rss_ingestion(config: dict[str, Any], *, days_back: int) -> tuple[list[dict[str, Any]], int]:
    subreddits = [item for item in config.get("subreddits", []) if isinstance(item, str) and item.strip()]
    keywords = [item for item in config.get("keywords", []) if isinstance(item, str) and item.strip()]

    now_utc = datetime.now(tz=timezone.utc)
    after_iso = (now_utc - timedelta(days=max(days_back, 0))).isoformat()
    before_iso = now_utc.isoformat()

    max_pages = int(os.getenv("REDDIT_RSS_MAX_PAGES", "3"))
    delay_seconds = float(os.getenv("REDDIT_RSS_DELAY_SECONDS", "1.0"))
    base_url = os.getenv("PUBLIC_REDDIT_BASE_URL", "https://www.reddit.com")
    user_agent = os.getenv("PUBLIC_REDDIT_USER_AGENT") or os.getenv("REDDIT_USER_AGENT") or "reviewAnalyzer/0.1 (reddit-rss-ingestion)"

    seen_ids: set[str] = set()
    docs: list[dict[str, Any]] = []
    query_terms = keywords or [""]

    for subreddit in subreddits:
        for query in query_terms:
            submissions = search_rss_submissions(
                subreddit=subreddit,
                query=query,
                after_iso=after_iso,
                before_iso=before_iso,
                max_pages=max_pages,
                base_url=base_url,
                user_agent=user_agent,
                request_delay_seconds=delay_seconds,
            )
            for raw_submission in submissions:
                normalized = normalize_pushshift_submission(raw_submission)
                external_id = normalized.get("external_id")
                if isinstance(external_id, str) and external_id in seen_ids:
                    continue
                if isinstance(external_id, str):
                    seen_ids.add(external_id)
                docs.append(normalized)

    return docs, len(docs)


def run_for_platform(platform: str, config: dict[str, Any], *, days_back: int) -> dict[str, int]:
    """Run ingestion for a single platform and return run counters."""

    session = SessionLocal()
    run_repo = IngestionRunRepository(session)
    run_id: int | None = None

    try:
        _safe_ensure_dedupe_constraints(session)
        source_id = _ensure_reddit_source(session) if platform == "reddit" else _ensure_source(session, platform)
        session.commit()

        run_id = run_repo.start_run(source_name=platform)
        fetch_backend = os.getenv("REDDIT_FETCH_BACKEND", "praw").strip().lower()
        if platform == "reddit" and fetch_backend in {"pushshift", "public_json", "rss"}:
            fallback_chain: list[tuple[str, Any]]
            if fetch_backend == "pushshift":
                fallback_chain = [
                    ("pushshift", _run_pushshift_ingestion),
                    ("public_json", _run_public_json_ingestion),
                    ("rss", _run_rss_ingestion),
                ]
            elif fetch_backend == "public_json":
                fallback_chain = [
                    ("public_json", _run_public_json_ingestion),
                    ("rss", _run_rss_ingestion),
                ]
            else:
                fallback_chain = [("rss", _run_rss_ingestion)]

            docs = []
            fetched_count = 0
            failover_events: list[str] = []

            for backend_name, backend_runner in fallback_chain:
                try:
                    candidate_docs, candidate_count = backend_runner(config, days_back=days_back)
                except (PushshiftError, PublicRedditError, RedditRssError, requests.RequestException) as exc:
                    message = f"{backend_name} failed: {exc}"
                    failover_events.append(message)
                    logger.warning("Reddit ingestion backend failed", extra={"backend": backend_name, "error": str(exc)})
                    continue

                if candidate_count > 0:
                    docs = candidate_docs
                    fetched_count = candidate_count
                    if failover_events:
                        logger.info(
                            "Reddit ingestion succeeded after failover",
                            extra={"start_backend": fetch_backend, "used_backend": backend_name, "events": failover_events},
                        )
                    break

                failover_events.append(f"{backend_name} returned 0 docs")
                logger.info("Reddit ingestion backend returned 0 docs", extra={"backend": backend_name})

            if fetched_count == 0:
                details = "; ".join(failover_events) if failover_events else "No backend attempts were executed"
                raise RuntimeError(f"Reddit ingestion failed: all backend attempts returned zero docs ({details})")
        else:
            adapter_class = get_adapter_class(platform)
            ingestor = adapter_class()
            docs, stats = ingestor.run(config=config, days_back=days_back)
            fetched_count = stats.docs_emitted
        inserted = _insert_documents(session, source_id, docs)

        run_repo.complete_run(
            run_id=run_id,
            records_fetched=fetched_count,
            records_inserted=inserted,
            status="completed",
        )
        logger.info(
            "%s refresh completed",
            platform,
            extra={"records_fetched": fetched_count, "records_inserted": inserted},
        )
        return {"records_fetched": fetched_count, "records_inserted": inserted}
    except Exception as exc:
        if run_id is not None:
            run_repo.complete_run(
                run_id=run_id,
                records_fetched=0,
                records_inserted=0,
                status="failed",
                error_message=str(exc),
            )
        logger.exception("%s refresh failed", platform)
        raise
    finally:
        session.close()


def run() -> None:
    """Backward-compatible reddit-only refresh entrypoint."""

    from app.config.source_loader import get_enabled_platform_configs

    setup_logging()
    configs = [config for config in get_enabled_platform_configs() if config.platform == "reddit"]
    if not configs:
        raise RuntimeError("Reddit platform is not enabled in merged source configuration")

    reddit_config = configs[0]
    run_for_platform("reddit", reddit_config.config, days_back=reddit_config.days_back)


if __name__ == "__main__":
    run()
