"""Job entry point for running a single platform ingestion refresh."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, time, timedelta, timezone
from typing import Any

import requests
from sqlalchemy import text

from app.config.settings import IngestionSettings, get_ingestion_settings
from app.db.repositories import IngestionRunRepository
from app.db.session import SessionLocal
from app.ingestion.normalizers import normalize_pushshift_submission
from app.ingestion.public_reddit_client import PublicRedditError
from app.ingestion.pushshift_client import PushshiftError, search_submissions
from app.ingestion.public_reddit_client import search_submissions as search_public_json_submissions
from app.ingestion.reddit_rss_client import search_submissions as search_rss_submissions
from app.ingestion.registry import get_adapter_class
from app.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


def _build_ingestion_diagnostics(
    platform: str,
    config: dict[str, Any],
    *,
    days_back: int,
    ingestion_window: tuple[datetime, datetime],
    backend_requested: str | None,
) -> dict[str, Any]:
    subreddits = [item for item in config.get("subreddits", []) if isinstance(item, str) and item.strip()]
    keywords = [item for item in config.get("keywords", []) if isinstance(item, str) and item.strip()]
    return {
        "platform": platform,
        "backend_requested": backend_requested,
        "backend_used": None,
        "fallback_activated": False,
        "fallback_chain": [],
        "effective_config": {
            "subreddits": subreddits,
            "keywords": keywords,
            "post_limit": int(config.get("post_limit", 200)),
            "days_back": days_back,
            "date_from": ingestion_window[0].isoformat(),
            "date_to": ingestion_window[1].isoformat(),
        },
        "stages": {
            "fetch": {"status": "pending"},
            "normalize": {"status": "pending", "count": 0},
            "dedupe": {"status": "pending", "skipped": 0},
            "insert": {"status": "pending", "inserted": 0},
            "enrich_trigger": {"status": "pending"},
        },
        "counters": {
            "fetch_started": 0,
            "fetch_succeeded": 0,
            "fetch_failed": 0,
            "normalize_count": 0,
            "dedupe_skipped": 0,
            "inserted_count": 0,
        },
        "first_failing_stage": None,
        "error_summary": None,
    }


def _record_stage_failure(diagnostics: dict[str, Any], stage: str, exc: Exception) -> None:
    diagnostics["stages"].setdefault(stage, {})
    diagnostics["stages"][stage]["status"] = "failed"
    diagnostics["stages"][stage]["error"] = {
        "class": exc.__class__.__name__,
        "message": str(exc),
    }
    diagnostics["first_failing_stage"] = diagnostics["first_failing_stage"] or stage
    diagnostics["error_summary"] = f"{exc.__class__.__name__}: {exc}"


def _resolve_ingestion_window(days_back: int) -> tuple[datetime, datetime]:
    date_from_raw = os.getenv("REDDIT_INGEST_DATE_FROM", "").strip()
    date_to_raw = os.getenv("REDDIT_INGEST_DATE_TO", "").strip()
    if date_from_raw and date_to_raw:
        try:
            date_from = datetime.strptime(date_from_raw, "%Y-%m-%d").date()
            date_to = datetime.strptime(date_to_raw, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("REDDIT_INGEST_DATE_FROM/TO must be YYYY-MM-DD") from exc
        if date_from > date_to:
            raise ValueError("REDDIT_INGEST_DATE_FROM cannot be after REDDIT_INGEST_DATE_TO")
        after_dt = datetime.combine(date_from, time.min, tzinfo=timezone.utc)
        before_dt = datetime.combine(date_to, time.max, tzinfo=timezone.utc)
        return after_dt, before_dt

    now_utc = datetime.now(tz=timezone.utc)
    after_dt = now_utc - timedelta(days=max(days_back, 0))
    return after_dt, now_utc


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


def _run_pushshift_ingestion(
    config: dict[str, Any],
    *,
    days_back: int,
    ingestion_window: tuple[datetime, datetime] | None = None,
    settings: IngestionSettings | None = None,
) -> tuple[list[dict[str, Any]], int]:
    subreddits = [item for item in config.get("subreddits", []) if isinstance(item, str) and item.strip()]
    keywords = [item for item in config.get("keywords", []) if isinstance(item, str) and item.strip()]
    post_limit = int(config.get("post_limit", 200))

    after_dt, before_dt = ingestion_window or _resolve_ingestion_window(days_back)
    after = int(after_dt.timestamp())
    before = int(before_dt.timestamp())

    active_settings = settings or get_ingestion_settings()
    base_url = active_settings.reddit_pushshift_base_url or None

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


def _run_public_json_ingestion(
    config: dict[str, Any],
    *,
    days_back: int,
    ingestion_window: tuple[datetime, datetime] | None = None,
    settings: IngestionSettings | None = None,
) -> tuple[list[dict[str, Any]], int]:
    subreddits = [item for item in config.get("subreddits", []) if isinstance(item, str) and item.strip()]
    keywords = [item for item in config.get("keywords", []) if isinstance(item, str) and item.strip()]

    after_dt, before_dt = ingestion_window or _resolve_ingestion_window(days_back)
    after_iso = after_dt.isoformat()
    before_iso = before_dt.isoformat()

    active_settings = settings or get_ingestion_settings()
    page_size = active_settings.public_reddit_page_size
    max_pages = active_settings.public_reddit_max_pages
    delay_seconds = active_settings.public_reddit_delay_seconds
    base_url = active_settings.public_reddit_base_url
    user_agent = active_settings.public_reddit_user_agent or active_settings.reddit_user_agent

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


def _run_rss_ingestion(
    config: dict[str, Any],
    *,
    days_back: int,
    ingestion_window: tuple[datetime, datetime] | None = None,
    settings: IngestionSettings | None = None,
) -> tuple[list[dict[str, Any]], int]:
    subreddits = [item for item in config.get("subreddits", []) if isinstance(item, str) and item.strip()]
    keywords = [item for item in config.get("keywords", []) if isinstance(item, str) and item.strip()]

    after_dt, before_dt = ingestion_window or _resolve_ingestion_window(days_back)
    after_iso = after_dt.isoformat()
    before_iso = before_dt.isoformat()

    max_pages = int(os.getenv("REDDIT_RSS_MAX_PAGES", "3"))
    delay_seconds = float(os.getenv("REDDIT_RSS_DELAY_SECONDS", "1.0"))
    active_settings = settings or get_ingestion_settings()
    base_url = active_settings.public_reddit_base_url
    user_agent = active_settings.public_reddit_user_agent or active_settings.reddit_user_agent

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
    diagnostics: dict[str, Any] = {}
    fetched_count = 0
    inserted = 0

    try:
        settings = get_ingestion_settings()
        ingestion_after_dt, ingestion_before_dt = _resolve_ingestion_window(days_back)
        ingestion_window = (ingestion_after_dt, ingestion_before_dt)
        fetch_backend = settings.reddit_fetch_backend.strip().lower() if platform == "reddit" else None
        diagnostics = _build_ingestion_diagnostics(
            platform,
            config,
            days_back=days_back,
            ingestion_window=ingestion_window,
            backend_requested=fetch_backend,
        )
        _safe_ensure_dedupe_constraints(session)
        source_id = _ensure_reddit_source(session) if platform == "reddit" else _ensure_source(session, platform)
        session.commit()

        run_id = run_repo.start_run(source_name=platform)
        if platform == "reddit":
            diagnostics["stages"]["fetch"]["status"] = "running"
            diagnostics["counters"]["fetch_started"] = 1
            logger.info(
                "Reddit ingestion window resolved",
                extra={
                    "date_from": ingestion_after_dt.date().isoformat(),
                    "date_to": ingestion_before_dt.date().isoformat(),
                    "after_iso": ingestion_after_dt.isoformat(),
                    "before_iso": ingestion_before_dt.isoformat(),
                    "days_back_default": days_back,
                },
            )
            docs = []

            if fetch_backend == "pushshift":
                failover_events: list[str] = []
                fallback_chain = [
                    ("pushshift", _run_pushshift_ingestion),
                    ("public_json", _run_public_json_ingestion),
                    ("rss", _run_rss_ingestion),
                ]

                for backend_name, backend_runner in fallback_chain:
                    try:
                        diagnostics["fallback_chain"].append({"backend": backend_name, "status": "running"})
                        candidate_docs, candidate_count = backend_runner(
                            config,
                            days_back=days_back,
                            ingestion_window=ingestion_window,
                        )
                    except (PushshiftError, PublicRedditError, requests.RequestException) as exc:
                        diagnostics["counters"]["fetch_failed"] += 1
                        diagnostics["fallback_chain"][-1]["status"] = "failed"
                        diagnostics["fallback_chain"][-1]["error"] = {
                            "class": exc.__class__.__name__,
                            "message": str(exc),
                        }
                        message = f"{backend_name} failed: {exc}"
                        failover_events.append(message)
                        logger.warning(
                            "Reddit ingestion backend failed",
                            extra={"backend": backend_name, "error": str(exc)},
                        )
                        continue

                    if candidate_count > 0:
                        docs = candidate_docs
                        fetched_count = candidate_count
                        diagnostics["backend_used"] = backend_name
                        diagnostics["stages"]["fetch"]["status"] = "succeeded"
                        diagnostics["counters"]["fetch_succeeded"] = candidate_count
                        diagnostics["fallback_chain"][-1]["status"] = "succeeded"
                        if failover_events:
                            diagnostics["fallback_activated"] = True
                            logger.info(
                                "Reddit ingestion succeeded after failover",
                                extra={
                                    "start_backend": fetch_backend,
                                    "used_backend": backend_name,
                                    "events": failover_events,
                                },
                            )
                        break

                    failover_events.append(f"{backend_name} returned 0 docs")
                    diagnostics["fallback_chain"][-1]["status"] = "empty"
                    logger.info("Reddit ingestion backend returned 0 docs", extra={"backend": backend_name})

                if fetched_count == 0:
                    diagnostics["counters"]["fetch_failed"] += 1
                    details = "; ".join(failover_events) if failover_events else "No backend attempts were executed"
                    failure = RuntimeError(
                        "Reddit ingestion failed: all backend attempts returned zero docs "
                        f"({details}); window={ingestion_after_dt.date().isoformat()}.."
                        f"{ingestion_before_dt.date().isoformat()}"
                    )
                    _record_stage_failure(diagnostics, "fetch", failure)
                    raise failure
            elif fetch_backend == "public_json":
                failover_events = []
                try:
                    docs, fetched_count = _run_public_json_ingestion(
                        config,
                        days_back=days_back,
                        ingestion_window=ingestion_window,
                    )
                    diagnostics["backend_used"] = "public_json"
                    diagnostics["counters"]["fetch_succeeded"] = fetched_count
                    diagnostics["stages"]["fetch"]["status"] = "succeeded" if fetched_count > 0 else "empty"
                    if fetched_count == 0:
                        diagnostics["counters"]["fetch_failed"] += 1
                except (PublicRedditError, requests.RequestException) as exc:
                    diagnostics["counters"]["fetch_failed"] += 1
                    failover_events.append(f"public_json failed: {exc}")
                    logger.warning(
                        "Reddit ingestion backend failed",
                        extra={"backend": "public_json", "error": str(exc)},
                    )
                    docs, fetched_count = _run_rss_ingestion(
                        config,
                        days_back=days_back,
                        ingestion_window=ingestion_window,
                    )
                    diagnostics["fallback_activated"] = True
                    diagnostics["backend_used"] = "rss"
                    diagnostics["counters"]["fetch_succeeded"] = fetched_count
                    diagnostics["stages"]["fetch"]["status"] = "succeeded" if fetched_count > 0 else "empty"
                    if fetched_count > 0:
                        logger.info(
                            "Reddit ingestion succeeded after failover",
                            extra={
                                "start_backend": fetch_backend,
                                "used_backend": "rss",
                                "events": failover_events,
                            },
                        )
            else:
                adapter_class = get_adapter_class(platform)
                ingestor = adapter_class()
                docs, stats = ingestor.run(config=config, days_back=days_back)
                fetched_count = stats.docs_emitted
                diagnostics["backend_used"] = fetch_backend
                diagnostics["counters"]["fetch_succeeded"] = fetched_count
                diagnostics["stages"]["fetch"]["status"] = "succeeded"
        else:
            adapter_class = get_adapter_class(platform)
            ingestor = adapter_class()
            docs, stats = ingestor.run(config=config, days_back=days_back)
            fetched_count = stats.docs_emitted
            diagnostics["stages"]["fetch"]["status"] = "succeeded"
            diagnostics["counters"]["fetch_succeeded"] = fetched_count

        diagnostics["stages"]["normalize"]["status"] = "succeeded"
        diagnostics["stages"]["normalize"]["count"] = len(docs)
        diagnostics["counters"]["normalize_count"] = len(docs)

        diagnostics["stages"]["insert"]["status"] = "running"
        inserted = _insert_documents(session, source_id, docs)
        diagnostics["stages"]["insert"]["status"] = "succeeded"
        diagnostics["stages"]["insert"]["inserted"] = inserted
        diagnostics["counters"]["inserted_count"] = inserted
        diagnostics["stages"]["dedupe"]["status"] = "succeeded"
        diagnostics["stages"]["dedupe"]["skipped"] = max(len(docs) - inserted, 0)
        diagnostics["counters"]["dedupe_skipped"] = max(len(docs) - inserted, 0)
        diagnostics["stages"]["enrich_trigger"]["status"] = "not_triggered"
        diagnostics["stages"]["enrich_trigger"]["reason"] = "Enrichment is run as a separate admin action."

        run_repo.complete_run(
            run_id=run_id,
            records_fetched=fetched_count,
            records_inserted=inserted,
            status="completed",
            error_message=json.dumps(diagnostics, default=str),
        )
        logger.info(
            "%s refresh completed",
            platform,
            extra={"records_fetched": fetched_count, "records_inserted": inserted},
        )
        return {"records_fetched": fetched_count, "records_inserted": inserted}
    except Exception as exc:
        if diagnostics:
            if diagnostics["first_failing_stage"] is None:
                diagnostics["first_failing_stage"] = "insert" if diagnostics["stages"]["insert"]["status"] == "running" else "unknown"
            if diagnostics["error_summary"] is None:
                diagnostics["error_summary"] = f"{exc.__class__.__name__}: {exc}"
            if diagnostics["stages"]["insert"]["status"] == "running":
                _record_stage_failure(diagnostics, "insert", exc)
        if run_id is not None:
            run_repo.complete_run(
                run_id=run_id,
                records_fetched=fetched_count,
                records_inserted=inserted,
                status="failed",
                error_message=json.dumps(diagnostics, default=str) if diagnostics else str(exc),
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
