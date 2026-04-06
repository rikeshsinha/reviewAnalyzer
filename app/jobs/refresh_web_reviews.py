"""Job entry point for refreshing crawled web review documents."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from datetime import datetime, time, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

from app.config.source_loader import get_enabled_platform_configs
from app.db.repositories import IngestionRunRepository
from app.db.session import SessionLocal
from app.ingestion.web_reviews_client import WebReviewsClient
from app.ingestion.web_reviews_normalizer import normalize_web_review_article
from app.jobs.refresh_reddit import _ensure_source, _insert_documents, _safe_ensure_dedupe_constraints
from app.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


def _normalize_site_homepage(site: str) -> str:
    cleaned = (site or "").strip()
    if not cleaned:
        raise ValueError("Encountered empty site value in web_reviews config")
    if not cleaned.startswith(("http://", "https://")):
        cleaned = f"https://{cleaned}"
    parsed = urlparse(cleaned)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid site URL for web_reviews ingestion: {site}")
    return f"{parsed.scheme}://{parsed.netloc}"


def _canonicalize_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url.strip())
    if not parsed.scheme or not parsed.netloc:
        return None
    hostname = parsed.hostname.lower() if parsed.hostname else ""
    if not hostname:
        return None

    port = f":{parsed.port}" if parsed.port and parsed.port not in {80, 443} else ""
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"
    return f"{parsed.scheme.lower()}://{hostname}{port}{path}"


def _canonical_web_dedupe_key(doc: dict[str, Any]) -> str:
    canonical_url = _canonicalize_url(doc.get("url")) or ""
    content = " ".join(
        filter(
            None,
            [
                str(doc.get("title") or "").strip().lower(),
                str(doc.get("content") or "").strip().lower(),
            ],
        )
    )
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    payload = f"web_reviews|{canonical_url}|{content_hash}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"web_reviews:canonical:{digest}"


def _resolve_ingestion_window(
    days_back: int,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[datetime, datetime]:
    resolved_date_from = (date_from or os.getenv("WEB_REVIEWS_INGEST_DATE_FROM", "")).strip()
    resolved_date_to = (date_to or os.getenv("WEB_REVIEWS_INGEST_DATE_TO", "")).strip()

    if resolved_date_from or resolved_date_to:
        if not (resolved_date_from and resolved_date_to):
            raise ValueError(
                "WEB_REVIEWS_INGEST_DATE_FROM and WEB_REVIEWS_INGEST_DATE_TO must both be provided "
                "when overriding the date range"
            )
        try:
            parsed_from = datetime.strptime(resolved_date_from, "%Y-%m-%d").date()
            parsed_to = datetime.strptime(resolved_date_to, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("Web review date range values must be YYYY-MM-DD") from exc
        if parsed_from > parsed_to:
            raise ValueError("WEB_REVIEWS_INGEST_DATE_FROM cannot be after WEB_REVIEWS_INGEST_DATE_TO")
        return (
            datetime.combine(parsed_from, time.min, tzinfo=timezone.utc),
            datetime.combine(parsed_to, time.max, tzinfo=timezone.utc),
        )

    now_utc = datetime.now(tz=timezone.utc)
    return now_utc - timedelta(days=max(days_back, 0)), now_utc


def _is_within_window(created_at: str | None, start: datetime, end: datetime) -> bool:
    if not created_at:
        return False
    parsed_text = str(created_at).strip()
    if parsed_text.endswith("Z"):
        parsed_text = f"{parsed_text[:-1]}+00:00"
    try:
        parsed_dt = datetime.fromisoformat(parsed_text)
    except ValueError:
        return False
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    normalized = parsed_dt.astimezone(timezone.utc)
    return start <= normalized <= end


def run_for_web_reviews(
    config: dict[str, Any],
    *,
    days_back: int,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, int]:
    """Run web review ingestion and persist into documents/ingestion_runs."""

    session = SessionLocal()
    run_repo = IngestionRunRepository(session)
    run_id: int | None = None
    fetched_count = 0
    inserted = 0

    try:
        window_start, window_end = _resolve_ingestion_window(days_back, date_from=date_from, date_to=date_to)
        sites = [site for site in config.get("sites", []) if isinstance(site, str) and site.strip()]
        if not sites:
            raise ValueError("web_reviews ingestion requires non-empty config.sites")

        max_pages_per_site = max(int(config.get("max_pages_per_site", 50)), 1)
        min_content_chars = max(int(config.get("min_content_chars", 500)), 1)
        category_urls_by_site = config.get("category_urls_by_site", {})

        _safe_ensure_dedupe_constraints(session)
        source_id = _ensure_source(session, "web_reviews")
        session.commit()
        run_id = run_repo.start_run(source_name="web_reviews")

        client = WebReviewsClient()
        docs: list[dict[str, Any]] = []
        seen_dedupe_keys: set[str] = set()

        for site in sites:
            homepage_url = _normalize_site_homepage(site)
            category_urls: list[str] = []
            if isinstance(category_urls_by_site, dict):
                configured = category_urls_by_site.get(site) or category_urls_by_site.get(homepage_url)
                if isinstance(configured, list):
                    category_urls = [str(url).strip() for url in configured if str(url).strip()]

            candidate_urls = client.discover_candidate_article_urls(
                homepage_url=homepage_url,
                category_urls=category_urls,
            )
            if len(candidate_urls) > max_pages_per_site:
                candidate_urls = candidate_urls[:max_pages_per_site]

            fetched_html = client.fetch_articles(candidate_urls)
            fetched_count += len(fetched_html)

            for article_url, article_html in fetched_html.items():
                normalized = normalize_web_review_article(
                    {"url": article_url, "html": article_html},
                    min_content_chars=min_content_chars,
                )
                if normalized is None:
                    continue
                if not _is_within_window(normalized.get("created_at"), window_start, window_end):
                    continue

                canonical_url = _canonicalize_url(normalized.get("url"))
                normalized["external_id"] = canonical_url or normalized.get("url")
                normalized["dedupe_key"] = _canonical_web_dedupe_key(normalized)
                if normalized["dedupe_key"] in seen_dedupe_keys:
                    continue
                seen_dedupe_keys.add(normalized["dedupe_key"])
                docs.append(normalized)

        inserted = _insert_documents(session, source_id, docs)

        run_repo.complete_run(
            run_id=run_id,
            records_fetched=fetched_count,
            records_inserted=inserted,
            status="completed",
            error_message=json.dumps(
                {
                    "date_from": window_start.isoformat(),
                    "date_to": window_end.isoformat(),
                    "sites": sites,
                }
            ),
        )

        logger.info(
            "web_reviews refresh completed",
            extra={"records_fetched": fetched_count, "records_inserted": inserted},
        )
        return {"records_fetched": fetched_count, "records_inserted": inserted}
    except Exception as exc:
        if run_id is not None:
            run_repo.complete_run(
                run_id=run_id,
                records_fetched=fetched_count,
                records_inserted=inserted,
                status="failed",
                error_message=str(exc),
            )
        logger.exception("web_reviews refresh failed")
        raise
    finally:
        session.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh web review documents")
    parser.add_argument("--days-back", type=int, default=None)
    parser.add_argument("--date-from", type=str, default=None, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--date-to", type=str, default=None, help="Inclusive YYYY-MM-DD")
    return parser.parse_args()


def run() -> None:
    setup_logging()

    args = _parse_args()
    configs = [config for config in get_enabled_platform_configs() if config.platform == "web_reviews"]
    if not configs:
        raise RuntimeError("web_reviews platform is not enabled in merged source configuration")

    web_config = configs[0]
    days_back = web_config.days_back if args.days_back is None else args.days_back
    run_for_web_reviews(
        web_config.config,
        days_back=days_back,
        date_from=args.date_from,
        date_to=args.date_to,
    )


if __name__ == "__main__":
    run()
