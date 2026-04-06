from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.jobs import refresh_web_reviews


def _build_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    schema_path = Path(__file__).resolve().parents[1] / "app" / "db" / "schema.sql"

    with engine.begin() as connection:
        connection.connection.executescript(schema_path.read_text(encoding="utf-8"))

    return Session(bind=engine, future=True)


def test_resolve_ingestion_window_accepts_explicit_dates() -> None:
    start, end = refresh_web_reviews._resolve_ingestion_window(
        30,
        date_from="2026-03-01",
        date_to="2026-03-03",
    )

    assert start.isoformat() == "2026-03-01T00:00:00+00:00"
    assert end.isoformat() == "2026-03-03T23:59:59.999999+00:00"


def test_run_for_web_reviews_persists_docs_and_run_stats(monkeypatch) -> None:
    session = _build_session()
    monkeypatch.setattr(refresh_web_reviews, "SessionLocal", lambda: session)

    class _FakeClient:
        def discover_candidate_article_urls(
            self,
            *,
            homepage_url: str,
            category_urls: list[str] | None = None,
            keywords: list[str] | None = None,
            prioritize_keywords: bool = False,
        ) -> list[str]:
            del category_urls
            del keywords
            del prioritize_keywords
            return [
                f"{homepage_url}/reviews/galaxy-watch-8-review",
                f"{homepage_url}/reviews/galaxy-watch-8-review?utm=campaign",
            ]

        def fetch_articles(self, article_urls: list[str]) -> dict[str, str]:
            recent_date_a = (datetime.now(tz=timezone.utc) - timedelta(days=1)).isoformat().replace("+00:00", "Z")
            recent_date_b = (datetime.now(tz=timezone.utc) - timedelta(days=2)).isoformat().replace("+00:00", "Z")
            return {
                article_urls[0]: (
                    "<html><head><title>Galaxy Watch 8 Review</title></head>"
                    f"<meta property='article:published_time' content='{recent_date_a}'/>"
                    "<body><article><p>" + ("Great battery life. " * 80) + "</p></article></body></html>"
                ),
                article_urls[1]: (
                    "<html><head><title>Galaxy Watch 8 Review</title></head>"
                    f"<meta property='article:published_time' content='{recent_date_b}'/>"
                    "<body><article><p>" + ("Great battery life. " * 80) + "</p></article></body></html>"
                ),
            }

    monkeypatch.setattr(refresh_web_reviews, "WebReviewsClient", lambda: _FakeClient())

    stats = refresh_web_reviews.run_for_web_reviews(
        {
            "sites": ["example.com"],
            "max_pages_per_site": 10,
            "min_content_chars": 500,
        },
        days_back=30,
    )

    assert stats["records_fetched"] == 2
    assert stats["records_inserted"] == 1

    rows = session.execute(text("SELECT external_id, dedupe_key, raw_json FROM documents")).fetchall()
    assert len(rows) == 1
    assert rows[0].external_id == "https://example.com/reviews/galaxy-watch-8-review"
    assert rows[0].dedupe_key.startswith("web_reviews:canonical:")

    payload = json.loads(rows[0].raw_json)
    assert payload["platform"] == "web_reviews"
    assert payload["entity_type"] == "review"

    run_row = session.execute(
        text("SELECT source_name, status, records_fetched, records_inserted FROM ingestion_runs ORDER BY id DESC LIMIT 1")
    ).first()
    assert run_row is not None
    assert run_row.source_name == "web_reviews"
    assert run_row.status == "completed"
    assert run_row.records_fetched == 2
    assert run_row.records_inserted == 1


def test_run_for_web_reviews_honors_explicit_date_range(monkeypatch) -> None:
    session = _build_session()
    monkeypatch.setattr(refresh_web_reviews, "SessionLocal", lambda: session)

    class _FakeClient:
        def discover_candidate_article_urls(
            self,
            *,
            homepage_url: str,
            category_urls: list[str] | None = None,
            keywords: list[str] | None = None,
            prioritize_keywords: bool = False,
        ) -> list[str]:
            del category_urls
            del keywords
            del prioritize_keywords
            return [
                f"{homepage_url}/reviews/in-range-review",
                f"{homepage_url}/reviews/out-of-range-review",
            ]

        def fetch_articles(self, article_urls: list[str]) -> dict[str, str]:
            return {
                article_urls[0]: (
                    "<html><head><title>In Range Review</title>"
                    "<meta property='article:published_time' content='2026-03-02T12:00:00Z'/></head>"
                    "<body><article><p>" + ("In range review text. " * 80) + "</p></article></body></html>"
                ),
                article_urls[1]: (
                    "<html><head><title>Out of Range Review</title>"
                    "<meta property='article:published_time' content='2026-02-01T12:00:00Z'/></head>"
                    "<body><article><p>" + ("Out of range review text. " * 80) + "</p></article></body></html>"
                ),
            }

    monkeypatch.setattr(refresh_web_reviews, "WebReviewsClient", lambda: _FakeClient())

    stats = refresh_web_reviews.run_for_web_reviews(
        {
            "sites": ["example.com"],
            "max_pages_per_site": 10,
            "min_content_chars": 500,
        },
        days_back=365,
        date_from="2026-03-01",
        date_to="2026-03-03",
    )

    assert stats["records_fetched"] == 2
    assert stats["records_inserted"] == 1
    inserted_external_ids = session.execute(text("SELECT external_id FROM documents")).scalars().all()
    assert inserted_external_ids == ["https://example.com/reviews/in-range-review"]
