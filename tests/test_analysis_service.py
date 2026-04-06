from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.services.analysis_service import AnalysisConfig, AnalysisService
from app.ui.pages.dashboard import _fetch_ranked_complaints, _where_clause


def _build_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    schema_path = Path(__file__).resolve().parents[1] / "app" / "db" / "schema.sql"

    with engine.begin() as connection:
        connection.connection.executescript(schema_path.read_text(encoding="utf-8"))

    return Session(bind=engine, future=True)


def _seed_sentiment_doc(session: Session) -> None:
    source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('reddit', 'reddit', 'reddit', '{}')
                """
            )
        ).lastrowid
    )

    seeded_rows = [
        ("x-1", "2026-03-02T00:00:00", "negative"),
        ("x-2", "2026-03-02T01:00:00", "positive"),
        ("x-3", "2026-03-03T00:00:00", "neutral"),
        ("x-4", "2026-03-03T01:00:00", "mixed"),
    ]
    for external_id, published_at, sentiment in seeded_rows:
        doc_id = int(
            session.execute(
                text(
                    """
                    INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
                    VALUES (:source_id, :external_id, 'Battery issue', 'battery drains quickly', 'u', 'https://example.com', :published_at, '{}')
                    """
                ),
                {"source_id": source_id, "external_id": external_id, "published_at": published_at},
            ).lastrowid
        )

        session.execute(
            text(
                """
                INSERT INTO enrichments (document_id, model_name, summary, metadata_json)
                VALUES (:document_id, 'mock', 'summary', :metadata_json)
                """
            ),
            {"document_id": doc_id, "metadata_json": json.dumps({"sentiment_label": sentiment})},
        )
    session.commit()


def test_insight_cache_miss_then_hit() -> None:
    session = _build_session()
    _seed_sentiment_doc(session)

    service = AnalysisService(session=session, client=None, config=AnalysisConfig(cache_ttl_minutes=60))
    filters = {"subreddit": None}

    first = service.generate_sentiment_insight(filters)
    cache_rows = session.execute(text("SELECT COUNT(*) FROM saved_insights")).scalar_one()

    second = service.generate_sentiment_insight(filters)
    cache_rows_after = session.execute(text("SELECT COUNT(*) FROM saved_insights")).scalar_one()

    assert cache_rows == 1
    assert cache_rows_after == 1
    assert first == second
    assert "daily_sentiment_trend" in first["metrics"]
    trend_rows = first["metrics"]["daily_sentiment_trend"]
    assert trend_rows
    assert {"day", "positive_count", "negative_count", "neutral_count", "mixed_count"}.issubset(
        trend_rows[0].keys()
    )


def test_refresh_cache_forces_cache_miss_and_new_row() -> None:
    session = _build_session()
    _seed_sentiment_doc(session)

    service = AnalysisService(session=session, client=None, config=AnalysisConfig(cache_ttl_minutes=60))

    service.generate_sentiment_insight({})
    before = session.execute(text("SELECT COUNT(*) FROM saved_insights")).scalar_one()

    refreshed = service.generate_sentiment_insight({"refresh_cache": True})
    after = session.execute(text("SELECT COUNT(*) FROM saved_insights")).scalar_one()

    assert before == 1
    assert after == 2
    assert refreshed["evidence"][0]["evidence_url"]


def test_complaints_insight_includes_other_category() -> None:
    session = _build_session()
    source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('reddit', 'reddit', 'reddit', '{}')
                """
            )
        ).lastrowid
    )

    for idx, category in enumerate(["other", "sync"]):
        doc_id = int(
            session.execute(
                text(
                    """
                    INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
                    VALUES (:source_id, :external_id, 'Issue', 'Issue body', 'u', 'https://example.com', '2026-03-02T00:00:00', '{}')
                    """
                ),
                {"source_id": source_id, "external_id": f"doc-{idx}"},
            ).lastrowid
        )
        session.execute(
            text(
                """
                INSERT INTO enrichments (document_id, model_name, summary, metadata_json)
                VALUES (:document_id, 'mock', 'summary', :metadata_json)
                """
            ),
            {"document_id": doc_id, "metadata_json": json.dumps({"primary_issue_category": category})},
        )
    session.commit()

    service = AnalysisService(session=session, client=None, config=AnalysisConfig(cache_ttl_minutes=60))
    payload = service.generate_complaints_insight({})

    assert payload["metrics"]["complaint_docs"] == 2
    categories = {row["category"] for row in payload["metrics"]["top_issue_categories"]}
    assert "other" in categories


def test_dashboard_ranked_complaints_can_filter_by_issue_category() -> None:
    session = _build_session()
    source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('reddit', 'reddit', 'reddit', '{}')
                """
            )
        ).lastrowid
    )

    seeded_rows = [
        ("doc-1", "Bug report", "fails to sync", "https://example.com/1", "2026-03-01T10:00:00", "bug", "negative"),
        ("doc-2", "Perf report", "app is slow", "https://example.com/2", "2026-03-02T10:00:00", "performance", "mixed"),
    ]
    for external_id, title, body, url, published_at, category, sentiment in seeded_rows:
        doc_id = int(
            session.execute(
                text(
                    """
                    INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
                    VALUES (:source_id, :external_id, :title, :body, 'u', :url, :published_at, '{}')
                    """
                ),
                {
                    "source_id": source_id,
                    "external_id": external_id,
                    "title": title,
                    "body": body,
                    "url": url,
                    "published_at": published_at,
                },
            ).lastrowid
        )
        session.execute(
            text(
                """
                INSERT INTO enrichments (document_id, model_name, summary, metadata_json)
                VALUES (:document_id, 'mock', 'summary', :metadata_json)
                """
            ),
            {
                "document_id": doc_id,
                "metadata_json": json.dumps(
                    {"primary_issue_category": category, "sentiment_label": sentiment}
                ),
            },
        )
    session.commit()

    complaints = _fetch_ranked_complaints(session, {"issue_category": "bug"})

    assert len(complaints) == 1
    assert complaints[0]["issue_category"] == "bug"
    assert complaints[0]["title"] == "Bug report"


def test_dashboard_ranked_complaints_orders_by_severity_then_recency() -> None:
    session = _build_session()
    source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('reddit', 'reddit', 'reddit', '{}')
                """
            )
        ).lastrowid
    )

    seeded_rows = [
        ("doc-neg-old", "Negative old", "old negative", "https://example.com/neg-old", "2026-03-01T09:00:00", "bug", "negative"),
        ("doc-mixed", "Mixed", "mixed signal", "https://example.com/mixed", "2026-03-03T09:00:00", "bug", "mixed"),
        ("doc-neg-new", "Negative new", "new negative", "https://example.com/neg-new", "2026-03-02T09:00:00", "bug", "negative"),
        ("doc-neutral", "Neutral", "neutral text", "https://example.com/neutral", "2026-03-04T09:00:00", "bug", "neutral"),
    ]
    for external_id, title, body, url, published_at, category, sentiment in seeded_rows:
        doc_id = int(
            session.execute(
                text(
                    """
                    INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
                    VALUES (:source_id, :external_id, :title, :body, 'u', :url, :published_at, '{}')
                    """
                ),
                {
                    "source_id": source_id,
                    "external_id": external_id,
                    "title": title,
                    "body": body,
                    "url": url,
                    "published_at": published_at,
                },
            ).lastrowid
        )
        session.execute(
            text(
                """
                INSERT INTO enrichments (document_id, model_name, summary, metadata_json)
                VALUES (:document_id, 'mock', 'summary', :metadata_json)
                """
            ),
            {
                "document_id": doc_id,
                "metadata_json": json.dumps(
                    {"primary_issue_category": category, "sentiment_label": sentiment}
                ),
            },
        )
    session.commit()

    complaints = _fetch_ranked_complaints(session, {})
    ordered_titles = [row["title"] for row in complaints]

    assert ordered_titles[:4] == ["Negative new", "Negative old", "Mixed", "Neutral"]


def test_insights_page_render_smoke() -> None:
    from app.ui.pages import insights

    payload = {
        "summary": "Mock summary",
        "metrics": {
            "daily_sentiment_trend": [
                {
                    "day": "2026-03-02",
                    "positive_count": 1,
                    "negative_count": 1,
                    "neutral_count": 0,
                    "mixed_count": 0,
                }
            ],
            "daily_complaint_trend": [{"day": "2026-03-02", "complaint_count": 2}],
            "daily_feature_request_trend": [{"day": "2026-03-02", "feature_request_count": 1}],
            "top_issue_categories": [{"category": "bug", "count": 2}],
        },
        "evidence": [],
    }

    insights._render_payload("sentiment", payload, {})
    insights._render_payload("complaints", payload, {})
    insights._render_payload("features", payload, {})


def test_dashboard_where_clause_supports_multi_source_and_web_domain() -> None:
    where_sql, params = _where_clause(
        {
            "sources": ["reddit", "web_reviews"],
            "web_domain": "techradar.com",
        }
    )

    assert "s.name IN" in where_sql
    assert params["source_0"] == "reddit"
    assert params["source_1"] == "web_reviews"
    assert params["web_domain"] == "techradar.com"
