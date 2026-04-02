from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.services.analysis_service import AnalysisConfig, AnalysisService


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

    doc_id = int(
        session.execute(
            text(
                """
                INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
                VALUES (:source_id, 'x', 'Battery issue', 'battery drains quickly', 'u', 'https://example.com', '2026-03-02T00:00:00', '{}')
                """
            ),
            {"source_id": source_id},
        ).lastrowid
    )

    session.execute(
        text(
            """
            INSERT INTO enrichments (document_id, model_name, summary, metadata_json)
            VALUES (:document_id, 'mock', 'summary', :metadata_json)
            """
        ),
        {"document_id": doc_id, "metadata_json": json.dumps({"sentiment_label": "negative"})},
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
