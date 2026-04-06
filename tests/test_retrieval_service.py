from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.services.retrieval_service import RetrievalService


def _build_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    schema_path = Path(__file__).resolve().parents[1] / "app" / "db" / "schema.sql"

    with engine.begin() as connection:
        connection.connection.executescript(schema_path.read_text(encoding="utf-8"))

    return Session(bind=engine, future=True)


def test_search_documents_applies_filters_and_pagination() -> None:
    session = _build_session()
    service = RetrievalService(session)

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

    rows = [
        {
            "external_id": "a",
            "title": "Battery feedback",
            "body": "battery performance details",
            "published_at": "2026-02-03T10:00:00",
            "subreddit": "android",
            "tags": [("product", "pixel"), ("issue", "battery")],
        },
        {
            "external_id": "b",
            "title": "Battery feedback",
            "body": "battery performance details",
            "published_at": "2026-02-01T10:00:00",
            "subreddit": "android",
            "tags": [("product", "pixel"), ("issue", "battery")],
        },
    ]

    for row in rows:
        doc_id = int(
            session.execute(
                text(
                    """
                    INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
                    VALUES (:source_id, :external_id, :title, :body, 'u', 'http://x', :published_at, :raw_json)
                    """
                ),
                {
                    "source_id": source_id,
                    "external_id": row["external_id"],
                    "title": row["title"],
                    "body": row["body"],
                    "published_at": row["published_at"],
                    "raw_json": json.dumps({"subreddit": row["subreddit"]}),
                },
            ).lastrowid
        )
        for tag_type, tag_value in row["tags"]:
            session.execute(
                text(
                    """
                    INSERT INTO document_tags (document_id, tag_type, tag_value)
                    VALUES (:document_id, :tag_type, :tag_value)
                    """
                ),
                {"document_id": doc_id, "tag_type": tag_type, "tag_value": tag_value},
            )
    session.commit()

    results = service.search_documents(
        query="battery",
        filters={"subreddit": "android", "product_tags": ["pixel"], "issue_tags": ["battery"]},
        limit=1,
        offset=0,
    )
    assert len(results) == 1
    assert results[0]["external_id"] == "a"

    next_page = service.search_documents(
        query="battery",
        filters={"subreddit": "android", "product_tags": ["pixel"], "issue_tags": ["battery"]},
        limit=1,
        offset=1,
    )
    assert len(next_page) == 1
    assert next_page[0]["external_id"] == "b"


def test_get_documents_by_ids_preserves_input_order() -> None:
    session = _build_session()
    service = RetrievalService(session)

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

    ids: list[int] = []
    for external_id in ["x", "y", "z"]:
        doc_id = int(
            session.execute(
                text(
                    """
                    INSERT INTO documents (source_id, external_id, title, body)
                    VALUES (:source_id, :external_id, :title, :body)
                    """
                ),
                {
                    "source_id": source_id,
                    "external_id": external_id,
                    "title": external_id,
                    "body": "body",
                },
            ).lastrowid
        )
        ids.append(doc_id)
    session.commit()

    fetched = service.get_documents_by_ids([ids[2], ids[0]])
    assert [row["id"] for row in fetched] == [ids[2], ids[0]]


def test_search_documents_filter_logic_source_date_and_sentiment() -> None:
    session = _build_session()
    service = RetrievalService(session)

    reddit_source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('reddit', 'reddit', 'reddit', '{}')
                """
            )
        ).lastrowid
    )
    forum_source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('forum', 'forum', 'forum', '{}')
                """
            )
        ).lastrowid
    )

    matching_doc_id = int(
        session.execute(
            text(
                """
                INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
                VALUES (:source_id, 'd1', 'Battery issues', 'battery drains quickly', 'u1', 'http://x/1', :published_at, :raw_json)
                """
            ),
            {
                "source_id": reddit_source_id,
                "published_at": "2026-03-12T09:00:00",
                "raw_json": json.dumps({"subreddit": "android"}),
            },
        ).lastrowid
    )
    non_matching_doc_id = int(
        session.execute(
            text(
                """
                INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
                VALUES (:source_id, 'd2', 'Battery note', 'battery drains quickly', 'u2', 'http://x/2', :published_at, :raw_json)
                """
            ),
            {
                "source_id": forum_source_id,
                "published_at": "2026-03-20T09:00:00",
                "raw_json": json.dumps({"subreddit": "android"}),
            },
        ).lastrowid
    )

    session.execute(
        text(
            """
            INSERT INTO enrichments (document_id, metadata_json)
            VALUES (:document_id, :metadata_json)
            """
        ),
        {"document_id": matching_doc_id, "metadata_json": json.dumps({"sentiment_label": "negative"})},
    )
    session.execute(
        text(
            """
            INSERT INTO enrichments (document_id, metadata_json)
            VALUES (:document_id, :metadata_json)
            """
        ),
        {"document_id": non_matching_doc_id, "metadata_json": json.dumps({"sentiment_label": "positive"})},
    )
    session.commit()

    results = service.search_documents(
        query="battery",
        filters={
            "source": "reddit",
            "date_from": "2026-03-10",
            "date_to": "2026-03-15",
            "sentiment_label": "negative",
        },
        limit=20,
        offset=0,
    )

    assert [row["external_id"] for row in results] == ["d1"]


def test_search_documents_supports_multi_source_and_web_domain_filters() -> None:
    session = _build_session()
    service = RetrievalService(session)

    reddit_source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('reddit', 'reddit', 'reddit', '{}')
                """
            )
        ).lastrowid
    )
    web_source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('web_reviews', 'web_reviews', 'web_reviews', '{}')
                """
            )
        ).lastrowid
    )

    session.execute(
        text(
            """
            INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
            VALUES (:source_id, 'reddit-doc', 'Battery post', 'battery details', 'u1', 'http://x/reddit', :published_at, :raw_json)
            """
        ),
        {
            "source_id": reddit_source_id,
            "published_at": "2026-03-15T09:00:00",
            "raw_json": json.dumps({"subreddit": "android"}),
        },
    )
    session.execute(
        text(
            """
            INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
            VALUES (:source_id, 'web-doc-a', 'Battery review', 'battery details', 'u2', 'http://x/web-a', :published_at, :raw_json)
            """
        ),
        {
            "source_id": web_source_id,
            "published_at": "2026-03-15T10:00:00",
            "raw_json": json.dumps({"community_or_channel": "techradar.com"}),
        },
    )
    session.execute(
        text(
            """
            INSERT INTO documents (source_id, external_id, title, body, author, url, published_at, raw_json)
            VALUES (:source_id, 'web-doc-b', 'Battery review', 'battery details', 'u3', 'http://x/web-b', :published_at, :raw_json)
            """
        ),
        {
            "source_id": web_source_id,
            "published_at": "2026-03-16T10:00:00",
            "raw_json": json.dumps({"community_or_channel": "cnet.com"}),
        },
    )
    session.commit()

    mixed_results = service.search_documents(
        query="battery",
        filters={"sources": ["reddit", "web_reviews"]},
        limit=20,
        offset=0,
    )
    assert {row["external_id"] for row in mixed_results} == {"reddit-doc", "web-doc-a", "web-doc-b"}

    narrowed_results = service.search_documents(
        query="battery",
        filters={"sources": ["reddit", "web_reviews"], "web_domain": "techradar.com"},
        limit=20,
        offset=0,
    )
    assert [row["external_id"] for row in narrowed_results] == ["web-doc-a"]
