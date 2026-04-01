from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.jobs.refresh_reddit import _insert_documents, _safe_ensure_dedupe_constraints
from app.utils.hashing import make_dedupe_key


def _build_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    schema_path = Path(__file__).resolve().parents[1] / "app" / "db" / "schema.sql"

    with engine.begin() as connection:
        connection.connection.executescript(schema_path.read_text(encoding="utf-8"))

    return Session(bind=engine, future=True)


def test_insert_documents_dedupes_duplicate_external_ids() -> None:
    session = _build_session()
    _safe_ensure_dedupe_constraints(session)

    source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('reddit', 'reddit', 'Reddit', '{}')
                """
            )
        ).lastrowid
    )

    docs = [
        {
            "external_id": "same-id",
            "title": "Battery issue",
            "content": "The battery drains",
            "author": "user1",
            "url": "https://reddit.com/r/a",
            "created_at": "2026-03-10T00:00:00",
            "parent_external_id": None,
            "doc_type": "post",
            "entity_type": "post",
            "platform": "reddit",
            "community_or_channel": "android",
            "subreddit": "android",
            "platform_metadata": {"subreddit": "android", "parent_external_id": None},
            "ingestion_ts": "2026-03-10T00:00:01",
            "dedupe_key": None,
            "raw_payload": {"x": 1},
        },
        {
            "external_id": "same-id",
            "title": "Battery issue duplicate",
            "content": "The battery drains duplicate",
            "author": "user2",
            "url": "https://reddit.com/r/b",
            "created_at": "2026-03-10T00:01:00",
            "parent_external_id": None,
            "doc_type": "post",
            "entity_type": "post",
            "platform": "reddit",
            "community_or_channel": "android",
            "subreddit": "android",
            "platform_metadata": {"subreddit": "android", "parent_external_id": None},
            "ingestion_ts": "2026-03-10T00:01:01",
            "dedupe_key": None,
            "raw_payload": {"x": 2},
        },
    ]

    inserted = _insert_documents(session, source_id, docs)
    count = session.execute(text("SELECT COUNT(*) FROM documents")).scalar_one()
    raw_json = session.execute(text("SELECT raw_json FROM documents LIMIT 1")).scalar_one()
    payload = json.loads(raw_json)

    assert inserted == 1
    assert count == 1
    assert payload["platform"] == "reddit"
    assert payload["entity_type"] == "post"
    assert payload["community_or_channel"] == "android"
    assert payload["platform_metadata"]["subreddit"] == "android"


def test_insert_documents_skips_duplicate_google_play_fallback_dedupe() -> None:
    session = _build_session()
    _safe_ensure_dedupe_constraints(session)

    source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('google_play', 'google_play', 'Google Play', '{}')
                """
            )
        ).lastrowid
    )
    dedupe_key = make_dedupe_key(
        "google_play",
        None,
        app_id="com.test.app",
        author="alice",
        created_at="2026-03-10T00:00:00+00:00",
        text="Battery drains fast",
    )
    docs = [
        {
            "external_id": None,
            "title": None,
            "content": "Battery drains fast",
            "author": "alice",
            "url": "https://play.google.com/store/apps/details?id=com.test.app",
            "created_at": "2026-03-10T00:00:00+00:00",
            "doc_type": "review",
            "entity_type": "review",
            "platform": "google_play",
            "community_or_channel": "com.test.app",
            "platform_metadata": {"app_id": "com.test.app"},
            "ingestion_ts": "2026-03-10T00:00:01+00:00",
            "dedupe_key": dedupe_key,
            "raw_payload": {"x": 1},
        },
        {
            "external_id": None,
            "title": None,
            "content": "Battery drains fast",
            "author": "alice",
            "url": "https://play.google.com/store/apps/details?id=com.test.app",
            "created_at": "2026-03-10T00:00:00+00:00",
            "doc_type": "review",
            "entity_type": "review",
            "platform": "google_play",
            "community_or_channel": "com.test.app",
            "platform_metadata": {"app_id": "com.test.app"},
            "ingestion_ts": "2026-03-10T00:00:02+00:00",
            "dedupe_key": dedupe_key,
            "raw_payload": {"x": 2},
        },
    ]

    inserted = _insert_documents(session, source_id, docs)
    count = session.execute(text("SELECT COUNT(*) FROM documents")).scalar_one()

    assert inserted == 1
    assert count == 1


def test_same_text_across_platforms_not_deduped() -> None:
    session = _build_session()
    _safe_ensure_dedupe_constraints(session)

    gp_source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('google_play', 'google_play', 'Google Play', '{}')
                """
            )
        ).lastrowid
    )
    reddit_source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES ('reddit', 'reddit', 'Reddit', '{}')
                """
            )
        ).lastrowid
    )

    common_text = "Battery drains fast"
    gp_docs = [
        {
            "external_id": None,
            "title": None,
            "content": common_text,
            "author": "alice",
            "url": "https://play.google.com/store/apps/details?id=com.test.app",
            "created_at": "2026-03-10T00:00:00+00:00",
            "doc_type": "review",
            "entity_type": "review",
            "platform": "google_play",
            "community_or_channel": "com.test.app",
            "platform_metadata": {"app_id": "com.test.app"},
            "ingestion_ts": "2026-03-10T00:00:01+00:00",
            "dedupe_key": make_dedupe_key(
                "google_play",
                None,
                app_id="com.test.app",
                author="alice",
                created_at="2026-03-10T00:00:00+00:00",
                text=common_text,
            ),
            "raw_payload": {"x": 1},
        }
    ]
    reddit_docs = [
        {
            "external_id": None,
            "title": None,
            "content": common_text,
            "author": "alice",
            "url": "https://reddit.com/r/android",
            "created_at": "2026-03-10T00:00:00+00:00",
            "doc_type": "comment",
            "entity_type": "comment",
            "platform": "reddit",
            "community_or_channel": "android",
            "platform_metadata": {"subreddit": "android"},
            "ingestion_ts": "2026-03-10T00:00:01+00:00",
            "dedupe_key": make_dedupe_key(
                "reddit",
                None,
                app_id=None,
                author="alice",
                created_at="2026-03-10T00:00:00+00:00",
                text=common_text,
            ),
            "raw_payload": {"x": 1},
        }
    ]

    gp_inserted = _insert_documents(session, gp_source_id, gp_docs)
    reddit_inserted = _insert_documents(session, reddit_source_id, reddit_docs)
    count = session.execute(text("SELECT COUNT(*) FROM documents")).scalar_one()

    assert gp_inserted == 1
    assert reddit_inserted == 1
    assert count == 2
