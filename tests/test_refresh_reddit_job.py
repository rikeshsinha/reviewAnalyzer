from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.ingestion.pushshift_client import PushshiftError
from app.jobs.refresh_reddit import (
    _insert_documents,
    _run_public_json_ingestion,
    _run_pushshift_ingestion,
    _safe_ensure_dedupe_constraints,
    run_for_platform,
)
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
            "rating": 2,
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
            "rating": 1,
            "ingestion_ts": "2026-03-10T00:00:02+00:00",
            "dedupe_key": dedupe_key,
            "raw_payload": {"x": 2},
        },
    ]

    inserted = _insert_documents(session, source_id, docs)
    count = session.execute(text("SELECT COUNT(*) FROM documents")).scalar_one()
    raw_json = session.execute(text("SELECT raw_json FROM documents LIMIT 1")).scalar_one()
    payload = json.loads(raw_json)

    assert inserted == 1
    assert count == 1
    assert payload["rating"] == 2


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


def test_run_pushshift_ingestion_normalizes_and_dedupes(monkeypatch) -> None:
    calls: list[dict[str, str]] = []

    def _fake_search_submissions(**kwargs):
        calls.append({"subreddit": kwargs["subreddit"], "query": kwargs["query"]})
        return [
            {
                "id": "abc123",
                "title": "Battery issue",
                "selftext": "Battery drains quickly",
                "subreddit": kwargs["subreddit"],
                "author": "alice",
                "created_utc": 1_710_000_000,
                "permalink": "/r/android/comments/abc123/test/",
            }
        ]

    monkeypatch.setattr("app.jobs.refresh_reddit.search_submissions", _fake_search_submissions)
    monkeypatch.setenv("REDDIT_PUSHSHIFT_BASE_URL", "https://example.pushshift.invalid/reddit/search/submission/")

    docs, fetched_count = _run_pushshift_ingestion(
        {"subreddits": ["android"], "keywords": ["battery", "drain"], "post_limit": 10},
        days_back=7,
    )

    assert fetched_count == 1
    assert len(docs) == 1
    assert docs[0]["source"] == "reddit"
    assert docs[0]["entity_type"] == "post"
    assert docs[0]["external_id"] == "abc123"
    assert calls == [{"subreddit": "android", "query": "battery"}, {"subreddit": "android", "query": "drain"}]


def test_run_for_platform_uses_pushshift_backend_and_persists_reddit_payload(monkeypatch) -> None:
    session = _build_session()

    docs = [
        {
            "external_id": "abc123",
            "title": "Battery issue",
            "content": "Battery drains quickly",
            "author": "alice",
            "url": "https://reddit.com/r/android/comments/abc123/test/",
            "created_at": "2026-03-10T00:00:00+00:00",
            "parent_external_id": None,
            "doc_type": "post",
            "entity_type": "post",
            "platform": "reddit",
            "community_or_channel": "android",
            "subreddit": "android",
            "platform_metadata": {"subreddit": "android", "parent_external_id": None},
            "ingestion_ts": "2026-03-10T00:00:01+00:00",
            "dedupe_key": "reddit:abc123",
            "raw_payload": {"id": "abc123", "source": "pushshift"},
        }
    ]

    pushshift_calls: list[dict[str, object]] = []

    def _fake_run_pushshift_ingestion(config: dict[str, object], *, days_back: int):
        pushshift_calls.append({"config": config, "days_back": days_back})
        return docs, len(docs)

    monkeypatch.setattr("app.jobs.refresh_reddit.SessionLocal", lambda: session)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_pushshift_ingestion", _fake_run_pushshift_ingestion)
    monkeypatch.setenv("REDDIT_FETCH_BACKEND", "pushshift")

    stats = run_for_platform(
        "reddit",
        {"subreddits": ["android"], "keywords": ["battery"], "post_limit": 10},
        days_back=7,
    )

    row = session.execute(text("SELECT raw_json FROM documents WHERE external_id='abc123' LIMIT 1")).first()
    assert row is not None
    payload = json.loads(row.raw_json)

    assert stats == {"records_fetched": 1, "records_inserted": 1}
    assert pushshift_calls == [
        {
            "config": {"subreddits": ["android"], "keywords": ["battery"], "post_limit": 10},
            "days_back": 7,
        }
    ]
    assert payload["platform"] == "reddit"
    assert payload["raw_payload"] == {"id": "abc123", "source": "pushshift"}


def test_run_for_platform_uses_public_json_backend(monkeypatch) -> None:
    session = _build_session()

    docs = [
        {
            "external_id": "pjson1",
            "title": "Sleep tracking",
            "content": "Sleep tracking is inaccurate",
            "author": "bob",
            "url": "https://reddit.com/r/android/comments/pjson1/test/",
            "created_at": "2026-03-10T00:00:00+00:00",
            "parent_external_id": None,
            "doc_type": "post",
            "entity_type": "post",
            "platform": "reddit",
            "community_or_channel": "android",
            "subreddit": "android",
            "platform_metadata": {"subreddit": "android", "parent_external_id": None},
            "ingestion_ts": "2026-03-10T00:00:01+00:00",
            "dedupe_key": "reddit:pjson1",
            "raw_payload": {"id": "pjson1", "source": "public_json"},
        }
    ]

    public_json_calls: list[dict[str, object]] = []

    def _fake_run_public_json_ingestion(config: dict[str, object], *, days_back: int):
        public_json_calls.append({"config": config, "days_back": days_back})
        return docs, len(docs)

    monkeypatch.setattr("app.jobs.refresh_reddit.SessionLocal", lambda: session)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_public_json_ingestion", _fake_run_public_json_ingestion)
    monkeypatch.setenv("REDDIT_FETCH_BACKEND", "public_json")

    stats = run_for_platform(
        "reddit",
        {"subreddits": ["android"], "keywords": ["sleep"], "post_limit": 10},
        days_back=7,
    )

    row = session.execute(text("SELECT raw_json FROM documents WHERE external_id='pjson1' LIMIT 1")).first()
    assert row is not None
    payload = json.loads(row.raw_json)

    assert stats == {"records_fetched": 1, "records_inserted": 1}
    assert public_json_calls == [
        {
            "config": {"subreddits": ["android"], "keywords": ["sleep"], "post_limit": 10},
            "days_back": 7,
        }
    ]
    assert payload["raw_payload"] == {"id": "pjson1", "source": "public_json"}


def test_run_for_platform_falls_back_to_public_json_when_pushshift_fails(monkeypatch) -> None:
    session = _build_session()

    fallback_docs = [
        {
            "external_id": "fallback1",
            "title": "Workout",
            "content": "Workout sync issue",
            "author": "carol",
            "url": "https://reddit.com/r/android/comments/fallback1/test/",
            "created_at": "2026-03-10T00:00:00+00:00",
            "parent_external_id": None,
            "doc_type": "post",
            "entity_type": "post",
            "platform": "reddit",
            "community_or_channel": "android",
            "subreddit": "android",
            "platform_metadata": {"subreddit": "android", "parent_external_id": None},
            "ingestion_ts": "2026-03-10T00:00:01+00:00",
            "dedupe_key": "reddit:fallback1",
            "raw_payload": {"id": "fallback1", "source": "public_json"},
        }
    ]

    def _fake_run_pushshift_ingestion(config: dict[str, object], *, days_back: int):
        raise PushshiftError("Pushshift request failed")

    fallback_calls: list[dict[str, object]] = []

    def _fake_run_public_json_ingestion(config: dict[str, object], *, days_back: int):
        fallback_calls.append({"config": config, "days_back": days_back})
        return fallback_docs, len(fallback_docs)

    monkeypatch.setattr("app.jobs.refresh_reddit.SessionLocal", lambda: session)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_pushshift_ingestion", _fake_run_pushshift_ingestion)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_public_json_ingestion", _fake_run_public_json_ingestion)
    monkeypatch.setenv("REDDIT_FETCH_BACKEND", "pushshift")

    stats = run_for_platform(
        "reddit",
        {"subreddits": ["android"], "keywords": ["workout"], "post_limit": 10},
        days_back=7,
    )

    assert stats == {"records_fetched": 1, "records_inserted": 1}
    assert fallback_calls == [
        {
            "config": {"subreddits": ["android"], "keywords": ["workout"], "post_limit": 10},
            "days_back": 7,
        }
    ]


def test_run_public_json_ingestion_skips_failed_pairs_and_continues(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def _fake_search_public_json_submissions(**kwargs):
        subreddit = kwargs["subreddit"]
        query = kwargs["query"]
        calls.append((subreddit, query))
        if query == "bad":
            from app.ingestion.public_reddit_client import PublicRedditError

            raise PublicRedditError("403")
        return [
            {
                "id": f"{subreddit}-{query}",
                "title": "Title",
                "selftext": "Body",
                "subreddit": subreddit,
                "author": "alice",
                "created_utc": 1_710_000_000,
                "permalink": f"/r/{subreddit}/comments/{subreddit}-{query}/x/",
            }
        ]

    monkeypatch.setattr(
        "app.jobs.refresh_reddit.search_public_json_submissions",
        _fake_search_public_json_submissions,
    )

    docs, fetched_count = _run_public_json_ingestion(
        {"subreddits": ["GalaxyWatch"], "keywords": ["bad", "good"]},
        days_back=30,
    )

    assert calls == [("GalaxyWatch", "bad"), ("GalaxyWatch", "good")]
    assert fetched_count == 1
    assert len(docs) == 1
    assert docs[0]["external_id"] == "GalaxyWatch-good"
