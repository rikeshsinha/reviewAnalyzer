from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.ingestion.pushshift_client import PushshiftError
from app.ingestion.public_reddit_client import PublicRedditError
from app.ingestion.reddit_rss_client import RedditRssError
from app.jobs.refresh_reddit import (
    _insert_documents,
    _run_public_json_ingestion,
    _run_rss_ingestion,
    _run_pushshift_ingestion,
    _resolve_ingestion_window,
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


def test_resolve_ingestion_window_uses_explicit_env_dates(monkeypatch) -> None:
    monkeypatch.setenv("REDDIT_INGEST_DATE_FROM", "2026-03-01")
    monkeypatch.setenv("REDDIT_INGEST_DATE_TO", "2026-03-07")

    after_dt, before_dt = _resolve_ingestion_window(days_back=30)

    assert after_dt.isoformat() == "2026-03-01T00:00:00+00:00"
    assert before_dt.isoformat() == "2026-03-07T23:59:59.999999+00:00"


def test_run_public_json_ingestion_uses_explicit_window(monkeypatch) -> None:
    observed_window: dict[str, str] = {}

    def _fake_public_json_search(**kwargs):
        observed_window["after_iso"] = kwargs["after_iso"]
        observed_window["before_iso"] = kwargs["before_iso"]
        return []

    monkeypatch.setattr("app.jobs.refresh_reddit.search_public_json_submissions", _fake_public_json_search)
    monkeypatch.setattr("app.jobs.refresh_reddit.fetch_subreddit_new", lambda **kwargs: [])
    monkeypatch.setenv("REDDIT_INGEST_DATE_FROM", "2026-03-01")
    monkeypatch.setenv("REDDIT_INGEST_DATE_TO", "2026-03-07")

    _run_public_json_ingestion({"subreddits": ["android"], "keywords": ["battery"]}, days_back=99)

    assert observed_window["after_iso"] == "2026-03-01T00:00:00+00:00"
    assert observed_window["before_iso"] == "2026-03-07T23:59:59.999999+00:00"


def test_run_public_json_ingestion_continues_after_failed_pair(monkeypatch) -> None:
    calls: list[dict[str, str]] = []

    def _fake_public_json_search(**kwargs):
        calls.append({"subreddit": kwargs["subreddit"], "query": kwargs["query"]})
        if kwargs["query"] == "battery":
            raise PublicRedditError("403 blocked")
        return [
            {
                "id": "ok1",
                "title": "Sleep issue",
                "selftext": "Sleep details",
                "subreddit": kwargs["subreddit"],
                "author": "alice",
                "created_utc": 1_710_000_000,
                "permalink": "/r/android/comments/ok1/test/",
            }
        ]

    monkeypatch.setattr("app.jobs.refresh_reddit.search_public_json_submissions", _fake_public_json_search)
    monkeypatch.setenv("PUBLIC_REDDIT_PAGE_SIZE", "100")
    monkeypatch.setenv("PUBLIC_REDDIT_MAX_PAGES", "2")
    monkeypatch.setenv("PUBLIC_REDDIT_DELAY_SECONDS", "0")

    docs, fetched_count = _run_public_json_ingestion(
        {"subreddits": ["android"], "keywords": ["battery", "sleep"], "post_limit": 10},
        days_back=7,
    )

    assert calls == [{"subreddit": "android", "query": "battery"}, {"subreddit": "android", "query": "sleep"}]
    assert fetched_count == 1
    assert len(docs) == 1
    assert docs[0]["external_id"] == "ok1"


def test_run_public_json_ingestion_falls_back_to_new_when_search_empty(monkeypatch) -> None:
    def _fake_public_json_search(**kwargs):
        return []

    def _fake_fetch_subreddit_new(**kwargs):
        return [
            {
                "id": "new1",
                "title": "Samsung Health sleep tracking",
                "selftext": "sleep details",
                "subreddit": kwargs["subreddit"],
                "author": "alice",
                "created_utc": 1_710_000_000,
                "permalink": "/r/android/comments/new1/test/",
            }
        ]

    monkeypatch.setattr("app.jobs.refresh_reddit.search_public_json_submissions", _fake_public_json_search)
    monkeypatch.setattr("app.jobs.refresh_reddit.fetch_subreddit_new", _fake_fetch_subreddit_new)

    diagnostics: dict[str, object] = {}
    docs, fetched_count = _run_public_json_ingestion(
        {"subreddits": ["android"], "keywords": ["sleep"], "post_limit": 10},
        days_back=7,
        fetch_diagnostics=diagnostics,
    )

    assert fetched_count == 1
    assert docs[0]["external_id"] == "new1"
    assert diagnostics["search_hits_total"] == 0
    assert diagnostics["new_fallback_hits_total"] == 1


def test_run_public_json_ingestion_fallback_keyword_filter_is_case_insensitive(monkeypatch) -> None:
    monkeypatch.setattr("app.jobs.refresh_reddit.search_public_json_submissions", lambda **kwargs: [])
    monkeypatch.setattr(
        "app.jobs.refresh_reddit.fetch_subreddit_new",
        lambda **kwargs: [
            {
                "id": "new2",
                "title": "SLEEP report",
                "selftext": "tracking details",
                "subreddit": kwargs["subreddit"],
                "author": "alice",
                "created_utc": 1_710_000_000,
                "permalink": "/r/android/comments/new2/test/",
            }
        ],
    )

    docs, fetched_count = _run_public_json_ingestion(
        {"subreddits": ["android"], "keywords": ["sleep"], "post_limit": 10},
        days_back=7,
    )

    assert fetched_count == 1
    assert docs[0]["external_id"] == "new2"


def test_run_public_json_ingestion_returns_recent_when_keyword_filter_empty_if_enabled(monkeypatch) -> None:
    monkeypatch.setattr("app.jobs.refresh_reddit.search_public_json_submissions", lambda **kwargs: [])
    monkeypatch.setattr(
        "app.jobs.refresh_reddit.fetch_subreddit_new",
        lambda **kwargs: [
            {
                "id": "new3",
                "title": "general update",
                "selftext": "no keyword",
                "subreddit": kwargs["subreddit"],
                "author": "alice",
                "created_utc": 1_710_000_000,
                "permalink": "/r/android/comments/new3/test/",
            }
        ],
    )
    monkeypatch.setenv("PUBLIC_REDDIT_INCLUDE_RECENT_WHEN_NO_KEYWORD_HITS", "true")

    docs, fetched_count = _run_public_json_ingestion(
        {"subreddits": ["android"], "keywords": ["sleep"], "post_limit": 10},
        days_back=7,
    )

    assert fetched_count == 1
    assert docs[0]["external_id"] == "new3"


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

    def _fake_run_pushshift_ingestion(
        config: dict[str, object], *, days_back: int, ingestion_window: object | None = None
    ):
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
    run_row = session.execute(
        text("SELECT status, records_fetched, records_inserted, error_message FROM ingestion_runs ORDER BY id DESC LIMIT 1")
    ).first()
    assert run_row is not None
    diagnostics = json.loads(run_row.error_message)

    assert stats == {"records_fetched": 1, "records_inserted": 1}
    assert pushshift_calls == [
        {
            "config": {"subreddits": ["android"], "keywords": ["battery"], "post_limit": 10},
            "days_back": 7,
        }
    ]
    assert payload["platform"] == "reddit"
    assert payload["raw_payload"] == {"id": "abc123", "source": "pushshift"}
    assert run_row.status == "completed"
    assert run_row.records_fetched == 1
    assert run_row.records_inserted == 1
    assert diagnostics["backend_requested"] == "pushshift"
    assert diagnostics["backend_used"] == "pushshift"
    assert diagnostics["counters"]["normalize_count"] == 1
    assert diagnostics["counters"]["inserted_count"] == 1


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

    def _fake_run_public_json_ingestion(
        config: dict[str, object],
        *,
        days_back: int,
        ingestion_window: object | None = None,
        fetch_diagnostics: dict[str, object] | None = None,
    ):
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

    def _fake_run_pushshift_ingestion(
        config: dict[str, object], *, days_back: int, ingestion_window: object | None = None
    ):
        raise PushshiftError("Pushshift request failed")

    fallback_calls: list[dict[str, object]] = []

    def _fake_run_public_json_ingestion(
        config: dict[str, object],
        *,
        days_back: int,
        ingestion_window: object | None = None,
        fetch_diagnostics: dict[str, object] | None = None,
    ):
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
    run_row = session.execute(text("SELECT error_message FROM ingestion_runs ORDER BY id DESC LIMIT 1")).first()
    assert run_row is not None
    diagnostics = json.loads(run_row.error_message)

    assert stats == {"records_fetched": 1, "records_inserted": 1}
    assert fallback_calls == [
        {
            "config": {"subreddits": ["android"], "keywords": ["workout"], "post_limit": 10},
            "days_back": 7,
        }
    ]
    assert diagnostics["fallback_activated"] is True
    assert diagnostics["backend_used"] == "public_json"


def test_run_for_platform_public_json_persists_fetch_diagnostics(monkeypatch) -> None:
    session = _build_session()
    monkeypatch.setattr("app.jobs.refresh_reddit.SessionLocal", lambda: session)
    monkeypatch.setenv("REDDIT_FETCH_BACKEND", "public_json")
    monkeypatch.setattr("app.jobs.refresh_reddit.search_public_json_submissions", lambda **kwargs: [])
    monkeypatch.setattr(
        "app.jobs.refresh_reddit.fetch_subreddit_new",
        lambda **kwargs: [
            {
                "id": "diag1",
                "title": "sleep note",
                "selftext": "sleep details",
                "subreddit": kwargs["subreddit"],
                "author": "alice",
                "created_utc": 1_710_000_000,
                "permalink": "/r/android/comments/diag1/test/",
            }
        ],
    )

    stats = run_for_platform(
        "reddit",
        {"subreddits": ["android"], "keywords": ["sleep"], "post_limit": 10},
        days_back=7,
    )
    run_row = session.execute(text("SELECT error_message FROM ingestion_runs ORDER BY id DESC LIMIT 1")).first()
    assert run_row is not None
    diagnostics = json.loads(run_row.error_message)

    assert stats == {"records_fetched": 1, "records_inserted": 1}
    assert diagnostics["fetch_diagnostics"]["queries_attempted"] == 1
    assert diagnostics["fetch_diagnostics"]["search_hits_total"] == 0
    assert diagnostics["fetch_diagnostics"]["new_fallback_hits_total"] == 1


def test_run_rss_ingestion_returns_normalized_docs(monkeypatch) -> None:
    def _fake_search_rss_submissions(**kwargs):
        assert kwargs["subreddit"] == "android"
        return [
            {
                "id": "rss1",
                "title": "RSS title",
                "selftext": "RSS content",
                "subreddit": "android",
                "author": "dora",
                "created_utc": 1_710_000_000,
                "permalink": "/r/android/comments/rss1/test/",
            }
        ]

    monkeypatch.setattr("app.jobs.refresh_reddit.search_rss_submissions", _fake_search_rss_submissions)

    docs, fetched_count = _run_rss_ingestion(
        {"subreddits": ["android"], "keywords": ["battery"], "post_limit": 10},
        days_back=7,
    )

    assert fetched_count == 1
    assert len(docs) == 1
    assert docs[0]["external_id"] == "rss1"
    assert docs[0]["entity_type"] == "post"
    assert docs[0]["source"] == "reddit"


def test_run_for_platform_falls_back_to_rss_when_public_json_fails(monkeypatch) -> None:
    session = _build_session()
    fallback_docs = [
        {
            "external_id": "rssfb1",
            "title": "RSS fallback",
            "content": "Recovered via RSS",
            "author": "erin",
            "url": "https://reddit.com/r/android/comments/rssfb1/test/",
            "created_at": "2026-03-10T00:00:00+00:00",
            "parent_external_id": None,
            "doc_type": "post",
            "entity_type": "post",
            "platform": "reddit",
            "community_or_channel": "android",
            "subreddit": "android",
            "platform_metadata": {"subreddit": "android", "parent_external_id": None},
            "ingestion_ts": "2026-03-10T00:00:01+00:00",
            "dedupe_key": "reddit:rssfb1",
            "raw_payload": {"id": "rssfb1", "source": "rss"},
        }
    ]

    def _fake_public_json(
        config: dict[str, object],
        *,
        days_back: int,
        ingestion_window: object | None = None,
        fetch_diagnostics: dict[str, object] | None = None,
    ):
        raise PublicRedditError("public json blocked")

    rss_calls: list[dict[str, object]] = []

    def _fake_rss(config: dict[str, object], *, days_back: int, ingestion_window: object | None = None):
        rss_calls.append({"config": config, "days_back": days_back})
        return fallback_docs, len(fallback_docs)

    monkeypatch.setattr("app.jobs.refresh_reddit.SessionLocal", lambda: session)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_public_json_ingestion", _fake_public_json)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_rss_ingestion", _fake_rss)
    monkeypatch.setenv("REDDIT_FETCH_BACKEND", "public_json")

    stats = run_for_platform(
        "reddit",
        {"subreddits": ["android"], "keywords": ["workout"], "post_limit": 10},
        days_back=7,
    )

    assert stats == {"records_fetched": 1, "records_inserted": 1}
    assert rss_calls == [
        {
            "config": {"subreddits": ["android"], "keywords": ["workout"], "post_limit": 10},
            "days_back": 7,
        }
    ]


def test_run_for_platform_uses_full_no_key_failover_chain(monkeypatch) -> None:
    session = _build_session()
    rss_docs = [
        {
            "external_id": "rsschain1",
            "title": "Chain fallback",
            "content": "Recovered after two failures",
            "author": "frank",
            "url": "https://reddit.com/r/android/comments/rsschain1/test/",
            "created_at": "2026-03-10T00:00:00+00:00",
            "parent_external_id": None,
            "doc_type": "post",
            "entity_type": "post",
            "platform": "reddit",
            "community_or_channel": "android",
            "subreddit": "android",
            "platform_metadata": {"subreddit": "android", "parent_external_id": None},
            "ingestion_ts": "2026-03-10T00:00:01+00:00",
            "dedupe_key": "reddit:rsschain1",
            "raw_payload": {"id": "rsschain1", "source": "rss"},
        }
    ]

    def _fake_pushshift(config: dict[str, object], *, days_back: int, ingestion_window: object | None = None):
        raise PushshiftError("pushshift host 403")

    def _fake_public_json(
        config: dict[str, object],
        *,
        days_back: int,
        ingestion_window: object | None = None,
        fetch_diagnostics: dict[str, object] | None = None,
    ):
        raise PublicRedditError("public json host 403")

    def _fake_rss(config: dict[str, object], *, days_back: int, ingestion_window: object | None = None):
        return rss_docs, len(rss_docs)

    monkeypatch.setattr("app.jobs.refresh_reddit.SessionLocal", lambda: session)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_pushshift_ingestion", _fake_pushshift)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_public_json_ingestion", _fake_public_json)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_rss_ingestion", _fake_rss)
    monkeypatch.setenv("REDDIT_FETCH_BACKEND", "pushshift")

    stats = run_for_platform(
        "reddit",
        {"subreddits": ["android"], "keywords": ["workout"], "post_limit": 10},
        days_back=7,
    )

    assert stats == {"records_fetched": 1, "records_inserted": 1}


def test_run_for_platform_marks_failed_when_all_failovers_return_zero_docs(monkeypatch) -> None:
    session = _build_session()

    def _fake_pushshift(config: dict[str, object], *, days_back: int, ingestion_window: object | None = None):
        return [], 0

    def _fake_public_json(
        config: dict[str, object],
        *,
        days_back: int,
        ingestion_window: object | None = None,
        fetch_diagnostics: dict[str, object] | None = None,
    ):
        return [], 0

    def _fake_rss(config: dict[str, object], *, days_back: int, ingestion_window: object | None = None):
        return [], 0

    monkeypatch.setattr("app.jobs.refresh_reddit.SessionLocal", lambda: session)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_pushshift_ingestion", _fake_pushshift)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_public_json_ingestion", _fake_public_json)
    monkeypatch.setattr("app.jobs.refresh_reddit._run_rss_ingestion", _fake_rss)
    monkeypatch.setenv("REDDIT_FETCH_BACKEND", "pushshift")

    try:
        run_for_platform(
            "reddit",
            {"subreddits": ["android"], "keywords": ["workout"], "post_limit": 10},
            days_back=7,
        )
        assert False, "Expected run_for_platform to fail when all failovers return zero docs"
    except RuntimeError as exc:
        assert "all backend attempts returned zero docs" in str(exc)

    run_row = session.execute(
        text("SELECT status, error_message FROM ingestion_runs ORDER BY id DESC LIMIT 1")
    ).first()
    assert run_row is not None
    assert run_row.status == "failed"
    diagnostics = json.loads(run_row.error_message)
    assert diagnostics["first_failing_stage"] == "fetch"
    assert "returned zero docs" in diagnostics["error_summary"]
