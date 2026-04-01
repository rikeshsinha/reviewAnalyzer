from __future__ import annotations

from app.ingestion.normalizers import normalize_pushshift_submission
from app.jobs.refresh_reddit import _run_pushshift_ingestion


def test_pushshift_submission_normalizes_to_canonical_fields() -> None:
    raw = {
        "id": "abc123",
        "title": "Battery issue",
        "selftext": "Battery drains quickly",
        "subreddit": "android",
        "author": "alice",
        "created_utc": 1_710_000_000,
        "full_link": "https://reddit.com/r/android/comments/abc123/test/",
        "permalink": "/r/android/comments/abc123/test/",
    }

    normalized = normalize_pushshift_submission(raw)

    assert normalized["source"] == "reddit"
    assert normalized["platform"] == "reddit"
    assert normalized["doc_type"] == "post"
    assert normalized["entity_type"] == "post"
    assert normalized["external_id"] == "abc123"
    assert normalized["community_or_channel"] == "android"
    assert normalized["platform_metadata"]["subreddit"] == "android"
    assert normalized["author"] == "alice"
    assert normalized["title"] == "Battery issue"
    assert normalized["content"] == "Battery drains quickly"
    assert normalized["url"] == "https://reddit.com/r/android/comments/abc123/test/"
    assert normalized["created_at"] is not None
    assert normalized["ingestion_ts"] is not None
    assert normalized["dedupe_key"] is None
    assert normalized["raw_payload"] == raw


def test_pushshift_permalink_fallback_constructs_url() -> None:
    raw = {
        "id": "abc123",
        "title": "Battery issue",
        "selftext": "Battery drains quickly",
        "subreddit": "android",
        "author": "alice",
        "created_utc": 1_710_000_000,
        "permalink": "/r/android/comments/abc123/test/",
    }

    normalized = normalize_pushshift_submission(raw)

    assert normalized["url"] == "https://reddit.com/r/android/comments/abc123/test/"


def test_run_pushshift_ingestion_dedupes_same_external_id_across_fetch_batches(monkeypatch) -> None:
    def _fake_search_submissions(**kwargs):
        if kwargs["query"] == "battery":
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
        return [
            {
                "id": "abc123",
                "title": "Battery issue duplicate",
                "selftext": "Battery drains quickly duplicate",
                "subreddit": kwargs["subreddit"],
                "author": "bob",
                "created_utc": 1_710_000_001,
                "permalink": "/r/android/comments/abc123/test2/",
            }
        ]

    monkeypatch.setattr("app.jobs.refresh_reddit.search_submissions", _fake_search_submissions)

    docs, fetched_count = _run_pushshift_ingestion(
        {"subreddits": ["android"], "keywords": ["battery", "drain"], "post_limit": 10},
        days_back=7,
    )

    assert fetched_count == 1
    assert len(docs) == 1
    assert docs[0]["external_id"] == "abc123"
    assert docs[0]["title"] == "Battery issue"
