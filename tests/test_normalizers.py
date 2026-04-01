from __future__ import annotations

from types import SimpleNamespace

from app.ingestion.normalizers import (
    normalize_comment,
    normalize_pushshift_submission,
    normalize_submission,
)
from app.utils.hashing import make_dedupe_key


def test_normalize_submission_output_shape() -> None:
    raw = SimpleNamespace(
        id="sub_1",
        title="Battery issue",
        selftext="Drains overnight",
        subreddit="android",
        author=SimpleNamespace(name="alice"),
        created_utc=1700000000,
        url="https://reddit.com/r/android/sub_1",
    )

    normalized = normalize_submission(raw)

    assert normalized["doc_type"] == "post"
    assert normalized["external_id"] == "sub_1"
    assert normalized["parent_external_id"] is None
    assert normalized["title"] == "Battery issue"
    assert normalized["content"] == "Drains overnight"
    assert normalized["source"] == "reddit"
    assert normalized["platform"] == "reddit"
    assert normalized["entity_type"] == "post"
    assert normalized["community_or_channel"] == "android"
    assert normalized["platform_metadata"]["subreddit"] == "android"
    assert normalized["platform_metadata"]["parent_external_id"] is None
    assert isinstance(normalized["raw_payload"], dict)


def test_normalize_comment_output_shape() -> None:
    parent = SimpleNamespace(id="sub_parent")
    raw = SimpleNamespace(
        id="c_1",
        body="Same here",
        subreddit="android",
        author=SimpleNamespace(name="bob"),
        created_utc=1700000100,
        link_id="t3_sub_parent",
        permalink="/r/android/comments/sub_parent/c_1/",
    )

    normalized = normalize_comment(raw, parent_submission=parent)

    assert normalized["doc_type"] == "comment"
    assert normalized["external_id"] == "c_1"
    assert normalized["parent_external_id"] == "sub_parent"
    assert normalized["entity_type"] == "comment"
    assert normalized["community_or_channel"] == "android"
    assert normalized["platform_metadata"]["subreddit"] == "android"
    assert normalized["platform_metadata"]["parent_external_id"] == "sub_parent"
    assert normalized["title"] is None
    assert normalized["content"] == "Same here"
    assert normalized["url"].startswith("https://reddit.com/")
    assert isinstance(normalized["raw_payload"], dict)


def test_normalize_pushshift_submission_output_shape() -> None:
    raw = {
        "id": "sub_2",
        "title": "Need help with battery",
        "selftext": "",
        "subreddit": "android",
        "author": "charlie",
        "created_utc": 1700000200,
        "permalink": "/r/android/comments/sub_2/need_help_with_battery/",
    }

    normalized = normalize_pushshift_submission(raw)

    assert normalized["doc_type"] == "post"
    assert normalized["external_id"] == "sub_2"
    assert normalized["parent_external_id"] is None
    assert normalized["entity_type"] == "post"
    assert normalized["title"] == "Need help with battery"
    assert normalized["content"] == "Need help with battery"
    assert normalized["community_or_channel"] == "android"
    assert normalized["platform_metadata"]["subreddit"] == "android"
    assert normalized["platform_metadata"]["parent_external_id"] is None
    assert (
        normalized["url"]
        == "https://reddit.com/r/android/comments/sub_2/need_help_with_battery/"
    )
    assert normalized["raw_payload"] is raw


def test_make_dedupe_key_uses_fallback_hash_for_missing_external_id() -> None:
    key = make_dedupe_key(
        "google_play",
        None,
        app_id="com.test.app",
        author="Alice",
        created_at="2026-03-01T00:00:00+00:00",
        text=" Battery drains   FAST ",
    )

    equivalent_key = make_dedupe_key(
        "google_play",
        None,
        app_id="com.test.app",
        author="alice",
        created_at="2026-03-01T00:00:00+00:00",
        text="battery   drains fast",
    )

    assert key is not None
    assert key.startswith("google_play:fallback:")
    assert key == equivalent_key


def test_make_dedupe_key_returns_none_when_external_id_exists() -> None:
    key = make_dedupe_key(
        "reddit",
        "abc123",
        app_id=None,
        author="alice",
        created_at="2026-03-01T00:00:00+00:00",
        text="hello world",
    )

    assert key is None
