from __future__ import annotations

from types import SimpleNamespace

from app.ingestion.normalizers import normalize_comment, normalize_submission


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
