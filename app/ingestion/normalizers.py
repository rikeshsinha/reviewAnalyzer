"""Normalize Reddit submissions/comments into a unified shape."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.utils.hashing import make_dedupe_key

SOURCE = "reddit"


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _iso_from_epoch(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def _author_name(raw: Any) -> str | None:
    author = getattr(raw, "author", None)
    if author is None:
        return None
    return getattr(author, "name", None) or str(author)


def _raw_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    data = getattr(raw, "__dict__", None)
    if isinstance(data, dict):
        return dict(data)
    return {"value": str(raw)}


def normalize_submission(raw_submission: Any) -> dict[str, Any]:
    """Normalize a PRAW submission to the unified document shape."""

    external_id = getattr(raw_submission, "id", None)
    title = getattr(raw_submission, "title", None)
    content = getattr(raw_submission, "selftext", None) or title or ""

    return {
        "source": SOURCE,
        "platform": SOURCE,
        "external_id": external_id,
        "parent_external_id": None,
        "doc_type": "post",
        "entity_type": "post",
        "community_or_channel": str(getattr(raw_submission, "subreddit", "")) or None,
        "platform_metadata": {
            "subreddit": str(getattr(raw_submission, "subreddit", "")) or None,
            "parent_external_id": None,
        },
        "author": _author_name(raw_submission),
        "title": title,
        "content": content,
        "created_at": _iso_from_epoch(getattr(raw_submission, "created_utc", None)),
        "url": getattr(raw_submission, "url", None),
        "ingestion_ts": _now_iso(),
        "dedupe_key": make_dedupe_key(
            SOURCE,
            external_id,
            app_id=None,
            author=_author_name(raw_submission),
            created_at=_iso_from_epoch(getattr(raw_submission, "created_utc", None)),
            text=f"{title}\n{content}",
        ),
        "raw_payload": _raw_payload(raw_submission),
    }


def normalize_pushshift_submission(raw_submission: dict[str, Any]) -> dict[str, Any]:
    """Normalize a Pushshift submission payload to the unified document shape."""

    external_id = raw_submission.get("id")
    title = raw_submission.get("title")
    content = raw_submission.get("selftext") or title or ""
    subreddit = raw_submission.get("subreddit")
    permalink = raw_submission.get("permalink")

    url = raw_submission.get("full_link")
    if not url and permalink:
        url = f"https://reddit.com{permalink}"

    created_at = _iso_from_epoch(raw_submission.get("created_utc"))
    author = raw_submission.get("author")

    return {
        "source": SOURCE,
        "platform": SOURCE,
        "external_id": external_id,
        "parent_external_id": None,
        "doc_type": "post",
        "entity_type": "post",
        "community_or_channel": subreddit,
        "platform_metadata": {
            "subreddit": subreddit,
            "parent_external_id": None,
        },
        "author": author,
        "title": title,
        "content": content,
        "created_at": created_at,
        "url": url,
        "ingestion_ts": _now_iso(),
        "dedupe_key": make_dedupe_key(
            SOURCE,
            external_id,
            app_id=None,
            author=author,
            created_at=created_at,
            text=f"{title}\n{content}",
        ),
        "raw_payload": raw_submission,
    }


def normalize_comment(raw_comment: Any, parent_submission: Any = None) -> dict[str, Any]:
    """Normalize a PRAW comment to the unified document shape."""

    external_id = getattr(raw_comment, "id", None)
    content = getattr(raw_comment, "body", None) or ""

    parent_external_id = getattr(raw_comment, "link_id", None)
    if isinstance(parent_external_id, str) and parent_external_id.startswith("t3_"):
        parent_external_id = parent_external_id[3:]
    if parent_submission is not None:
        parent_external_id = getattr(parent_submission, "id", parent_external_id)

    permalink = getattr(raw_comment, "permalink", None)
    comment_url = f"https://reddit.com{permalink}" if permalink else None

    return {
        "source": SOURCE,
        "platform": SOURCE,
        "external_id": external_id,
        "parent_external_id": parent_external_id,
        "doc_type": "comment",
        "entity_type": "comment",
        "community_or_channel": str(getattr(raw_comment, "subreddit", "")) or None,
        "platform_metadata": {
            "subreddit": str(getattr(raw_comment, "subreddit", "")) or None,
            "parent_external_id": parent_external_id,
        },
        "author": _author_name(raw_comment),
        "title": None,
        "content": content,
        "created_at": _iso_from_epoch(getattr(raw_comment, "created_utc", None)),
        "url": comment_url,
        "ingestion_ts": _now_iso(),
        "dedupe_key": make_dedupe_key(
            SOURCE,
            external_id,
            app_id=None,
            author=_author_name(raw_comment),
            created_at=_iso_from_epoch(getattr(raw_comment, "created_utc", None)),
            text=f"{parent_external_id}\n{content}",
        ),
        "raw_payload": _raw_payload(raw_comment),
    }
