"""Reddit ingestion orchestrator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from app.ingestion.base import BaseIngestionAdapter
from app.ingestion.normalizers import normalize_comment, normalize_submission
from app.ingestion.reddit_client import get_reddit_client


@dataclass
class IngestionStats:
    posts_seen: int = 0
    posts_matched: int = 0
    comments_seen: int = 0
    docs_emitted: int = 0


class RedditIngestor(BaseIngestionAdapter):
    """Coordinates Reddit fetch/search/comment collection."""

    def __init__(self, client: Any | None = None) -> None:
        self.client = client or get_reddit_client()

    @property
    def platform_name(self) -> str:
        return "reddit"

    def validate_config(self, config: dict[str, Any]) -> None:
        subreddits = config.get("subreddits")
        if not isinstance(subreddits, list) or not subreddits:
            raise ValueError("Reddit config requires non-empty list: subreddits")

    def fetch_subreddit_posts(
        self,
        subreddits: Iterable[str],
        *,
        days_back: int = 30,
        limit: int = 200,
    ) -> list[Any]:
        """Fetch recent posts from configured subreddits."""

        min_ts = (datetime.now(tz=timezone.utc) - timedelta(days=max(days_back, 0))).timestamp()
        posts: list[Any] = []
        for subreddit_name in subreddits:
            subreddit = self.client.subreddit(subreddit_name)
            for submission in subreddit.new(limit=limit):
                created = getattr(submission, "created_utc", 0) or 0
                if created >= min_ts:
                    posts.append(submission)
        return posts

    def keyword_search(
        self,
        keywords: Iterable[str],
        subreddits: Iterable[str],
        *,
        days_back: int = 30,
        limit: int = 200,
    ) -> list[Any]:
        """Search configured subreddits by keyword query."""

        terms = [kw.strip() for kw in keywords if kw and kw.strip()]
        if not terms:
            return []

        min_ts = (datetime.now(tz=timezone.utc) - timedelta(days=max(days_back, 0))).timestamp()
        seen: set[str] = set()
        matched: list[Any] = []
        query = " OR ".join(f'"{kw}"' for kw in terms)

        for subreddit_name in subreddits:
            subreddit = self.client.subreddit(subreddit_name)
            for submission in subreddit.search(query, sort="new", time_filter="month", limit=limit):
                post_id = str(getattr(submission, "id", "") or "")
                created = getattr(submission, "created_utc", 0) or 0
                if not post_id or post_id in seen or created < min_ts:
                    continue
                seen.add(post_id)
                matched.append(submission)

        return matched

    def fetch_comments_for_matched_posts(
        self, submissions: Iterable[Any], *, limit_per_post: int = 200
    ) -> list[tuple[Any, Any]]:
        """Fetch comments for matched posts."""

        pairs: list[tuple[Any, Any]] = []
        for submission in submissions:
            submission.comments.replace_more(limit=0)
            for comment in submission.comments.list()[: max(limit_per_post, 0)]:
                pairs.append((submission, comment))
        return pairs

    def run(
        self,
        config: dict[str, Any],
        days_back: int,
    ) -> tuple[list[dict[str, Any]], IngestionStats]:
        """Execute ingest/search/comment stages and return normalized docs."""

        self.validate_config(config)
        subreddits = config.get("subreddits", [])
        keywords = config.get("keywords", [])
        post_limit = int(config.get("post_limit", 200))
        comment_limit = int(config.get("comment_limit", 200))

        stats = IngestionStats()
        recent_posts = self.fetch_subreddit_posts(subreddits, days_back=days_back, limit=post_limit)
        stats.posts_seen = len(recent_posts)

        matched_posts = self.keyword_search(
            keywords, subreddits, days_back=days_back, limit=post_limit
        )
        stats.posts_matched = len(matched_posts)

        matched_ids = {getattr(post, "id", None) for post in matched_posts}
        docs = [
            normalize_submission(post)
            for post in recent_posts
            if getattr(post, "id", None) in matched_ids
        ]

        comment_pairs = self.fetch_comments_for_matched_posts(
            matched_posts, limit_per_post=comment_limit
        )
        stats.comments_seen = len(comment_pairs)
        for submission, comment in comment_pairs:
            docs.append(normalize_comment(comment, parent_submission=submission))

        stats.docs_emitted = len(docs)
        return docs, stats
