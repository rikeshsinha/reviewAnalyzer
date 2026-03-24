"""Reddit API client construction helpers."""

from __future__ import annotations

import os

import praw


def get_reddit_client() -> praw.Reddit:
    """Build a configured PRAW Reddit client from environment variables."""

    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID", ""),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET", ""),
        user_agent=os.getenv("REDDIT_USER_AGENT", "reviewAnalyzer/0.1"),
        username=os.getenv("REDDIT_USERNAME") or None,
        password=os.getenv("REDDIT_PASSWORD") or None,
    )
