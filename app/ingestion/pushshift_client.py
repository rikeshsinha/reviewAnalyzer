"""Tiny Pushshift HTTP client for Reddit submission search."""

from __future__ import annotations

import time
from typing import Any

import requests

PUSHSHIFT_SUBMISSION_SEARCH_URL = "https://api.pushshift.io/reddit/search/submission/"
DEFAULT_TIMEOUT_SECONDS = 10
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF_SECONDS = 1.0
MAX_PAGES = 20
MAX_RECORDS = 1_000


class PushshiftError(RuntimeError):
    """Raised when Pushshift requests fail after retries."""


def _request_with_retries(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    *,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
) -> list[dict[str, Any]]:
    """Execute a Pushshift request with conservative timeout/status retries."""

    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            response = session.get(url, params=params, timeout=timeout)
            if response.status_code == 200:
                payload = response.json()
                data = payload.get("data", [])
                if isinstance(data, list):
                    return [item for item in data if isinstance(item, dict)]
                return []

            last_error = PushshiftError(
                f"Pushshift returned status {response.status_code} for params={params!r}"
            )
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc

        if attempt < retries:
            time.sleep(backoff_seconds * (2**attempt))

    raise PushshiftError(f"Pushshift request failed after {retries + 1} attempts") from last_error


def search_submissions(
    subreddit: str,
    query: str,
    after: int,
    before: int,
    size: int,
    base_url: str | None = None,
    sort: str = "desc",
    sort_type: str = "created_utc",
) -> list[dict]:
    """Search Pushshift submissions and paginate using a ``created_utc`` cursor.

    Returns raw Pushshift record dictionaries without normalization.
    """

    if sort_type != "created_utc":
        raise ValueError("Pagination cursor support requires sort_type='created_utc'")
    if sort not in {"asc", "desc"}:
        raise ValueError("sort must be either 'asc' or 'desc'")
    request_url = base_url or PUSHSHIFT_SUBMISSION_SEARCH_URL

    page_size = max(1, min(size, 100))
    all_records: list[dict[str, Any]] = []

    cursor_after = after
    cursor_before = before

    with requests.Session() as session:
        for _ in range(MAX_PAGES):
            if len(all_records) >= MAX_RECORDS:
                break

            params: dict[str, Any] = {
                "subreddit": subreddit,
                "q": query,
                "after": cursor_after,
                "before": cursor_before,
                "size": min(page_size, MAX_RECORDS - len(all_records)),
                "sort": sort,
                "sort_type": sort_type,
            }

            page = _request_with_retries(session, request_url, params)
            if not page:
                break

            all_records.extend(page)

            created_values = [item.get("created_utc") for item in page if isinstance(item.get("created_utc"), int)]
            if not created_values:
                break

            if sort == "desc":
                next_before = min(created_values) - 1
                if next_before <= cursor_after:
                    break
                cursor_before = next_before
            else:
                next_after = max(created_values) + 1
                if next_after >= cursor_before:
                    break
                cursor_after = next_after

    return all_records[:MAX_RECORDS]
