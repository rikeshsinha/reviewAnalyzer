"""Public Reddit JSON client for read-only ingestion without OAuth keys."""

from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any

import requests

DEFAULT_TIMEOUT_SECONDS = 10
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_USER_AGENT = "reviewAnalyzer/0.1 (public-json-ingestion)"
BASE_URL = "https://www.reddit.com"


class PublicRedditError(RuntimeError):
    """Raised when public Reddit JSON requests fail."""


def _iso_to_epoch_seconds(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value).timestamp())
    except ValueError:
        return None


def _request_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    *,
    timeout: int,
) -> dict[str, Any]:
    response = session.get(url, params=params, timeout=timeout)
    if response.status_code != 200:
        raise PublicRedditError(
            f"Reddit public JSON returned status {response.status_code} for url={url} params={params!r}"
        )
    payload = response.json()
    if not isinstance(payload, dict):
        raise PublicRedditError("Reddit public JSON response was not an object")
    return payload


def _extract_children(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    children = data.get("children", []) if isinstance(data, dict) else []
    result: list[dict[str, Any]] = []
    for child in children:
        if not isinstance(child, dict):
            continue
        child_data = child.get("data")
        if isinstance(child_data, dict):
            result.append(child_data)
    return result


def search_submissions(
    *,
    subreddit: str,
    query: str,
    after_iso: str | None,
    before_iso: str | None,
    page_size: int = 100,
    max_pages: int = 10,
    base_url: str = BASE_URL,
    user_agent: str = DEFAULT_USER_AGENT,
    request_delay_seconds: float = DEFAULT_DELAY_SECONDS,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    """Search subreddit posts through Reddit's public JSON endpoint."""

    if not subreddit:
        return []

    max_pages = max(1, max_pages)
    page_size = min(max(1, page_size), 100)

    query_value = query.strip() if query else ""
    if query_value:
        q = f"subreddit:{subreddit} {query_value}"
    else:
        q = f"subreddit:{subreddit}"

    params: dict[str, Any] = {
        "q": q,
        "restrict_sr": "on",
        "sort": "new",
        "limit": page_size,
        "type": "link",
        "t": "all",
    }

    after_epoch = _iso_to_epoch_seconds(after_iso)
    before_epoch = _iso_to_epoch_seconds(before_iso)

    all_records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    next_after: str | None = None

    headers = {"User-Agent": user_agent}
    search_url = f"{base_url.rstrip('/')}/r/{subreddit}/search.json"

    with requests.Session() as session:
        session.headers.update(headers)

        for page_index in range(max_pages):
            if next_after:
                params["after"] = next_after
            elif "after" in params:
                params.pop("after")

            payload = _request_json(session, search_url, params, timeout=timeout)
            page_records = _extract_children(payload)
            if not page_records:
                break

            for record in page_records:
                post_id = record.get("id")
                if isinstance(post_id, str) and post_id in seen_ids:
                    continue

                created_utc = record.get("created_utc")
                if isinstance(created_utc, (int, float)):
                    if after_epoch is not None and created_utc < after_epoch:
                        continue
                    if before_epoch is not None and created_utc > before_epoch:
                        continue

                if isinstance(post_id, str):
                    seen_ids.add(post_id)
                all_records.append(record)

            listing_data = payload.get("data", {}) if isinstance(payload, dict) else {}
            next_after = listing_data.get("after") if isinstance(listing_data, dict) else None
            if not isinstance(next_after, str) or not next_after:
                break

            if page_index < max_pages - 1 and request_delay_seconds > 0:
                time.sleep(request_delay_seconds)

    return all_records
