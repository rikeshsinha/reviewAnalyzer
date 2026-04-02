"""Reddit RSS/Atom client for read-only ingestion without OAuth keys."""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import html
import re
import time
from typing import Any
import xml.etree.ElementTree as ET

import requests

DEFAULT_TIMEOUT_SECONDS = 10
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_USER_AGENT = "reviewAnalyzer/0.1 (reddit-rss-ingestion)"
BASE_URL = "https://www.reddit.com"
ATOM_NS = "{http://www.w3.org/2005/Atom}"


class RedditRssError(RuntimeError):
    """Raised when Reddit RSS requests fail."""


def _strip_html_tags(value: str | None) -> str:
    if not value:
        return ""
    without_tags = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(without_tags)
    return " ".join(cleaned.split())


def _epoch_from_iso(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value).timestamp())
    except ValueError:
        return None


def _epoch_from_date_value(value: str | None) -> int | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except (TypeError, ValueError):
        pass

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        return None


def _parse_atom_entries(root: ET.Element) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for entry in root.findall(f"{ATOM_NS}entry"):
        entry_id = entry.findtext(f"{ATOM_NS}id")
        post_id = entry_id.rsplit("/", maxsplit=1)[-1] if isinstance(entry_id, str) and entry_id else None

        title = entry.findtext(f"{ATOM_NS}title")
        summary = entry.findtext(f"{ATOM_NS}content") or entry.findtext(f"{ATOM_NS}summary")
        content = _strip_html_tags(summary) or title or ""

        author = None
        author_node = entry.find(f"{ATOM_NS}author")
        if author_node is not None:
            author = author_node.findtext(f"{ATOM_NS}name")

        permalink = None
        full_link = None
        for link in entry.findall(f"{ATOM_NS}link"):
            href = link.attrib.get("href")
            rel = link.attrib.get("rel")
            if rel in {"alternate", None} and href:
                full_link = href
                if "reddit.com" in href:
                    parts = href.split("reddit.com", maxsplit=1)
                    permalink = parts[1] if len(parts) == 2 else href
                break

        published_raw = entry.findtext(f"{ATOM_NS}updated") or entry.findtext(f"{ATOM_NS}published")
        created_utc = _epoch_from_date_value(published_raw)

        subreddit = None
        category = entry.find(f"{ATOM_NS}category")
        if category is not None:
            subreddit = category.attrib.get("term")

        result.append(
            {
                "id": post_id,
                "title": title,
                "selftext": content,
                "subreddit": subreddit,
                "author": author,
                "created_utc": created_utc,
                "permalink": permalink,
                "full_link": full_link,
            }
        )

    return result


def _parse_rss_items(root: ET.Element) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in root.findall("./channel/item"):
        link = item.findtext("link")
        guid = item.findtext("guid")
        identifier = guid or link
        post_id = None
        if isinstance(identifier, str) and identifier:
            post_id = identifier.rstrip("/").rsplit("/", maxsplit=1)[-1]

        title = item.findtext("title")
        description = item.findtext("description")
        author = item.findtext("author")
        pub_date = item.findtext("pubDate")

        subreddit = None
        for category in item.findall("category"):
            term = category.text
            if isinstance(term, str) and term:
                subreddit = term
                break

        permalink = None
        if isinstance(link, str) and "reddit.com" in link:
            parts = link.split("reddit.com", maxsplit=1)
            permalink = parts[1] if len(parts) == 2 else link

        result.append(
            {
                "id": post_id,
                "title": title,
                "selftext": _strip_html_tags(description) or title or "",
                "subreddit": subreddit,
                "author": author,
                "created_utc": _epoch_from_date_value(pub_date),
                "permalink": permalink,
                "full_link": link,
            }
        )

    return result


def _parse_feed(xml_text: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise RedditRssError("Reddit RSS feed response was not valid XML") from exc

    if root.tag == f"{ATOM_NS}feed":
        return _parse_atom_entries(root)
    if root.tag == "rss":
        return _parse_rss_items(root)
    return []


def search_submissions(
    *,
    subreddit: str,
    query: str,
    after_iso: str | None,
    before_iso: str | None,
    max_pages: int = 3,
    base_url: str = BASE_URL,
    user_agent: str = DEFAULT_USER_AGENT,
    request_delay_seconds: float = DEFAULT_DELAY_SECONDS,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    """Search subreddit posts via Reddit RSS/Atom without OAuth credentials."""

    if not subreddit:
        return []

    after_epoch = _epoch_from_iso(after_iso)
    before_epoch = _epoch_from_iso(before_iso)

    headers = {"User-Agent": user_agent}
    feed_url = f"{base_url.rstrip('/')}/r/{subreddit}/search.rss"

    seen_ids: set[str] = set()
    all_records: list[dict[str, Any]] = []
    next_after: str | None = None

    with requests.Session() as session:
        session.headers.update(headers)

        for page_index in range(max(1, max_pages)):
            q = query.strip() if query else ""
            params: dict[str, Any] = {
                "q": q,
                "restrict_sr": "on",
                "sort": "new",
                "t": "all",
            }
            if next_after:
                params["after"] = next_after

            response = session.get(feed_url, params=params, timeout=timeout)
            if response.status_code != 200:
                raise RedditRssError(
                    f"Reddit RSS returned status {response.status_code} for url={feed_url} params={params!r}"
                )

            page_records = _parse_feed(response.text)
            if not page_records:
                break

            last_seen_id: str | None = None
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
                    last_seen_id = post_id
                all_records.append(record)

            if not last_seen_id:
                break
            next_after = f"t3_{last_seen_id}"

            if page_index < max_pages - 1 and request_delay_seconds > 0:
                time.sleep(request_delay_seconds)

    return all_records
