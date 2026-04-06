"""Normalize crawled web review articles into the unified document shape."""

from __future__ import annotations

from datetime import datetime, timezone
from html.parser import HTMLParser
import re
from typing import Any
from urllib.parse import urlparse

from app.utils.hashing import make_dedupe_key

SOURCE = "web_reviews"
DEFAULT_MIN_CONTENT_CHARS = 500

_SKIP_TAGS = {"script", "style", "nav", "header", "footer", "aside", "form", "noscript", "svg"}
_TEXT_TAGS = {"p", "li", "blockquote", "h2", "h3", "h4"}
_CONTENT_HINTS = (
    "article",
    "content",
    "post-body",
    "entry-content",
    "review-body",
    "story-body",
)
_BOILERPLATE_HINTS = (
    "nav",
    "menu",
    "footer",
    "header",
    "subscribe",
    "newsletter",
    "breadcrumb",
    "sidebar",
    "cookie",
    "advert",
    "social",
    "related",
    "promo",
)
_NON_EDITORIAL_URL_PATTERNS = (
    "/author/",
    "/authors/",
    "/category/",
    "/categories/",
    "/tag/",
    "/tags/",
    "/topic/",
    "/topics/",
    "/search",
    "/subscribe",
    "/membership",
    "/newsletter",
)
_WHITESPACE_RE = re.compile(r"\s+")


class _ArticleHTMLParser(HTMLParser):
    """Extract likely article metadata and readable body text from HTML."""

    def __init__(self) -> None:
        super().__init__()
        self._skip_depth = 0
        self._content_depth = 0
        self._in_title = False
        self._in_text_tag_depth = 0
        self.meta: dict[str, str] = {}
        self.title_chunks: list[str] = []
        self.content_chunks: list[str] = []

    @staticmethod
    def _attrs_to_dict(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        return {key.lower(): (value or "") for key, value in attrs}

    @staticmethod
    def _is_boilerplate_attr(attrs: dict[str, str]) -> bool:
        haystack = " ".join(
            [attrs.get("class", ""), attrs.get("id", ""), attrs.get("role", "")]
        ).lower()
        return any(marker in haystack for marker in _BOILERPLATE_HINTS)

    @staticmethod
    def _is_content_attr(attrs: dict[str, str]) -> bool:
        if attrs.get("role", "").lower() == "main":
            return True
        haystack = " ".join([attrs.get("class", ""), attrs.get("id", "")]).lower()
        return any(marker in haystack for marker in _CONTENT_HINTS)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attr_map = self._attrs_to_dict(attrs)

        if tag == "meta":
            key = (attr_map.get("property") or attr_map.get("name") or "").lower().strip()
            value = (attr_map.get("content") or "").strip()
            if key and value:
                self.meta[key] = value
            return

        if tag == "title":
            self._in_title = True

        is_skipped = tag in _SKIP_TAGS or self._is_boilerplate_attr(attr_map)
        if self._skip_depth > 0:
            self._skip_depth += 1
        elif is_skipped:
            self._skip_depth = 1

        if self._skip_depth == 0:
            is_content_root = tag in {"article", "main"} or self._is_content_attr(attr_map)
            if is_content_root:
                self._content_depth += 1
            if tag in _TEXT_TAGS:
                self._in_text_tag_depth += 1

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "title":
            self._in_title = False

        if self._skip_depth > 0:
            self._skip_depth -= 1
            return

        if tag in {"article", "main"} and self._content_depth > 0:
            self._content_depth -= 1
        if tag in _TEXT_TAGS and self._in_text_tag_depth > 0:
            self._in_text_tag_depth -= 1

    def handle_data(self, data: str) -> None:
        text = _clean_text(data)
        if not text:
            return

        if self._in_title:
            self.title_chunks.append(text)

        if self._skip_depth > 0:
            return

        if self._content_depth > 0 or self._in_text_tag_depth > 0:
            self.content_chunks.append(text)


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    return _WHITESPACE_RE.sub(" ", value).strip()


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _normalize_domain(url: str | None) -> str | None:
    if not url:
        return None
    hostname = urlparse(url).netloc.lower().strip()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname or None


def _parse_datetime(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
        except (TypeError, ValueError, OSError):
            return None

    text = str(value).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _is_non_editorial_page(url: str | None, title: str | None) -> bool:
    lowered_url = (url or "").lower()
    if any(pattern in lowered_url for pattern in _NON_EDITORIAL_URL_PATTERNS):
        return True

    lowered_title = (title or "").lower()
    return any(
        marker in lowered_title
        for marker in (
            "authors",
            "categories",
            "tags",
            "subscribe",
            "newsletter",
            "sign in",
            "log in",
        )
    )


def _extract_fields_from_html(html: str) -> dict[str, str | None]:
    parser = _ArticleHTMLParser()
    parser.feed(html)

    title = _clean_text(parser.meta.get("og:title") or " ".join(parser.title_chunks)) or None
    author = _clean_text(
        parser.meta.get("author")
        or parser.meta.get("article:author")
        or parser.meta.get("og:article:author")
    ) or None
    published = (
        parser.meta.get("article:published_time")
        or parser.meta.get("published_time")
        or parser.meta.get("pubdate")
        or parser.meta.get("date")
    )
    content = _clean_text("\n".join(parser.content_chunks))

    return {
        "title": title,
        "author": author,
        "published": published,
        "content": content,
    }


def normalize_web_review_article(
    raw_article: dict[str, Any],
    *,
    min_content_chars: int = DEFAULT_MIN_CONTENT_CHARS,
) -> dict[str, Any] | None:
    """Normalize a crawled web article into canonical ingestion schema.

    Returns ``None`` when quality filters fail.
    """

    if not isinstance(raw_article, dict):
        return None

    url = str(raw_article.get("url") or "").strip() or None
    html = str(raw_article.get("html") or "")
    extracted = _extract_fields_from_html(html) if html else {}

    title = _clean_text(
        raw_article.get("title")
        or extracted.get("title")
        or raw_article.get("headline")
    ) or None
    author = _clean_text(
        raw_article.get("author")
        or raw_article.get("byline")
        or extracted.get("author")
    ) or None
    content = _clean_text(
        raw_article.get("content")
        or raw_article.get("body")
        or raw_article.get("review_text")
        or extracted.get("content")
    )

    if _is_non_editorial_page(url, title):
        return None
    if len(content) < max(int(min_content_chars), 1):
        return None

    published_at = _parse_datetime(
        raw_article.get("published_at")
        or raw_article.get("published")
        or raw_article.get("date")
        or extracted.get("published")
    )
    domain = _normalize_domain(url)

    return {
        "source": SOURCE,
        "platform": SOURCE,
        "external_id": url,
        "parent_external_id": None,
        "doc_type": "article",
        "entity_type": "review",
        "community_or_channel": domain,
        "subreddit_or_site": domain,
        "platform_metadata": {
            "site": domain,
            "subreddit_or_site": domain,
        },
        "author": author,
        "author_handle": author,
        "title": title,
        "content": content,
        "review_text": content,
        "created_at": published_at,
        "url": url,
        "ingestion_ts": _now_iso(),
        "dedupe_key": make_dedupe_key(
            SOURCE,
            url,
            app_id=domain,
            author=author,
            created_at=published_at,
            text=f"{title or ''}\n{content}",
        ),
        "raw_payload": dict(raw_article),
    }
