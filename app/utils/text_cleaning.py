"""Helpers for normalizing text before rule-based tagging."""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

_DELETED_PATTERNS = [
    r"^\s*\[deleted\]\s*$",
    r"^\s*\[removed\]\s*$",
    r"^\s*deleted\s*$",
    r"^\s*removed\s*$",
    r"^\s*n/?a\s*$",
]

_WHITESPACE_RE = re.compile(r"\s+")



def clean_whitespace(text: str | None) -> str:
    """Collapse repeated whitespace and trim leading/trailing spaces."""
    if not text:
        return ""
    return _WHITESPACE_RE.sub(" ", text).strip()



def is_deleted_content(text: str | None) -> bool:
    """Return True when content is considered deleted/removed."""
    normalized = clean_whitespace(text).lower()
    if not normalized:
        return True
    return any(re.match(pattern, normalized) for pattern in _DELETED_PATTERNS)



def normalize_url(url: str | None) -> str:
    """Normalize URLs by lowercasing host and dropping tracking query params."""
    if not url:
        return ""

    parsed = urlparse(url.strip())
    query = parse_qsl(parsed.query, keep_blank_values=True)
    filtered_query = [
        (key, value)
        for key, value in query
        if not key.lower().startswith("utm_") and key.lower() not in {"fbclid", "gclid"}
    ]

    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        query=urlencode(filtered_query),
        fragment="",
    )
    return urlunparse(normalized)



def prepare_text_for_tagging(text: str | None) -> str:
    """Return an analyzable string for tagging rules."""
    if is_deleted_content(text):
        return ""
    return clean_whitespace(text).lower()
