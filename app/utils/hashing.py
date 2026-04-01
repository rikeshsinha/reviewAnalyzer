"""Hashing utilities for deduplication."""

from __future__ import annotations

import hashlib


FALLBACK_TEXT_PREFIX_LEN = 200


def _normalize_text_prefix(text: str | None, *, size: int = FALLBACK_TEXT_PREFIX_LEN) -> str:
    normalized = " ".join((text or "").split()).lower()
    return normalized[:size]


def make_dedupe_key(
    source: str,
    external_id: str | None,
    *,
    app_id: str | None,
    author: str | None,
    created_at: str | None,
    text: str | None,
) -> str | None:
    """Create dedupe key using fallback hash only when external IDs are missing."""

    if external_id:
        return None

    source_norm = (source or "unknown").strip().lower()
    payload = "|".join(
        [
            source_norm,
            (app_id or "").strip().lower(),
            (author or "").strip().lower(),
            (created_at or "").strip(),
            _normalize_text_prefix(text),
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"{source_norm}:fallback:{digest}"
