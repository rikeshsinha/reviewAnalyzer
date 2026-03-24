"""Hashing utilities for deduplication."""

from __future__ import annotations

import hashlib


def make_dedupe_key(source: str, external_id: str | None, fallback_text: str | None) -> str:
    """Create a dedupe key, preferring source/external_id where available."""

    source_norm = (source or "unknown").strip().lower()
    if external_id:
        return f"{source_norm}:{str(external_id).strip()}"

    text_norm = " ".join((fallback_text or "").split()).lower()
    digest = hashlib.sha256(f"{source_norm}|{text_norm}".encode("utf-8")).hexdigest()
    return f"{source_norm}:fallback:{digest}"
