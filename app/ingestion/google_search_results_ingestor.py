"""Google Search Results ingestion adapter."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from typing import Any
from urllib.parse import urlparse

from app.config.settings import get_ingestion_settings
from app.ingestion.base import BaseIngestionAdapter
from app.ingestion.google_search_client import GoogleSearchError, search_google
from app.utils.text_cleaning import normalize_url

SOURCE = "google_search_results"


@dataclass
class IngestionStats:
    terms_seen: int = 0
    results_seen: int = 0
    duplicates_skipped: int = 0
    docs_emitted: int = 0


def _stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class GoogleSearchResultsIngestor(BaseIngestionAdapter):
    """Collects search result metadata from an API-backed Google-compatible provider."""

    @property
    def platform_name(self) -> str:
        return SOURCE

    def validate_config(self, config: dict[str, Any]) -> None:
        terms = config.get("search_terms")
        if not isinstance(terms, list) or not all(isinstance(term, str) for term in terms) or not any(
            term.strip() for term in terms
        ):
            raise ValueError("google_search_results config requires non-empty list: search_terms")
        max_results = config.get("max_results_per_term", 20)
        if not isinstance(max_results, int) or max_results <= 0:
            raise ValueError("google_search_results field 'max_results_per_term' must be a positive integer")

    def _normalize_result(self, *, query: str, rank: int, item: dict[str, Any], ingestion_ts: str) -> dict[str, Any]:
        link = str(item.get("link") or "").strip()
        canonical_url = normalize_url(link)
        display_link = str(item.get("displayLink") or urlparse(canonical_url).netloc or "").strip()
        external_id = _stable_hash(canonical_url) if canonical_url else _stable_hash(f"{SOURCE}|{query}|{rank}")
        dedupe_key = (
            f"{SOURCE}:url:{_stable_hash(canonical_url)}"
            if canonical_url
            else f"{SOURCE}:query_url:{external_id}"
        )
        snippet = str(item.get("snippet") or item.get("htmlSnippet") or "").strip()
        title = str(item.get("title") or item.get("htmlTitle") or "").strip()
        provider_date = None
        pagemap = item.get("pagemap")
        if isinstance(pagemap, dict):
            metatags = pagemap.get("metatags")
            if isinstance(metatags, list):
                for metatag in metatags:
                    if not isinstance(metatag, dict):
                        continue
                    for key in ("article:published_time", "date", "datepublished", "published_time"):
                        value = metatag.get(key)
                        if value:
                            provider_date = str(value)
                            break
                    if provider_date:
                        break

        platform_metadata = {
            "query_term": query,
            "rank": rank,
            "display_link": display_link,
            "domain": urlparse(canonical_url).netloc or display_link,
            "provider": "google_custom_search_json_api",
        }

        return {
            "source": SOURCE,
            "platform": SOURCE,
            "external_id": external_id,
            "parent_external_id": None,
            "doc_type": "search_result",
            "entity_type": "search_result",
            "community_or_channel": display_link,
            "platform_metadata": platform_metadata,
            "author": None,
            "rating": None,
            "title": title,
            "content": snippet,
            "created_at": provider_date or ingestion_ts,
            "url": canonical_url,
            "ingestion_ts": ingestion_ts,
            "dedupe_key": dedupe_key,
            "raw_payload": {"query_term": query, "rank": rank, "item": dict(item)},
        }

    def run(self, config: dict[str, Any], days_back: int) -> tuple[list[dict[str, Any]], IngestionStats]:
        self.validate_config(config)
        settings = get_ingestion_settings()
        api_key = settings.google_search_api_key
        search_engine_id = settings.google_search_engine_id or settings.google_search_cx
        if not api_key:
            raise GoogleSearchError("Missing Google Search API key (GOOGLE_SEARCH_API_KEY)")
        if not search_engine_id:
            raise GoogleSearchError("Missing Google Search engine ID (GOOGLE_SEARCH_ENGINE_ID)")

        terms = [str(term).strip() for term in config.get("search_terms", []) if str(term).strip()]
        max_results = int(config.get("max_results_per_term") or settings.google_search_results_per_term or 20)
        country = config.get("country") or settings.google_search_country
        language = config.get("language") or settings.google_search_language
        stats = IngestionStats(terms_seen=len(terms))
        docs: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        ingestion_ts = datetime.now(tz=timezone.utc).isoformat()

        for term in terms:
            items = search_google(
                term,
                api_key=api_key,
                search_engine_id=search_engine_id,
                max_results=max_results,
                country=str(country) if country else None,
                language=str(language) if language else None,
            )
            for index, item in enumerate(items, start=1):
                stats.results_seen += 1
                normalized_url = normalize_url(str(item.get("link") or ""))
                if normalized_url and normalized_url in seen_urls:
                    stats.duplicates_skipped += 1
                    continue
                if normalized_url:
                    seen_urls.add(normalized_url)
                docs.append(self._normalize_result(query=term, rank=index, item=item, ingestion_ts=ingestion_ts))

        stats.docs_emitted = len(docs)
        return docs, stats
