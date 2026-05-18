from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from app.ingestion import google_search_results_ingestor
from app.ingestion.google_search_results_ingestor import GoogleSearchResultsIngestor


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        google_search_api_key="key",
        google_search_engine_id="cx",
        google_search_cx=None,
        google_search_results_per_term=20,
        google_search_country="us",
        google_search_language="en",
    )


def test_ingestor_maps_result_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(google_search_results_ingestor, "get_ingestion_settings", _settings)
    monkeypatch.setattr(
        google_search_results_ingestor,
        "search_google",
        lambda *args, **kwargs: [
            {
                "title": "Samsung Health Review",
                "snippet": "Helpful sleep tracking notes",
                "link": "https://Example.com/review?utm_source=x",
                "displayLink": "Example.com",
                "pagemap": {"metatags": [{"article:published_time": "2026-01-02T00:00:00Z"}]},
            }
        ],
    )

    docs, stats = GoogleSearchResultsIngestor().run(
        {"search_terms": ["Samsung Health review"], "max_results_per_term": 20, "country": "us", "language": "en"},
        days_back=30,
    )

    assert stats.docs_emitted == 1
    doc = docs[0]
    assert doc["source"] == "google_search_results"
    assert doc["doc_type"] == "search_result"
    assert doc["title"] == "Samsung Health Review"
    assert doc["content"] == "Helpful sleep tracking notes"
    assert doc["url"] == "https://example.com/review"
    assert doc["platform_metadata"]["query_term"] == "Samsung Health review"
    assert doc["platform_metadata"]["rank"] == 1
    assert doc["raw_payload"]["item"]["displayLink"] == "Example.com"


def test_ingestor_deduplicates_duplicate_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(google_search_results_ingestor, "get_ingestion_settings", _settings)
    monkeypatch.setattr(
        google_search_results_ingestor,
        "search_google",
        lambda *args, **kwargs: [
            {"title": "A", "snippet": "one", "link": "https://example.com/a?utm_medium=x"},
            {"title": "A2", "snippet": "two", "link": "https://example.com/a"},
        ],
    )

    docs, stats = GoogleSearchResultsIngestor().run({"search_terms": ["q"], "max_results_per_term": 20}, days_back=30)

    assert len(docs) == 1
    assert stats.duplicates_skipped == 1


def test_ingestor_missing_credentials_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        google_search_results_ingestor,
        "get_ingestion_settings",
        lambda: SimpleNamespace(google_search_api_key=None, google_search_engine_id=None, google_search_cx=None),
    )

    with pytest.raises(Exception, match="Missing Google Search API key"):
        GoogleSearchResultsIngestor().run({"search_terms": ["q"], "max_results_per_term": 20}, days_back=30)
