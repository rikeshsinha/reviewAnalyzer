from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.ingestion.google_play_ingestor import GooglePlayIngestor
from app.ingestion.registry import get_adapter_class


class FakeGooglePlayClient:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def fetch_reviews(self, app_id, *, lang, country, count, continuation_token):
        self.calls.append((app_id, lang, country, count, continuation_token))
        if not self.pages:
            return [], None
        return self.pages.pop(0)


def test_google_play_run_normalizes_reviews_and_stats() -> None:
    now = datetime.now(tz=timezone.utc)
    pages = [
        (
            [
                {
                    "reviewId": "r-1",
                    "userName": "alice",
                    "content": "Battery drains fast",
                    "score": 2,
                    "at": now,
                    "reviewCreatedVersion": "1.2.3",
                    "device": "Pixel 8",
                    "osVersion": "14",
                }
            ],
            None,
        )
    ]
    client = FakeGooglePlayClient(pages)
    ingestor = GooglePlayIngestor(client=client)

    docs, stats = ingestor.run(
        config={"apps": ["com.test.app"], "keywords": ["battery"], "lang": "en", "country": "us"},
        days_back=30,
    )

    assert stats.apps_seen == 1
    assert stats.reviews_seen == 1
    assert stats.docs_emitted == 1
    assert len(docs) == 1

    doc = docs[0]
    assert doc["platform"] == "google_play"
    assert doc["doc_type"] == "review"
    assert doc["entity_type"] == "review"
    assert doc["external_id"] == "r-1"
    assert doc["title"] is None
    assert doc["content"] == "Battery drains fast"
    assert doc["author"] == "alice"
    assert doc["rating"] == 2
    assert doc["created_at"] is not None
    assert "reviewId=r-1" in doc["url"]
    assert doc["platform_metadata"]["app_version"] == "1.2.3"
    assert doc["platform_metadata"]["device"] == "Pixel 8"
    assert isinstance(doc["raw_payload"], dict)


def test_google_play_filters_old_reviews() -> None:
    old_dt = datetime.now(tz=timezone.utc) - timedelta(days=60)
    pages = [([{"reviewId": "old", "content": "Too old", "at": old_dt}], None)]
    ingestor = GooglePlayIngestor(client=FakeGooglePlayClient(pages))

    docs, stats = ingestor.run(config={"apps": ["com.test.app"]}, days_back=7)

    assert stats.reviews_seen == 1
    assert stats.docs_emitted == 0
    assert docs == []


def test_registry_maps_google_play_adapter() -> None:
    adapter_class = get_adapter_class("google_play")
    assert adapter_class is GooglePlayIngestor


def test_registry_maps_google_play_dash_alias() -> None:
    adapter_class = get_adapter_class("google-play")
    assert adapter_class is GooglePlayIngestor
