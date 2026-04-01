"""Registry for resolving ingestion adapters by platform key."""

from __future__ import annotations

from typing import Type

from app.ingestion.base import BaseIngestionAdapter
from app.ingestion.google_play_ingestor import GooglePlayIngestor
from app.ingestion.reddit_ingestor import RedditIngestor

INGESTION_ADAPTERS: dict[str, Type[BaseIngestionAdapter]] = {
    "reddit": RedditIngestor,
    "google_play": GooglePlayIngestor,
    "app_store": RedditIngestor,
    "youtube": RedditIngestor,
}


def get_adapter_class(platform_key: str) -> Type[BaseIngestionAdapter]:
    """Resolve adapter class for platform key or raise a clear error."""

    normalized = (platform_key or "").strip().lower()
    adapter_class = INGESTION_ADAPTERS.get(normalized)
    if adapter_class is None:
        supported = ", ".join(sorted(INGESTION_ADAPTERS))
        raise ValueError(f"Unsupported platform '{platform_key}'. Supported: {supported}")
    return adapter_class
