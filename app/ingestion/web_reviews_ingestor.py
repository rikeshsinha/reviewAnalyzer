"""Web reviews ingestion adapter placeholder.

This adapter keeps the multi-platform refresh flow functional while the
site-specific crawler implementation is being developed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.ingestion.base import BaseIngestionAdapter

SOURCE = "web_reviews"


@dataclass
class IngestionStats:
    sites_seen: int = 0
    docs_emitted: int = 0


class WebReviewsIngestor(BaseIngestionAdapter):
    """Validates config and emits no-op results for web reviews."""

    @property
    def platform_name(self) -> str:
        return SOURCE

    def validate_config(self, config: dict[str, Any]) -> None:
        sites = config.get("sites")
        if not isinstance(sites, list) or not sites:
            raise ValueError("Web reviews config requires non-empty list: sites")

    def run(self, config: dict[str, Any], days_back: int) -> tuple[list[dict[str, Any]], IngestionStats]:
        self.validate_config(config)
        sites = [str(site).strip() for site in config.get("sites", []) if str(site).strip()]
        stats = IngestionStats(sites_seen=len(sites), docs_emitted=0)
        return [], stats

