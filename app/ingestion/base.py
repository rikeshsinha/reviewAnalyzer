"""Base adapter contract for ingestion platforms."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TypeAlias

IngestionStats: TypeAlias = Any


class BaseIngestionAdapter(ABC):
    """Abstract adapter interface for platform-specific ingestion logic."""

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Unique platform key used by ingestion registry."""

    @abstractmethod
    def validate_config(self, config: dict[str, Any]) -> None:
        """Validate platform-specific configuration and raise on invalid input."""

    @abstractmethod
    def run(
        self,
        config: dict[str, Any],
        days_back: int,
    ) -> tuple[list[dict[str, Any]], IngestionStats]:
        """Run ingestion and return normalized documents with run stats."""
