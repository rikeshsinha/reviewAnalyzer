"""Google Play ingestion adapter."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app.ingestion.base import BaseIngestionAdapter
from app.utils.hashing import make_dedupe_key

SOURCE = "google_play"


@dataclass
class IngestionStats:
    apps_seen: int = 0
    reviews_seen: int = 0
    docs_emitted: int = 0


class _GooglePlayClient:
    """Thin wrapper around ``google_play_scraper.reviews`` for easier testing."""

    def fetch_reviews(
        self,
        app_id: str,
        *,
        lang: str,
        country: str,
        count: int,
        continuation_token: Any,
    ) -> tuple[list[dict[str, Any]], Any]:
        try:
            from google_play_scraper import Sort, reviews
        except ImportError as exc:  # pragma: no cover - exercised only when dependency missing.
            raise RuntimeError(
                "google_play_scraper is required for Google Play ingestion. "
                "Install it via `pip install google-play-scraper`."
            ) from exc

        items, token = reviews(
            app_id,
            lang=lang,
            country=country,
            sort=Sort.NEWEST,
            count=count,
            continuation_token=continuation_token,
        )
        return items or [], token


class GooglePlayIngestor(BaseIngestionAdapter):
    """Collects and normalizes Google Play reviews."""

    def __init__(self, client: Any | None = None) -> None:
        self.client = client or _GooglePlayClient()

    @property
    def platform_name(self) -> str:
        return SOURCE

    def validate_config(self, config: dict[str, Any]) -> None:
        apps = config.get("apps")
        if not isinstance(apps, list) or not apps:
            raise ValueError("Google Play config requires non-empty list: apps")

    def _iso_from_created(self, value: Any) -> str | None:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
        except (TypeError, ValueError, OSError):
            return None

    def _extract_created_datetime(self, review: dict[str, Any]) -> datetime | None:
        created = review.get("at")
        if isinstance(created, datetime):
            return created.astimezone(timezone.utc)
        created_ts = review.get("timestamp")
        if created_ts is None:
            return None
        try:
            return datetime.fromtimestamp(float(created_ts), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            return None

    def _normalize_review(self, app_id: str, review: dict[str, Any]) -> dict[str, Any]:
        external_id = review.get("reviewId")
        content = (review.get("content") or review.get("userContent") or "").strip()
        author = review.get("userName") or review.get("authorName")
        created_iso = self._iso_from_created(review.get("at") or review.get("timestamp"))

        platform_metadata = {
            "app_id": app_id,
            "app_version": review.get("reviewCreatedVersion") or review.get("appVersion"),
            "device": review.get("device"),
            "os_version": review.get("osVersion"),
            "thumbs_up_count": review.get("thumbsUpCount"),
            "reply_content": review.get("replyContent"),
            "replied_at": self._iso_from_created(review.get("repliedAt")),
        }

        return {
            "source": SOURCE,
            "platform": SOURCE,
            "external_id": external_id,
            "parent_external_id": None,
            "doc_type": "review",
            "entity_type": "review",
            "community_or_channel": app_id,
            "platform_metadata": platform_metadata,
            "author": author,
            "rating": review.get("score"),
            "title": None,
            "content": content,
            "created_at": created_iso,
            "url": (
                f"https://play.google.com/store/apps/details?id={app_id}&reviewId={external_id}"
                if external_id
                else f"https://play.google.com/store/apps/details?id={app_id}"
            ),
            "ingestion_ts": datetime.now(tz=timezone.utc).isoformat(),
            "dedupe_key": make_dedupe_key(SOURCE, external_id, f"{app_id}\n{author}\n{content}"),
            "raw_payload": dict(review),
        }

    def run(self, config: dict[str, Any], days_back: int) -> tuple[list[dict[str, Any]], IngestionStats]:
        self.validate_config(config)

        apps = [str(app).strip() for app in config.get("apps", []) if str(app).strip()]
        keywords = [str(term).strip().lower() for term in config.get("keywords", []) if str(term).strip()]
        per_page = max(int(config.get("page_size", 200)), 1)
        max_reviews_per_app = max(int(config.get("max_reviews_per_app", 1000)), 1)
        lang = str(config.get("lang", "en"))
        country = str(config.get("country", "us"))

        min_dt = datetime.now(tz=timezone.utc) - timedelta(days=max(days_back, 0))

        stats = IngestionStats(apps_seen=len(apps))
        docs: list[dict[str, Any]] = []

        for app_id in apps:
            continuation_token: Any = None
            app_seen = 0
            stop_app = False

            while app_seen < max_reviews_per_app and not stop_app:
                batch_size = min(per_page, max_reviews_per_app - app_seen)
                reviews, continuation_token = self.client.fetch_reviews(
                    app_id,
                    lang=lang,
                    country=country,
                    count=batch_size,
                    continuation_token=continuation_token,
                )
                if not reviews:
                    break

                for review in reviews:
                    app_seen += 1
                    stats.reviews_seen += 1

                    created_dt = self._extract_created_datetime(review)
                    if created_dt and created_dt < min_dt:
                        stop_app = True
                        continue

                    text = (review.get("content") or review.get("userContent") or "").lower()
                    if keywords and not any(term in text for term in keywords):
                        continue

                    docs.append(self._normalize_review(app_id, review))

                    if app_seen >= max_reviews_per_app:
                        break

                if continuation_token is None:
                    break

        stats.docs_emitted = len(docs)
        return docs, stats
