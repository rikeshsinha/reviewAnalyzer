"""Job entrypoint for incremental document enrichment."""

from __future__ import annotations

import os

from openai import OpenAI

from app.config.settings import get_settings
from app.db.session import SessionLocal
from app.services.enrichment_service import EnrichmentConfig, EnrichmentService


def _int_env(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def run() -> None:
    settings = get_settings()
    config = EnrichmentConfig(
        model_name=os.getenv("ENRICHMENT_MODEL", "gpt-4.1-mini"),
        batch_size=_int_env("ENRICHMENT_BATCH_SIZE", 3),
        max_docs_per_run=_int_env("ENRICHMENT_MAX_DOCS_PER_RUN", 100),
        min_text_chars=_int_env("ENRICHMENT_MIN_TEXT_CHARS", 20),
        max_text_chars=_int_env("ENRICHMENT_MAX_TEXT_CHARS", 3500),
        max_retries=_int_env("ENRICHMENT_MAX_RETRIES", 3),
    )

    session = SessionLocal()
    try:
        client = OpenAI(api_key=settings.openai_api_key)
        service = EnrichmentService(session=session, client=client, config=config)
        stats = service.enrich_new_documents()
        print(
            "enrichment completed: "
            f"candidates={stats['candidates']} "
            f"enriched={stats['enriched']} "
            f"skipped_short={stats['skipped_short']}"
        )
    finally:
        session.close()


if __name__ == "__main__":
    run()
