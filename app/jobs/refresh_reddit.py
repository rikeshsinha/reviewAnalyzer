"""Job entry point for running a single platform ingestion refresh."""

from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import text

from app.db.repositories import IngestionRunRepository
from app.db.session import SessionLocal
from app.ingestion.registry import get_adapter_class
from app.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


def _ensure_source(session: Any, platform: str) -> int:
    session.execute(
        text(
            """
            INSERT OR IGNORE INTO sources (platform, external_id, name, metadata_json)
            VALUES (:platform, :platform, :name, '{}')
            """
        ),
        {"platform": platform, "name": platform.replace("_", " ").title()},
    )
    row = session.execute(
        text("SELECT id FROM sources WHERE platform=:platform AND external_id=:platform LIMIT 1"),
        {"platform": platform},
    ).first()
    if row is None:
        raise RuntimeError(f"Unable to resolve source row for platform '{platform}'")
    return int(row.id)


def _ensure_dedupe_constraints(session: Any) -> None:
    session.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_source_external "
            "ON documents(source_id, external_id)"
        )
    )
    session.execute(text("ALTER TABLE documents ADD COLUMN dedupe_key TEXT"))


def _safe_ensure_dedupe_constraints(session: Any) -> None:
    try:
        _ensure_dedupe_constraints(session)
    except Exception:
        session.rollback()
        session.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_source_external "
                "ON documents(source_id, external_id)"
            )
        )
    session.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_dedupe_key ON documents(dedupe_key)"))
    session.commit()


def _insert_documents(session: Any, source_id: int, docs: list[dict[str, Any]]) -> int:
    inserted = 0
    for doc in docs:
        row = {
            "source_id": source_id,
            "external_id": doc.get("external_id"),
            "title": doc.get("title"),
            "body": doc.get("content"),
            "author": doc.get("author"),
            "url": doc.get("url"),
            "published_at": doc.get("created_at"),
            "raw_json": json.dumps(
                {
                    "parent_external_id": doc.get("parent_external_id"),
                    "doc_type": doc.get("doc_type"),
                    "subreddit": doc.get("subreddit"),
                    "ingestion_ts": doc.get("ingestion_ts"),
                    "dedupe_key": doc.get("dedupe_key"),
                    "raw_payload": doc.get("raw_payload"),
                },
                default=str,
            ),
            "dedupe_key": doc.get("dedupe_key"),
        }

        result = session.execute(
            text(
                """
                INSERT OR IGNORE INTO documents
                (source_id, external_id, title, body, author, url, published_at, raw_json, dedupe_key)
                VALUES
                (:source_id, :external_id, :title, :body, :author, :url, :published_at, :raw_json, :dedupe_key)
                """
            ),
            row,
        )
        if result.rowcount and result.rowcount > 0:
            inserted += 1

    session.commit()
    return inserted


def run_for_platform(platform: str, config: dict[str, Any], *, days_back: int) -> dict[str, int]:
    """Run ingestion for a single platform and return run counters."""

    session = SessionLocal()
    run_repo = IngestionRunRepository(session)
    run_id: int | None = None

    try:
        _safe_ensure_dedupe_constraints(session)
        source_id = _ensure_source(session, platform)
        session.commit()

        run_id = run_repo.start_run(source_name=platform)
        adapter_class = get_adapter_class(platform)
        ingestor = adapter_class()

        docs, stats = ingestor.run(config=config, days_back=days_back)
        inserted = _insert_documents(session, source_id, docs)

        run_repo.complete_run(
            run_id=run_id,
            records_fetched=stats.docs_emitted,
            records_inserted=inserted,
            status="completed",
        )
        logger.info(
            "%s refresh completed",
            platform,
            extra={"records_fetched": stats.docs_emitted, "records_inserted": inserted},
        )
        return {"records_fetched": stats.docs_emitted, "records_inserted": inserted}
    except Exception as exc:
        if run_id is not None:
            run_repo.complete_run(
                run_id=run_id,
                records_fetched=0,
                records_inserted=0,
                status="failed",
                error_message=str(exc),
            )
        logger.exception("%s refresh failed", platform)
        raise
    finally:
        session.close()


def run() -> None:
    """Backward-compatible reddit-only refresh entrypoint."""

    from app.config.source_loader import get_enabled_platform_configs

    setup_logging()
    configs = [config for config in get_enabled_platform_configs() if config.platform == "reddit"]
    if not configs:
        raise RuntimeError("Reddit platform is not enabled in merged source configuration")

    reddit_config = configs[0]
    run_for_platform("reddit", reddit_config.config, days_back=reddit_config.days_back)


if __name__ == "__main__":
    run()
