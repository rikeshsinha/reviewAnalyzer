"""Job entry point for refresh ingestion by platform key."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.db.repositories import IngestionRunRepository
from app.db.session import SessionLocal
from app.ingestion.registry import get_adapter_class
from app.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


def _load_reddit_source_config() -> tuple[list[str], list[str]]:
    """Load subreddits/keywords from env with fallback to source_config.yaml."""

    env_subreddits = [s.strip() for s in os.getenv("REDDIT_SUBREDDITS", "").split(",") if s.strip()]
    env_keywords = [k.strip() for k in os.getenv("REDDIT_KEYWORDS", "").split(",") if k.strip()]
    if env_subreddits:
        return env_subreddits, env_keywords

    config_path = Path(__file__).resolve().parents[1] / "config" / "source_config.yaml"
    subreddits: list[str] = []
    keywords: list[str] = []
    section: str | None = None

    if not config_path.exists():
        return subreddits, keywords

    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.endswith(":"):
            section = line[:-1]
            continue
        if line.startswith("-") and section in {"subreddits", "keywords"}:
            value = line[1:].strip()
            if section == "subreddits":
                subreddits.append(value)
            else:
                keywords.append(value)

    return subreddits, keywords


def _ensure_reddit_source(session: Any) -> int:
    session.execute(
        text(
            """
            INSERT OR IGNORE INTO sources (platform, external_id, name, metadata_json)
            VALUES ('reddit', 'reddit', 'Reddit', '{}')
            """
        )
    )
    row = session.execute(
        text("SELECT id FROM sources WHERE platform='reddit' AND external_id='reddit' LIMIT 1")
    ).first()
    if row is None:
        raise RuntimeError("Unable to resolve reddit source row")
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


def run() -> None:
    """Run Reddit ingestion for the previous 30 days and persist run stats."""

    setup_logging()
    subreddits, keywords = _load_reddit_source_config()
    if not subreddits:
        raise RuntimeError("No subreddits configured. Set REDDIT_SUBREDDITS or source_config.yaml")

    platform = os.getenv("INGEST_PLATFORM", "reddit").strip().lower()

    session = SessionLocal()
    run_repo = IngestionRunRepository(session)
    run_id: int | None = None

    try:
        _safe_ensure_dedupe_constraints(session)
        source_id = _ensure_reddit_source(session)
        session.commit()

        run_id = run_repo.start_run(source_name=platform)

        adapter_class = get_adapter_class(platform)
        ingestor = adapter_class()
        docs, stats = ingestor.run(
            config={"subreddits": subreddits, "keywords": keywords},
            days_back=30,
        )
        inserted = _insert_documents(session, source_id, docs)

        run_repo.complete_run(
            run_id=run_id,
            records_fetched=stats.docs_emitted,
            records_inserted=inserted,
            status="completed",
        )
        logger.info(
            f"{platform} refresh completed",
            extra={
                "records_fetched": stats.docs_emitted,
                "records_inserted": inserted,
                "subreddits": subreddits,
            },
        )
    except Exception as exc:
        if run_id is not None:
            run_repo.complete_run(
                run_id=run_id,
                records_fetched=0,
                records_inserted=0,
                status="failed",
                error_message=str(exc),
            )
        logger.exception(f"{platform} refresh failed")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    run()
