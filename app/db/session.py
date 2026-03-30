"""SQLAlchemy engine and session factory."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_URL = f"sqlite:///{REPO_ROOT / 'data' / 'app.db'}"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

logger = logging.getLogger(__name__)
REQUIRED_TABLES = (
    "sources",
    "documents",
    "document_tags",
    "enrichments",
    "ingestion_runs",
    "enrichment_runs",
    "saved_insights",
    "documents_fts",
)
SCHEMA_PATH = REPO_ROOT / "app" / "db" / "schema.sql"


def bootstrap_database() -> None:
    """Ensure required DB tables exist; if missing, initialize from schema.sql."""

    with engine.connect() as connection:
        rows = connection.exec_driver_sql(
            """
            SELECT name
            FROM sqlite_master
            WHERE type IN ('table', 'view')
              AND name IN (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            REQUIRED_TABLES,
        ).fetchall()

    existing_tables = {str(row[0]) for row in rows}
    missing_tables = sorted(set(REQUIRED_TABLES) - existing_tables)
    if not missing_tables:
        logger.info("Database bootstrap check passed. All required tables are present.")
        return

    logger.warning(
        "Database missing required tables (%s). Applying schema from %s.",
        ", ".join(missing_tables),
        SCHEMA_PATH,
    )

    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    raw_connection = engine.raw_connection()
    try:
        cursor = raw_connection.cursor()
        cursor.executescript(schema_sql)
        raw_connection.commit()
        logger.info("Database bootstrap completed successfully.")
    except Exception:
        raw_connection.rollback()
        logger.exception("Database bootstrap failed while applying %s.", SCHEMA_PATH)
        raise
    finally:
        raw_connection.close()
