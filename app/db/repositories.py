"""Repository classes for common database operations."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


class DocumentRepository:
    """Persistence helpers for the documents table."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def create(self, document: dict[str, Any]) -> int:
        query = text(
            """
            INSERT INTO documents (
                source_id, external_id, title, body, author, url, published_at, raw_json
            ) VALUES (
                :source_id, :external_id, :title, :body, :author, :url, :published_at, :raw_json
            )
            """
        )
        result = self.session.execute(query, document)
        self.session.commit()
        return int(result.lastrowid)


class TagRepository:
    """Persistence helpers for the document_tags table."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def add_tag(self, tag: dict[str, Any]) -> None:
        query = text(
            """
            INSERT OR IGNORE INTO document_tags (
                document_id, tag_type, tag_value, tag_source, confidence
            ) VALUES (
                :document_id, :tag_type, :tag_value, :tag_source, :confidence
            )
            """
        )
        self.session.execute(query, tag)
        self.session.commit()


class IngestionRunRepository:
    """Persistence helpers for the ingestion_runs table."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def start_run(self, source_name: str) -> int:
        query = text(
            """
            INSERT INTO ingestion_runs (source_name, status)
            VALUES (:source_name, 'running')
            """
        )
        result = self.session.execute(query, {"source_name": source_name})
        self.session.commit()
        return int(result.lastrowid)

    def complete_run(
        self,
        run_id: int,
        records_fetched: int,
        records_inserted: int,
        status: str = "completed",
        error_message: str | None = None,
    ) -> None:
        query = text(
            """
            UPDATE ingestion_runs
            SET completed_at = CURRENT_TIMESTAMP,
                status = :status,
                records_fetched = :records_fetched,
                records_inserted = :records_inserted,
                error_message = :error_message
            WHERE id = :run_id
            """
        )
        self.session.execute(
            query,
            {
                "run_id": run_id,
                "status": status,
                "records_fetched": records_fetched,
                "records_inserted": records_inserted,
                "error_message": error_message,
            },
        )
        self.session.commit()


class EnrichmentRunRepository:
    """Persistence helpers for the enrichment_runs table."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def start_run(self) -> int:
        query = text(
            """
            INSERT INTO enrichment_runs (status)
            VALUES ('running')
            """
        )
        result = self.session.execute(query)
        self.session.commit()
        return int(result.lastrowid)

    def complete_run(
        self,
        run_id: int,
        candidates: int,
        enriched: int,
        skipped_short: int,
        failed_batches: int,
        status: str = "completed",
        error_message: str | None = None,
    ) -> None:
        query = text(
            """
            UPDATE enrichment_runs
            SET completed_at = CURRENT_TIMESTAMP,
                status = :status,
                candidates = :candidates,
                enriched = :enriched,
                skipped_short = :skipped_short,
                failed_batches = :failed_batches,
                error_message = :error_message
            WHERE id = :run_id
            """
        )
        self.session.execute(
            query,
            {
                "run_id": run_id,
                "status": status,
                "candidates": candidates,
                "enriched": enriched,
                "skipped_short": skipped_short,
                "failed_batches": failed_batches,
                "error_message": error_message,
            },
        )
        self.session.commit()
