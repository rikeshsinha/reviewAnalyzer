"""Ingestion helpers for creating documents and auto-tagging them."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.db.repositories import DocumentRepository, TagRepository
from app.services.tagging_service import TaggingService
from app.utils.text_cleaning import clean_whitespace, is_deleted_content, normalize_url


class DocumentIngestionService:
    """Insert documents and persist rule-based tags in one flow."""

    def __init__(self, session: Session, tagging_service: TaggingService | None = None) -> None:
        self.document_repository = DocumentRepository(session)
        self.tag_repository = TagRepository(session)
        self.tagging_service = tagging_service or TaggingService()

    def ingest_document(self, document: dict[str, Any]) -> int:
        body = document.get("body")
        title = document.get("title")

        normalized_payload = {
            **document,
            "title": clean_whitespace(title),
            "body": "" if is_deleted_content(body) else clean_whitespace(body),
            "url": normalize_url(document.get("url")),
        }

        document_id = self.document_repository.create(normalized_payload)

        tag_text = " ".join(filter(None, [normalized_payload.get("title"), normalized_payload.get("body")]))
        for tag in self.tagging_service.extract_all_tags(tag_text):
            self.tag_repository.add_tag({"document_id": document_id, **tag})

        return document_id
