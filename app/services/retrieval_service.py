"""Retrieval service for filtered FTS-backed document search."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass
class _QueryParts:
    joins: list[str]
    where: list[str]
    params: dict[str, Any]


class RetrievalService:
    """High-level retrieval methods for explorer and Q&A workflows."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def search_documents(
        self,
        query: str,
        filters: dict[str, Any] | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Search documents with metadata filters + FTS ranking + recency tiebreak."""

        safe_limit = max(1, min(limit, 200))
        safe_offset = max(0, offset)
        filters = filters or {}

        parts = self._build_metadata_filters(filters)

        sql = f"""
            WITH filtered_docs AS (
                SELECT d.*
                FROM documents d
                {' '.join(parts.joins)}
                WHERE 1=1
                  {' '.join(parts.where)}
            )
            SELECT
                d.id,
                d.source_id,
                d.external_id,
                d.title,
                d.body,
                d.author,
                d.url,
                d.published_at,
                d.raw_json,
                d.created_at,
                bm25(documents_fts, 1.5, 1.0, 0.8) AS fts_score
            FROM filtered_docs d
            JOIN documents_fts ON documents_fts.rowid = d.id
            WHERE documents_fts MATCH :query
            ORDER BY fts_score ASC, datetime(COALESCE(d.published_at, d.created_at)) DESC, d.id DESC
            LIMIT :limit OFFSET :offset
        """

        params = {
            **parts.params,
            "query": query,
            "limit": safe_limit,
            "offset": safe_offset,
        }
        result = self.session.execute(text(sql), params)
        return [dict(row._mapping) for row in result.fetchall()]

    def retrieve_for_question(
        self,
        question: str,
        filters: dict[str, Any] | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Return top candidate documents for a user question."""

        return self.search_documents(query=question, filters=filters, limit=limit, offset=0)

    def get_documents_by_ids(self, ids: list[int]) -> list[dict[str, Any]]:
        """Fetch documents by ids while preserving caller-specified order."""

        if not ids:
            return []

        unique_ids = list(dict.fromkeys(ids))
        placeholders = []
        params: dict[str, Any] = {}
        for index, doc_id in enumerate(unique_ids):
            key = f"id_{index}"
            placeholders.append(f":{key}")
            params[key] = int(doc_id)

        sql = f"""
            SELECT
                id,
                source_id,
                external_id,
                title,
                body,
                author,
                url,
                published_at,
                raw_json,
                created_at
            FROM documents
            WHERE id IN ({', '.join(placeholders)})
        """
        rows = self.session.execute(text(sql), params).fetchall()
        row_map = {int(row.id): dict(row._mapping) for row in rows}

        return [row_map[doc_id] for doc_id in unique_ids if doc_id in row_map]

    def _build_metadata_filters(self, filters: dict[str, Any]) -> _QueryParts:
        joins: list[str] = []
        where: list[str] = []
        params: dict[str, Any] = {}

        if source := filters.get("source"):
            where.append("AND EXISTS (SELECT 1 FROM sources s WHERE s.id = d.source_id AND s.name = :source)")
            params["source"] = source

        if subreddit := filters.get("subreddit"):
            where.append("AND json_extract(d.raw_json, '$.subreddit') = :subreddit")
            params["subreddit"] = subreddit

        if date_from := filters.get("date_from"):
            where.append("AND datetime(d.published_at) >= datetime(:date_from)")
            params["date_from"] = date_from

        if date_to := filters.get("date_to"):
            where.append("AND datetime(d.published_at) <= datetime(:date_to)")
            params["date_to"] = date_to

        self._append_tag_filter("product", filters.get("product_tags"), where, params)
        self._append_tag_filter("issue", filters.get("issue_tags"), where, params)
        self._append_tag_filter("competitor", filters.get("competitor_tags"), where, params)

        if sentiment_label := filters.get("sentiment_label"):
            where.append(
                """
                AND EXISTS (
                    SELECT 1
                    FROM enrichments e
                    WHERE e.document_id = d.id
                      AND (
                        json_extract(e.metadata_json, '$.sentiment_label') = :sentiment_label
                        OR json_extract(e.metadata_json, '$.sentiment.label') = :sentiment_label
                      )
                )
                """
            )
            params["sentiment_label"] = sentiment_label

        return _QueryParts(joins=joins, where=where, params=params)

    def _append_tag_filter(
        self,
        tag_type: str,
        values: list[str] | None,
        where: list[str],
        params: dict[str, Any],
    ) -> None:
        if not values:
            return

        placeholders: list[str] = []
        for index, value in enumerate(values):
            key = f"{tag_type}_tag_{index}"
            placeholders.append(f":{key}")
            params[key] = value

        where.append(
            f"""
            AND EXISTS (
                SELECT 1
                FROM document_tags dt
                WHERE dt.document_id = d.id
                  AND dt.tag_type = :{tag_type}_tag_type
                  AND dt.tag_value IN ({', '.join(placeholders)})
            )
            """
        )
        params[f"{tag_type}_tag_type"] = tag_type
