"""LLM-based enrichment for newly ingested documents."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from openai import APIConnectionError, APITimeoutError, InternalServerError, OpenAI, RateLimitError
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.prompts.complaints import COMPLAINTS_INSTRUCTIONS, PRIMARY_ISSUE_CATEGORIES
from app.prompts.feature_requests import FEATURE_REQUESTS_INSTRUCTIONS
from app.prompts.sentiment import SENTIMENT_INSTRUCTIONS, SENTIMENT_PROMPT_VERSION

_ALLOWED_SENTIMENT_LABELS = {"positive", "neutral", "negative", "mixed"}

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentConfig:
    model_name: str = "gpt-4.1-mini"
    batch_size: int = 3
    max_docs_per_run: int = 100
    min_text_chars: int = 15
    max_text_chars: int = 3500
    max_retries: int = 3


class EnrichmentService:
    """Incrementally enrich documents that do not yet have enrichment rows."""

    def __init__(self, session: Session, client: OpenAI, config: EnrichmentConfig | None = None) -> None:
        self.session = session
        self.client = client
        self.config = config or EnrichmentConfig()

    def enrich_new_documents(self) -> dict[str, int]:
        documents = self._fetch_documents_missing_enrichment(limit=self.config.max_docs_per_run)
        if not documents:
            return {"candidates": 0, "enriched": 0, "skipped_short": 0, "failed_batches": 0}

        processable: list[dict[str, Any]] = []
        skipped_short = 0
        fallback_docs: list[dict[str, Any]] = []
        for doc in documents:
            prepared_text = self._prepare_text(doc)
            if len(prepared_text) < self.config.min_text_chars:
                skipped_short += 1
                fallback_docs.append({**doc, "prepared_text": prepared_text})
                continue
            processable.append({**doc, "prepared_text": prepared_text[: self.config.max_text_chars]})

        enriched_count = self._insert_fallback_enrichments(fallback_docs)
        failed_batches = 0
        for batch in self._batched(processable, batch_size=self.config.batch_size):
            try:
                enriched_count += self._enrich_batch(batch)
            except Exception:  # noqa: BLE001
                failed_batches += 1
                logger.exception("Enrichment batch failed", extra={"batch_size": len(batch)})

        return {
            "candidates": len(documents),
            "enriched": enriched_count,
            "skipped_short": skipped_short,
            "failed_batches": failed_batches,
        }

    def _fetch_documents_missing_enrichment(self, limit: int) -> list[dict[str, Any]]:
        rows = self.session.execute(
            text(
                """
                SELECT d.id, d.title, d.body
                     , s.platform
                     , json_extract(d.raw_json, '$.rating') AS rating
                FROM documents d
                JOIN sources s ON s.id = d.source_id
                LEFT JOIN enrichments e ON e.document_id = d.id
                WHERE e.id IS NULL
                ORDER BY d.id ASC
                LIMIT :limit
                """
            ),
            {"limit": max(1, limit)},
        ).fetchall()
        return [dict(row._mapping) for row in rows]

    def _enrich_batch(self, batch: list[dict[str, Any]]) -> int:
        if not batch:
            return 0

        payload = [
            {
                "document_id": doc["id"],
                "text": doc["prepared_text"],
                **({"rating": self._coerce_rating(doc.get("rating"))} if doc.get("platform") == "google_play" else {}),
            }
            for doc in batch
        ]
        response_json = self._invoke_llm_with_retry(payload)
        parsed_docs = self._parse_response(response_json)
        if not parsed_docs:
            return 0

        now_iso = datetime.now(tz=timezone.utc).isoformat()
        parsed_by_id = {int(item["document_id"]): item for item in parsed_docs if "document_id" in item}

        inserted = 0
        for source_doc in batch:
            doc_id = int(source_doc["id"])
            item = parsed_by_id.get(doc_id)
            if not item:
                continue

            metadata = {
                "rating": self._coerce_rating(source_doc.get("rating")),
                "sentiment_label": self._coerce_sentiment(item.get("sentiment_label")),
                "primary_issue_category": self._coerce_issue_category(item.get("primary_issue_category")),
                "issue_category": self._coerce_issue_category(item.get("primary_issue_category")),
                "feature_request_flag": bool(item.get("feature_request_flag", False)),
                "competitor_mentions": self._coerce_competitor_mentions(item.get("competitor_mentions")),
                "summary_snippet": str(item.get("summary_snippet", "")).strip()[:400],
                "model_name": self.config.model_name,
                "prompt_version": SENTIMENT_PROMPT_VERSION,
                "enriched_at": now_iso,
            }

            self.session.execute(
                text(
                    """
                    INSERT INTO enrichments (document_id, model_name, summary, sentiment_score, metadata_json)
                    VALUES (:document_id, :model_name, :summary, :sentiment_score, :metadata_json)
                    """
                ),
                {
                    "document_id": doc_id,
                    "model_name": self.config.model_name,
                    "summary": metadata["summary_snippet"],
                    "sentiment_score": None,
                    "metadata_json": json.dumps(metadata),
                },
            )
            inserted += 1

        self.session.commit()
        return inserted

    def _invoke_llm_with_retry(self, payload: list[dict[str, Any]]) -> str:
        retries = 0
        while True:
            try:
                completion = self.client.chat.completions.create(
                    model=self.config.model_name,
                    temperature=0,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": self._build_system_prompt()},
                        {
                            "role": "user",
                            "content": (
                                "Return JSON with key 'documents', where each item has: "
                                "document_id, sentiment_label, primary_issue_category, feature_request_flag, "
                                "competitor_mentions, summary_snippet.\n\n"
                                f"Input:\n{json.dumps(payload)}"
                            ),
                        },
                    ],
                )
                return completion.choices[0].message.content or "{}"
            except (RateLimitError, APITimeoutError, APIConnectionError, InternalServerError):
                retries += 1
                if retries > self.config.max_retries:
                    raise
                time.sleep(min(2**retries, 8))

    def _build_system_prompt(self) -> str:
        return (
            "You enrich customer feedback documents. Output strict JSON only.\n"
            f"{SENTIMENT_INSTRUCTIONS}\n"
            f"{COMPLAINTS_INSTRUCTIONS}\n"
            f"{FEATURE_REQUESTS_INSTRUCTIONS}\n"
            "primary_issue_category must be one of: "
            + ", ".join(PRIMARY_ISSUE_CATEGORIES)
            + ". summary_snippet must be <= 300 characters."
        )

    def _parse_response(self, raw_json: str) -> list[dict[str, Any]]:
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            return []

        documents = payload.get("documents", [])
        if not isinstance(documents, list):
            return []
        return [item for item in documents if isinstance(item, dict)]

    def _prepare_text(self, doc: dict[str, Any]) -> str:
        title = (doc.get("title") or "").strip()
        body = (doc.get("body") or "").strip()
        return "\n\n".join(part for part in [title, body] if part)

    def _batched(self, items: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
        size = max(1, batch_size)
        return [items[index : index + size] for index in range(0, len(items), size)]

    def _coerce_sentiment(self, raw_value: Any) -> str:
        label = str(raw_value or "neutral").strip().lower()
        if label not in _ALLOWED_SENTIMENT_LABELS:
            return "neutral"
        return label

    def _coerce_issue_category(self, raw_value: Any) -> str:
        category = str(raw_value or "other").strip().lower()
        if category not in PRIMARY_ISSUE_CATEGORIES:
            return "other"
        return category

    def _coerce_competitor_mentions(self, raw_value: Any) -> list[str]:
        if not isinstance(raw_value, list):
            return []

        deduped: list[str] = []
        seen: set[str] = set()
        for value in raw_value:
            normalized = str(value).strip()
            if not normalized:
                continue
            dedupe_key = normalized.lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            deduped.append(normalized)
        return deduped[:20]

    def _coerce_rating(self, raw_value: Any) -> int | None:
        if raw_value is None or raw_value == "":
            return None
        try:
            rating = int(float(raw_value))
        except (TypeError, ValueError):
            return None
        if rating < 1 or rating > 5:
            return None
        return rating

    def _fallback_sentiment(self, text: str, rating: int | None) -> str:
        normalized = text.lower()
        if rating is not None:
            if rating <= 2:
                return "negative"
            if rating >= 4:
                return "positive"
        if any(token in normalized for token in ("bad", "bug", "crash", "broken", "hate")):
            return "negative"
        if any(token in normalized for token in ("good", "great", "love", "nice")):
            return "positive"
        return "neutral"

    def _insert_fallback_enrichments(self, docs: list[dict[str, Any]]) -> int:
        if not docs:
            return 0

        now_iso = datetime.now(tz=timezone.utc).isoformat()
        inserted = 0
        for doc in docs:
            rating = self._coerce_rating(doc.get("rating"))
            prepared_text = str(doc.get("prepared_text") or "")
            metadata = {
                "rating": rating,
                "sentiment_label": self._fallback_sentiment(prepared_text, rating),
                "primary_issue_category": "other",
                "issue_category": "other",
                "feature_request_flag": False,
                "competitor_mentions": [],
                "summary_snippet": prepared_text[:300],
                "model_name": "fallback_rules",
                "prompt_version": SENTIMENT_PROMPT_VERSION,
                "enriched_at": now_iso,
            }
            self.session.execute(
                text(
                    """
                    INSERT INTO enrichments (document_id, model_name, summary, sentiment_score, metadata_json)
                    VALUES (:document_id, :model_name, :summary, :sentiment_score, :metadata_json)
                    """
                ),
                {
                    "document_id": int(doc["id"]),
                    "model_name": "fallback_rules",
                    "summary": metadata["summary_snippet"],
                    "sentiment_score": None,
                    "metadata_json": json.dumps(metadata),
                },
            )
            inserted += 1

        self.session.commit()
        return inserted
