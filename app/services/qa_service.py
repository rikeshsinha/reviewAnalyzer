"""Question-answering service grounded on retrieved Reddit evidence."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from openai import OpenAI
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.prompts.qa import QA_SYSTEM_INSTRUCTIONS
from app.services.retrieval_service import RetrievalService


@dataclass
class QAConfig:
    """Configuration for retrieval + answer synthesis."""

    model_name: str = "gpt-4.1-mini"
    max_evidence_docs: int = 8
    min_evidence_docs: int = 2
    max_snippet_chars: int = 280


class QAService:
    """Generate evidence-grounded answers for free-form user questions."""

    def __init__(
        self,
        session: Session,
        client: OpenAI,
        retrieval_service: RetrievalService | None = None,
        config: QAConfig | None = None,
    ) -> None:
        self.session = session
        self.client = client
        self.retrieval_service = retrieval_service or RetrievalService(session)
        self.config = config or QAConfig()

    def answer_question(
        self,
        question: str,
        filters: dict[str, Any] | None = None,
        top_n: int | None = None,
    ) -> dict[str, Any]:
        normalized_question = (question or "").strip()
        active_filters = dict(filters or {})
        if not normalized_question:
            return self._empty_response(reason="Question is empty.")

        retrieval_limit = max(1, min(top_n or self.config.max_evidence_docs, 25))
        docs = self.retrieval_service.retrieve_for_question(
            question=normalized_question,
            filters=active_filters,
            limit=retrieval_limit,
        )
        evidence = self._build_compact_evidence(docs)

        if len(evidence) < self.config.min_evidence_docs:
            return {
                **self._empty_response(reason="Insufficient evidence in selected filters."),
                "question": normalized_question,
                "filters": active_filters,
                "insufficient_evidence": True,
                "evidence": evidence,
            }

        llm_result = self._call_llm(
            question=normalized_question,
            filters=active_filters,
            evidence=evidence,
        )

        cited_ids = [eid for eid in llm_result.get("cited_evidence_ids", []) if isinstance(eid, int)]
        known_ids = {int(item["id"]) for item in evidence}
        cited_ids = [eid for eid in cited_ids if eid in known_ids]

        return {
            "question": normalized_question,
            "filters": active_filters,
            "insufficient_evidence": False,
            "answer": llm_result.get("answer") or "Based on Reddit data in selected filters.\n\nNo grounded answer available.",
            "key_points": self._ensure_str_list(llm_result.get("key_points")),
            "caveats": self._ensure_str_list(llm_result.get("caveats")),
            "contradictions": self._ensure_str_list(llm_result.get("contradictions")),
            "cited_evidence_ids": cited_ids,
            "evidence": evidence,
        }

    def _build_compact_evidence(self, docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        source_map = self._fetch_source_names([int(doc["source_id"]) for doc in docs if doc.get("source_id") is not None])

        compact_items: list[dict[str, Any]] = []
        for doc in docs:
            doc_id = int(doc["id"])
            snippet_source = (doc.get("body") or doc.get("title") or "").replace("\n", " ").strip()
            compact_items.append(
                {
                    "id": doc_id,
                    "date": (doc.get("published_at") or doc.get("created_at") or "")[:10],
                    "source": source_map.get(int(doc.get("source_id") or 0), "unknown"),
                    "url": doc.get("url") or "",
                    "snippet": snippet_source[: self.config.max_snippet_chars],
                    "title": (doc.get("title") or "").strip(),
                }
            )
        return compact_items

    def _fetch_source_names(self, source_ids: list[int]) -> dict[int, str]:
        if not source_ids:
            return {}

        unique_ids = list(dict.fromkeys(source_ids))
        placeholders: list[str] = []
        params: dict[str, Any] = {}
        for index, source_id in enumerate(unique_ids):
            key = f"sid_{index}"
            params[key] = source_id
            placeholders.append(f":{key}")

        rows = self.session.execute(
            text(
                f"""
                SELECT id, name
                FROM sources
                WHERE id IN ({', '.join(placeholders)})
                """
            ),
            params,
        ).fetchall()
        return {int(row.id): str(row.name) for row in rows}

    def _call_llm(self, question: str, filters: dict[str, Any], evidence: list[dict[str, Any]]) -> dict[str, Any]:
        completion = self.client.chat.completions.create(
            model=self.config.model_name,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": QA_SYSTEM_INSTRUCTIONS},
                {
                    "role": "user",
                    "content": (
                        "Question:\n"
                        f"{question}\n\n"
                        "Selected filters (already applied to retrieval):\n"
                        f"{json.dumps(filters, ensure_ascii=False)}\n\n"
                        "Evidence items:\n"
                        f"{json.dumps(evidence, ensure_ascii=False)}"
                    ),
                },
            ],
        )
        raw = completion.choices[0].message.content or "{}"
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return self._empty_response(reason="Model returned invalid JSON.")

        return payload if isinstance(payload, dict) else self._empty_response(reason="Model returned non-object JSON.")

    def _ensure_str_list(self, raw: Any) -> list[str]:
        if not isinstance(raw, list):
            return []
        return [str(item).strip() for item in raw if str(item).strip()][:8]

    def _empty_response(self, reason: str) -> dict[str, Any]:
        return {
            "answer": "Based on Reddit data in selected filters.\n\nInsufficient evidence to answer confidently.",
            "key_points": [],
            "caveats": [reason],
            "contradictions": [],
            "cited_evidence_ids": [],
            "evidence": [],
            "insufficient_evidence": True,
        }
