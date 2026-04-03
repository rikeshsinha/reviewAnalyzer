"""Insight generation service using SQL aggregates + concise LLM narratives."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
from typing import Any

from openai import OpenAI
from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass
class AnalysisConfig:
    """Configuration for insight generation."""

    model_name: str = "gpt-4.1-mini"
    cache_ttl_minutes: int = 120
    max_evidence_items: int = 8


class AnalysisService:
    """Build module-specific insights from SQL metrics and LLM narratives."""

    def __init__(
        self,
        session: Session,
        client: OpenAI | None = None,
        config: AnalysisConfig | None = None,
    ) -> None:
        self.session = session
        self.client = client
        self.config = config or AnalysisConfig()

    def generate_sentiment_insight(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        module_name = "sentiment"
        filters = filters or {}

        cached = self._get_cached_insight(module_name, filters)
        if cached is not None:
            return cached

        where_sql, params = self._build_document_filter_clause(filters)

        totals_row = self.session.execute(
            text(
                f"""
                SELECT
                    COUNT(*) AS total_docs,
                    SUM(CASE WHEN json_extract(e.metadata_json, '$.sentiment_label') = 'positive' THEN 1 ELSE 0 END) AS positive_count,
                    SUM(CASE WHEN json_extract(e.metadata_json, '$.sentiment_label') = 'neutral' THEN 1 ELSE 0 END) AS neutral_count,
                    SUM(CASE WHEN json_extract(e.metadata_json, '$.sentiment_label') = 'negative' THEN 1 ELSE 0 END) AS negative_count,
                    SUM(CASE WHEN json_extract(e.metadata_json, '$.sentiment_label') = 'mixed' THEN 1 ELSE 0 END) AS mixed_count
                FROM documents d
                JOIN enrichments e ON e.document_id = d.id
                WHERE 1=1 {where_sql}
                """
            ),
            params,
        ).fetchone()

        trend_rows = self.session.execute(
            text(
                f"""
                SELECT
                    DATE(COALESCE(d.published_at, d.created_at)) AS day,
                    SUM(CASE WHEN json_extract(e.metadata_json, '$.sentiment_label') = 'positive' THEN 1 ELSE 0 END) AS positive_count,
                    SUM(CASE WHEN json_extract(e.metadata_json, '$.sentiment_label') = 'negative' THEN 1 ELSE 0 END) AS negative_count,
                    SUM(CASE WHEN json_extract(e.metadata_json, '$.sentiment_label') = 'neutral' THEN 1 ELSE 0 END) AS neutral_count,
                    SUM(CASE WHEN json_extract(e.metadata_json, '$.sentiment_label') = 'mixed' THEN 1 ELSE 0 END) AS mixed_count
                FROM documents d
                JOIN enrichments e ON e.document_id = d.id
                WHERE 1=1 {where_sql}
                GROUP BY DATE(COALESCE(d.published_at, d.created_at))
                ORDER BY day DESC
                LIMIT 14
                """
            ),
            params,
        ).fetchall()

        metrics = {
            "total_docs": int(totals_row.total_docs or 0),
            "sentiment_distribution": {
                "positive": int(totals_row.positive_count or 0),
                "neutral": int(totals_row.neutral_count or 0),
                "negative": int(totals_row.negative_count or 0),
                "mixed": int(totals_row.mixed_count or 0),
            },
            "daily_sentiment_trend": [dict(row._mapping) for row in trend_rows],
        }

        evidence = self._build_evidence(
            where_sql=where_sql,
            params=params,
            extra_condition="AND json_extract(e.metadata_json, '$.sentiment_label') IN ('negative', 'mixed')",
            limit=self.config.max_evidence_items,
        )

        summary = self._generate_summary(module_name=module_name, metrics=metrics, evidence=evidence)
        payload = {"summary": summary, "metrics": metrics, "evidence": evidence}
        self._save_cached_insight(module_name, filters, payload)
        return payload

    def generate_complaints_insight(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        module_name = "complaints"
        filters = filters or {}

        cached = self._get_cached_insight(module_name, filters)
        if cached is not None:
            return cached

        where_sql, params = self._build_document_filter_clause(filters)

        totals_row = self.session.execute(
            text(
                f"""
                SELECT COUNT(*) AS complaint_docs
                FROM documents d
                JOIN enrichments e ON e.document_id = d.id
                WHERE 1=1 {where_sql}
                """
            ),
            params,
        ).fetchone()

        category_rows = self.session.execute(
            text(
                f"""
                SELECT
                    COALESCE(json_extract(e.metadata_json, '$.primary_issue_category'), 'other') AS category,
                    COUNT(*) AS count
                FROM documents d
                JOIN enrichments e ON e.document_id = d.id
                WHERE 1=1 {where_sql}
                GROUP BY category
                ORDER BY count DESC
                LIMIT 8
                """
            ),
            params,
        ).fetchall()

        trend_rows = self.session.execute(
            text(
                f"""
                SELECT
                    DATE(COALESCE(d.published_at, d.created_at)) AS day,
                    COUNT(*) AS complaint_count
                FROM documents d
                JOIN enrichments e ON e.document_id = d.id
                WHERE 1=1 {where_sql}
                GROUP BY DATE(COALESCE(d.published_at, d.created_at))
                ORDER BY day DESC
                LIMIT 14
                """
            ),
            params,
        ).fetchall()

        metrics = {
            "complaint_docs": int(totals_row.complaint_docs or 0),
            "top_issue_categories": [dict(row._mapping) for row in category_rows],
            "daily_complaint_trend": [dict(row._mapping) for row in trend_rows],
        }

        evidence = self._build_evidence(
            where_sql=where_sql,
            params=params,
            extra_condition="",
            limit=self.config.max_evidence_items,
        )

        summary = self._generate_summary(module_name=module_name, metrics=metrics, evidence=evidence)
        payload = {"summary": summary, "metrics": metrics, "evidence": evidence}
        self._save_cached_insight(module_name, filters, payload)
        return payload

    def generate_feature_requests_insight(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        module_name = "feature_requests"
        filters = filters or {}

        cached = self._get_cached_insight(module_name, filters)
        if cached is not None:
            return cached

        where_sql, params = self._build_document_filter_clause(filters)

        totals_row = self.session.execute(
            text(
                f"""
                SELECT COUNT(*) AS feature_request_docs
                FROM documents d
                JOIN enrichments e ON e.document_id = d.id
                WHERE 1=1 {where_sql}
                  AND COALESCE(json_extract(e.metadata_json, '$.feature_request_flag'), 0) = 1
                """
            ),
            params,
        ).fetchone()

        top_feature_rows = self.session.execute(
            text(
                f"""
                SELECT dt.tag_value AS feature, COUNT(*) AS count
                FROM documents d
                JOIN enrichments e ON e.document_id = d.id
                JOIN document_tags dt ON dt.document_id = d.id AND dt.tag_type = 'feature'
                WHERE 1=1 {where_sql}
                  AND COALESCE(json_extract(e.metadata_json, '$.feature_request_flag'), 0) = 1
                GROUP BY dt.tag_value
                ORDER BY count DESC
                LIMIT 10
                """
            ),
            params,
        ).fetchall()

        trend_rows = self.session.execute(
            text(
                f"""
                SELECT
                    DATE(COALESCE(d.published_at, d.created_at)) AS day,
                    COUNT(*) AS feature_request_count
                FROM documents d
                JOIN enrichments e ON e.document_id = d.id
                WHERE 1=1 {where_sql}
                  AND COALESCE(json_extract(e.metadata_json, '$.feature_request_flag'), 0) = 1
                GROUP BY DATE(COALESCE(d.published_at, d.created_at))
                ORDER BY day DESC
                LIMIT 14
                """
            ),
            params,
        ).fetchall()

        metrics = {
            "feature_request_docs": int(totals_row.feature_request_docs or 0),
            "top_requested_features": [dict(row._mapping) for row in top_feature_rows],
            "daily_feature_request_trend": [dict(row._mapping) for row in trend_rows],
        }

        evidence = self._build_evidence(
            where_sql=where_sql,
            params=params,
            extra_condition="AND COALESCE(json_extract(e.metadata_json, '$.feature_request_flag'), 0) = 1",
            limit=self.config.max_evidence_items,
        )

        summary = self._generate_summary(module_name=module_name, metrics=metrics, evidence=evidence)
        payload = {"summary": summary, "metrics": metrics, "evidence": evidence}
        self._save_cached_insight(module_name, filters, payload)
        return payload

    def _build_document_filter_clause(self, filters: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        where_parts: list[str] = []
        params: dict[str, Any] = {}

        if source := filters.get("source"):
            where_parts.append("AND EXISTS (SELECT 1 FROM sources s WHERE s.id = d.source_id AND s.name = :source)")
            params["source"] = source

        if subreddit := filters.get("subreddit"):
            where_parts.append("AND json_extract(d.raw_json, '$.subreddit') = :subreddit")
            params["subreddit"] = subreddit

        if google_play_app := filters.get("google_play_app"):
            where_parts.append("AND json_extract(d.raw_json, '$.community_or_channel') = :google_play_app")
            params["google_play_app"] = google_play_app

        if rating := filters.get("rating"):
            where_parts.append("AND CAST(json_extract(d.raw_json, '$.rating') AS INTEGER) = :rating")
            params["rating"] = int(rating)

        if date_from := filters.get("date_from"):
            where_parts.append("AND datetime(COALESCE(d.published_at, d.created_at)) >= datetime(:date_from)")
            params["date_from"] = date_from

        if date_to := filters.get("date_to"):
            where_parts.append("AND datetime(COALESCE(d.published_at, d.created_at)) <= datetime(:date_to)")
            params["date_to"] = date_to

        self._append_tag_filter("product", filters.get("product_tags"), where_parts, params)
        self._append_tag_filter("issue", filters.get("issue_tags"), where_parts, params)
        self._append_tag_filter("competitor", filters.get("competitor_tags"), where_parts, params)
        self._append_tag_filter("feature", filters.get("feature_tags"), where_parts, params)

        return " ".join(where_parts), params

    def _append_tag_filter(
        self,
        tag_type: str,
        values: list[str] | None,
        where_parts: list[str],
        params: dict[str, Any],
    ) -> None:
        if not values:
            return

        placeholders: list[str] = []
        for idx, value in enumerate(values):
            key = f"{tag_type}_value_{idx}"
            params[key] = value
            placeholders.append(f":{key}")

        params[f"{tag_type}_type"] = tag_type
        where_parts.append(
            f"""
            AND EXISTS (
                SELECT 1
                FROM document_tags dtf
                WHERE dtf.document_id = d.id
                  AND dtf.tag_type = :{tag_type}_type
                  AND dtf.tag_value IN ({', '.join(placeholders)})
            )
            """
        )

    def _build_evidence(
        self,
        where_sql: str,
        params: dict[str, Any],
        extra_condition: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        evidence_rows = self.session.execute(
            text(
                f"""
                SELECT
                    d.id AS doc_id,
                    d.title,
                    d.url,
                    SUBSTR(COALESCE(e.summary, d.body, ''), 1, 280) AS snippet,
                    COALESCE(d.published_at, d.created_at) AS date,
                    s.name AS source
                FROM documents d
                JOIN enrichments e ON e.document_id = d.id
                JOIN sources s ON s.id = d.source_id
                WHERE 1=1 {where_sql}
                {extra_condition}
                ORDER BY datetime(COALESCE(d.published_at, d.created_at)) DESC, d.id DESC
                LIMIT :evidence_limit
                """
            ),
            {**params, "evidence_limit": max(1, limit)},
        ).fetchall()

        normalized: list[dict[str, Any]] = []
        for row in evidence_rows:
            item = dict(row._mapping)
            url = str(item.get("url") or "").strip()
            item["evidence_url"] = url or f"#document-{item.get('doc_id')}"
            normalized.append(item)
        return normalized

    def _generate_summary(self, module_name: str, metrics: dict[str, Any], evidence: list[dict[str, Any]]) -> str:
        if self.client is None:
            return self._fallback_summary(module_name, metrics)

        prompt_payload = {
            "module": module_name,
            "metrics": metrics,
            "evidence_preview": [{"doc_id": item["doc_id"], "title": item.get("title")} for item in evidence[:4]],
        }
        completion = self.client.chat.completions.create(
            model=self.config.model_name,
            temperature=0.1,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You write concise analytics narratives. Use only provided metrics/evidence. "
                        "Return plain text in 2-4 sentences and do not invent facts."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Write a short insight summary for this payload:\n{json.dumps(prompt_payload)}",
                },
            ],
        )
        return (completion.choices[0].message.content or "").strip() or self._fallback_summary(module_name, metrics)

    def _fallback_summary(self, module_name: str, metrics: dict[str, Any]) -> str:
        if module_name == "sentiment":
            dist = metrics.get("sentiment_distribution", {})
            return (
                "Sentiment insight: "
                f"{metrics.get('total_docs', 0)} documents analyzed; "
                f"negative={dist.get('negative', 0)}, positive={dist.get('positive', 0)}, "
                f"neutral={dist.get('neutral', 0)}, mixed={dist.get('mixed', 0)}."
            )
        if module_name == "complaints":
            categories = metrics.get("top_issue_categories", [])
            top = categories[0]["category"] if categories else "none"
            return (
                "Complaints insight: "
                f"{metrics.get('complaint_docs', 0)} complaint-like documents, "
                f"with top issue category '{top}'."
            )
        top_features = metrics.get("top_requested_features", [])
        top = top_features[0]["feature"] if top_features else "none"
        return (
            "Feature request insight: "
            f"{metrics.get('feature_request_docs', 0)} feature request documents, "
            f"top requested feature '{top}'."
        )

    def _get_cached_insight(self, module_name: str, filters: dict[str, Any]) -> dict[str, Any] | None:
        if bool(filters.get("refresh_cache")):
            return None

        cache_ttl_minutes = int(filters.get("cache_ttl_minutes") or self.config.cache_ttl_minutes)
        cache_key = self._make_cache_key(module_name, filters)
        valid_after = (
            datetime.now(tz=timezone.utc) - timedelta(minutes=max(1, cache_ttl_minutes))
        ).strftime("%Y-%m-%d %H:%M:%S")

        row = self.session.execute(
            text(
                """
                SELECT body, metadata_json
                FROM saved_insights
                WHERE title = :module_name
                  AND json_extract(metadata_json, '$.cache_key') = :cache_key
                  AND datetime(created_at) >= datetime(:valid_after)
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT 1
                """
            ),
            {
                "module_name": module_name,
                "cache_key": cache_key,
                "valid_after": valid_after,
            },
        ).fetchone()

        if not row:
            return None

        try:
            metadata = json.loads(row.metadata_json or "{}")
            payload = metadata.get("payload")
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError:
            pass
        return None

    def _save_cached_insight(self, module_name: str, filters: dict[str, Any], payload: dict[str, Any]) -> None:
        cache_key = self._make_cache_key(module_name, filters)
        metadata = {
            "module": module_name,
            "cache_key": cache_key,
            "filters": self._clean_filters(filters),
            "payload": payload,
        }
        self.session.execute(
            text(
                """
                INSERT INTO saved_insights (title, body, created_by, metadata_json)
                VALUES (:title, :body, :created_by, :metadata_json)
                """
            ),
            {
                "title": module_name,
                "body": payload.get("summary", ""),
                "created_by": "analysis_service",
                "metadata_json": json.dumps(metadata),
            },
        )
        self.session.commit()

    def _make_cache_key(self, module_name: str, filters: dict[str, Any]) -> str:
        stable_payload = {"module": module_name, "filters": self._clean_filters(filters)}
        serialized = json.dumps(stable_payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _clean_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in filters.items()
            if key not in {"refresh_cache", "cache_ttl_minutes"}
        }
