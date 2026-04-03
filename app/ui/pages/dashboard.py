"""Dashboard page with KPIs, trend, complaints, and sentiment split."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from app.db.session import SessionLocal


def _where_clause(filters: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    where_parts: list[str] = []
    params: dict[str, Any] = {}

    if filters.get("source"):
        where_parts.append("AND EXISTS (SELECT 1 FROM sources s WHERE s.id = d.source_id AND s.name = :source)")
        params["source"] = filters["source"]
    if filters.get("subreddit"):
        where_parts.append("AND json_extract(d.raw_json, '$.subreddit') = :subreddit")
        params["subreddit"] = filters["subreddit"]
    if filters.get("google_play_app"):
        where_parts.append("AND json_extract(d.raw_json, '$.community_or_channel') = :google_play_app")
        params["google_play_app"] = filters["google_play_app"]
    if filters.get("rating"):
        where_parts.append("AND CAST(json_extract(d.raw_json, '$.rating') AS INTEGER) = :rating")
        params["rating"] = int(filters["rating"])
    if filters.get("date_from"):
        where_parts.append("AND DATE(COALESCE(d.published_at, d.created_at)) >= DATE(:date_from)")
        params["date_from"] = filters["date_from"]
    if filters.get("date_to"):
        where_parts.append("AND DATE(COALESCE(d.published_at, d.created_at)) <= DATE(:date_to)")
        params["date_to"] = filters["date_to"]

    for tag_type, filter_key in {
        "product": "product_tags",
        "issue": "issue_tags",
        "competitor": "competitor_tags",
    }.items():
        values = filters.get(filter_key) or []
        if not values:
            continue
        placeholders: list[str] = []
        for i, value in enumerate(values):
            param_key = f"{tag_type}_{i}"
            params[param_key] = value
            placeholders.append(f":{param_key}")
        params[f"{tag_type}_type"] = tag_type
        where_parts.append(
            f"""
            AND EXISTS (
                SELECT 1 FROM document_tags dt
                WHERE dt.document_id = d.id
                  AND dt.tag_type = :{tag_type}_type
                  AND dt.tag_value IN ({', '.join(placeholders)})
            )
            """
        )

    return " ".join(where_parts), params


def _fetch_ranked_complaints(session: Any, filters: dict[str, Any]) -> list[dict[str, Any]]:
    where_sql, params = _where_clause(filters)
    issue_category = filters.get("issue_category")
    issue_filter_sql = ""
    if issue_category and issue_category != "All":
        issue_filter_sql = " AND COALESCE(json_extract(e.metadata_json, '$.primary_issue_category'), 'other') = :issue_category"
        params["issue_category"] = issue_category

    rows = session.execute(
        text(
            f"""
            SELECT
                d.id,
                COALESCE(d.published_at, d.created_at) AS published_at,
                d.title,
                d.body,
                d.url,
                COALESCE(json_extract(e.metadata_json, '$.primary_issue_category'), 'other') AS issue_category,
                COALESCE(json_extract(e.metadata_json, '$.sentiment_label'), 'neutral') AS sentiment_label,
                CASE COALESCE(json_extract(e.metadata_json, '$.sentiment_label'), 'neutral')
                    WHEN 'negative' THEN 3
                    WHEN 'mixed' THEN 2
                    WHEN 'neutral' THEN 1
                    WHEN 'positive' THEN 0
                    ELSE 1
                END AS severity_rank
            FROM documents d
            JOIN enrichments e ON e.document_id = d.id
            WHERE 1=1 {where_sql} {issue_filter_sql}
            ORDER BY severity_rank DESC, COALESCE(d.published_at, d.created_at) DESC, d.id DESC
            LIMIT 100
            """
        ),
        params,
    ).fetchall()
    return [dict(row._mapping) for row in rows]


@st.cache_data(ttl=90)
def _load_ranked_complaints(filters: dict[str, Any]) -> list[dict[str, Any]]:
    session = SessionLocal()
    try:
        return _fetch_ranked_complaints(session, filters)
    finally:
        session.close()


@st.cache_data(ttl=90)
def _load_dashboard_data(filters: dict[str, Any]) -> dict[str, Any]:
    session = SessionLocal()
    try:
        where_sql, params = _where_clause(filters)

        totals = session.execute(
            text(
                f"""
                SELECT
                    COUNT(*) AS total_docs,
                    COUNT(DISTINCT json_extract(d.raw_json, '$.subreddit')) AS subreddits,
                    SUM(CASE WHEN json_extract(e.metadata_json, '$.sentiment_label') = 'negative' THEN 1 ELSE 0 END) AS negative_docs,
                    SUM(CASE WHEN COALESCE(json_extract(e.metadata_json, '$.feature_request_flag'), 0) = 1 THEN 1 ELSE 0 END) AS feature_requests
                FROM documents d
                LEFT JOIN enrichments e ON e.document_id = d.id
                WHERE 1=1 {where_sql}
                """
            ),
            params,
        ).first()

        trend_rows = session.execute(
            text(
                f"""
                SELECT DATE(COALESCE(d.published_at, d.created_at)) AS day, COUNT(*) AS docs
                FROM documents d
                WHERE 1=1 {where_sql}
                GROUP BY day
                ORDER BY day ASC
                """
            ),
            params,
        ).fetchall()

        sentiment_rows = session.execute(
            text(
                f"""
                SELECT
                    COALESCE(json_extract(e.metadata_json, '$.sentiment_label'), 'unknown') AS sentiment,
                    COUNT(*) AS count
                FROM documents d
                LEFT JOIN enrichments e ON e.document_id = d.id
                WHERE 1=1 {where_sql}
                GROUP BY sentiment
                ORDER BY count DESC
                """
            ),
            params,
        ).fetchall()

        return {
            "totals": dict(totals._mapping) if totals else {},
            "trend": [dict(row._mapping) for row in trend_rows],
            "sentiment": [dict(row._mapping) for row in sentiment_rows],
        }
    finally:
        session.close()


def render(filters: dict[str, Any]) -> None:
    st.subheader("Dashboard")
    with st.spinner("Loading dashboard analytics..."):
        payload = _load_dashboard_data(filters)

    totals = payload["totals"]
    a, b, c, d = st.columns(4)
    a.metric("Total docs", int(totals.get("total_docs") or 0))
    b.metric("Subreddits", int(totals.get("subreddits") or 0))
    c.metric("Negative docs", int(totals.get("negative_docs") or 0))
    d.metric("Feature requests", int(totals.get("feature_requests") or 0))

    trend_df = pd.DataFrame(payload["trend"])
    if not trend_df.empty:
        st.plotly_chart(px.line(trend_df, x="day", y="docs", title="Document volume trend"), width="stretch")
    else:
        st.info("No trend data for current filters.")

    left, right = st.columns(2)

    complaints_filters = dict(filters)
    complaints_rows = _load_ranked_complaints(complaints_filters)
    complaints_df = pd.DataFrame(complaints_rows)
    with left:
        st.markdown("#### Top complaints")
        if complaints_df.empty:
            st.info("No complaint examples matched these filters yet.")
        else:
            categories = sorted(complaints_df["issue_category"].dropna().unique().tolist())
            selected_issue = st.selectbox(
                "Issue category",
                options=["All", *categories],
                index=0,
                key="dashboard_issue_category",
            )
            visible_df = complaints_df
            if selected_issue != "All":
                visible_df = complaints_df[complaints_df["issue_category"] == selected_issue]

            display_df = visible_df[
                ["published_at", "issue_category", "sentiment_label", "title", "body", "url"]
            ].copy()
            st.caption("Ranked by sentiment label severity (negative > mixed > neutral > positive).")
            st.dataframe(
                display_df,
                width="stretch",
                column_config={"url": st.column_config.LinkColumn("Source", display_text="Open")},
                hide_index=True,
            )

    sentiment_df = pd.DataFrame(payload["sentiment"])
    with right:
        st.markdown("#### Sentiment split")
        if not sentiment_df.empty:
            sentiment_color_map = {
                "positive": "#2E7D32",  # green
                "negative": "#C62828",  # red
                "neutral": "#1565C0",  # blue
                "mixed": "#81C784",  # light green
                "unknown": "#9E9E9E",  # gray fallback
            }
            st.plotly_chart(
                px.pie(
                    sentiment_df,
                    values="count",
                    names="sentiment",
                    title="Sentiment distribution",
                    color="sentiment",
                    color_discrete_map=sentiment_color_map,
                ),
                width="stretch",
            )
        else:
            st.info("No sentiment data for current filters.")
