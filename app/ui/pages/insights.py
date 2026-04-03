"""Insights page with toggles for sentiment, complaints, and feature requests."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from app.db.session import SessionLocal
from app.services.analysis_service import AnalysisService


@st.cache_data(ttl=120)
def _load_insight(kind: str, filters: dict[str, Any]) -> dict[str, Any]:
    session = SessionLocal()
    try:
        service = AnalysisService(session=session, client=None)
        if kind == "sentiment":
            return service.generate_sentiment_insight(filters)
        if kind == "complaints":
            return service.generate_complaints_insight(filters)
        return service.generate_feature_requests_insight(filters)
    finally:
        session.close()


def _filter_label(filters: dict[str, Any]) -> str:
    source = filters.get("subreddit") or "all communities"
    date_from = filters.get("date_from") or "start"
    date_to = filters.get("date_to") or "today"
    return f"Source: {source} • Time range: {date_from} → {date_to}"


def _coverage_label(filters: dict[str, Any]) -> str:
    source = (filters.get("source") or "").strip().lower()
    if source == "reddit":
        selected = "Reddit"
    elif source == "google_play":
        selected = "Google Play"
    else:
        selected = "Reddit / Google Play"
    return f"Based on selected sources: {selected}"


def _render_sentiment_charts(metrics: dict[str, Any]) -> None:
    trend_rows = metrics.get("daily_sentiment_trend", [])
    if not trend_rows and metrics.get("daily_negative_trend"):
        trend_rows = metrics.get("daily_negative_trend", [])

    sentiment_trend_df = pd.DataFrame(trend_rows)
    if sentiment_trend_df.empty:
        st.info("No daily sentiment trend data for current filters.")
        return

    legacy_column_map = {
        "positive": "positive_count",
        "negative": "negative_count",
        "neutral": "neutral_count",
        "mixed": "mixed_count",
    }
    sentiment_trend_df = sentiment_trend_df.rename(columns=legacy_column_map)
    for required_column in ["positive_count", "negative_count", "neutral_count", "mixed_count"]:
        if required_column not in sentiment_trend_df.columns:
            sentiment_trend_df[required_column] = 0

    sentiment_trend_df = sentiment_trend_df.sort_values("day")
    melted_df = sentiment_trend_df.melt(
        id_vars=["day"],
        value_vars=["positive_count", "negative_count", "neutral_count", "mixed_count"],
        var_name="sentiment",
        value_name="count",
    )
    melted_df["sentiment"] = melted_df["sentiment"].str.replace("_count", "", regex=False)
    sentiment_color_map = {
        "positive": "#2E7D32",  # green
        "negative": "#C62828",  # red
        "neutral": "#1565C0",  # blue
        "mixed": "#81C784",  # light green
    }
    st.plotly_chart(
        px.bar(
            melted_df,
            x="day",
            y="count",
            color="sentiment",
            barmode="stack",
            title="Daily sentiment trend",
            color_discrete_map=sentiment_color_map,
        ),
        width="stretch",
    )


def _render_complaints_charts(metrics: dict[str, Any]) -> None:
    categories_df = pd.DataFrame(metrics.get("top_issue_categories", []))
    if categories_df.empty:
        st.info("No issue category data for current filters.")
    else:
        st.plotly_chart(
            px.pie(
                categories_df,
                values="count",
                names="category",
                title="Top complaint issue categories",
            ),
            width="stretch",
        )

    complaint_trend_df = pd.DataFrame(metrics.get("daily_complaint_trend", []))
    if complaint_trend_df.empty:
        st.info("No daily complaint trend data for current filters.")
    else:
        complaint_trend_df = complaint_trend_df.sort_values("day")
        st.plotly_chart(
            px.line(
                complaint_trend_df,
                x="day",
                y="complaint_count",
                title="Daily complaint trend",
            ),
            width="stretch",
        )


def _render_feature_request_charts(metrics: dict[str, Any]) -> None:
    feature_trend_df = pd.DataFrame(metrics.get("daily_feature_request_trend", []))
    if feature_trend_df.empty:
        st.info("No daily feature request trend data for current filters.")
        return

    feature_trend_df = feature_trend_df.sort_values("day")
    st.plotly_chart(
        px.line(
            feature_trend_df,
            x="day",
            y="feature_request_count",
            title="Daily feature request trend",
        ),
        width="stretch",
    )


def _render_payload(kind: str, payload: dict[str, Any], filters: dict[str, Any]) -> None:
    metrics = payload.get("metrics", {})
    st.caption(_coverage_label(filters))
    st.caption(_filter_label(filters))
    st.markdown(payload.get("summary", "No summary available."))

    if kind == "sentiment":
        _render_sentiment_charts(metrics)
    elif kind == "complaints":
        _render_complaints_charts(metrics)
    elif kind == "features":
        _render_feature_request_charts(metrics)

    with st.expander("Metrics", expanded=True):
        st.json(metrics)
    with st.expander("Evidence citations", expanded=True):
        evidence_items = payload.get("evidence", [])
        if not evidence_items:
            st.info("No evidence found for the selected filters. Try widening the source or date range.")
            return
        for item in evidence_items:
            title = item.get("title") or f"Document {item.get('doc_id')}"
            url = item.get("evidence_url") or item.get("url") or f"#document-{item.get('doc_id')}"
            snippet = item.get("snippet") or ""
            st.markdown(f"- [{title}]({url}) — {snippet}")


def render(filters: dict[str, Any]) -> None:
    st.subheader("Insights")

    tab_sentiment, tab_complaints, tab_features = st.tabs(
        ["Sentiment", "Complaints", "Feature Requests"]
    )

    with tab_sentiment:
        with st.spinner("Loading sentiment insight..."):
            _render_payload("sentiment", _load_insight("sentiment", filters), filters)
    with tab_complaints:
        with st.spinner("Loading complaints insight..."):
            _render_payload("complaints", _load_insight("complaints", filters), filters)
    with tab_features:
        with st.spinner("Loading feature request insight..."):
            _render_payload("features", _load_insight("features", filters), filters)
