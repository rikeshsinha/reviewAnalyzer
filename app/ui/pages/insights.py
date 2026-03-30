"""Insights page with toggles for sentiment, complaints, and feature requests."""

from __future__ import annotations

from typing import Any

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
    source = filters.get("subreddit") or "all subreddits"
    date_from = filters.get("date_from") or "start"
    date_to = filters.get("date_to") or "today"
    return f"Source: {source} • Time range: {date_from} → {date_to}"


def _render_payload(payload: dict[str, Any], filters: dict[str, Any]) -> None:
    st.caption(_filter_label(filters))
    st.markdown(payload.get("summary", "No summary available."))
    with st.expander("Metrics", expanded=True):
        st.json(payload.get("metrics", {}))
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
            _render_payload(_load_insight("sentiment", filters), filters)
    with tab_complaints:
        with st.spinner("Loading complaints insight..."):
            _render_payload(_load_insight("complaints", filters), filters)
    with tab_features:
        with st.spinner("Loading feature request insight..."):
            _render_payload(_load_insight("features", filters), filters)
