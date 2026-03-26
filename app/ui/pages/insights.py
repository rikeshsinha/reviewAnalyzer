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


def _render_payload(payload: dict[str, Any]) -> None:
    st.markdown(payload.get("summary", "No summary available."))
    with st.expander("Metrics", expanded=True):
        st.json(payload.get("metrics", {}))
    with st.expander("Evidence citations", expanded=True):
        for item in payload.get("evidence", []):
            title = item.get("title") or f"Document {item.get('doc_id')}"
            url = item.get("url") or ""
            snippet = item.get("snippet") or ""
            if url:
                st.markdown(f"- [{title}]({url}) — {snippet}")
            else:
                st.markdown(f"- **{title}** — {snippet}")


def render(filters: dict[str, Any]) -> None:
    st.subheader("Insights")

    tab_sentiment, tab_complaints, tab_features = st.tabs(
        ["Sentiment", "Complaints", "Feature Requests"]
    )

    with tab_sentiment:
        _render_payload(_load_insight("sentiment", filters))
    with tab_complaints:
        _render_payload(_load_insight("complaints", filters))
    with tab_features:
        _render_payload(_load_insight("features", filters))
