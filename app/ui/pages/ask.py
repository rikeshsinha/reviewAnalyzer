"""Ask page for free-form queries with evidence citations."""

from __future__ import annotations

from typing import Any

import streamlit as st

from app.db.session import SessionLocal
from app.services.retrieval_service import RetrievalService


@st.cache_data(ttl=90)
def _retrieve(query: str, filters: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    session = SessionLocal()
    try:
        service = RetrievalService(session)
        return service.retrieve_for_question(question=query, filters=filters, limit=limit)
    finally:
        session.close()


def _render_answer_stub(query: str, docs: list[dict[str, Any]]) -> None:
    if not docs:
        st.info("No evidence matched this query and filter set.")
        return
    st.markdown(
        f"Found **{len(docs)}** relevant documents for: _{query}_. "
        "Use the citations panel to validate details before making decisions."
    )


def render(filters: dict[str, Any]) -> None:
    st.subheader("Ask")

    query = st.text_area("Free-form question", placeholder="What are the biggest reliability complaints this week?")
    limit = st.slider("Max evidence documents", min_value=3, max_value=25, value=8)

    if st.button("Run query", type="primary"):
        docs = _retrieve(query.strip(), filters, limit) if query.strip() else []
        _render_answer_stub(query, docs)

        st.markdown("#### Evidence citations")
        for idx, doc in enumerate(docs, start=1):
            title = doc.get("title") or f"Doc {doc['id']}"
            snippet = (doc.get("body") or "").strip().replace("\n", " ")[:260]
            url = doc.get("url")
            citation = f"[{idx}] {title}"
            if url:
                st.markdown(f"- {citation}: [{url}]({url})")
            else:
                st.markdown(f"- {citation}: (no URL)")
            if snippet:
                st.caption(snippet)
