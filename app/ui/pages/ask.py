"""Ask page for free-form queries with evidence-grounded Q&A."""

from __future__ import annotations

from typing import Any

from openai import OpenAI
import streamlit as st

from app.config.settings import get_settings
from app.db.session import SessionLocal
from app.services.qa_service import QAConfig, QAService


@st.cache_data(ttl=60)
def _run_qa(query: str, filters: dict[str, Any], limit: int) -> dict[str, Any]:
    settings = get_settings()

    session = SessionLocal()
    try:
        qa_service = QAService(
            session=session,
            client=OpenAI(api_key=settings.openai_api_key),
            config=QAConfig(max_evidence_docs=limit),
        )
        return qa_service.answer_question(question=query, filters=filters, top_n=limit)
    finally:
        session.close()


def _render_bullets(title: str, items: list[str]) -> None:
    if not items:
        return
    st.markdown(f"**{title}**")
    for item in items:
        st.markdown(f"- {item}")


def render(filters: dict[str, Any]) -> None:
    st.subheader("Ask")

    query = st.text_area("Free-form question", placeholder="What are the biggest reliability complaints this week?")
    limit = st.slider("Max evidence documents", min_value=3, max_value=25, value=8)

    if st.button("Ask", type="primary"):
        if not query.strip():
            st.warning("Please enter a question to run retrieval + answer generation.")
            return

        try:
            with st.spinner("Gathering evidence and generating answer..."):
                result = _run_qa(query.strip(), filters, limit)
        except RuntimeError as exc:
            st.error(f"Missing configuration: {exc}")
            return
        except Exception as exc:  # pragma: no cover - defensive UI handling
            st.error(f"Unable to generate answer: {exc}")
            return

        if result.get("insufficient_evidence"):
            st.warning("Insufficient evidence in selected filters.")

        st.markdown("#### Answer")
        st.markdown(result.get("answer") or "Based on Reddit data in selected filters.\n\nNo answer generated.")

        _render_bullets("Key points", result.get("key_points") or [])
        _render_bullets("Caveats", result.get("caveats") or [])
        _render_bullets("Contradictions", result.get("contradictions") or [])

        cited_ids = set(result.get("cited_evidence_ids") or [])
        st.markdown("#### Evidence used")
        evidence_items = result.get("evidence") or []
        if not evidence_items:
            st.info("No evidence available for this query and filters. Try broadening source or date range.")
            return

        for item in evidence_items:
            doc_id = int(item.get("id", 0))
            marker = "✅" if doc_id in cited_ids else "•"
            source = item.get("source") or "unknown"
            date_str = item.get("date") or "unknown-date"
            title = item.get("title") or f"Doc {doc_id}"
            header = f"{marker} [{doc_id}] {title} ({source}, {date_str})"
            url = item.get("url") or ""

            if url:
                st.markdown(f"- {header}: [{url}]({url})")
            else:
                st.markdown(f"- {header}: (no URL)")

            snippet = item.get("snippet") or ""
            if snippet:
                st.caption(snippet)
