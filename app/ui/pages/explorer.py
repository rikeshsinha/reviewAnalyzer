"""Explorer page with searchable table, pagination, and CSV export."""

from __future__ import annotations

from io import StringIO
from typing import Any

import pandas as pd
import streamlit as st

from app.db.session import SessionLocal
from app.services.retrieval_service import RetrievalService


@st.cache_data(ttl=90)
def _search(query: str, filters: dict[str, Any], limit: int, offset: int) -> list[dict[str, Any]]:
    session = SessionLocal()
    try:
        service = RetrievalService(session)
        safe_query = query.strip() or "*"
        return service.search_documents(query=safe_query, filters=filters, limit=limit, offset=offset)
    finally:
        session.close()


def render(filters: dict[str, Any]) -> None:
    st.subheader("Explorer")

    query = st.text_input("Search", placeholder="battery OR crash OR refund")
    page_size = st.select_slider("Rows per page", options=[10, 20, 50, 100], value=20)
    page_number = st.number_input("Page", min_value=1, value=1, step=1)

    offset = (int(page_number) - 1) * int(page_size)
    rows = _search(query, filters, page_size, offset)

    table_rows = []
    for row in rows:
        table_rows.append(
            {
                "id": row.get("id"),
                "published_at": row.get("published_at"),
                "title": row.get("title"),
                "author": row.get("author"),
                "source_url": row.get("url"),
                "fts_score": row.get("fts_score"),
            }
        )

    df = pd.DataFrame(table_rows)
    st.dataframe(df, use_container_width=True)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    st.download_button(
        "Export current page CSV",
        data=csv_buffer.getvalue(),
        file_name="explorer_results.csv",
        mime="text/csv",
    )

    st.markdown("#### Source links")
    for row in table_rows:
        if row.get("source_url"):
            st.markdown(f"- [{row.get('title') or row.get('id')}]({row['source_url']})")
