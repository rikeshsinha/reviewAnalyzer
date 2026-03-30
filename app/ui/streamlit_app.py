"""Main Streamlit entrypoint with shared sidebar filters and page navigation."""

from __future__ import annotations

from datetime import date
from typing import Any

import streamlit as st
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.db.session import SessionLocal
from app.ui.pages import admin, ask, dashboard, explorer, insights


def _coerce_date(raw_value: Any) -> date | None:
    if raw_value is None:
        return None
    text_value = str(raw_value)
    try:
        return date.fromisoformat(text_value[:10])
    except ValueError:
        return None


@st.cache_data(ttl=120)
def _get_filter_options() -> dict[str, list[str]]:
    """Load sidebar filter options from DB and cache briefly for responsiveness."""

    default_options = {
        "min_date": [""],
        "max_date": [""],
        "subreddit": [],
        "product": [],
        "issue": [],
        "competitor": [],
        "db_unavailable": ["true"],
    }

    session = SessionLocal()
    try:
        try:
            date_bounds_row = session.execute(
                text(
                    """
                    SELECT
                        MIN(DATE(COALESCE(published_at, created_at))) AS min_date,
                        MAX(DATE(COALESCE(published_at, created_at))) AS max_date
                    FROM documents
                    """
                )
            ).first()

            def _distinct_tag_values(tag_type: str) -> list[str]:
                rows = session.execute(
                    text(
                        """
                        SELECT DISTINCT tag_value
                        FROM document_tags
                        WHERE tag_type = :tag_type
                        ORDER BY tag_value
                        """
                    ),
                    {"tag_type": tag_type},
                ).fetchall()
                return [str(row.tag_value) for row in rows]

            subreddit_rows = session.execute(
                text(
                    """
                    SELECT DISTINCT json_extract(raw_json, '$.subreddit') AS subreddit
                    FROM documents
                    WHERE json_extract(raw_json, '$.subreddit') IS NOT NULL
                    ORDER BY subreddit
                    """
                )
            ).fetchall()

            return {
                "min_date": [str(date_bounds_row.min_date) if date_bounds_row and date_bounds_row.min_date else ""],
                "max_date": [str(date_bounds_row.max_date) if date_bounds_row and date_bounds_row.max_date else ""],
                "subreddit": [str(row.subreddit) for row in subreddit_rows if row.subreddit],
                "product": _distinct_tag_values("product"),
                "issue": _distinct_tag_values("issue"),
                "competitor": _distinct_tag_values("competitor"),
                "db_unavailable": ["false"],
            }
        except OperationalError:
            return default_options
    finally:
        session.close()


def _build_sidebar_filters() -> dict[str, Any]:
    options = _get_filter_options()
    if options.get("db_unavailable", ["false"])[0] == "true":
        st.warning("Database not initialized yet. Run initialization/refresh.")

    min_date = _coerce_date(options.get("min_date", [None])[0]) or date.today()
    max_date = _coerce_date(options.get("max_date", [None])[0]) or date.today()

    st.sidebar.header("Global filters")
    selected_dates = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        date_from, date_to = selected_dates
    else:
        date_from, date_to = min_date, max_date

    subreddit = st.sidebar.selectbox("Subreddit", options=["All"] + options["subreddit"])
    product = st.sidebar.selectbox("Product", options=["All"] + options["product"])
    issue = st.sidebar.selectbox("Issue", options=["All"] + options["issue"])
    competitor = st.sidebar.selectbox("Competitor", options=["All"] + options["competitor"])

    return {
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
        "subreddit": None if subreddit == "All" else subreddit,
        "product_tags": [] if product == "All" else [product],
        "issue_tags": [] if issue == "All" else [issue],
        "competitor_tags": [] if competitor == "All" else [competitor],
    }


def main() -> None:
    st.set_page_config(page_title="Review Analyzer", layout="wide")
    st.title("Review Analyzer UI")

    st.session_state["global_filters"] = _build_sidebar_filters()

    page = st.sidebar.radio(
        "Page",
        options=["Dashboard", "Insights", "Explorer", "Ask", "Admin"],
        index=0,
    )

    filters = st.session_state["global_filters"]
    if page == "Dashboard":
        dashboard.render(filters)
    elif page == "Insights":
        insights.render(filters)
    elif page == "Explorer":
        explorer.render(filters)
    elif page == "Ask":
        ask.render(filters)
    else:
        admin.render(filters)


if __name__ == "__main__":
    main()
