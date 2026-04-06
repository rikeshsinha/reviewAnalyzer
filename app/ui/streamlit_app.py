"""Main Streamlit entrypoint with shared sidebar filters and page navigation."""

from __future__ import annotations

import importlib
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import streamlit as st
from sqlalchemy import text
from sqlalchemy.exc import OperationalError


@st.cache_data(ttl=120)
def _get_filter_options() -> dict[str, list[str]]:
    """Load sidebar filter options from DB and cache briefly for responsiveness."""

    from app.db.session import SessionLocal

    default_options = {
        "min_date": [""],
        "max_date": [""],
        "source": [],
        "subreddit": [],
        "web_domain": [],
        "google_play_app": [],
        "rating": [],
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

            source_rows = session.execute(
                text(
                    """
                    SELECT DISTINCT COALESCE(name, platform) AS source_name
                    FROM sources
                    ORDER BY source_name
                    """
                )
            ).fetchall()

            web_domain_rows = session.execute(
                text(
                    """
                    SELECT DISTINCT json_extract(d.raw_json, '$.community_or_channel') AS web_domain
                    FROM documents d
                    JOIN sources s ON s.id = d.source_id
                    WHERE s.name = 'web_reviews'
                      AND json_extract(d.raw_json, '$.community_or_channel') IS NOT NULL
                    ORDER BY web_domain
                    """
                )
            ).fetchall()

            google_play_app_rows = session.execute(
                text(
                    """
                    SELECT DISTINCT json_extract(d.raw_json, '$.community_or_channel') AS app_id
                    FROM documents d
                    JOIN sources s ON s.id = d.source_id
                    WHERE s.platform = 'google_play'
                      AND json_extract(d.raw_json, '$.community_or_channel') IS NOT NULL
                    ORDER BY app_id
                    """
                )
            ).fetchall()

            rating_rows = session.execute(
                text(
                    """
                    SELECT DISTINCT CAST(json_extract(d.raw_json, '$.rating') AS INTEGER) AS rating
                    FROM documents d
                    JOIN sources s ON s.id = d.source_id
                    WHERE s.platform = 'google_play'
                      AND json_extract(d.raw_json, '$.rating') IS NOT NULL
                      AND CAST(json_extract(d.raw_json, '$.rating') AS INTEGER) BETWEEN 1 AND 5
                    ORDER BY rating
                    """
                )
            ).fetchall()

            return {
                "min_date": [str(date_bounds_row.min_date) if date_bounds_row and date_bounds_row.min_date else ""],
                "max_date": [str(date_bounds_row.max_date) if date_bounds_row and date_bounds_row.max_date else ""],
                "source": [str(row.source_name) for row in source_rows if row.source_name],
                "subreddit": [str(row.subreddit) for row in subreddit_rows if row.subreddit],
                "web_domain": [str(row.web_domain) for row in web_domain_rows if row.web_domain],
                "google_play_app": [str(row.app_id) for row in google_play_app_rows if row.app_id],
                "rating": [str(row.rating) for row in rating_rows if row.rating],
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

    default_date_to = date.today()
    default_date_from = default_date_to - timedelta(days=30)

    st.sidebar.header("Global filters")
    selected_dates = st.sidebar.date_input(
        "Date range",
        value=(default_date_from, default_date_to),
    )
    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        date_from, date_to = selected_dates
    elif isinstance(selected_dates, list) and len(selected_dates) == 2:
        date_from, date_to = selected_dates[0], selected_dates[1]
    elif isinstance(selected_dates, date):
        date_from, date_to = selected_dates, selected_dates
    else:
        date_from, date_to = default_date_from, default_date_to

    source_label_to_value = {
        "Reddit": "reddit",
        "Web Reviews": "web_reviews",
        "Google Play": "google_play",
    }
    available_source_values = set(options["source"])
    source_labels = [
        label for label, value in source_label_to_value.items() if value and value in available_source_values
    ]

    preferred_default_sources = ["reddit", "web_reviews"]
    default_source_values = [source for source in preferred_default_sources if source in available_source_values]
    if not default_source_values:
        default_source_values = list(available_source_values)
    default_source_labels = [
        label for label, value in source_label_to_value.items() if value in default_source_values
    ]

    selected_source_labels = st.sidebar.multiselect(
        "Sources",
        options=source_labels,
        default=default_source_labels,
    )
    selected_sources = [source_label_to_value[label] for label in selected_source_labels if label in source_label_to_value]
    if not selected_sources:
        selected_sources = sorted(available_source_values)

    subreddit = None
    if "reddit" in selected_sources:
        subreddit = st.sidebar.selectbox("Subreddit", options=["All"] + options["subreddit"])

    web_domain = None
    if "web_reviews" in selected_sources:
        web_domain = st.sidebar.selectbox("Site / domain", options=["All"] + options["web_domain"])

    google_play_app = None
    if "google_play" in selected_sources:
        google_play_app = st.sidebar.selectbox("Google Play app/package", options=["All"] + options["google_play_app"])
    rating = "All"
    if "google_play" in selected_sources:
        rating = st.sidebar.selectbox("Rating (1-5 stars)", options=["All"] + options["rating"])
    product = st.sidebar.selectbox("Product", options=["All"] + options["product"])
    issue = st.sidebar.selectbox("Issue", options=["All"] + options["issue"])
    competitor = st.sidebar.selectbox("Competitor", options=["All"] + options["competitor"])

    return {
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
        "source": selected_sources[0] if len(selected_sources) == 1 else None,
        "sources": selected_sources,
        "subreddit": None if subreddit == "All" else subreddit,
        "web_domain": None if web_domain == "All" else web_domain,
        "google_play_app": None if google_play_app in (None, "All") else google_play_app,
        "rating": None if rating == "All" else int(rating),
        "product_tags": [] if product == "All" else [product],
        "issue_tags": [] if issue == "All" else [issue],
        "competitor_tags": [] if competitor == "All" else [competitor],
    }


def _verify_package_markers() -> list[str]:
    required = [
        Path("app/__init__.py"),
        Path("app/db/__init__.py"),
        Path("app/ui/__init__.py"),
        Path("app/ui/pages/__init__.py"),
    ]
    missing = [str(path) for path in required if not path.exists()]
    return missing


def _startup_self_check() -> tuple[bool, str | None, str | None]:
    missing_markers = _verify_package_markers()
    if missing_markers:
        return False, "package markers", f"Missing __init__.py files: {', '.join(missing_markers)}"

    required_modules = [
        "app",
        "app.db",
        "app.db.session",
        "app.ui",
        "app.ui.pages",
        "app.ui.pages.dashboard",
        "app.ui.pages.insights",
        "app.ui.pages.explorer",
        "app.ui.pages.ask",
        "app.ui.pages.admin",
    ]
    for module_name in required_modules:
        try:
            importlib.import_module(module_name)
        except Exception as exc:  # pragma: no cover - diagnostic surface
            return False, module_name, str(exc)
    return True, None, None


def _show_startup_error(module_name: str | None, details: str | None) -> None:
    failing_module = module_name or "unknown"
    st.error(
        "\n".join(
            [
                "Startup import check failed.",
                f"Failing module: {failing_module}",
                "Recommended action: restart the app, clear Streamlit cache, and verify package markers (__init__.py).",
                f"Details: {details or 'No extra details available.'}",
            ]
        )
    )


def main() -> None:
    st.set_page_config(page_title="Review Analyzer", layout="wide")
    st.title("Review Analyzer UI")

    ok, module_name, details = _startup_self_check()
    if not ok:
        _show_startup_error(module_name, details)
        return

    from app.db.session import bootstrap_database
    from app.ui.pages import admin, ask, dashboard, explorer, insights

    if not st.session_state.get("_db_bootstrapped", False):
        bootstrap_database()
        st.session_state["_db_bootstrapped"] = True

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
