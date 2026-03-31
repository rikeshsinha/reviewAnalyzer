"""Admin page to run maintenance jobs and inspect latest run status."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import streamlit as st
from sqlalchemy import text

from app.config.source_loader import (
    BASE_SOURCE_CONFIG_PATH,
    RUNTIME_SOURCE_CONFIG_PATH,
)
from app.db.session import SessionLocal
from app.services.analysis_service import AnalysisService


@st.cache_data(ttl=30)
def _load_last_ingestion_runs() -> list[dict[str, Any]]:
    session = SessionLocal()
    try:
        rows = session.execute(
            text(
                """
                SELECT id, source_name, started_at, completed_at, status,
                       records_fetched, records_inserted, error_message
                FROM ingestion_runs
                ORDER BY id DESC
                LIMIT 10
                """
            )
        ).fetchall()
        return [dict(row._mapping) for row in rows]
    finally:
        session.close()


@st.cache_data(ttl=30)
def _load_last_enrichment_runs() -> list[dict[str, Any]]:
    session = SessionLocal()
    try:
        rows = session.execute(
            text(
                """
                SELECT id, started_at, completed_at, status,
                       candidates, enriched, skipped_short, failed_batches, error_message
                FROM enrichment_runs
                ORDER BY id DESC
                LIMIT 10
                """
            )
        ).fetchall()
        return [dict(row._mapping) for row in rows]
    finally:
        session.close()


def _run_command(module: str) -> tuple[bool, str]:
    proc = subprocess.run(
        [sys.executable, "-m", module],
        capture_output=True,
        text=True,
        check=False,
    )
    output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    return proc.returncode == 0, output.strip()


def _rebuild_insight_cache(filters: dict[str, Any]) -> tuple[bool, str]:
    session = SessionLocal()
    try:
        service = AnalysisService(session=session, client=None)
        payload_filters = {**filters, "refresh_cache": True}
        service.generate_sentiment_insight(payload_filters)
        service.generate_complaints_insight(payload_filters)
        service.generate_feature_requests_insight(payload_filters)
        return True, "Insight cache rebuilt for sentiment, complaints, and feature requests."
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
    finally:
        session.close()


def _read_reddit_config(config_path: Path) -> tuple[list[str], list[str]]:
    if not config_path.exists():
        return [], []

    lines = config_path.read_text(encoding="utf-8").splitlines()
    in_reddit_block = False
    communities: list[str] = []
    keywords: list[str] = []

    for raw_line in lines:
        line = raw_line.strip()
        indent = len(raw_line) - len(raw_line.lstrip(" "))

        if indent == 2 and line.startswith("reddit:"):
            in_reddit_block = True
            continue
        if in_reddit_block and indent == 2 and line.endswith(":") and not line.startswith("reddit:"):
            break

        if not in_reddit_block or indent != 4 or ":" not in line:
            continue

        key, raw_value = line.split(":", 1)
        value = raw_value.strip()
        if not (value.startswith("[") and value.endswith("]")):
            continue
        parsed = _parse_inline_list(value)
        if key.strip() == "communities":
            communities = parsed
        elif key.strip() == "keywords":
            keywords = parsed

    return communities, keywords


def _parse_inline_list(raw: str) -> list[str]:
    inner = raw.strip()[1:-1].strip()
    if not inner:
        return []
    items: list[str] = []
    for part in inner.split(","):
        normalized = part.strip().strip('"').strip("'").strip()
        if normalized:
            items.append(normalized)
    return items


def _normalize_text_list(raw_text: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for line in raw_text.splitlines():
        normalized = line.strip()
        if not normalized:
            continue
        lowered = normalized.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        values.append(normalized)
    return values


def _render_inline_list(values: list[str]) -> str:
    quoted = [f'"{value}"' for value in values]
    return "[" + ", ".join(quoted) + "]"


def _write_runtime_source_config(communities: list[str], keywords: list[str]) -> None:
    base_text = BASE_SOURCE_CONFIG_PATH.read_text(encoding="utf-8")
    lines = base_text.splitlines()
    output_lines: list[str] = []
    in_reddit_block = False

    for raw_line in lines:
        line = raw_line.strip()
        indent = len(raw_line) - len(raw_line.lstrip(" "))

        if indent == 2 and line.startswith("reddit:"):
            in_reddit_block = True
            output_lines.append(raw_line)
            continue
        if in_reddit_block and indent == 2 and line.endswith(":") and not line.startswith("reddit:"):
            in_reddit_block = False

        if in_reddit_block and indent == 4 and line.startswith("communities:"):
            output_lines.append(f"    communities: {_render_inline_list(communities)}")
            continue
        if in_reddit_block and indent == 4 and line.startswith("keywords:"):
            output_lines.append(f"    keywords: {_render_inline_list(keywords)}")
            continue

        output_lines.append(raw_line)

    RUNTIME_SOURCE_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_SOURCE_CONFIG_PATH.write_text("\n".join(output_lines) + "\n", encoding="utf-8")


def render(filters: dict[str, Any]) -> None:
    st.subheader("Admin")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Refresh Reddit ingestion"):
            with st.spinner("Running Reddit ingestion job..."):
                ok, logs = _run_command("app.jobs.refresh_reddit")
            (st.success if ok else st.error)("Refresh completed." if ok else "Refresh failed.")
            st.code(logs or "No output")

    with c2:
        if st.button("Run enrichment"):
            with st.spinner("Running enrichment job..."):
                ok, logs = _run_command("app.jobs.enrich_new_docs")
            (st.success if ok else st.error)("Enrichment completed." if ok else "Enrichment failed.")
            st.code(logs or "No output")

    with c3:
        if st.button("Rebuild insight cache"):
            with st.spinner("Rebuilding insight cache..."):
                ok, message = _rebuild_insight_cache(filters)
            (st.success if ok else st.error)(message)

    st.markdown("#### Ingestion run stats / errors")
    ingestion_rows = _load_last_ingestion_runs()
    if ingestion_rows:
        st.dataframe(ingestion_rows, use_container_width=True)
    else:
        st.info("No ingestion runs yet. Run 'Refresh Reddit ingestion' to populate this table.")

    st.markdown("#### Enrichment run stats / errors")
    enrichment_rows = _load_last_enrichment_runs()
    if enrichment_rows:
        st.dataframe(enrichment_rows, use_container_width=True)
    else:
        st.info("No enrichment runs yet. Run 'Run enrichment' after ingestion to populate this table.")

    st.markdown("#### Source configuration")
    st.caption(f"Runtime config path: `{RUNTIME_SOURCE_CONFIG_PATH}`")

    initial_path = RUNTIME_SOURCE_CONFIG_PATH if RUNTIME_SOURCE_CONFIG_PATH.exists() else BASE_SOURCE_CONFIG_PATH
    initial_communities, initial_keywords = _read_reddit_config(initial_path)

    if "admin_communities_text" not in st.session_state:
        st.session_state.admin_communities_text = "\n".join(initial_communities)
    if "admin_keywords_text" not in st.session_state:
        st.session_state.admin_keywords_text = "\n".join(initial_keywords)

    communities_col, keywords_col = st.columns(2)
    with communities_col:
        st.markdown("**Reddit communities**")
        st.text_area(
            "Subreddit list editor",
            key="admin_communities_text",
            height=180,
            help="One subreddit per line.",
        )
    with keywords_col:
        st.markdown("**Reddit keywords**")
        st.text_area(
            "Keyword list editor",
            key="admin_keywords_text",
            height=180,
            help="One keyword per line.",
        )

    action_col1, action_col2 = st.columns(2)
    with action_col1:
        if st.button("Save config", type="primary"):
            communities = _normalize_text_list(st.session_state.admin_communities_text)
            keywords = _normalize_text_list(st.session_state.admin_keywords_text)
            if not communities:
                st.error("At least one subreddit is required.")
            else:
                try:
                    _write_runtime_source_config(communities, keywords)
                    st.session_state.admin_communities_text = "\n".join(communities)
                    st.session_state.admin_keywords_text = "\n".join(keywords)
                    st.success(f"Saved runtime config to `{RUNTIME_SOURCE_CONFIG_PATH}`.")
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Failed to save config: {exc}")

    with action_col2:
        if st.button("Reset to defaults"):
            default_communities, default_keywords = _read_reddit_config(BASE_SOURCE_CONFIG_PATH)
            st.session_state.admin_communities_text = "\n".join(default_communities)
            st.session_state.admin_keywords_text = "\n".join(default_keywords)
            try:
                _write_runtime_source_config(default_communities, default_keywords)
                st.success("Reset runtime config to defaults from base source_config.yaml.")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Failed to reset runtime config: {exc}")
