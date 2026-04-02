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
def _load_ingestion_metrics_by_platform() -> list[dict[str, Any]]:
    session = SessionLocal()
    try:
        rows = session.execute(
            text(
                """
                SELECT source_name AS platform,
                       SUM(records_fetched) AS fetched,
                       SUM(records_inserted) AS inserted,
                       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS errors,
                       MAX(completed_at) AS last_completed_at
                FROM ingestion_runs
                GROUP BY source_name
                ORDER BY last_completed_at DESC, platform ASC
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


def _set_admin_config_notice(level: str, message: str) -> None:
    st.session_state.admin_config_notice_level = level
    st.session_state.admin_config_notice_message = message


def _save_runtime_config_callback() -> None:
    communities_text = st.session_state.get("admin_communities_input", st.session_state.get("admin_communities_draft", ""))
    keywords_text = st.session_state.get("admin_keywords_input", st.session_state.get("admin_keywords_draft", ""))

    communities = _normalize_text_list(communities_text)
    keywords = _normalize_text_list(keywords_text)

    if not communities:
        _set_admin_config_notice("error", "At least one subreddit is required.")
        return

    try:
        _write_runtime_source_config(communities, keywords)
        st.session_state.admin_communities_draft = "\n".join(communities)
        st.session_state.admin_keywords_draft = "\n".join(keywords)
        _set_admin_config_notice(
            "success",
            f"Saved runtime config to `{RUNTIME_SOURCE_CONFIG_PATH}`.",
        )
        st.rerun()
    except Exception as exc:  # noqa: BLE001
        _set_admin_config_notice("error", f"Failed to save config: {exc}")


def _reset_to_defaults_callback() -> None:
    default_communities, default_keywords = _read_reddit_config(BASE_SOURCE_CONFIG_PATH)
    st.session_state.admin_communities_draft = "\n".join(default_communities)
    st.session_state.admin_keywords_draft = "\n".join(default_keywords)

    try:
        _write_runtime_source_config(default_communities, default_keywords)
        _set_admin_config_notice(
            "success",
            "Reset runtime config to defaults from base source_config.yaml.",
        )
    except Exception as exc:  # noqa: BLE001
        _set_admin_config_notice("error", f"Failed to reset runtime config: {exc}")

    st.rerun()


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

    st.markdown("#### Ingestion run metrics by platform")
    ingestion_platform_metrics = _load_ingestion_metrics_by_platform()
    if ingestion_platform_metrics:
        st.dataframe(ingestion_platform_metrics, width="stretch")
    else:
        st.info("No ingestion runs yet. Run 'Refresh Reddit ingestion' to populate this table.")

    st.markdown("#### Recent ingestion runs")
    ingestion_rows = _load_last_ingestion_runs()
    if ingestion_rows:
        st.dataframe(ingestion_rows, width="stretch")
    else:
        st.info("No ingestion runs yet. Run 'Refresh Reddit ingestion' to populate this table.")

    st.markdown("#### Enrichment run stats / errors")
    enrichment_rows = _load_last_enrichment_runs()
    if enrichment_rows:
        st.dataframe(enrichment_rows, width="stretch")
    else:
        st.info("No enrichment runs yet. Run 'Run enrichment' after ingestion to populate this table.")

    st.markdown("#### Source configuration")
    st.caption(f"Runtime config path: `{RUNTIME_SOURCE_CONFIG_PATH}`")

    initial_path = RUNTIME_SOURCE_CONFIG_PATH if RUNTIME_SOURCE_CONFIG_PATH.exists() else BASE_SOURCE_CONFIG_PATH
    initial_communities, initial_keywords = _read_reddit_config(initial_path)

    if "admin_communities_draft" not in st.session_state:
        st.session_state.admin_communities_draft = "\n".join(initial_communities)
    if "admin_keywords_draft" not in st.session_state:
        st.session_state.admin_keywords_draft = "\n".join(initial_keywords)

    notice_message = st.session_state.get("admin_config_notice_message")
    notice_level = st.session_state.get("admin_config_notice_level", "info")
    if notice_message:
        if notice_level == "success":
            st.success(notice_message)
        elif notice_level == "error":
            st.error(notice_message)
        else:
            st.info(notice_message)

    # Manual regression check: edit values, click Save, click Reset, and verify no
    # `st.session_state` widget mutation errors are raised.
    with st.form("admin_source_config_form"):
        communities_col, keywords_col = st.columns(2)
        with communities_col:
            st.markdown("**Reddit communities**")
            st.text_area(
                "Subreddit list editor",
                key="admin_communities_input",
                value=st.session_state.admin_communities_draft,
                height=180,
                help="One subreddit per line.",
            )
        with keywords_col:
            st.markdown("**Reddit keywords**")
            st.text_area(
                "Keyword list editor",
                key="admin_keywords_input",
                value=st.session_state.admin_keywords_draft,
                height=180,
                help="One keyword per line.",
            )

        action_col1, action_col2 = st.columns(2)
        with action_col1:
            st.form_submit_button("Save config", type="primary", on_click=_save_runtime_config_callback)

        with action_col2:
            st.form_submit_button("Reset to defaults", on_click=_reset_to_defaults_callback)
