"""Admin page to run maintenance jobs and inspect latest run status."""

from __future__ import annotations

import subprocess
import sys
from datetime import date, timedelta
import os
import json
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


def _run_command(module: str, env_overrides: dict[str, str] | None = None) -> tuple[bool, str]:
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    proc = subprocess.run(
        [sys.executable, "-m", module],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    return proc.returncode == 0, output.strip()


def _parse_ingestion_diagnostics(error_message: str | None) -> dict[str, Any] | None:
    if not error_message:
        return None
    try:
        payload = json.loads(error_message)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


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

    default_to = date.today()
    default_from = default_to - timedelta(days=30)
    date_range = st.date_input(
        "Ingestion date range",
        value=(default_from, default_to),
        help="Applies only to the 'Refresh Reddit ingestion' action on this page.",
    )
    ingestion_date_from: date | None = None
    ingestion_date_to: date | None = None

    if isinstance(date_range, tuple) and len(date_range) == 2:
        ingestion_date_from, ingestion_date_to = date_range
    elif isinstance(date_range, list) and len(date_range) == 2:
        ingestion_date_from, ingestion_date_to = date_range[0], date_range[1]

    invalid_date_range = (
        ingestion_date_from is None or ingestion_date_to is None or ingestion_date_from > ingestion_date_to
    )
    if invalid_date_range:
        st.error("Invalid ingestion date range: start date must be on or before end date.")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Refresh Reddit ingestion"):
            if invalid_date_range or ingestion_date_from is None or ingestion_date_to is None:
                st.error("Please select a valid ingestion date range before running refresh.")
                st.stop()
            env_overrides = {
                "REDDIT_INGEST_DATE_FROM": ingestion_date_from.isoformat(),
                "REDDIT_INGEST_DATE_TO": ingestion_date_to.isoformat(),
            }
            with st.spinner("Running Reddit ingestion job..."):
                ok, logs = _run_command("app.jobs.refresh_reddit", env_overrides=env_overrides)
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

        st.markdown("#### Ingestion diagnostics")
        for run in ingestion_rows:
            diagnostics = _parse_ingestion_diagnostics(run.get("error_message"))
            if not diagnostics:
                continue

            run_id = run.get("id")
            backend_selected = diagnostics.get("backend_requested") or "n/a"
            backend_used = diagnostics.get("backend_used") or "n/a"
            first_failing_stage = diagnostics.get("first_failing_stage") or "-"
            error_summary = diagnostics.get("error_summary") or "-"
            fallback_activated = diagnostics.get("fallback_activated")
            fallback_label = "yes" if fallback_activated else "no"
            effective_config = diagnostics.get("effective_config", {})

            with st.expander(f"Run #{run_id} diagnostics ({run.get('status', 'unknown')})"):
                st.markdown(
                    f"**Backend selected:** `{backend_selected}`  \n"
                    f"**Backend used:** `{backend_used}`  \n"
                    f"**Fallback activated:** `{fallback_label}`  \n"
                    f"**First failing stage:** `{first_failing_stage}`  \n"
                    f"**Error summary:** `{error_summary}`"
                )

                stages = diagnostics.get("stages", {})
                stage_rows: list[dict[str, Any]] = []
                for stage_name in ["fetch", "normalize", "dedupe", "insert", "enrich_trigger"]:
                    stage_data = stages.get(stage_name, {})
                    error_data = stage_data.get("error") or {}
                    stage_rows.append(
                        {
                            "stage": stage_name,
                            "status": stage_data.get("status", "unknown"),
                            "count": stage_data.get("count"),
                            "inserted": stage_data.get("inserted"),
                            "dedupe_skipped": stage_data.get("skipped"),
                            "error_class": error_data.get("class"),
                            "error_message": error_data.get("message"),
                        }
                    )
                st.dataframe(stage_rows, width="stretch")

                st.caption("Effective config snapshot")
                st.json(
                    {
                        "subreddits": effective_config.get("subreddits", []),
                        "keywords": effective_config.get("keywords", []),
                        "date_from": effective_config.get("date_from"),
                        "date_to": effective_config.get("date_to"),
                        "post_limit": effective_config.get("post_limit"),
                        "days_back": effective_config.get("days_back"),
                    },
                    expanded=False,
                )

                fetch_diagnostics = diagnostics.get("fetch_diagnostics", {})
                if isinstance(fetch_diagnostics, dict):
                    st.caption("Fetch diagnostics")
                    st.json(fetch_diagnostics, expanded=False)

                fetch_status = stages.get("fetch", {}).get("status")
                has_fetch_error = bool(stages.get("fetch", {}).get("error"))
                if fetch_status == "empty" and not has_fetch_error:
                    st.warning(
                        "Fetch returned no documents without a hard error. "
                        "Recommendations: widen the date range, reduce keyword strictness, "
                        "or enable recent-post fallback mode."
                    )
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
