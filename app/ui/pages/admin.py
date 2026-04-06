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


def _read_platform_config(config_path: Path, platform: str) -> dict[str, Any]:
    if not config_path.exists():
        return {}

    lines = config_path.read_text(encoding="utf-8").splitlines()
    in_platform_block = False
    values: dict[str, Any] = {}
    pending_list_key: str | None = None
    pending_list_items: list[str] = []

    for raw_line in lines:
        line = raw_line.strip()
        indent = len(raw_line) - len(raw_line.lstrip(" "))

        if indent == 2 and line == f"{platform}:":
            in_platform_block = True
            continue
        if in_platform_block and indent == 2 and line.endswith(":") and line != f"{platform}:":
            break
        if not in_platform_block:
            continue
        if not line or line.startswith("#"):
            continue

        if pending_list_key:
            if line.startswith("]"):
                values[pending_list_key] = pending_list_items
                pending_list_key = None
                pending_list_items = []
                continue
            normalized = line.rstrip(",").strip().strip('"').strip("'").strip()
            if normalized:
                pending_list_items.append(normalized)
            continue

        if indent != 4 or ":" not in line:
            continue

        key, raw_value = line.split(":", 1)
        key = key.strip()
        value = raw_value.strip()

        if value == "[":
            pending_list_key = key
            pending_list_items = []
            continue
        if value.startswith("[") and value.endswith("]"):
            values[key] = _parse_inline_list(value)
            continue
        if value.isdigit():
            values[key] = int(value)
            continue
        if value.lower() in {"true", "false"}:
            values[key] = value.lower() == "true"
            continue
        values[key] = value.strip('"').strip("'")

    return values


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


def _write_runtime_source_config(
    *,
    reddit_communities: list[str],
    reddit_keywords: list[str],
    web_sites: list[str],
    web_keywords: list[str],
    web_max_pages_per_site: int,
    web_min_content_chars: int,
) -> None:
    base_text = BASE_SOURCE_CONFIG_PATH.read_text(encoding="utf-8")
    lines = base_text.splitlines()
    output_lines: list[str] = []
    in_reddit_block = False
    in_web_reviews_block = False
    skipping_web_sites_multiline = False
    web_keywords_written = False

    for raw_line in lines:
        line = raw_line.strip()
        indent = len(raw_line) - len(raw_line.lstrip(" "))

        if indent == 2 and line.startswith("reddit:"):
            in_reddit_block = True
            in_web_reviews_block = False
            output_lines.append(raw_line)
            continue
        if indent == 2 and line.startswith("web_reviews:"):
            in_web_reviews_block = True
            in_reddit_block = False
            output_lines.append(raw_line)
            continue
        if in_reddit_block and indent == 2 and line.endswith(":") and not line.startswith("reddit:"):
            in_reddit_block = False
        if in_web_reviews_block and indent == 2 and line.endswith(":") and not line.startswith("web_reviews:"):
            if not web_keywords_written:
                output_lines.append(f"    keywords: {_render_inline_list(web_keywords)}")
                web_keywords_written = True
            in_web_reviews_block = False

        if skipping_web_sites_multiline:
            if line.startswith("]"):
                skipping_web_sites_multiline = False
            continue

        if in_reddit_block and indent == 4 and line.startswith("communities:"):
            output_lines.append(f"    communities: {_render_inline_list(reddit_communities)}")
            continue
        if in_reddit_block and indent == 4 and line.startswith("keywords:"):
            output_lines.append(f"    keywords: {_render_inline_list(reddit_keywords)}")
            continue

        if in_web_reviews_block and indent == 4 and line.startswith("sites:"):
            output_lines.append(f"    sites: {_render_inline_list(web_sites)}")
            if line.endswith("[") or line == "sites:":
                skipping_web_sites_multiline = True
            continue
        if in_web_reviews_block and indent == 4 and line.startswith("keywords:"):
            output_lines.append(f"    keywords: {_render_inline_list(web_keywords)}")
            web_keywords_written = True
            continue
        if in_web_reviews_block and indent == 4 and line.startswith("max_pages_per_site:"):
            output_lines.append(f"    max_pages_per_site: {web_max_pages_per_site}")
            continue
        if in_web_reviews_block and indent == 4 and line.startswith("min_content_chars:"):
            output_lines.append(f"    min_content_chars: {web_min_content_chars}")
            continue

        output_lines.append(raw_line)

    if in_web_reviews_block and not web_keywords_written:
        output_lines.append(f"    keywords: {_render_inline_list(web_keywords)}")

    RUNTIME_SOURCE_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_SOURCE_CONFIG_PATH.write_text("\n".join(output_lines) + "\n", encoding="utf-8")


def _set_admin_config_notice(level: str, message: str) -> None:
    st.session_state.admin_config_notice_level = level
    st.session_state.admin_config_notice_message = message


def _save_runtime_config_callback() -> None:
    communities_text = st.session_state.get("admin_communities_input", st.session_state.get("admin_communities_draft", ""))
    keywords_text = st.session_state.get("admin_keywords_input", st.session_state.get("admin_keywords_draft", ""))
    web_sites_text = st.session_state.get("admin_web_sites_input", st.session_state.get("admin_web_sites_draft", ""))
    web_keywords_text = st.session_state.get(
        "admin_web_keywords_input",
        st.session_state.get("admin_web_keywords_draft", ""),
    )
    web_max_pages_per_site = int(
        st.session_state.get("admin_web_max_pages_input", st.session_state.get("admin_web_max_pages_draft", 50))
    )
    web_min_content_chars = int(
        st.session_state.get(
            "admin_web_min_chars_input",
            st.session_state.get("admin_web_min_chars_draft", 500),
        )
    )

    communities = _normalize_text_list(communities_text)
    keywords = _normalize_text_list(keywords_text)
    web_sites = _normalize_text_list(web_sites_text)
    web_keywords = _normalize_text_list(web_keywords_text)

    if not communities:
        _set_admin_config_notice("error", "At least one subreddit is required.")
        return
    if not web_sites:
        _set_admin_config_notice("error", "At least one web review site is required.")
        return
    if web_max_pages_per_site <= 0:
        _set_admin_config_notice("error", "Web max pages per site must be a positive integer.")
        return
    if web_min_content_chars <= 0:
        _set_admin_config_notice("error", "Web min content length must be a positive integer.")
        return

    try:
        _write_runtime_source_config(
            reddit_communities=communities,
            reddit_keywords=keywords,
            web_sites=web_sites,
            web_keywords=web_keywords,
            web_max_pages_per_site=web_max_pages_per_site,
            web_min_content_chars=web_min_content_chars,
        )
        st.session_state.admin_communities_draft = "\n".join(communities)
        st.session_state.admin_keywords_draft = "\n".join(keywords)
        st.session_state.admin_web_sites_draft = "\n".join(web_sites)
        st.session_state.admin_web_keywords_draft = "\n".join(web_keywords)
        st.session_state.admin_web_max_pages_draft = web_max_pages_per_site
        st.session_state.admin_web_min_chars_draft = web_min_content_chars
        _set_admin_config_notice(
            "success",
            f"Saved runtime config to `{RUNTIME_SOURCE_CONFIG_PATH}`.",
        )
        st.rerun()
    except Exception as exc:  # noqa: BLE001
        _set_admin_config_notice("error", f"Failed to save config: {exc}")


def _reset_to_defaults_callback() -> None:
    default_reddit = _read_platform_config(BASE_SOURCE_CONFIG_PATH, "reddit")
    default_web = _read_platform_config(BASE_SOURCE_CONFIG_PATH, "web_reviews")

    default_communities = [str(item) for item in default_reddit.get("communities", [])]
    default_keywords = [str(item) for item in default_reddit.get("keywords", [])]
    default_web_sites = [str(item) for item in default_web.get("sites", [])]
    default_web_keywords = [str(item) for item in default_web.get("keywords", [])]
    default_web_max_pages = int(default_web.get("max_pages_per_site", 50))
    default_web_min_chars = int(default_web.get("min_content_chars", 500))

    st.session_state.admin_communities_draft = "\n".join(default_communities)
    st.session_state.admin_keywords_draft = "\n".join(default_keywords)
    st.session_state.admin_web_sites_draft = "\n".join(default_web_sites)
    st.session_state.admin_web_keywords_draft = "\n".join(default_web_keywords)
    st.session_state.admin_web_max_pages_draft = default_web_max_pages
    st.session_state.admin_web_min_chars_draft = default_web_min_chars

    try:
        _write_runtime_source_config(
            reddit_communities=default_communities,
            reddit_keywords=default_keywords,
            web_sites=default_web_sites,
            web_keywords=default_web_keywords,
            web_max_pages_per_site=default_web_max_pages,
            web_min_content_chars=default_web_min_chars,
        )
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
    reddit_date_range = st.date_input(
        "Reddit ingestion date range",
        value=(default_from, default_to),
        help="Applies only to the 'Refresh Reddit ingestion' action on this page.",
    )
    reddit_ingestion_date_from: date | None = None
    reddit_ingestion_date_to: date | None = None

    if isinstance(reddit_date_range, tuple) and len(reddit_date_range) == 2:
        reddit_ingestion_date_from, reddit_ingestion_date_to = reddit_date_range
    elif isinstance(reddit_date_range, list) and len(reddit_date_range) == 2:
        reddit_ingestion_date_from, reddit_ingestion_date_to = reddit_date_range[0], reddit_date_range[1]

    invalid_reddit_date_range = (
        reddit_ingestion_date_from is None
        or reddit_ingestion_date_to is None
        or reddit_ingestion_date_from > reddit_ingestion_date_to
    )
    if invalid_reddit_date_range:
        st.error("Invalid Reddit ingestion date range: start date must be on or before end date.")

    web_date_range = st.date_input(
        "Web ingestion date range",
        value=(default_from, default_to),
        help="Applies only to the 'Refresh Web Reviews' action on this page.",
    )
    web_ingestion_date_from: date | None = None
    web_ingestion_date_to: date | None = None
    if isinstance(web_date_range, tuple) and len(web_date_range) == 2:
        web_ingestion_date_from, web_ingestion_date_to = web_date_range
    elif isinstance(web_date_range, list) and len(web_date_range) == 2:
        web_ingestion_date_from, web_ingestion_date_to = web_date_range[0], web_date_range[1]

    invalid_web_date_range = (
        web_ingestion_date_from is None or web_ingestion_date_to is None or web_ingestion_date_from > web_ingestion_date_to
    )
    if invalid_web_date_range:
        st.error("Invalid web ingestion date range: start date must be on or before end date.")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Refresh Reddit"):
            if invalid_reddit_date_range or reddit_ingestion_date_from is None or reddit_ingestion_date_to is None:
                st.error("Please select a valid ingestion date range before running refresh.")
                st.stop()
            env_overrides = {
                "REDDIT_INGEST_DATE_FROM": reddit_ingestion_date_from.isoformat(),
                "REDDIT_INGEST_DATE_TO": reddit_ingestion_date_to.isoformat(),
            }
            with st.spinner("Running Reddit ingestion job..."):
                ok, logs = _run_command("app.jobs.refresh_reddit", env_overrides=env_overrides)
            (st.success if ok else st.error)("Refresh completed." if ok else "Refresh failed.")
            st.code(logs or "No output")

    with c2:
        if st.button("Refresh Web Reviews"):
            if invalid_web_date_range or web_ingestion_date_from is None or web_ingestion_date_to is None:
                st.error("Please select a valid web ingestion date range before running refresh.")
                st.stop()
            env_overrides = {
                "WEB_REVIEWS_INGEST_DATE_FROM": web_ingestion_date_from.isoformat(),
                "WEB_REVIEWS_INGEST_DATE_TO": web_ingestion_date_to.isoformat(),
            }
            with st.spinner("Running web review ingestion job..."):
                ok, logs = _run_command("app.jobs.refresh_web_reviews", env_overrides=env_overrides)
            (st.success if ok else st.error)("Refresh completed." if ok else "Refresh failed.")
            st.code(logs or "No output")

    with c3:
        if st.button("Run enrichment"):
            with st.spinner("Running enrichment job..."):
                ok, logs = _run_command("app.jobs.enrich_new_docs")
            (st.success if ok else st.error)("Enrichment completed." if ok else "Enrichment failed.")
            st.code(logs or "No output")

    c4, _, _ = st.columns(3)
    with c4:
        if st.button("Rebuild insight cache"):
            with st.spinner("Rebuilding insight cache..."):
                ok, message = _rebuild_insight_cache(filters)
            (st.success if ok else st.error)(message)

    st.markdown("#### Ingestion run metrics by platform")
    ingestion_platform_metrics = _load_ingestion_metrics_by_platform()
    if ingestion_platform_metrics:
        st.dataframe(ingestion_platform_metrics, width="stretch")
    else:
        st.info("No ingestion runs yet. Run 'Refresh Reddit' or 'Refresh Web Reviews' to populate this table.")

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
        st.info("No ingestion runs yet. Run 'Refresh Reddit' or 'Refresh Web Reviews' to populate this table.")

    st.markdown("#### Enrichment run stats / errors")
    enrichment_rows = _load_last_enrichment_runs()
    if enrichment_rows:
        st.dataframe(enrichment_rows, width="stretch")
    else:
        st.info("No enrichment runs yet. Run 'Run enrichment' after ingestion to populate this table.")

    st.markdown("#### Source configuration")
    st.caption(f"Runtime config path: `{RUNTIME_SOURCE_CONFIG_PATH}`")

    initial_path = RUNTIME_SOURCE_CONFIG_PATH if RUNTIME_SOURCE_CONFIG_PATH.exists() else BASE_SOURCE_CONFIG_PATH
    initial_reddit = _read_platform_config(initial_path, "reddit")
    initial_web_reviews = _read_platform_config(initial_path, "web_reviews")

    initial_communities = [str(item) for item in initial_reddit.get("communities", [])]
    initial_keywords = [str(item) for item in initial_reddit.get("keywords", [])]
    initial_web_sites = [str(item) for item in initial_web_reviews.get("sites", [])]
    initial_web_keywords = [str(item) for item in initial_web_reviews.get("keywords", [])]
    initial_web_max_pages = int(initial_web_reviews.get("max_pages_per_site", 50))
    initial_web_min_chars = int(initial_web_reviews.get("min_content_chars", 500))

    if "admin_communities_draft" not in st.session_state:
        st.session_state.admin_communities_draft = "\n".join(initial_communities)
    if "admin_keywords_draft" not in st.session_state:
        st.session_state.admin_keywords_draft = "\n".join(initial_keywords)
    if "admin_web_sites_draft" not in st.session_state:
        st.session_state.admin_web_sites_draft = "\n".join(initial_web_sites)
    if "admin_web_keywords_draft" not in st.session_state:
        st.session_state.admin_web_keywords_draft = "\n".join(initial_web_keywords)
    if "admin_web_max_pages_draft" not in st.session_state:
        st.session_state.admin_web_max_pages_draft = initial_web_max_pages
    if "admin_web_min_chars_draft" not in st.session_state:
        st.session_state.admin_web_min_chars_draft = initial_web_min_chars

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
        reddit_communities_col, reddit_keywords_col = st.columns(2)
        with reddit_communities_col:
            st.markdown("**Reddit communities**")
            st.text_area(
                "Subreddit list editor",
                key="admin_communities_input",
                value=st.session_state.admin_communities_draft,
                height=180,
                help="One subreddit per line.",
            )
        with reddit_keywords_col:
            st.markdown("**Reddit keywords**")
            st.text_area(
                "Keyword list editor",
                key="admin_keywords_input",
                value=st.session_state.admin_keywords_draft,
                height=180,
                help="One keyword per line.",
            )

        web_sites_col, web_keywords_col = st.columns(2)
        with web_sites_col:
            st.markdown("**Web review sites**")
            st.text_area(
                "Web site list editor",
                key="admin_web_sites_input",
                value=st.session_state.admin_web_sites_draft,
                height=180,
                help="One site domain per line.",
            )
        with web_keywords_col:
            st.markdown("**Web review keywords**")
            st.text_area(
                "Web keyword list editor",
                key="admin_web_keywords_input",
                value=st.session_state.admin_web_keywords_draft,
                height=180,
                help="One keyword per line.",
            )

        web_settings_col1, web_settings_col2 = st.columns(2)
        with web_settings_col1:
            st.number_input(
                "Max pages per site",
                min_value=1,
                step=1,
                key="admin_web_max_pages_input",
                value=int(st.session_state.admin_web_max_pages_draft),
            )
        with web_settings_col2:
            st.number_input(
                "Min content length (chars)",
                min_value=1,
                step=50,
                key="admin_web_min_chars_input",
                value=int(st.session_state.admin_web_min_chars_draft),
            )

        action_col1, action_col2 = st.columns(2)
        with action_col1:
            st.form_submit_button("Save config", type="primary", on_click=_save_runtime_config_callback)

        with action_col2:
            st.form_submit_button("Reset to defaults", on_click=_reset_to_defaults_callback)
