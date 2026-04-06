"""Admin page to run maintenance jobs and inspect latest run status."""

from __future__ import annotations

import subprocess
import sys
import os
from pathlib import Path
from typing import Any

import streamlit as st
from sqlalchemy import text

from app.config.source_loader import (
    BASE_SOURCE_CONFIG_PATH,
    RUNTIME_SOURCE_CONFIG_PATH,
    load_source_config,
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


def _run_command(module: str, *, env_overrides: dict[str, str] | None = None) -> tuple[bool, str]:
    env = None
    if env_overrides:
        env = {**os.environ, **env_overrides}
    proc = subprocess.run(
        [sys.executable, "-m", module],
        capture_output=True,
        text=True,
        check=False,
        env=env,
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


def _platform_config(platform: str, *, config_path: Path | None = None) -> dict[str, Any]:
    for entry in load_source_config(config_path=config_path):
        if entry.platform == platform:
            return {"enabled": entry.enabled, "days_back": entry.days_back, **entry.config}
    return {}


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


def _write_runtime_source_config(platform: str, overrides: dict[str, Any]) -> None:
    current_overrides: dict[str, dict[str, Any]] = {}
    if RUNTIME_SOURCE_CONFIG_PATH.exists():
        current_platform = ""
        for raw_line in RUNTIME_SOURCE_CONFIG_PATH.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            indent = len(raw_line) - len(raw_line.lstrip(" "))
            if not line or line.startswith("#") or line == "platforms:":
                continue
            if indent == 2 and line.endswith(":"):
                current_platform = line[:-1].strip()
                current_overrides.setdefault(current_platform, {})
                continue
            if indent == 4 and ":" in line and current_platform:
                key, value = line.split(":", 1)
                value = value.strip()
                if value.startswith("[") and value.endswith("]"):
                    parsed = [part.strip().strip('"').strip("'") for part in value[1:-1].split(",") if part.strip()]
                    current_overrides[current_platform][key.strip()] = parsed
                elif value.lower() in {"true", "false"}:
                    current_overrides[current_platform][key.strip()] = value.lower() == "true"
                elif value.isdigit():
                    current_overrides[current_platform][key.strip()] = int(value)
                else:
                    current_overrides[current_platform][key.strip()] = value.strip('"').strip("'")

    current_overrides[platform] = dict(overrides)
    output_lines: list[str] = ["platforms:"]
    for platform_name in sorted(current_overrides):
        output_lines.append(f"  {platform_name}:")
        for key, value in current_overrides[platform_name].items():
            if isinstance(value, list):
                output_lines.append(f"    {key}: {_render_inline_list([str(v) for v in value])}")
            elif isinstance(value, bool):
                output_lines.append(f"    {key}: {'true' if value else 'false'}")
            else:
                output_lines.append(f"    {key}: {value}")
    RUNTIME_SOURCE_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_SOURCE_CONFIG_PATH.write_text("\n".join(output_lines) + "\n", encoding="utf-8")


def render(filters: dict[str, Any]) -> None:
    st.subheader("Admin")

    source_options = ["reddit", "web_reviews", "google_play"]
    selected_source = st.selectbox(
        "Platform",
        options=source_options,
        index=0,
        help="Choose which source to configure and refresh.",
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Refresh selected ingestion"):
            with st.spinner(f"Running {selected_source} ingestion job..."):
                ok, logs = _run_command(
                    "app.jobs.refresh_sources",
                    env_overrides={"INGESTION_PLATFORMS": selected_source},
                )
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

    platform_cfg = _platform_config(selected_source)
    list_field_one = "communities" if selected_source == "reddit" else ("sites" if selected_source == "web_reviews" else "apps")
    list_label_one = "Reddit communities" if selected_source == "reddit" else ("Web review sites" if selected_source == "web_reviews" else "Google Play app IDs")
    list_field_two = "keywords"
    list_label_two = "Keywords"
    one_default = "\n".join(platform_cfg.get("subreddits", platform_cfg.get(list_field_one, [])))
    two_default = "\n".join(platform_cfg.get(list_field_two, []))

    state_prefix = f"admin_{selected_source}"
    draft_one_key = f"{state_prefix}_{list_field_one}_draft"
    draft_two_key = f"{state_prefix}_{list_field_two}_draft"
    if draft_one_key not in st.session_state:
        st.session_state[draft_one_key] = one_default
    if draft_two_key not in st.session_state:
        st.session_state[draft_two_key] = two_default

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**{list_label_one}**")
        values_one_text = st.text_area(
            f"{list_label_one} editor",
            key=f"{state_prefix}_{list_field_one}_input",
            value=st.session_state[draft_one_key],
            height=180,
            help="One value per line.",
        )
    with col2:
        st.markdown(f"**{list_label_two}**")
        values_two_text = st.text_area(
            f"{list_label_two} editor",
            key=f"{state_prefix}_{list_field_two}_input",
            value=st.session_state[draft_two_key],
            height=180,
            help="One value per line.",
        )

    action_col1, action_col2 = st.columns(2)
    with action_col1:
        if st.button("Save config", type="primary"):
            values_one = _normalize_text_list(values_one_text)
            keywords = _normalize_text_list(values_two_text)
            if not values_one:
                st.error("At least one value is required.")
            else:
                try:
                    payload: dict[str, Any] = {list_field_one: values_one, "keywords": keywords}
                    if selected_source == "reddit":
                        payload["communities"] = values_one
                    if selected_source == "web_reviews":
                        payload["max_pages_per_site"] = int(platform_cfg.get("max_pages_per_site", 50))
                    if selected_source == "google_play":
                        payload["countries"] = platform_cfg.get("countries", ["us"])
                        payload["languages"] = platform_cfg.get("languages", ["en"])
                        payload["max_reviews_per_app"] = int(platform_cfg.get("max_reviews_per_app", 1000))
                    _write_runtime_source_config(selected_source, payload)
                    st.session_state[draft_one_key] = "\n".join(values_one)
                    st.session_state[draft_two_key] = "\n".join(keywords)
                    st.success(f"Saved runtime config to `{RUNTIME_SOURCE_CONFIG_PATH}`.")
                    st.rerun()
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Failed to save config: {exc}")

    with action_col2:
        if st.button("Reset to defaults"):
            base_cfg = _platform_config(selected_source, config_path=BASE_SOURCE_CONFIG_PATH)
            default_list_one = base_cfg.get("subreddits", base_cfg.get(list_field_one, []))
            default_keywords = base_cfg.get("keywords", [])
            try:
                payload: dict[str, Any] = {list_field_one: default_list_one, "keywords": default_keywords}
                if selected_source == "reddit":
                    payload["communities"] = default_list_one
                if selected_source == "web_reviews":
                    payload["max_pages_per_site"] = int(base_cfg.get("max_pages_per_site", 50))
                if selected_source == "google_play":
                    payload["countries"] = base_cfg.get("countries", ["us"])
                    payload["languages"] = base_cfg.get("languages", ["en"])
                    payload["max_reviews_per_app"] = int(base_cfg.get("max_reviews_per_app", 1000))
                _write_runtime_source_config(selected_source, payload)
                st.session_state[draft_one_key] = "\n".join(default_list_one)
                st.session_state[draft_two_key] = "\n".join(default_keywords)
                st.success(f"Reset {selected_source} runtime config to defaults from base source_config.yaml.")
                st.rerun()
            except Exception as exc:  # noqa: BLE001
                st.error(f"Failed to reset runtime config: {exc}")
