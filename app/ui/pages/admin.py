"""Admin page to run maintenance jobs and inspect latest run status."""

from __future__ import annotations

import subprocess
import sys
from datetime import date, timedelta
import os
import json
from typing import Any

import streamlit as st
from sqlalchemy import text

from app.config.source_loader import (
    RUNTIME_SOURCE_CONFIG_PATH,
    load_raw_platforms,
    load_source_config,
    write_runtime_platform_overrides,
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


def _run_command(
    module: str,
    env_overrides: dict[str, str] | None = None,
    args: list[str] | None = None,
) -> tuple[bool, str]:
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    command = [sys.executable, "-m", module]
    if args:
        command.extend(args)
    proc = subprocess.run(
        command,
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


PLATFORM_OPTIONS = ["reddit", "web_reviews", "google_play"]


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


def _set_admin_config_notice(level: str, message: str) -> None:
    st.session_state.admin_config_notice_level = level
    st.session_state.admin_config_notice_message = message


def _build_refresh_sources_env(selected_platform: str) -> dict[str, str]:
    return {"INGESTION_PLATFORMS": selected_platform.strip()}


def _get_platform_view_model(platform: str, config: dict[str, Any]) -> dict[str, Any]:
    model = {"enabled": bool(config.get("enabled", False)), "days_back": int(config.get("days_back", 30))}
    if platform == "reddit":
        model["communities"] = list(config.get("subreddits", config.get("communities", [])))
        model["keywords"] = list(config.get("keywords", []))
    elif platform == "web_reviews":
        model["sites"] = list(config.get("sites", []))
        model["keywords"] = list(config.get("keywords", []))
        model["max_pages_per_site"] = int(config.get("max_pages_per_site", 50))
    elif platform == "google_play":
        model["apps"] = list(config.get("apps", []))
        model["countries"] = list(config.get("countries", []))
        model["languages"] = list(config.get("languages", []))
        model["max_reviews_per_app"] = int(config.get("max_reviews_per_app", 1000))
        model["keywords"] = list(config.get("keywords", []))
    return model


def _get_selected_platform_config(platform: str) -> dict[str, Any]:
    for cfg in load_source_config():
        if cfg.platform == platform:
            return _get_platform_view_model(platform, {"enabled": cfg.enabled, "days_back": cfg.days_back, **cfg.config})
    return _get_platform_view_model(platform, {})


def _get_platform_override_inputs(platform: str) -> dict[str, Any]:
    values: dict[str, Any] = {
        "enabled": bool(st.session_state.get("admin_platform_enabled", False)),
        "days_back": int(st.session_state.get("admin_platform_days_back", 30)),
    }
    if platform == "reddit":
        values["communities"] = _normalize_text_list(st.session_state.get("admin_platform_communities", ""))
        values["keywords"] = _normalize_text_list(st.session_state.get("admin_platform_keywords", ""))
    elif platform == "web_reviews":
        values["sites"] = _normalize_text_list(st.session_state.get("admin_platform_sites", ""))
        values["keywords"] = _normalize_text_list(st.session_state.get("admin_platform_keywords", ""))
        values["max_pages_per_site"] = int(st.session_state.get("admin_platform_max_pages_per_site", 50))
    elif platform == "google_play":
        values["apps"] = _normalize_text_list(st.session_state.get("admin_platform_apps", ""))
        values["countries"] = _normalize_text_list(st.session_state.get("admin_platform_countries", ""))
        values["languages"] = _normalize_text_list(st.session_state.get("admin_platform_languages", ""))
        values["max_reviews_per_app"] = int(st.session_state.get("admin_platform_max_reviews_per_app", 1000))
        values["keywords"] = _normalize_text_list(st.session_state.get("admin_platform_keywords", ""))
    return values


def _validate_platform_override(platform: str, values: dict[str, Any]) -> str | None:
    if values["days_back"] < 0:
        return "Days back must be zero or greater."
    if platform == "reddit" and values["enabled"] and not values.get("communities"):
        return "At least one subreddit/community is required when Reddit is enabled."
    if platform == "web_reviews":
        if values["enabled"] and not values.get("sites"):
            return "At least one web review site is required when web_reviews is enabled."
        if int(values.get("max_pages_per_site", 0)) <= 0:
            return "Max pages per site must be a positive integer."
    if platform == "google_play":
        if values["enabled"] and not values.get("apps"):
            return "At least one Google Play app is required when google_play is enabled."
        if int(values.get("max_reviews_per_app", 0)) <= 0:
            return "Max reviews per app must be a positive integer."
    return None


def _platform_override_payload(platform: str, values: dict[str, Any]) -> dict[str, Any]:
    payload = {"enabled": values["enabled"], "days_back": values["days_back"]}
    if platform == "reddit":
        payload["communities"] = values["communities"]
        payload["keywords"] = values["keywords"]
    elif platform == "web_reviews":
        payload["sites"] = values["sites"]
        payload["keywords"] = values["keywords"]
        payload["max_pages_per_site"] = values["max_pages_per_site"]
    elif platform == "google_play":
        payload["apps"] = values["apps"]
        payload["countries"] = values["countries"]
        payload["languages"] = values["languages"]
        payload["max_reviews_per_app"] = values["max_reviews_per_app"]
        payload["keywords"] = values["keywords"]
    return payload


def _save_selected_platform_override(platform: str, values: dict[str, Any]) -> None:
    overrides = load_raw_platforms(RUNTIME_SOURCE_CONFIG_PATH)
    overrides[platform] = _platform_override_payload(platform, values)
    write_runtime_platform_overrides(overrides)


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
            with st.spinner("Running web review ingestion job..."):
                ok, logs = _run_command(
                    "app.jobs.refresh_web_reviews",
                    args=[
                        "--date-from",
                        web_ingestion_date_from.isoformat(),
                        "--date-to",
                        web_ingestion_date_to.isoformat(),
                    ],
                )
            (st.success if ok else st.error)("Refresh completed." if ok else "Refresh failed.")
            st.code(logs or "No output")

    with c3:
        if st.button("Run enrichment"):
            with st.spinner("Running enrichment job..."):
                ok, logs = _run_command("app.jobs.enrich_new_docs")
            (st.success if ok else st.error)("Enrichment completed." if ok else "Enrichment failed.")
            st.code(logs or "No output")

    selected_platform = st.selectbox(
        "Admin platform",
        options=PLATFORM_OPTIONS,
        key="admin_selected_platform",
        help="Select one platform for config editing and refresh operations.",
    )
    if st.session_state.get("admin_last_selected_platform") != selected_platform:
        for key in [
            "admin_platform_enabled",
            "admin_platform_days_back",
            "admin_platform_keywords",
            "admin_platform_communities",
            "admin_platform_sites",
            "admin_platform_max_pages_per_site",
            "admin_platform_apps",
            "admin_platform_countries",
            "admin_platform_languages",
            "admin_platform_max_reviews_per_app",
        ]:
            st.session_state.pop(key, None)
        st.session_state["admin_last_selected_platform"] = selected_platform

    st.markdown("#### Platform-scoped ingestion refresh")
    st.caption("Refresh runs only selected platform.")
    if selected_platform == "web_reviews":
        st.info("Placeholder adapter: validates config; crawler not yet implemented.")
    if st.button("Refresh selected platform"):
        selected_config = _get_selected_platform_config(selected_platform)
        if not selected_config.get("enabled", False):
            st.warning(
                f"Cannot refresh `{selected_platform}` because it is disabled in merged source config. "
                "Enable it in Source configuration first."
            )
        else:
            refresh_module = "app.jobs.refresh_sources"
            with st.spinner(f"Running ingestion refresh for: {selected_platform}"):
                ok, logs = _run_command(
                    refresh_module,
                    env_overrides=_build_refresh_sources_env(selected_platform),
                )
            summary = f"Selected platform: {selected_platform}\nJob module: {refresh_module}"
            if ok:
                st.success(f"Refresh completed for selected platform `{selected_platform}`.")
            else:
                st.error(f"Refresh failed for selected platform `{selected_platform}`.")
                if "Selected platform(s) are not enabled or not defined" in logs:
                    st.warning("Selected platform is not enabled/defined in merged config; refresh was blocked.")
            st.code(f"{summary}\n\n{logs}" if logs else summary)

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
    current_platform_config = _get_selected_platform_config(selected_platform)
    st.caption("Refresh runs only selected platform.")
    if selected_platform == "web_reviews":
        st.info("Placeholder adapter: validates config; crawler not yet implemented.")

    st.session_state["admin_platform_enabled"] = st.session_state.get(
        "admin_platform_enabled",
        current_platform_config["enabled"],
    )
    st.session_state["admin_platform_days_back"] = st.session_state.get(
        "admin_platform_days_back",
        current_platform_config["days_back"],
    )
    st.session_state["admin_platform_keywords"] = st.session_state.get(
        "admin_platform_keywords",
        "\n".join(current_platform_config.get("keywords", [])),
    )

    notice_message = st.session_state.get("admin_config_notice_message")
    notice_level = st.session_state.get("admin_config_notice_level", "info")
    if notice_message:
        if notice_level == "success":
            st.success(notice_message)
        elif notice_level == "error":
            st.error(notice_message)
        else:
            st.info(notice_message)
    with st.form("admin_source_config_form"):
        st.checkbox("Enabled", key="admin_platform_enabled")
        st.number_input("Days back", min_value=0, step=1, key="admin_platform_days_back")

        if selected_platform == "reddit":
            st.session_state["admin_platform_communities"] = st.session_state.get(
                "admin_platform_communities",
                "\n".join(current_platform_config.get("communities", [])),
            )
            st.text_area("Communities / subreddits", key="admin_platform_communities", height=160)
            st.text_area("Keywords", key="admin_platform_keywords", height=140)
        elif selected_platform == "web_reviews":
            st.session_state["admin_platform_sites"] = st.session_state.get(
                "admin_platform_sites",
                "\n".join(current_platform_config.get("sites", [])),
            )
            st.text_area("Sites", key="admin_platform_sites", height=160)
            st.text_area("Keywords", key="admin_platform_keywords", height=140)
            st.session_state["admin_platform_max_pages_per_site"] = st.session_state.get(
                "admin_platform_max_pages_per_site",
                current_platform_config.get("max_pages_per_site", 50),
            )
            st.number_input(
                "Max pages per site",
                min_value=1,
                step=1,
                key="admin_platform_max_pages_per_site",
            )
        elif selected_platform == "google_play":
            st.session_state["admin_platform_apps"] = st.session_state.get(
                "admin_platform_apps",
                "\n".join(current_platform_config.get("apps", [])),
            )
            st.session_state["admin_platform_countries"] = st.session_state.get(
                "admin_platform_countries",
                "\n".join(current_platform_config.get("countries", [])),
            )
            st.session_state["admin_platform_languages"] = st.session_state.get(
                "admin_platform_languages",
                "\n".join(current_platform_config.get("languages", [])),
            )
            st.text_area("Apps", key="admin_platform_apps", height=120)
            st.text_area("Countries (ISO-2)", key="admin_platform_countries", height=120)
            st.text_area("Languages", key="admin_platform_languages", height=120)
            st.text_area("Keywords", key="admin_platform_keywords", height=120)
            st.session_state["admin_platform_max_reviews_per_app"] = st.session_state.get(
                "admin_platform_max_reviews_per_app",
                current_platform_config.get("max_reviews_per_app", 1000),
            )
            st.number_input(
                "Max reviews per app",
                min_value=1,
                step=50,
                key="admin_platform_max_reviews_per_app",
            )

        submitted = st.form_submit_button("Save selected platform config", type="primary")
        if submitted:
            override_values = _get_platform_override_inputs(selected_platform)
            validation_message = _validate_platform_override(selected_platform, override_values)
            if validation_message:
                _set_admin_config_notice("error", validation_message)
            else:
                try:
                    _save_selected_platform_override(selected_platform, override_values)
                    _set_admin_config_notice(
                        "success",
                        f"Saved `{selected_platform}` runtime override to `{RUNTIME_SOURCE_CONFIG_PATH}`.",
                    )
                    st.rerun()
                except Exception as exc:  # noqa: BLE001
                    _set_admin_config_notice("error", f"Failed to save config: {exc}")
