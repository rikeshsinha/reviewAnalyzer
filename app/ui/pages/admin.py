"""Admin page to run maintenance jobs and inspect latest run status."""

from __future__ import annotations

import subprocess
import sys
from typing import Any

import streamlit as st
from sqlalchemy import text

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
