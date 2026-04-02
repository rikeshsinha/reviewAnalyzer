from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.config import settings as settings_module
from app.jobs import enrich_new_docs
from app.services.enrichment_service import EnrichmentConfig, EnrichmentService


def _build_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    schema_path = Path(__file__).resolve().parents[1] / "app" / "db" / "schema.sql"

    with engine.begin() as connection:
        connection.connection.executescript(schema_path.read_text(encoding="utf-8"))

    return Session(bind=engine, future=True)


class FakeClient:
    def __init__(self, response: str = '{"documents": []}') -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=self.response))]
        )


def _insert_source_and_doc(
    session: Session,
    *,
    platform: str,
    body: str,
    rating: int | None,
    external_id: str,
) -> int:
    source_id = int(
        session.execute(
            text(
                """
                INSERT INTO sources (platform, external_id, name, metadata_json)
                VALUES (:platform, :external_id, :name, '{}')
                """
            ),
            {"platform": platform, "external_id": platform, "name": platform},
        ).lastrowid
    )

    raw_json = json.dumps({"platform": platform, "rating": rating})
    doc_id = int(
        session.execute(
            text(
                """
                INSERT INTO documents
                (source_id, external_id, title, body, author, url, published_at, raw_json)
                VALUES (:source_id, :external_id, '', :body, 'u', 'https://example.com', '2026-03-02T00:00:00', :raw_json)
                """
            ),
            {"source_id": source_id, "external_id": external_id, "body": body, "raw_json": raw_json},
        ).lastrowid
    )
    session.commit()
    return doc_id


def test_short_google_play_review_skips_llm_and_uses_rating_fallback() -> None:
    session = _build_session()
    _insert_source_and_doc(
        session,
        platform="google_play",
        body="bad app",
        rating=1,
        external_id="gp-short",
    )
    client = FakeClient()
    service = EnrichmentService(session, client, EnrichmentConfig(min_text_chars=15))

    stats = service.enrich_new_documents()

    assert stats["skipped_short"] == 1
    assert stats["enriched"] == 1
    assert client.calls == []

    row = session.execute(text("SELECT metadata_json FROM enrichments LIMIT 1")).first()
    metadata = json.loads(row.metadata_json)
    assert metadata["rating"] == 1
    assert metadata["sentiment_label"] == "negative"
    assert metadata["issue_category"] == "other"
    assert metadata["feature_request_flag"] is False


def test_google_play_prompt_payload_includes_rating() -> None:
    session = _build_session()
    doc_id = _insert_source_and_doc(
        session,
        platform="google_play",
        body="This is long enough to use the model.",
        rating=2,
        external_id="gp-1",
    )
    _insert_source_and_doc(
        session,
        platform="reddit",
        body="This is long enough too for reddit.",
        rating=None,
        external_id="rd-1",
    )

    response = json.dumps(
        {
            "documents": [
                {
                    "document_id": doc_id,
                    "sentiment_label": "negative",
                    "primary_issue_category": "other",
                    "feature_request_flag": False,
                    "competitor_mentions": [],
                    "summary_snippet": "summary",
                }
            ]
        }
    )
    client = FakeClient(response=response)
    service = EnrichmentService(
        session,
        client,
        EnrichmentConfig(batch_size=10, min_text_chars=15),
    )

    service.enrich_new_documents()

    assert len(client.calls) == 1
    user_content = client.calls[0]["messages"][1]["content"]
    assert f'"document_id": {doc_id}' in user_content
    assert '"rating": 2' in user_content


def test_enrich_job_run_starts_with_only_openai_env(monkeypatch) -> None:
    settings_module.get_enrichment_settings.cache_clear()
    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    for env_name in (
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "REDDIT_USER_AGENT",
        "PUBLIC_REDDIT_BASE_URL",
        "PUBLIC_REDDIT_USER_AGENT",
        "PUBLIC_REDDIT_PAGE_SIZE",
    ):
        monkeypatch.delenv(env_name, raising=False)

    class _FakeSession:
        def close(self) -> None:
            return None

    class _FakeRunRepo:
        def __init__(self, _session) -> None:
            self.completed_status: str | None = None

        def start_run(self) -> int:
            return 1

        def complete_run(self, **kwargs) -> None:
            self.completed_status = kwargs.get("status")

    class _FakeService:
        def __init__(self, **kwargs) -> None:
            assert kwargs["config"].batch_size >= 1

        def enrich_new_documents(self) -> dict[str, int]:
            return {"candidates": 0, "enriched": 0, "skipped_short": 0, "failed_batches": 0}

    monkeypatch.setattr(enrich_new_docs, "setup_logging", lambda: None)
    monkeypatch.setattr(enrich_new_docs, "SessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(enrich_new_docs, "EnrichmentRunRepository", _FakeRunRepo)
    monkeypatch.setattr(enrich_new_docs, "OpenAI", lambda api_key: SimpleNamespace(api_key=api_key))
    monkeypatch.setattr(enrich_new_docs, "EnrichmentService", _FakeService)

    enrich_new_docs.run()
