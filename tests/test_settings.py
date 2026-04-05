from __future__ import annotations

import pytest

from app.config import settings as settings_module


def _clear_settings_caches() -> None:
    cache_clear = getattr(settings_module.get_ingestion_settings, "cache_clear", None)
    if callable(cache_clear):
        cache_clear()
    settings_module.get_enrichment_settings.cache_clear()


@pytest.fixture(autouse=True)
def _reset_settings(monkeypatch):
    for env_name in (
        "OPENAI_API_KEY",
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "REDDIT_USER_AGENT",
        "REDDIT_FETCH_BACKEND",
        "REDDIT_PUSHSHIFT_BASE_URL",
        "PUSHSHIFT_BASE_URL",
        "PUSHSHIFT_PAGE_SIZE",
        "PUSHSHIFT_MAX_PAGES",
        "PUBLIC_REDDIT_BASE_URL",
        "PUBLIC_REDDIT_USER_AGENT",
        "PUBLIC_REDDIT_PAGE_SIZE",
        "PUBLIC_REDDIT_MAX_PAGES",
        "PUBLIC_REDDIT_DELAY_SECONDS",
        "PUBLIC_REDDIT_INCLUDE_RECENT_WHEN_NO_KEYWORD_HITS",
    ):
        monkeypatch.delenv(env_name, raising=False)
    _clear_settings_caches()
    yield
    _clear_settings_caches()


def test_enrichment_settings_require_only_openai_api_key(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "dummy-key")

    settings = settings_module.get_enrichment_settings()

    assert settings.openai_api_key == "dummy-key"


def test_ingestion_settings_allow_missing_public_reddit_vars_with_defaults() -> None:
    settings = settings_module.get_ingestion_settings()

    assert settings.reddit_fetch_backend == "praw"
    assert settings.public_reddit_base_url == "https://www.reddit.com"
    assert settings.public_reddit_user_agent == "reviewAnalyzer/0.1 (public-json-ingestion)"
    assert settings.public_reddit_page_size == 100
    assert settings.public_reddit_max_pages == 5
    assert settings.public_reddit_include_recent_when_no_keyword_hits is True


def test_blank_reddit_user_agent_uses_default(monkeypatch) -> None:
    monkeypatch.setenv("REDDIT_USER_AGENT", "")

    settings = settings_module.get_ingestion_settings()

    assert settings.reddit_user_agent == "reviewAnalyzer/0.1"
