"""Application settings loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
import os
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError


class WebReviewsSourceSettings(BaseModel):
    """Typed settings for web review crawling source configuration."""

    enabled: bool = False
    sites: list[str] = Field(default_factory=list)
    max_pages_per_site: int = Field(50, ge=1)
    min_content_chars: int = Field(500, ge=1)
    crawl_paths: list[str] = Field(default_factory=lambda: ["homepage", "category"])
    prioritize_keywords: bool = False


class CommonSettings(BaseModel):
    """Settings shared by app components."""

    openai_api_key: str = Field(..., alias="OPENAI_API_KEY", min_length=1)


class IngestionSettings(BaseModel):
    """Settings required for ingestion jobs and clients."""

    reddit_client_id: str | None = Field(None, alias="REDDIT_CLIENT_ID")
    reddit_client_secret: str | None = Field(None, alias="REDDIT_CLIENT_SECRET")
    reddit_username: str | None = Field(None, alias="REDDIT_USERNAME")
    reddit_password: str | None = Field(None, alias="REDDIT_PASSWORD")
    reddit_user_agent: str = Field("reviewAnalyzer/0.1", alias="REDDIT_USER_AGENT", min_length=1)
    reddit_fetch_backend: str = Field("praw", alias="REDDIT_FETCH_BACKEND", min_length=1)
    reddit_pushshift_base_url: str | None = Field(None, alias="REDDIT_PUSHSHIFT_BASE_URL")
    pushshift_base_url: str = Field(
        "https://api.pushshift.io/reddit/search/submission/",
        alias="PUSHSHIFT_BASE_URL",
        min_length=1,
    )
    pushshift_page_size: int = Field(100, alias="PUSHSHIFT_PAGE_SIZE", ge=1, le=100)
    pushshift_max_pages: int = Field(20, alias="PUSHSHIFT_MAX_PAGES", ge=1, le=1000)
    public_reddit_base_url: str = Field("https://www.reddit.com", alias="PUBLIC_REDDIT_BASE_URL", min_length=1)
    public_reddit_user_agent: str = Field(
        "reviewAnalyzer/0.1 (public-json-ingestion)",
        alias="PUBLIC_REDDIT_USER_AGENT",
        min_length=1,
    )
    public_reddit_page_size: int = Field(100, alias="PUBLIC_REDDIT_PAGE_SIZE", ge=1, le=100)
    public_reddit_max_pages: int = Field(5, alias="PUBLIC_REDDIT_MAX_PAGES", ge=1, le=1000)
    public_reddit_delay_seconds: float = Field(1.0, alias="PUBLIC_REDDIT_DELAY_SECONDS", ge=0, le=30)
    public_reddit_include_recent_when_no_keyword_hits: bool = Field(
        True,
        alias="PUBLIC_REDDIT_INCLUDE_RECENT_WHEN_NO_KEYWORD_HITS",
    )
    runtime_source_config_path: str = Field("data/runtime_source_config.yaml", alias="RUNTIME_SOURCE_CONFIG_PATH")


class EnrichmentSettings(CommonSettings):
    """Settings required by enrichment jobs."""


Settings = EnrichmentSettings


def _build_env_values() -> dict[str, Any]:
    """Load env values and drop missing/blank values so model defaults can apply."""

    load_dotenv()
    raw_values = {
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "REDDIT_CLIENT_ID": os.getenv("REDDIT_CLIENT_ID"),
        "REDDIT_CLIENT_SECRET": os.getenv("REDDIT_CLIENT_SECRET"),
        "REDDIT_USERNAME": os.getenv("REDDIT_USERNAME"),
        "REDDIT_PASSWORD": os.getenv("REDDIT_PASSWORD"),
        "REDDIT_USER_AGENT": os.getenv("REDDIT_USER_AGENT"),
        "REDDIT_FETCH_BACKEND": os.getenv("REDDIT_FETCH_BACKEND"),
        "REDDIT_PUSHSHIFT_BASE_URL": os.getenv("REDDIT_PUSHSHIFT_BASE_URL"),
        "PUSHSHIFT_BASE_URL": os.getenv("PUSHSHIFT_BASE_URL"),
        "PUSHSHIFT_PAGE_SIZE": os.getenv("PUSHSHIFT_PAGE_SIZE"),
        "PUSHSHIFT_MAX_PAGES": os.getenv("PUSHSHIFT_MAX_PAGES"),
        "PUBLIC_REDDIT_BASE_URL": os.getenv("PUBLIC_REDDIT_BASE_URL"),
        "PUBLIC_REDDIT_USER_AGENT": os.getenv("PUBLIC_REDDIT_USER_AGENT"),
        "PUBLIC_REDDIT_PAGE_SIZE": os.getenv("PUBLIC_REDDIT_PAGE_SIZE"),
        "PUBLIC_REDDIT_MAX_PAGES": os.getenv("PUBLIC_REDDIT_MAX_PAGES"),
        "PUBLIC_REDDIT_DELAY_SECONDS": os.getenv("PUBLIC_REDDIT_DELAY_SECONDS"),
        "PUBLIC_REDDIT_INCLUDE_RECENT_WHEN_NO_KEYWORD_HITS": os.getenv(
            "PUBLIC_REDDIT_INCLUDE_RECENT_WHEN_NO_KEYWORD_HITS"
        ),
        "RUNTIME_SOURCE_CONFIG_PATH": os.getenv("RUNTIME_SOURCE_CONFIG_PATH"),
    }
    cleaned_values: dict[str, Any] = {}
    for key, value in raw_values.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        cleaned_values[key] = value
    return cleaned_values


def get_ingestion_settings() -> IngestionSettings:
    """Load and validate ingestion settings from environment variables."""

    try:
        return IngestionSettings.model_validate(_build_env_values())
    except ValidationError as exc:
        raise RuntimeError(
            "Missing or invalid ingestion environment variables. Please check your .env file."
        ) from exc


@lru_cache(maxsize=1)
def get_enrichment_settings() -> EnrichmentSettings:
    """Load and validate enrichment settings from environment variables."""

    try:
        return EnrichmentSettings.model_validate(_build_env_values())
    except ValidationError as exc:
        raise RuntimeError(
            "Missing or invalid enrichment environment variables. Please check your .env file."
        ) from exc


def get_settings() -> Settings:
    """Backward-compatible alias for enrichment-focused settings."""

    return get_enrichment_settings()
