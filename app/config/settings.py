"""Application settings loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
import os

from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError


class Settings(BaseModel):
    """Typed application settings."""

    openai_api_key: str = Field(..., alias="OPENAI_API_KEY", min_length=1)
    reddit_client_id: str | None = Field(None, alias="REDDIT_CLIENT_ID")
    reddit_client_secret: str | None = Field(None, alias="REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = Field("reviewAnalyzer/0.1", alias="REDDIT_USER_AGENT", min_length=1)
    reddit_fetch_backend: str = Field("praw", alias="REDDIT_FETCH_BACKEND", min_length=1)
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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load and validate settings from environment variables."""

    load_dotenv()

    raw_values = {
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "REDDIT_CLIENT_ID": os.getenv("REDDIT_CLIENT_ID"),
        "REDDIT_CLIENT_SECRET": os.getenv("REDDIT_CLIENT_SECRET"),
        "REDDIT_USER_AGENT": os.getenv("REDDIT_USER_AGENT"),
        "REDDIT_FETCH_BACKEND": os.getenv("REDDIT_FETCH_BACKEND"),
        "PUSHSHIFT_BASE_URL": os.getenv("PUSHSHIFT_BASE_URL"),
        "PUSHSHIFT_PAGE_SIZE": os.getenv("PUSHSHIFT_PAGE_SIZE"),
        "PUSHSHIFT_MAX_PAGES": os.getenv("PUSHSHIFT_MAX_PAGES"),
        "PUBLIC_REDDIT_BASE_URL": os.getenv("PUBLIC_REDDIT_BASE_URL"),
        "PUBLIC_REDDIT_USER_AGENT": os.getenv("PUBLIC_REDDIT_USER_AGENT"),
        "PUBLIC_REDDIT_PAGE_SIZE": os.getenv("PUBLIC_REDDIT_PAGE_SIZE"),
        "PUBLIC_REDDIT_MAX_PAGES": os.getenv("PUBLIC_REDDIT_MAX_PAGES"),
        "PUBLIC_REDDIT_DELAY_SECONDS": os.getenv("PUBLIC_REDDIT_DELAY_SECONDS"),
    }

    try:
        return Settings.model_validate(raw_values)
    except ValidationError as exc:
        raise RuntimeError(
            "Missing or invalid environment variables. Please check your .env file."
        ) from exc
