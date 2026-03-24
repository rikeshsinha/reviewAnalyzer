"""Application settings loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
import os

from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError


class Settings(BaseModel):
    """Typed application settings."""

    openai_api_key: str = Field(..., alias="OPENAI_API_KEY", min_length=1)
    reddit_client_id: str = Field(..., alias="REDDIT_CLIENT_ID", min_length=1)
    reddit_client_secret: str = Field(..., alias="REDDIT_CLIENT_SECRET", min_length=1)
    reddit_user_agent: str = Field(..., alias="REDDIT_USER_AGENT", min_length=1)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load and validate settings from environment variables."""

    load_dotenv()

    raw_values = {
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "REDDIT_CLIENT_ID": os.getenv("REDDIT_CLIENT_ID"),
        "REDDIT_CLIENT_SECRET": os.getenv("REDDIT_CLIENT_SECRET"),
        "REDDIT_USER_AGENT": os.getenv("REDDIT_USER_AGENT"),
    }

    try:
        return Settings.model_validate(raw_values)
    except ValidationError as exc:
        raise RuntimeError(
            "Missing or invalid environment variables. Please check your .env file."
        ) from exc
