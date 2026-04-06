from __future__ import annotations

from pathlib import Path

import pytest

from app.config import source_loader
from app.ui.pages import admin


def test_build_refresh_sources_env_passes_selected_platform() -> None:
    env = admin._build_refresh_sources_env(" google_play ")  # noqa: SLF001

    assert env == {"INGESTION_PLATFORMS": "google_play"}


def test_save_selected_platform_override_updates_only_selected_platform(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime_path = tmp_path / "runtime_source_config.yaml"
    runtime_path.write_text(
        """
platforms:
  reddit:
    enabled: true
    communities: ["android"]
    keywords: ["sleep"]
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(admin, "RUNTIME_SOURCE_CONFIG_PATH", runtime_path)
    monkeypatch.setattr(source_loader, "RUNTIME_SOURCE_CONFIG_PATH", runtime_path)

    admin._save_selected_platform_override(  # noqa: SLF001
        "google_play",
        {
            "enabled": True,
            "days_back": 7,
            "apps": ["com.test.app"],
            "countries": ["us"],
            "languages": ["en"],
            "max_reviews_per_app": 300,
            "keywords": ["battery"],
        },
    )

    text = runtime_path.read_text(encoding="utf-8")
    assert "reddit:" in text
    assert "google_play:" in text
    assert "max_reviews_per_app: 300" in text


def test_validate_platform_override_disabled_platform_message() -> None:
    message = admin._validate_platform_override(  # noqa: SLF001
        "reddit",
        {"enabled": True, "days_back": 30, "communities": [], "keywords": ["x"]},
    )

    assert "At least one subreddit" in (message or "")
