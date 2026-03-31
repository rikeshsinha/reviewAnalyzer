from __future__ import annotations

from pathlib import Path

import pytest

from app.config.source_loader import SourceConfigError, get_enabled_platform_configs, load_source_config


def test_load_source_config_normalizes_platforms(tmp_path: Path) -> None:
    config_path = tmp_path / "source_config.yaml"
    config_path.write_text(
        """
platforms:
  reddit:
    enabled: true
    communities: ["GalaxyWatch", " ", "Android"]
    keywords: ["sleep", ""]
    days_back: 14
  google_play:
    enabled: false
    apps: ["com.example.app"]
    keywords: []
""".strip(),
        encoding="utf-8",
    )

    configs = load_source_config(config_path)

    assert len(configs) == 2
    reddit = next(config for config in configs if config.platform == "reddit")
    assert reddit.enabled is True
    assert reddit.days_back == 14
    assert reddit.config["subreddits"] == ["GalaxyWatch", "Android"]
    assert reddit.config["keywords"] == ["sleep"]


def test_get_enabled_platform_configs_filters_disabled(tmp_path: Path) -> None:
    config_path = tmp_path / "source_config.yaml"
    config_path.write_text(
        """
platforms:
  reddit:
    enabled: true
    communities: ["Android"]
    keywords: []
  google_play:
    enabled: false
    apps: []
    keywords: []
""".strip(),
        encoding="utf-8",
    )

    enabled = get_enabled_platform_configs(config_path)

    assert [config.platform for config in enabled] == ["reddit"]


def test_load_source_config_rejects_enabled_reddit_without_communities(tmp_path: Path) -> None:
    config_path = tmp_path / "source_config.yaml"
    config_path.write_text(
        """
platforms:
  reddit:
    enabled: true
    communities: []
    keywords: []
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(SourceConfigError):
        load_source_config(config_path)
