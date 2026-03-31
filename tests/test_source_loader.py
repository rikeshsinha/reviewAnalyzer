from __future__ import annotations

from pathlib import Path

import pytest

from app.config import source_loader
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


def test_load_source_config_uses_runtime_file_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base_path = tmp_path / "source_config.yaml"
    runtime_path = tmp_path / "runtime_source_config.yaml"

    base_path.write_text(
        """
platforms:
  reddit:
    enabled: true
    communities: ["BaseOnly"]
    keywords: []
""".strip(),
        encoding="utf-8",
    )
    runtime_path.write_text(
        """
platforms:
  reddit:
    enabled: true
    communities: ["RuntimeOnly"]
    keywords: []
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(source_loader, "BASE_SOURCE_CONFIG_PATH", base_path)
    monkeypatch.setattr(source_loader, "RUNTIME_SOURCE_CONFIG_PATH", runtime_path)

    configs = load_source_config()

    assert configs[0].config["subreddits"] == ["RuntimeOnly"]


def test_load_source_config_merges_base_and_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base_path = tmp_path / "source_config.yaml"
    runtime_path = tmp_path / "runtime_source_config.yaml"

    base_path.write_text(
        """
platforms:
  reddit:
    enabled: true
    communities: ["BaseOnly"]
    keywords: ["base_keyword"]
    days_back: 30
  google_play:
    enabled: false
    apps: ["com.base.app"]
    keywords: []
""".strip(),
        encoding="utf-8",
    )
    runtime_path.write_text(
        """
platforms:
  reddit:
    communities: ["RuntimeOnly"]
    days_back: 14
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(source_loader, "BASE_SOURCE_CONFIG_PATH", base_path)
    monkeypatch.setattr(source_loader, "RUNTIME_SOURCE_CONFIG_PATH", runtime_path)

    configs = load_source_config()
    reddit = next(config for config in configs if config.platform == "reddit")
    google_play = next(config for config in configs if config.platform == "google_play")

    assert reddit.enabled is True
    assert reddit.days_back == 14
    assert reddit.config["subreddits"] == ["RuntimeOnly"]
    assert reddit.config["keywords"] == ["base_keyword"]
    assert google_play.config["apps"] == ["com.base.app"]


def test_load_source_config_ignores_unknown_runtime_platforms(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    base_path = tmp_path / "source_config.yaml"
    runtime_path = tmp_path / "runtime_source_config.yaml"

    base_path.write_text(
        """
platforms:
  reddit:
    enabled: true
    communities: ["BaseOnly"]
    keywords: []
""".strip(),
        encoding="utf-8",
    )
    runtime_path.write_text(
        """
platforms:
  hacker_news:
    enabled: true
    communities: ["news"]
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(source_loader, "BASE_SOURCE_CONFIG_PATH", base_path)
    monkeypatch.setattr(source_loader, "RUNTIME_SOURCE_CONFIG_PATH", runtime_path)

    with caplog.at_level("WARNING"):
        configs = load_source_config()

    assert [config.platform for config in configs] == ["reddit"]
    assert "Ignoring unknown runtime platform override 'hacker_news'" in caplog.text
