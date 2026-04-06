from __future__ import annotations

from pathlib import Path

import pytest

from app.config import source_loader
from app.config.source_loader import (
    SourceConfigError,
    get_enabled_platform_configs,
    load_raw_platforms,
    load_source_config,
    write_runtime_platform_overrides,
)


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
    countries: ["US", "gb"]
    languages: ["en"]
    max_reviews_per_app: 250
    keywords: []
  web_reviews:
    enabled: true
    sites: ["example.com", "reviews.example.org"]
    max_pages_per_site: 75
    min_content_chars: 600
    crawl_paths: ["homepage"]
    prioritize_keywords: true
""".strip(),
        encoding="utf-8",
    )

    configs = load_source_config(config_path)

    assert len(configs) == 3
    reddit = next(config for config in configs if config.platform == "reddit")
    assert reddit.enabled is True
    assert reddit.days_back == 14
    assert reddit.config["subreddits"] == ["GalaxyWatch", "Android"]
    assert reddit.config["keywords"] == ["sleep"]
    google_play = next(config for config in configs if config.platform == "google_play")
    assert google_play.config["apps"] == ["com.example.app"]
    assert google_play.config["countries"] == ["us", "gb"]
    assert google_play.config["languages"] == ["en"]
    assert google_play.config["max_reviews_per_app"] == 250
    web_reviews = next(config for config in configs if config.platform == "web_reviews")
    assert web_reviews.enabled is True
    assert web_reviews.config["sites"] == ["example.com", "reviews.example.org"]
    assert web_reviews.config["max_pages_per_site"] == 75
    assert web_reviews.config["min_content_chars"] == 600
    assert web_reviews.config["crawl_paths"] == ["homepage"]
    assert web_reviews.config["prioritize_keywords"] is True


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
  web_reviews:
    enabled: true
    sites: ["example.com"]
    keywords: ["watch"]
""".strip(),
        encoding="utf-8",
    )

    enabled = get_enabled_platform_configs(config_path)

    assert [config.platform for config in enabled] == ["reddit", "web_reviews"]


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


def test_load_source_config_rejects_enabled_google_play_without_apps(tmp_path: Path) -> None:
    config_path = tmp_path / "source_config.yaml"
    config_path.write_text(
        """
platforms:
  google_play:
    enabled: true
    apps: []
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(SourceConfigError, match="requires non-empty 'apps'"):
        load_source_config(config_path)


def test_load_source_config_rejects_invalid_google_play_country_code(tmp_path: Path) -> None:
    config_path = tmp_path / "source_config.yaml"
    config_path.write_text(
        """
platforms:
  google_play:
    enabled: true
    apps: ["com.example.app"]
    countries: ["usa", "1n"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(SourceConfigError, match="countries"):
        load_source_config(config_path)


def test_load_source_config_rejects_invalid_google_play_package_id(tmp_path: Path) -> None:
    config_path = tmp_path / "source_config.yaml"
    config_path.write_text(
        """
platforms:
  google_play:
    enabled: true
    apps: ["bad package"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(SourceConfigError, match="invalid package IDs"):
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
  web_reviews:
    enabled: true
    sites: ["base.example"]
    keywords: ["base"]
""".strip(),
        encoding="utf-8",
    )
    runtime_path.write_text(
        """
platforms:
  reddit:
    communities: ["RuntimeOnly"]
    days_back: 14
  web_reviews:
    sites: ["runtime.example"]
    max_pages_per_site: 60
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(source_loader, "BASE_SOURCE_CONFIG_PATH", base_path)
    monkeypatch.setattr(source_loader, "RUNTIME_SOURCE_CONFIG_PATH", runtime_path)

    configs = load_source_config()
    reddit = next(config for config in configs if config.platform == "reddit")
    google_play = next(config for config in configs if config.platform == "google_play")
    web_reviews = next(config for config in configs if config.platform == "web_reviews")

    assert reddit.enabled is True
    assert reddit.days_back == 14
    assert reddit.config["subreddits"] == ["RuntimeOnly"]
    assert reddit.config["keywords"] == ["base_keyword"]
    assert google_play.config["apps"] == ["com.base.app"]
    assert web_reviews.config["sites"] == ["runtime.example"]
    assert web_reviews.config["keywords"] == ["base"]
    assert web_reviews.config["max_pages_per_site"] == 60
    assert web_reviews.config["min_content_chars"] == 500
    assert web_reviews.config["crawl_paths"] == ["homepage", "category"]
    assert web_reviews.config["prioritize_keywords"] is False


def test_load_source_config_supports_block_style_lists(tmp_path: Path) -> None:
    config_path = tmp_path / "source_config.yaml"
    config_path.write_text(
        """
platforms:
  reddit:
    enabled: true
    communities:
      - GalaxyWatch
      - Android
    keywords:
      - sleep
      - workout
""".strip(),
        encoding="utf-8",
    )

    configs = load_source_config(config_path)
    reddit = next(config for config in configs if config.platform == "reddit")

    assert reddit.config["subreddits"] == ["GalaxyWatch", "Android"]
    assert reddit.config["keywords"] == ["sleep", "workout"]


def test_load_source_config_handles_runtime_with_missing_optional_sections(
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
    keywords: ["base_keyword"]
""".strip(),
        encoding="utf-8",
    )
    runtime_path.write_text(
        """
platforms:
  reddit:
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(source_loader, "BASE_SOURCE_CONFIG_PATH", base_path)
    monkeypatch.setattr(source_loader, "RUNTIME_SOURCE_CONFIG_PATH", runtime_path)

    configs = load_source_config()
    reddit = next(config for config in configs if config.platform == "reddit")

    assert reddit.config["subreddits"] == ["BaseOnly"]
    assert reddit.config["keywords"] == ["base_keyword"]


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



def test_load_source_config_parses_mixed_inline_and_block_lists(tmp_path: Path) -> None:
    config_path = tmp_path / "source_config.yaml"
    config_path.write_text(
        """
platforms:
  reddit:
    enabled: true
    communities: ["GalaxyWatch", "Android"]
    keywords:
      - sleep
      - workout
  web_reviews:
    enabled: true
    sites:
      - trustpilot.com
      - g2.com
    keywords: ["battery", "health"]
    max_pages_per_site: 25
""".strip(),
        encoding="utf-8",
    )

    configs = load_source_config(config_path)
    reddit = next(config for config in configs if config.platform == "reddit")
    web_reviews = next(config for config in configs if config.platform == "web_reviews")

    assert reddit.config["subreddits"] == ["GalaxyWatch", "Android"]
    assert reddit.config["keywords"] == ["sleep", "workout"]
    assert web_reviews.config["sites"] == ["trustpilot.com", "g2.com"]
    assert web_reviews.config["keywords"] == ["battery", "health"]


def test_load_source_config_reports_path_and_line_for_invalid_yaml(tmp_path: Path) -> None:
    config_path = tmp_path / "source_config.yaml"
    config_path.write_text(
        """
platforms:
  reddit:
    enabled true
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(SourceConfigError, match=r"source_config.yaml: line 3"):
        load_source_config(config_path)


def test_load_source_config_ignores_invalid_runtime_override_and_uses_base(
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
    keywords: ["base_keyword"]
""".strip(),
        encoding="utf-8",
    )
    runtime_path.write_text(
        """
platforms:
  reddit:
    communities: ["Broken"
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setattr(source_loader, "BASE_SOURCE_CONFIG_PATH", base_path)
    monkeypatch.setattr(source_loader, "RUNTIME_SOURCE_CONFIG_PATH", runtime_path)

    with caplog.at_level("WARNING"):
        configs = load_source_config()

    reddit = next(config for config in configs if config.platform == "reddit")
    assert reddit.config["subreddits"] == ["BaseOnly"]
    assert reddit.config["keywords"] == ["base_keyword"]
    assert "Ignoring runtime source override" in caplog.text


def test_runtime_overrides_write_and_read_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime_path = tmp_path / "runtime_source_config.yaml"
    monkeypatch.setattr(source_loader, "RUNTIME_SOURCE_CONFIG_PATH", runtime_path)

    overrides = {
        "google_play": {
            "enabled": True,
            "days_back": 7,
            "apps": ["com.test.app"],
            "countries": ["us"],
            "languages": ["en"],
            "max_reviews_per_app": 250,
            "keywords": ["battery"],
        },
        "reddit": {
            "enabled": False,
            "days_back": 14,
            "communities": ["Android"],
            "keywords": ["sleep"],
        },
    }

    write_runtime_platform_overrides(overrides)
    loaded = load_raw_platforms(runtime_path)

    assert loaded["reddit"] == {
        "communities": ["Android"],
        "days_back": 14,
        "enabled": False,
        "keywords": ["sleep"],
    }
    assert loaded["google_play"] == {
        "apps": ["com.test.app"],
        "countries": ["us"],
        "days_back": 7,
        "enabled": True,
        "keywords": ["battery"],
        "languages": ["en"],
        "max_reviews_per_app": 250,
    }
