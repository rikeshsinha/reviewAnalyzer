from __future__ import annotations

import pytest

from app.config.source_loader import PlatformSourceConfig
from app.jobs import refresh_sources


def test_refresh_sources_runs_enabled_platforms(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object], int]] = []

    monkeypatch.setattr(
        refresh_sources,
        "get_enabled_platform_configs",
        lambda: [
            PlatformSourceConfig(
                platform="reddit",
                enabled=True,
                days_back=30,
                config={"subreddits": ["android"], "keywords": ["watch"]},
            ),
            PlatformSourceConfig(
                platform="google_play",
                enabled=True,
                days_back=7,
                config={"apps": ["com.test.app"], "keywords": ["battery"]},
            ),
        ],
    )

    monkeypatch.setattr(
        refresh_sources,
        "_run_platform_refresh",
        lambda platform, config, days_back: calls.append((platform, config, days_back)),
    )

    refresh_sources.run()

    assert calls == [
        ("reddit", {"subreddits": ["android"], "keywords": ["watch"]}, 30),
        ("google_play", {"apps": ["com.test.app"], "keywords": ["battery"]}, 7),
    ]


def test_refresh_sources_continues_after_platform_failure(monkeypatch, caplog) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        refresh_sources,
        "get_enabled_platform_configs",
        lambda: [
            PlatformSourceConfig(platform="reddit", enabled=True, days_back=30, config={"subreddits": ["a"]}),
            PlatformSourceConfig(platform="google_play", enabled=True, days_back=7, config={"apps": ["com.app"]}),
        ],
    )

    def _run_platform_refresh(platform: str, config: dict[str, object], days_back: int) -> None:
        del config, days_back
        calls.append(platform)
        if platform == "reddit":
            raise RuntimeError("reddit error")

    monkeypatch.setattr(refresh_sources, "_run_platform_refresh", _run_platform_refresh)
    monkeypatch.delenv("INGESTION_FAIL_FAST", raising=False)

    refresh_sources.run()

    assert calls == ["reddit", "google_play"]
    assert "platform failures" in caplog.text


def test_refresh_sources_fail_fast(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        refresh_sources,
        "get_enabled_platform_configs",
        lambda: [
            PlatformSourceConfig(platform="reddit", enabled=True, days_back=30, config={"subreddits": ["a"]}),
            PlatformSourceConfig(platform="google_play", enabled=True, days_back=7, config={"apps": ["com.app"]}),
        ],
    )

    def _run_platform_refresh(platform: str, config: dict[str, object], days_back: int) -> None:
        del config, days_back
        calls.append(platform)
        raise RuntimeError("boom")

    monkeypatch.setattr(refresh_sources, "_run_platform_refresh", _run_platform_refresh)
    monkeypatch.setenv("INGESTION_FAIL_FAST", "true")

    with pytest.raises(RuntimeError, match="INGESTION_FAIL_FAST"):
        refresh_sources.run()

    assert calls == ["reddit"]


def test_run_platform_refresh_routes_web_reviews_to_dedicated_job(monkeypatch) -> None:
    web_calls: list[tuple[dict[str, object], int]] = []

    monkeypatch.setattr(
        refresh_sources,
        "run_for_web_reviews",
        lambda config, days_back: web_calls.append((config, days_back)),
    )
    monkeypatch.setattr(
        refresh_sources,
        "run_for_platform",
        lambda platform, config, days_back: (_ for _ in ()).throw(RuntimeError(platform)),
    )

    refresh_sources._run_platform_refresh("web_reviews", {"sites": ["example.com"]}, 14)

    assert web_calls == [({"sites": ["example.com"]}, 14)]
