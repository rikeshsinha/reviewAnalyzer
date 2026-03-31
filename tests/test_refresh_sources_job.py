from __future__ import annotations

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
        "run_for_platform",
        lambda platform, config, days_back: calls.append((platform, config, days_back)),
    )

    refresh_sources.run()

    assert calls == [
        ("reddit", {"subreddits": ["android"], "keywords": ["watch"]}, 30),
        ("google_play", {"apps": ["com.test.app"], "keywords": ["battery"]}, 7),
    ]
