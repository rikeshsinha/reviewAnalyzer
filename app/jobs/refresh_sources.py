"""Job entry point for running ingestion refreshes across enabled platforms."""

from __future__ import annotations

import logging
import os

from app.config.settings import get_ingestion_settings
from app.config.source_loader import PlatformSourceConfig, get_enabled_platform_configs
from app.jobs.refresh_reddit import run_for_platform
from app.jobs.refresh_web_reviews import run_for_web_reviews
from app.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


def _should_fail_fast() -> bool:
    """Return whether the refresh should stop after the first platform failure."""

    raw_value = (os.getenv("INGESTION_FAIL_FAST") or "false").strip().lower()
    return raw_value in {"1", "true", "yes", "on"}


def _run_platform_refresh(platform: str, config: dict[str, object], days_back: int) -> None:
    if platform == "web_reviews":
        run_for_web_reviews(config, days_back=days_back)
        return
    run_for_platform(platform=platform, config=config, days_back=days_back)


def _get_selected_platforms() -> list[str]:
    raw_value = (os.getenv("INGESTION_PLATFORMS") or "").strip()
    if not raw_value:
        return []
    selected: list[str] = []
    seen: set[str] = set()
    for chunk in raw_value.split(","):
        platform = chunk.strip().lower()
        if not platform or platform in seen:
            continue
        seen.add(platform)
        selected.append(platform)
    return selected


def _filter_selected_platforms(
    enabled_configs: list[PlatformSourceConfig],
    selected_platforms: list[str],
) -> list[PlatformSourceConfig]:
    if not selected_platforms:
        return enabled_configs

    enabled_by_name = {cfg.platform: cfg for cfg in enabled_configs}
    missing = [name for name in selected_platforms if name not in enabled_by_name]
    if missing:
        available = ", ".join(sorted(enabled_by_name)) or "none"
        requested = ", ".join(missing)
        raise RuntimeError(
            "Selected platform(s) are not enabled or not defined: "
            + requested
            + ". Enabled platforms: "
            + available
            + ". Update source config or adjust INGESTION_PLATFORMS."
        )

    return [enabled_by_name[name] for name in selected_platforms]


def run() -> None:
    """Run ingestion for each enabled platform from source config."""

    setup_logging()
    get_ingestion_settings()
    enabled_configs = get_enabled_platform_configs()
    if not enabled_configs:
        raise RuntimeError("No enabled platforms found in merged source configuration")
    selected_platforms = _get_selected_platforms()
    if selected_platforms:
        enabled_configs = _filter_selected_platforms(enabled_configs, selected_platforms)

    fail_fast = _should_fail_fast()
    failures: list[str] = []

    for platform_config in enabled_configs:
        logger.info("Starting refresh for platform %s", platform_config.platform)
        try:
            _run_platform_refresh(
                platform=platform_config.platform,
                config=platform_config.config,
                days_back=platform_config.days_back,
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{platform_config.platform}: {exc}")
            logger.exception("Refresh failed for platform %s", platform_config.platform)
            if fail_fast:
                raise RuntimeError(
                    "Refresh stopped after platform failure because INGESTION_FAIL_FAST is enabled"
                ) from exc

    if failures:
        logger.warning(
            "Refresh completed with platform failures: %s",
            "; ".join(failures),
        )


if __name__ == "__main__":
    run()
