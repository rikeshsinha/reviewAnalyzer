"""Job entry point for running ingestion refreshes across enabled platforms."""

from __future__ import annotations

import logging

from app.config.source_loader import get_enabled_platform_configs
from app.jobs.refresh_reddit import run_for_platform
from app.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


def run() -> None:
    """Run ingestion for each enabled platform from source config."""

    setup_logging()
    enabled_configs = get_enabled_platform_configs()
    if not enabled_configs:
        raise RuntimeError("No enabled platforms found in source_config.yaml")

    for platform_config in enabled_configs:
        logger.info("Starting refresh for platform %s", platform_config.platform)
        run_for_platform(
            platform=platform_config.platform,
            config=platform_config.config,
            days_back=platform_config.days_back,
        )


if __name__ == "__main__":
    run()
