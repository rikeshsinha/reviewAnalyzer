"""Load and validate source platform configuration."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Any


BASE_SOURCE_CONFIG_PATH = Path(__file__).resolve().parent / "source_config.yaml"
RUNTIME_SOURCE_CONFIG_PATH = Path("data/runtime_source_config.yaml")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PlatformSourceConfig:
    """Normalized platform ingestion configuration."""

    platform: str
    enabled: bool
    days_back: int
    config: dict[str, Any]


class SourceConfigError(ValueError):
    """Raised when source configuration shape is invalid."""


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if value.isdigit():
        return int(value)
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        items = []
        for raw_item in inner.split(","):
            item = raw_item.strip().strip('"').strip("'")
            if item:
                items.append(item)
        return items
    return value.strip('"').strip("'")


def _parse_source_yaml(text: str) -> dict[str, dict[str, Any]]:
    platforms: dict[str, dict[str, Any]] = {}
    in_platforms_block = False
    current_platform: str | None = None

    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        if not raw_line.strip() or raw_line.strip().startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()

        if indent == 0 and line == "platforms:":
            in_platforms_block = True
            current_platform = None
            continue

        if not in_platforms_block:
            raise SourceConfigError(f"line {line_no}: expected top-level 'platforms:' block")

        if indent == 2 and line.endswith(":"):
            current_platform = line[:-1].strip().lower()
            if not current_platform:
                raise SourceConfigError(f"line {line_no}: platform key cannot be empty")
            platforms[current_platform] = {}
            continue

        if indent == 4 and current_platform and ":" in line:
            key, raw_value = line.split(":", 1)
            platforms[current_platform][key.strip()] = _parse_scalar(raw_value)
            continue

        raise SourceConfigError(f"line {line_no}: unsupported YAML structure")

    return platforms


def _normalize_string_list(value: Any, *, field_name: str, platform: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise SourceConfigError(
            f"platform '{platform}' field '{field_name}' must be a list of strings"
        )
    return [item.strip() for item in value if item.strip()]


def _normalize_platform_config(platform: str, raw: dict[str, Any]) -> PlatformSourceConfig:
    if not isinstance(raw, dict):
        raise SourceConfigError(f"platform '{platform}' config must be a mapping")

    enabled = bool(raw.get("enabled", False))

    days_back_raw = raw.get("days_back", 30)
    if not isinstance(days_back_raw, int) or days_back_raw < 0:
        raise SourceConfigError(f"platform '{platform}' field 'days_back' must be a non-negative integer")

    normalized: dict[str, Any] = {
        "keywords": _normalize_string_list(raw.get("keywords", []), field_name="keywords", platform=platform),
    }

    if platform == "reddit":
        communities = _normalize_string_list(
            raw.get("communities", []), field_name="communities", platform=platform
        )
        if enabled and not communities:
            raise SourceConfigError("platform 'reddit' requires non-empty 'communities' when enabled")
        normalized["subreddits"] = communities
    elif platform == "google_play":
        normalized["apps"] = _normalize_string_list(raw.get("apps", []), field_name="apps", platform=platform)
    else:
        for key, value in raw.items():
            if key in {"enabled", "days_back", "keywords"}:
                continue
            normalized[key] = value

    return PlatformSourceConfig(
        platform=platform,
        enabled=enabled,
        days_back=days_back_raw,
        config=normalized,
    )


def _merge_platform_values(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, list):
            merged[key] = list(value)
            continue
        merged[key] = value
    return merged


def _load_raw_platforms(config_path: Path) -> dict[str, dict[str, Any]]:
    if not config_path.exists():
        return {}
    return _parse_source_yaml(config_path.read_text(encoding="utf-8"))


def _get_merged_platforms() -> dict[str, dict[str, Any]]:
    base_platforms = _load_raw_platforms(BASE_SOURCE_CONFIG_PATH)
    runtime_platforms = _load_raw_platforms(RUNTIME_SOURCE_CONFIG_PATH)

    merged_platforms = dict(base_platforms)
    for runtime_platform, runtime_values in runtime_platforms.items():
        if runtime_platform not in base_platforms:
            logger.warning("Ignoring unknown runtime platform override '%s'", runtime_platform)
            continue
        merged_platforms[runtime_platform] = _merge_platform_values(
            base=base_platforms[runtime_platform],
            override=runtime_values,
        )
    return merged_platforms


def load_source_config(config_path: Path | None = None) -> list[PlatformSourceConfig]:
    """Load source config YAML and return normalized platform entries."""

    if config_path is not None:
        platforms = _load_raw_platforms(config_path)
    else:
        platforms = _get_merged_platforms()
    return [_normalize_platform_config(name, raw) for name, raw in platforms.items()]


def get_enabled_platform_configs(config_path: Path | None = None) -> list[PlatformSourceConfig]:
    """Return only enabled platform configs."""

    return [config for config in load_source_config(config_path=config_path) if config.enabled]


def get_default_source_config_path() -> Path:
    """Return runtime source config path when present, else bundled defaults.

    Deprecated for ingestion reads: use merged load_source_config() flow.
    """

    if RUNTIME_SOURCE_CONFIG_PATH.exists():
        return RUNTIME_SOURCE_CONFIG_PATH
    return BASE_SOURCE_CONFIG_PATH
