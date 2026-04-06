"""Load and validate source platform configuration."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import re
from typing import Any

from app.config.settings import get_ingestion_settings


BASE_SOURCE_CONFIG_PATH = Path(__file__).resolve().parent / "source_config.yaml"
RUNTIME_SOURCE_CONFIG_PATH = Path(get_ingestion_settings().runtime_source_config_path)
logger = logging.getLogger(__name__)
COUNTRY_CODE_PATTERN = re.compile(r"^[a-z]{2}$")
GOOGLE_PLAY_PACKAGE_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z0-9_]+)+$")


@dataclass(frozen=True)
class PlatformSourceConfig:
    """Normalized platform ingestion configuration."""

    platform: str
    enabled: bool
    days_back: int
    config: dict[str, Any]


class SourceConfigError(ValueError):
    """Raised when source configuration shape is invalid."""


def _strip_inline_comment(value: str) -> str:
    in_quote: str | None = None
    escaped = False
    result_chars: list[str] = []
    for char in value:
        if escaped:
            result_chars.append(char)
            escaped = False
            continue
        if char == "\\":
            escaped = True
            result_chars.append(char)
            continue
        if in_quote:
            if char == in_quote:
                in_quote = None
            result_chars.append(char)
            continue
        if char in {"'", '"'}:
            in_quote = char
            result_chars.append(char)
            continue
        if char == "#":
            break
        result_chars.append(char)
    return "".join(result_chars).rstrip()


def _split_inline_list_items(inner: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    in_quote: str | None = None
    escaped = False
    for char in inner:
        if escaped:
            current.append(char)
            escaped = False
            continue
        if char == "\\":
            escaped = True
            current.append(char)
            continue
        if in_quote:
            if char == in_quote:
                in_quote = None
            current.append(char)
            continue
        if char in {"'", '"'}:
            in_quote = char
            current.append(char)
            continue
        if char == ",":
            token = "".join(current).strip()
            if token:
                items.append(token)
            current = []
            continue
        current.append(char)

    token = "".join(current).strip()
    if token:
        items.append(token)
    return items


def _parse_scalar(value: str, *, line_no: int) -> Any:
    normalized = _strip_inline_comment(value).strip()
    if normalized == "":
        return ""

    lowered = normalized.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"

    if re.fullmatch(r"-?\d+", normalized):
        return int(normalized)

    if normalized.startswith("["):
        if not normalized.endswith("]"):
            raise SourceConfigError(f"line {line_no}: inline list is missing closing ']'")
        inner = normalized[1:-1].strip()
        if not inner:
            return []
        parsed_items: list[Any] = []
        for token in _split_inline_list_items(inner):
            parsed_items.append(_parse_scalar(token, line_no=line_no))
        return parsed_items

    if (normalized.startswith('"') and normalized.endswith('"')) or (
        normalized.startswith("'") and normalized.endswith("'")
    ):
        return normalized[1:-1]

    return normalized


def _parse_source_yaml(text: str, *, source_path: Path | None = None) -> dict[str, dict[str, Any]]:
    path_label = str(source_path) if source_path else "<config>"
    root: dict[str, Any] = {}
    current_platform: str | None = None
    current_list_key: str | None = None
    current_list_indent: int | None = None

    lines = text.splitlines()
    for index, raw_line in enumerate(lines):
        line_no = index + 1
        line_without_comments = _strip_inline_comment(raw_line)
        if not line_without_comments.strip():
            continue

        indent = len(line_without_comments) - len(line_without_comments.lstrip(" "))
        line = line_without_comments.strip()

        if indent == 0:
            current_platform = None
            current_list_key = None
            current_list_indent = None
            if line != "platforms:":
                raise SourceConfigError(f"{path_label}: line {line_no}: expected top-level 'platforms:' mapping")
            if "platforms" in root and isinstance(root["platforms"], dict):
                raise SourceConfigError(f"{path_label}: line {line_no}: duplicate top-level 'platforms' key")
            root["platforms"] = {}
            continue

        platforms = root.get("platforms")
        if not isinstance(platforms, dict):
            raise SourceConfigError(f"{path_label}: line {line_no}: expected top-level 'platforms:' block")

        if indent == 2 and line.endswith(":"):
            platform_name = line[:-1].strip().lower()
            if not platform_name:
                raise SourceConfigError(f"{path_label}: line {line_no}: platform key cannot be empty")
            current_platform = platform_name
            current_list_key = None
            current_list_indent = None
            platforms[platform_name] = {}
            continue

        if current_platform is None:
            raise SourceConfigError(
                f"{path_label}: line {line_no}: unsupported YAML structure before platform declaration"
            )

        platform_values = platforms.get(current_platform)
        if not isinstance(platform_values, dict):
            raise SourceConfigError(f"{path_label}: line {line_no}: platform '{current_platform}' must be a mapping")

        if current_list_key is not None:
            if current_list_indent is None:
                raise SourceConfigError(
                    f"{path_label}: line {line_no}: internal parser state invalid for list '{current_list_key}'"
                )
            if indent > current_list_indent and line.startswith("- "):
                platform_values[current_list_key].append(_parse_scalar(line[2:], line_no=line_no))
                continue
            current_list_key = None
            current_list_indent = None

        if indent == 4:
            if ":" not in line:
                raise SourceConfigError(
                    f"{path_label}: line {line_no}: unsupported YAML structure for platform '{current_platform}'"
                )
            key, raw_value = line.split(":", 1)
            normalized_key = key.strip()
            if not normalized_key:
                raise SourceConfigError(
                    f"{path_label}: line {line_no}: platform '{current_platform}' field name cannot be empty"
                )

            parsed = _parse_scalar(raw_value, line_no=line_no)
            if parsed == "" and _strip_inline_comment(raw_value).strip() == "":
                platform_values[normalized_key] = []
                current_list_key = normalized_key
                current_list_indent = indent
                continue
            platform_values[normalized_key] = parsed
            continue

        raise SourceConfigError(
            f"{path_label}: line {line_no}: unsupported YAML structure for platform '{current_platform}'. "
            "Use inline lists ([\"a\", \"b\"]) or block lists with '-' items."
        )

    if "platforms" not in root:
        raise SourceConfigError(f"{path_label}: missing required top-level 'platforms' mapping")

    platforms = root["platforms"]
    if not isinstance(platforms, dict):
        raise SourceConfigError(f"{path_label}: top-level 'platforms' value must be a mapping")

    for platform, raw_platform in platforms.items():
        if not isinstance(raw_platform, dict):
            raise SourceConfigError(f"{path_label}: platform '{platform}' config must be a mapping")

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
        apps = _normalize_string_list(raw.get("apps", []), field_name="apps", platform=platform)
        if enabled and not apps:
            raise SourceConfigError("platform 'google_play' requires non-empty 'apps' when enabled")
        invalid_apps = [app for app in apps if not GOOGLE_PLAY_PACKAGE_PATTERN.fullmatch(app)]
        if invalid_apps:
            raise SourceConfigError(
                "platform 'google_play' field 'apps' contains invalid package IDs: "
                + ", ".join(sorted(invalid_apps))
            )

        countries_raw = _normalize_string_list(raw.get("countries", []), field_name="countries", platform=platform)
        countries = [country.lower() for country in countries_raw]
        invalid_countries = [country for country in countries if not COUNTRY_CODE_PATTERN.fullmatch(country)]
        if invalid_countries:
            raise SourceConfigError(
                "platform 'google_play' field 'countries' must contain ISO-3166-1 alpha-2 codes: "
                + ", ".join(sorted(invalid_countries))
            )

        languages = _normalize_string_list(raw.get("languages", []), field_name="languages", platform=platform)

        max_reviews_per_app = raw.get("max_reviews_per_app", 1000)
        if not isinstance(max_reviews_per_app, int) or max_reviews_per_app <= 0:
            raise SourceConfigError(
                "platform 'google_play' field 'max_reviews_per_app' must be a positive integer"
            )

        normalized["apps"] = apps
        normalized["countries"] = countries
        normalized["languages"] = languages
        normalized["max_reviews_per_app"] = max_reviews_per_app
    elif platform == "web_reviews":
        sites = _normalize_string_list(raw.get("sites", []), field_name="sites", platform=platform)
        if enabled and not sites:
            raise SourceConfigError("platform 'web_reviews' requires non-empty 'sites' when enabled")

        max_pages_per_site = raw.get("max_pages_per_site", 50)
        if not isinstance(max_pages_per_site, int) or max_pages_per_site <= 0:
            raise SourceConfigError(
                "platform 'web_reviews' field 'max_pages_per_site' must be a positive integer"
            )

        min_content_chars = raw.get("min_content_chars", 500)
        if not isinstance(min_content_chars, int) or min_content_chars <= 0:
            raise SourceConfigError(
                "platform 'web_reviews' field 'min_content_chars' must be a positive integer"
            )

        crawl_paths = _normalize_string_list(
            raw.get("crawl_paths", ["homepage", "category"]),
            field_name="crawl_paths",
            platform=platform,
        )
        if not crawl_paths:
            raise SourceConfigError("platform 'web_reviews' field 'crawl_paths' cannot be empty")

        prioritize_keywords = raw.get("prioritize_keywords", False)
        if not isinstance(prioritize_keywords, bool):
            raise SourceConfigError("platform 'web_reviews' field 'prioritize_keywords' must be a boolean")

        normalized["sites"] = sites
        normalized["max_pages_per_site"] = max_pages_per_site
        normalized["min_content_chars"] = min_content_chars
        normalized["crawl_paths"] = crawl_paths
        normalized["prioritize_keywords"] = prioritize_keywords
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
    text = config_path.read_text(encoding="utf-8")
    if not text.strip():
        return {}
    return _parse_source_yaml(text, source_path=config_path)


def _get_merged_platforms() -> dict[str, dict[str, Any]]:
    base_platforms = _load_raw_platforms(BASE_SOURCE_CONFIG_PATH)

    runtime_platforms: dict[str, dict[str, Any]] = {}
    try:
        runtime_platforms = _load_raw_platforms(RUNTIME_SOURCE_CONFIG_PATH)
    except SourceConfigError as exc:
        logger.warning(
            "Ignoring runtime source override '%s' because it is invalid: %s",
            RUNTIME_SOURCE_CONFIG_PATH,
            exc,
        )

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
