"""Rule-based tagging using taxonomy aliases."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from app.utils.text_cleaning import prepare_text_for_tagging

VALID_TAG_TYPES = ("product", "issue", "competitor", "feature")
TagMatch = dict[str, Any]


class TaggingService:
    """Extract product, issue, competitor and feature tags from text."""

    def __init__(self, taxonomy_path: str | Path = "app/config/taxonomy.yaml") -> None:
        self.taxonomy = self._load_taxonomy(Path(taxonomy_path))

    def extract_product_tags(self, text: str | None) -> list[str]:
        return self._extract_by_type(text, "product")

    def extract_issue_tags(self, text: str | None) -> list[str]:
        return self._extract_by_type(text, "issue")

    def extract_competitor_tags(self, text: str | None) -> list[str]:
        return self._extract_by_type(text, "competitor")

    def extract_feature_tags(self, text: str | None) -> list[str]:
        return self._extract_by_type(text, "feature")

    def extract_all_tags(self, text: str | None, confidence: float | None = 1.0) -> list[TagMatch]:
        """Return all tags as persistence-ready dictionaries."""
        tags: list[TagMatch] = []
        for tag_type in VALID_TAG_TYPES:
            extractor = getattr(self, f"extract_{tag_type}_tags")
            for value in extractor(text):
                tags.append(
                    {
                        "tag_type": tag_type,
                        "tag_value": value,
                        "tag_source": "rules",
                        "confidence": confidence,
                    }
                )
        return tags

    def _extract_by_type(self, text: str | None, singular_tag_type: str) -> list[str]:
        normalized_text = prepare_text_for_tagging(text)
        if not normalized_text:
            return []

        config_plural = f"{singular_tag_type}s"
        aliases_key = f"{singular_tag_type}_aliases"

        canonical_values = [value.strip().lower() for value in self.taxonomy.get(config_plural, [])]
        configured_aliases: dict[str, list[str]] = self.taxonomy.get(aliases_key, {})

        alias_entries: list[tuple[str, str]] = []
        for canonical in canonical_values:
            alias_entries.append((canonical, canonical))
            for alias in configured_aliases.get(canonical, []):
                alias_norm = alias.strip().lower()
                if alias_norm:
                    alias_entries.append((canonical, alias_norm))

        alias_entries.sort(key=lambda entry: len(entry[1]), reverse=True)

        reserved_spans: list[tuple[int, int]] = []
        seen_tags: set[str] = set()
        for canonical, alias in alias_entries:
            pattern = re.compile(rf"(?<!\w){re.escape(alias)}(?!\w)")
            for match in pattern.finditer(normalized_text):
                start, end = match.span()
                overlaps = any(not (end <= span_start or start >= span_end) for span_start, span_end in reserved_spans)
                if overlaps:
                    continue
                reserved_spans.append((start, end))
                seen_tags.add(canonical)
                break

        return sorted(seen_tags)

    def _load_taxonomy(self, taxonomy_path: Path) -> dict[str, Any]:
        """Load taxonomy from YAML with a small fallback parser for this project."""
        try:
            import yaml  # type: ignore

            loaded = yaml.safe_load(taxonomy_path.read_text(encoding="utf-8")) or {}
            return self._normalize_taxonomy(loaded)
        except ModuleNotFoundError:
            return self._normalize_taxonomy(self._parse_yaml_subset(taxonomy_path.read_text(encoding="utf-8")))

    def _normalize_taxonomy(self, raw: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for tag_type in VALID_TAG_TYPES:
            plural_key = f"{tag_type}s"
            aliases_key = f"{tag_type}_aliases"

            values = [str(item).strip().lower() for item in (raw.get(plural_key) or []) if str(item).strip()]
            normalized[plural_key] = values

            alias_map: dict[str, list[str]] = {}
            raw_aliases = raw.get(aliases_key) or {}
            for canonical, aliases in raw_aliases.items():
                canonical_key = str(canonical).strip().lower()
                if not canonical_key:
                    continue
                alias_map[canonical_key] = [
                    str(alias).strip().lower() for alias in (aliases or []) if str(alias).strip()
                ]
            normalized[aliases_key] = alias_map

        return normalized

    def _parse_yaml_subset(self, content: str) -> dict[str, Any]:
        """Parse a minimal YAML subset that matches taxonomy.yaml structure."""
        lines = content.splitlines()
        data: dict[str, Any] = {}
        index = 0
        while index < len(lines):
            line = lines[index]
            if not line.strip() or line.strip().startswith("#"):
                index += 1
                continue
            if line.startswith(" "):
                index += 1
                continue

            key, _, trailing = line.partition(":")
            key = key.strip()
            trailing = trailing.strip()
            if trailing == "{}":
                data[key] = {}
                index += 1
                continue

            index += 1
            block: list[str] = []
            while index < len(lines) and (lines[index].startswith("  ") or not lines[index].strip()):
                block.append(lines[index])
                index += 1

            block = [entry for entry in block if entry.strip()]
            if block and all(entry.strip().startswith("- ") for entry in block):
                data[key] = [entry.strip()[2:].strip() for entry in block]
                continue

            nested: dict[str, list[str]] = {}
            current_key: str | None = None
            for entry in block:
                stripped = entry.rstrip()
                if stripped.startswith("  ") and stripped.strip().endswith(":") and not stripped.startswith("    - "):
                    current_key = stripped.strip()[:-1]
                    nested[current_key] = []
                elif stripped.startswith("    - ") and current_key is not None:
                    nested[current_key].append(stripped.strip()[2:].strip())
            data[key] = nested

        return data
