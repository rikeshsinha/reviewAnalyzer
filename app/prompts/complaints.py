"""Prompt fragments for complaint / primary issue labeling."""

from __future__ import annotations

PRIMARY_ISSUE_CATEGORIES = [
    "bug",
    "performance",
    "ux",
    "pricing",
    "support",
    "integration",
    "account",
    "reliability",
    "other",
]

COMPLAINTS_INSTRUCTIONS = """
Set primary_issue_category to the single best category that explains the main pain point.
If no clear issue is present, use other.
""".strip()
