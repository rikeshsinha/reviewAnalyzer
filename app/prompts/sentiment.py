"""Prompt fragments for sentiment classification."""

from __future__ import annotations

SENTIMENT_PROMPT_VERSION = "v1"

SENTIMENT_INSTRUCTIONS = """
Classify sentiment_label as one of: positive, neutral, negative, mixed.
Use mixed only when there are clearly meaningful positive and negative signals.
""".strip()
