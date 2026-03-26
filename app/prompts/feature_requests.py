"""Prompt fragments for feature request and competitor extraction."""

from __future__ import annotations

FEATURE_REQUESTS_INSTRUCTIONS = """
Set feature_request_flag to true only when the document asks for a new capability
or an enhancement that does not exist yet.
Extract competitor_mentions as a deduplicated list of explicitly named competitors/products.
""".strip()
