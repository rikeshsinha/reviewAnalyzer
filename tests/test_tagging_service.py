"""Unit tests for rule-based tagging edge cases."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services.tagging_service import TaggingService


class TaggingServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        taxonomy_path = Path(self.temp_dir.name) / "taxonomy.yaml"
        taxonomy_path.write_text(
            """
products:
  - galaxy watch
  - watch
product_aliases:
  galaxy watch:
    - samsung watch
  watch:
    - watch

issues:
  - battery drain
issue_aliases:
  battery drain:
    - battery dies fast

competitors:
  - google_wearables
competitor_aliases:
  google_wearables:
    - pixel watch
    - google fit

features:
  - notifications
feature_aliases:
  notifications:
    - alerts
            """.strip(),
            encoding="utf-8",
        )

        self.service = TaggingService(taxonomy_path=taxonomy_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_alias_collision_prefers_longest_alias(self) -> None:
        text = "My samsung watch battery dies fast and this watch syncs slowly."
        product_tags = self.service.extract_product_tags(text)
        issue_tags = self.service.extract_issue_tags(text)

        self.assertIn("galaxy watch", product_tags)
        self.assertIn("watch", product_tags)
        self.assertIn("battery drain", issue_tags)

    def test_mixed_mentions_across_tag_types(self) -> None:
        text = "Compared to pixel watch and google fit, alerts are better on samsung watch."
        all_tags = self.service.extract_all_tags(text)
        flattened = {(tag["tag_type"], tag["tag_value"], tag["tag_source"]) for tag in all_tags}

        self.assertIn(("product", "galaxy watch", "rules"), flattened)
        self.assertIn(("competitor", "google_wearables", "rules"), flattened)
        self.assertIn(("feature", "notifications", "rules"), flattened)


if __name__ == "__main__":
    unittest.main()
