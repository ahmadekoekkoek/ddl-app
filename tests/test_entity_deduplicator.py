"""
Unit tests for Entity Deduplicator Module
"""

import unittest
from entity_deduplicator import (
    deduplicate_families,
    extract_id_keluarga,
    get_deduplication_summary
)


class TestEntityDeduplicator(unittest.TestCase):

    def test_extract_id_keluarga_standard(self):
        """Test extraction from standard field name."""
        family = {"id_keluarga": "12345", "name": "Test"}
        self.assertEqual(extract_id_keluarga(family), "12345")

    def test_extract_id_keluarga_uppercase(self):
        """Test extraction from uppercase field name."""
        family = {"ID_KELUARGA": "67890", "name": "Test"}
        self.assertEqual(extract_id_keluarga(family), "67890")

    def test_extract_id_keluarga_parent(self):
        """Test extraction from parent field name."""
        family = {"id_keluarga_parent": "ABC123", "name": "Test"}
        self.assertEqual(extract_id_keluarga(family), "ABC123")

    def test_extract_id_keluarga_missing(self):
        """Test extraction when no ID field exists."""
        family = {"name": "Test", "other": "value"}
        self.assertIsNone(extract_id_keluarga(family))

    def test_deduplicate_removes_duplicates(self):
        """Test that duplicate id_keluarga values are removed."""
        families = [
            {"id_keluarga": "001", "name": "Family A"},
            {"id_keluarga": "002", "name": "Family B"},
            {"id_keluarga": "001", "name": "Family A Duplicate"},  # Duplicate
            {"id_keluarga": "003", "name": "Family C"},
        ]

        unique, removed = deduplicate_families(families)

        self.assertEqual(len(unique), 3)
        self.assertEqual(removed, 1)

        # Verify first occurrence is kept
        ids = [f["id_keluarga"] for f in unique]
        self.assertEqual(ids, ["001", "002", "003"])

        # Verify correct data preserved (first occurrence)
        self.assertEqual(unique[0]["name"], "Family A")

    def test_deduplicate_preserves_order(self):
        """Test that first occurrence of each ID is preserved."""
        families = [
            {"id_keluarga": "A", "order": 1},
            {"id_keluarga": "B", "order": 2},
            {"id_keluarga": "A", "order": 3},  # Duplicate, should be removed
        ]

        unique, _ = deduplicate_families(families)
        self.assertEqual(unique[0]["order"], 1)

    def test_deduplicate_handles_empty_list(self):
        """Test deduplication with empty input."""
        unique, removed = deduplicate_families([])

        self.assertEqual(len(unique), 0)
        self.assertEqual(removed, 0)

    def test_deduplicate_no_duplicates(self):
        """Test deduplication when no duplicates exist."""
        families = [
            {"id_keluarga": "001", "name": "A"},
            {"id_keluarga": "002", "name": "B"},
            {"id_keluarga": "003", "name": "C"},
        ]

        unique, removed = deduplicate_families(families)

        self.assertEqual(len(unique), 3)
        self.assertEqual(removed, 0)

    def test_summary_no_duplicates(self):
        """Test summary message when no duplicates removed."""
        summary = get_deduplication_summary(100, 0)
        self.assertIn("100", summary)
        self.assertIn("tidak ada duplikat", summary)

    def test_summary_with_duplicates(self):
        """Test summary message when duplicates removed."""
        summary = get_deduplication_summary(100, 25)
        self.assertIn("75", summary)    # Unique count
        self.assertIn("25", summary)    # Duplicates removed
        self.assertIn("100", summary)   # Total
        self.assertIn("duplikat", summary)


if __name__ == '__main__':
    unittest.main()
