"""Tests for meta_index_ceiling.py: over the ceiling fails, and measuring nothing never passes."""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from meta_index_ceiling import check  # noqa: E402


def rec(store, phase, rss):
    return json.dumps({"store": store, "phase": phase, "keys": 10000000, "peak_rss_mib": rss})


class Ceiling(unittest.TestCase):
    def test_under_the_ceiling_passes(self):
        code, out = check([rec("prod", "build", 54.7), rec("prod", "query", 49.4)], 120)
        self.assertEqual(code, 0, out)

    def test_one_line_over_fails(self):
        code, out = check([rec("prod", "build", 54.7), rec("prod", "query", 400.0)], 120)
        self.assertEqual(code, 1)
        self.assertIn("OVER", out[1])

    def test_a_cold_query_counts(self):
        self.assertEqual(check([rec("prod", "query_cold", 121.0)], 120)[0], 1)

    def test_other_stores_and_phases_are_ignored(self):
        lines = [rec("redb", "build", 999.0), rec("prod", "writers", 999.0), rec("prod", "build", 50.0)]
        self.assertEqual(check(lines, 120)[0], 0)

    def test_nothing_measured_is_not_a_pass(self):
        self.assertEqual(check([rec("redb", "build", 50.0), "not json"], 120)[0], 2)

    def test_a_line_without_rss_is_not_a_pass(self):
        self.assertEqual(check([rec("prod", "build", None)], 120)[0], 2)


if __name__ == "__main__":
    unittest.main()
