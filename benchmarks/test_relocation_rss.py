"""Tests for relocation_rss.py's verdict: what each exit code means."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from relocation_rss import verdict  # noqa: E402


class Verdict(unittest.TestCase):
    def test_no_relocation_is_not_measured_whatever_the_rss(self):
        self.assertEqual(verdict(10.0, 32, 16, 0), 2)
        self.assertEqual(verdict(999.0, 32, 16, 0), 2)

    def test_at_the_ceiling_passes_and_over_it_fails(self):
        self.assertEqual(verdict(48.0, 32, 16, 1), 0)
        self.assertEqual(verdict(48.1, 32, 16, 1), 1)

    def test_a_peak_the_size_of_the_object_fails(self):
        # The bug this guards: the copy faulted the whole 64 MiB object into the RSS.
        self.assertEqual(verdict(30.0 + 64.0, 32, 16, 1), 1)


if __name__ == "__main__":
    unittest.main()
