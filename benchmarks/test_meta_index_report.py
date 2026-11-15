"""Tests for meta_index_report.py: the comparison only counts when both stores answered alike."""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from meta_index_report import load, report  # noqa: E402


def rec(store, phase, **kw):
    base = {"store": store, "phase": phase, "keys": 10, "answers": "ab", "wrong": 0}
    base.update(kw)
    return json.dumps(base)


FULL = [rec(s, p) for s in ("redb", "seg") for p in ("build", "query")]


class Report(unittest.TestCase):
    def test_both_alike_passes(self):
        code, text = report(load(FULL), "t")
        self.assertEqual(code, 0, text)
        self.assertIn("the same.", text)

    def test_different_answers_fail(self):
        lines = FULL[:3] + [rec("seg", "query", answers="cd")]
        code, text = report(load(lines), "t")
        self.assertEqual(code, 1)
        self.assertIn("DIFFERENT", text)

    def test_a_wrong_answer_fails(self):
        lines = FULL[:3] + [rec("seg", "query", wrong=1)]
        self.assertEqual(report(load(lines), "t")[0], 1)

    def test_a_missing_phase_is_not_a_result(self):
        self.assertEqual(report(load(FULL[:3]), "t")[0], 2)

    def test_a_missing_number_says_not_measured(self):
        code, text = report(load(FULL), "t")
        self.assertIn("not measured", text)

    def test_non_json_lines_are_ignored(self):
        self.assertEqual(report(load(["Compiling...", *FULL]), "t")[0], 0)


if __name__ == "__main__":
    unittest.main()
