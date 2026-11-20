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

    def test_three_stores_must_all_agree(self):
        prod = [rec("prod", p) for p in ("build", "query")]
        code, text = report(load(FULL + prod), "t")
        self.assertEqual(code, 0, text)
        self.assertIn("production", text)
        code, text = report(load(FULL + prod[:1] + [rec("prod", "query", answers="cd")]), "t")
        self.assertEqual(code, 1)

    def test_redb_is_required(self):
        lines = [rec(s, p) for s in ("seg", "prod") for p in ("build", "query")]
        self.assertEqual(report(load(lines), "t")[0], 2)

    def test_writers_rows_appear_for_the_stores_that_ran_them(self):
        lines = FULL + [rec("redb", "writers", writers=64, puts_per_s=2000, put_p99_us=90.5)]
        code, text = report(load(lines), "t")
        self.assertEqual(code, 0, text)
        row = next(l for l in text.splitlines() if l.startswith("| inserts per second, 64 writers"))
        self.assertEqual(row, "| inserts per second, 64 writers at once | 2,000 | not measured |")
        self.assertIn("| put P99 with 64 writers, us | 90.500 | not measured |", text)
        self.assertNotIn("writers", report(load(FULL), "t")[1])

    def test_non_json_lines_are_ignored(self):
        self.assertEqual(report(load(["Compiling...", *FULL]), "t")[0], 0)


if __name__ == "__main__":
    unittest.main()
