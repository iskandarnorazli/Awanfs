"""Tests for hot_set_scale.py's report: a run counts only when every answer was right."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from hot_set_scale import report  # noqa: E402


def window(objects, **kw):
    r = {"phase": "window", "objects": objects, "put_ops": 3000, "put_p50_us": 900, "put_p99_us": 4000,
         "get_during_count": 10, "get_during_p50_us": 300, "get_during_p99_us": 2000, "burst_ops": 9000,
         "burst_p50_us": 500, "burst_p99_us": 3000, "wrong": 0, "server_peak_rss_mib": 120.0}
    r.update(kw)
    return r


RESTART = {"phase": "restart", "objects": 200_000, "boot_secs": 42.5, "server_peak_rss_mib": 150.0}
VERIFY = {"phase": "verify", "objects": 200_000, "gets": 20_000, "get_ops": 8000, "get_p50_us": 500,
          "get_p99_us": 2500, "wrong": 0}
FULL = [window(100_000, get_during_count=0), window(200_000), RESTART, VERIFY]


class Report(unittest.TestCase):
    def test_a_full_run_passes_and_marks_the_hot_set(self):
        code, text = report(FULL, "t")
        self.assertEqual(code, 0, text)
        self.assertIn("200,000 (past the hot set)", text)
        self.assertNotIn("100,000 (past", text)
        self.assertIn("after 42.5 s", text)

    def test_the_first_window_has_no_gets_during_puts(self):
        self.assertIn("| not measured |", report(FULL, "t")[1])

    def test_a_wrong_get_in_a_window_fails(self):
        self.assertEqual(report([window(100_000), window(200_000, wrong=1), RESTART, VERIFY], "t")[0], 1)

    def test_a_wrong_get_after_the_restart_fails(self):
        self.assertEqual(report(FULL[:3] + [dict(VERIFY, wrong=3)], "t")[0], 1)

    def test_a_failed_load_is_not_a_result(self):
        self.assertEqual(report([{"phase": "load_failed", "exit": 1}], "t")[0], 1)

    def test_a_missing_restart_is_not_a_result(self):
        self.assertEqual(report(FULL[:2] + [VERIFY], "t")[0], 2)


if __name__ == "__main__":
    unittest.main()
