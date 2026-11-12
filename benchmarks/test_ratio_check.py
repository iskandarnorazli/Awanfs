"""Self-tests for ratio_check.py: how ratios are formed and when a run fails."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ratio_check as rc  # noqa: E402

BASE = {"tolerance": 0.20, "ratios": {"get_64k": 1.5, "put_64k": 10.0}}


class RatioTests(unittest.TestCase):
    def test_ratio_is_median_over_median(self):
        r = rc.median_ratios({"get_64k": [300, 100, 200]}, {"get_64k": [100, 400, 50]})
        self.assertAlmostEqual(r["get_64k"], 2.0)

    def test_minio_without_bytes_has_no_ratio_and_fails(self):
        r = rc.median_ratios({"get_64k": [100]}, {"get_64k": [0, 0, 0]})
        self.assertIsNone(r["get_64k"])
        _, bad = rc.compare(r, BASE)
        self.assertEqual(len(bad), 1)


class CompareTests(unittest.TestCase):
    def test_within_tolerance_passes(self):
        rows, bad = rc.compare({"get_64k": 1.25, "put_64k": 9.0}, BASE)
        self.assertEqual(bad, [])
        self.assertTrue(all(v == "ok" for *_, v in rows), rows)

    def test_drop_past_tolerance_fails(self):
        rows, bad = rc.compare({"get_64k": 1.19, "put_64k": 10.0}, BASE)
        self.assertEqual(len(bad), 1)
        self.assertIn("get_64k", bad[0])

    def test_no_baseline_reports_only(self):
        rows, bad = rc.compare({"get_64k": 0.1}, None)
        self.assertEqual(bad, [])
        self.assertEqual(rows[0][3], "no baseline")

    def test_much_faster_asks_to_raise_the_baseline(self):
        rows, _ = rc.compare({"get_64k": 2.0}, BASE)
        self.assertIn("raise the baseline", rows[0][3])

    def test_report_starts_with_its_command(self):
        rows, _ = rc.compare({"get_64k": 1.5}, BASE)
        md = rc.render(rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "python benchmarks/ratio_check.py --runs 5", 5)
        self.assertTrue(md.startswith("Command: `python benchmarks/ratio_check.py --runs 5`"))


if __name__ == "__main__":
    unittest.main()
