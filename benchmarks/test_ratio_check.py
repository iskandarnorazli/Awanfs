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


class RssTable(unittest.TestCase):
    def test_peak_rss_is_reported_when_both_servers_have_it(self):
        rows = [("get_64k", 1.5, 1.4, "ok")]
        md = rc.render(rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "cmd", 1, {"awanfs": [80.0, 100.0, 90.0], "minio": [300.0]})
        self.assertIn("| AwanFS | 90.0 |", md)
        self.assertIn("| MinIO | 300.0 |", md)

    def test_no_rss_table_without_numbers(self):
        rows = [("get_64k", 1.5, 1.4, "ok")]
        md = rc.render(rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "cmd", 1, {"awanfs": [], "minio": [300.0]})
        self.assertNotIn("peak RSS", md)



class RssCeiling(unittest.TestCase):
    """The ceiling the nightly ratio run enforces. The numbers are the measured ones: 117.0 MiB
    on github ubuntu-latest (run 43400978818), and 2258.9 MiB with the PUT path caching views of
    the volume mapping (#126, reverted; run 43413585215)."""

    CEIL = {"tolerance": 0.20, "rss_ceiling_mib": 250, "ratios": {"get_64k": 1.5}}

    def test_measured_rss_is_under_the_ceiling(self):
        med, ceiling, bad = rc.check_rss({"awanfs": [117.0, 117.9], "minio": [559.8]}, self.CEIL)
        self.assertEqual(bad, [])
        self.assertEqual(ceiling, 250)
        self.assertAlmostEqual(med, 117.45)

    def test_the_126_regression_fails_the_run(self):
        _, _, bad = rc.check_rss({"awanfs": [2258.9], "minio": [569.5]}, self.CEIL)
        self.assertEqual(len(bad), 1)
        self.assertIn("2258.9", bad[0])
        self.assertIn("250.0", bad[0])

    def test_a_baseline_without_a_ceiling_only_reports(self):
        med, ceiling, bad = rc.check_rss({"awanfs": [9999.0], "minio": [1.0]}, {"tolerance": 0.2})
        self.assertEqual((ceiling, bad), (None, []))
        self.assertEqual(med, 9999.0)

    def test_the_report_names_the_verdict_and_the_ceiling(self):
        rows = [("get_64k", 1.5, 1.4, "ok")]
        over = rc.render(rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "cmd", 1,
                         {"awanfs": [2258.9], "minio": [569.5]}, 250)
        self.assertIn("| AwanFS | 2258.9 | OVER THE CEILING |", over)
        under = rc.render(rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "cmd", 1,
                          {"awanfs": [117.0], "minio": [559.8]}, 250)
        self.assertIn("| AwanFS | 117.0 | ok |", under)
        self.assertIn("Ceiling 250.0 MiB", under)

    def test_the_committed_baseline_carries_a_ceiling(self):
        import json
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ratio_baseline.json"), encoding="utf-8") as fh:
            base = json.load(fh)
        self.assertIn("rss_ceiling_mib", base)
        _, _, bad = rc.check_rss({"awanfs": [2258.9], "minio": [1.0]}, base)
        self.assertEqual(len(bad), 1, "the committed ceiling must catch the measured regression")


if __name__ == "__main__":
    unittest.main()
