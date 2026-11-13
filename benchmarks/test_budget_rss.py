"""Self-tests for budget_rss.py: the ceiling arithmetic and the sizes it accepts."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import budget_rss as br  # noqa: E402


class Sizes(unittest.TestCase):
    def test_the_sizes_are_powers_of_two_in_bytes(self):
        self.assertEqual(br.SIZES["1MiB"], 1048576)
        self.assertEqual(br.SIZES["64KiB"], 65536)
        for name, n in br.SIZES.items():
            self.assertEqual(n & (n - 1), 0, f"{name} is not a power of two")


class Ceiling(unittest.TestCase):
    """The step in bench-ratio.yml runs --budget-mib 256 --overhead 200, so the ceiling is 456."""

    def test_the_ceiling_is_the_budget_plus_the_stated_overhead(self):
        self.assertEqual(br.ceiling_mib(256, 200), 456.0)
        self.assertEqual(br.ceiling_mib(0, 0), 0.0)

    def test_a_peak_over_the_ceiling_fails_and_one_under_it_passes(self):
        self.assertTrue(br.over_ceiling(2258.9, 256, 200), "the #126 regression would fail this check")
        self.assertFalse(br.over_ceiling(121.0, 256, 200), "the measured peak RSS passes")
        self.assertFalse(br.over_ceiling(456.0, 256, 200), "exactly at the ceiling passes")
        self.assertTrue(br.over_ceiling(456.1, 256, 200), "a tenth of a MiB over fails")

    def test_raising_the_overhead_is_visible_as_raising_the_ceiling(self):
        self.assertTrue(br.over_ceiling(700.0, 256, 200))
        self.assertFalse(br.over_ceiling(700.0, 256, 500), "a run only passes by stating a bigger overhead")


class Verdict(unittest.TestCase):
    """The first CI runs of this script reported "ok" for a run in which every request failed:
    runs 43504508701 and 43504519493, put 0.00 MiB/s at 532.5 ops/s, get 0.00 MiB/s at
    7450.7 ops/s, peak RSS 16.9 MiB. Those are the rows below."""

    MIB = 1 << 20

    def test_the_run_that_failed_every_request_is_not_measured(self):
        rows = [("put", 0.0, 532.5, 25.15, 14.9), ("get", 0.0, 7450.7, 33.43, 16.9)]
        result, reasons = br.verdict(rows, 16.9, 256, 200, self.MIB)
        self.assertEqual(result, "NOT MEASURED")
        self.assertEqual(len(reasons), 2, reasons)

    def test_one_failed_phase_is_enough_to_refuse_the_run(self):
        rows = [("put", 300.0, 300.0, 40.0, 200.0), ("get", 0.0, 9000.0, 1.0, 200.0)]
        self.assertEqual(br.verdict(rows, 200.0, 256, 200, self.MIB)[0], "NOT MEASURED")

    def test_a_measured_run_under_the_ceiling_is_ok_and_over_it_is_over(self):
        rows = [("put", 300.0, 300.0, 40.0, 300.0), ("get", 900.0, 900.0, 10.0, 310.0)]
        self.assertEqual(br.verdict(rows, 310.0, 256, 200, self.MIB), ("ok", []))
        self.assertEqual(br.verdict(rows, 500.0, 256, 200, self.MIB)[0], "OVER")

    def test_the_bucket_is_created_before_the_phases(self):
        import inspect
        src = inspect.getsource(br.main)
        self.assertLess(src.index("create_bucket(port"), src.index("run_workload_phase("))


class Cli(unittest.TestCase):
    def test_the_parsers_defaults_are_the_workflow_steps_values(self):
        a = br.build_parser().parse_args(["--server-bin", "x"])
        self.assertEqual((a.budget, a.budget_mib, a.concurrency, a.size, a.duration, a.overhead),
                         ("256MiB", 256.0, 64, "1MiB", 10.0, 200.0))

    def test_an_unknown_size_is_refused(self):
        with self.assertRaises(SystemExit):
            br.build_parser().parse_args(["--server-bin", "x", "--size", "7MiB"])

    def test_the_module_states_its_exit_codes(self):
        self.assertIn("Exit codes: 0 under the ceiling, 1 over it", br.__doc__)


if __name__ == "__main__":
    unittest.main()
