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
