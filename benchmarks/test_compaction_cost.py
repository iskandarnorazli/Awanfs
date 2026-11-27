"""Tests for compaction_cost.py: report from given numbers and zero-reclaim detection."""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from compaction_cost import load, main, report  # noqa: E402

MIXES = ("small-heavy", "balanced", "90-10")
FRACTIONS = (0.50, 0.90)


def make_run(
    mix,
    delete_fraction,
    *,
    kind="mix",
    small_bytes_copied=67_108_864,
    large_bytes_copied=268_451_840,
    bytes_reclaimed=100_659_200,
    seconds=1.25,
    volume_removed=True,
    busy=False,
):
    bytes_copied = small_bytes_copied + large_bytes_copied
    return {
        "kind": kind,
        "mix": mix,
        "delete_fraction": delete_fraction,
        "small_count": 16384,
        "small_size": 4096,
        "large_count": 0 if kind == "small-only" else 4,
        "large_size": 67_108_864,
        "small_deleted": int(16384 * delete_fraction),
        "small_bytes_copied": small_bytes_copied,
        "large_bytes_copied": large_bytes_copied,
        "bytes_copied": bytes_copied,
        "bytes_reclaimed": bytes_reclaimed,
        "seconds": seconds,
        "large_share": (large_bytes_copied / bytes_copied) if bytes_copied else 0.0,
        "volume_removed": volume_removed,
        "busy": busy,
    }


def full_dataset():
    rows = []
    # Mix 1: 33.3% large, Mix 2: 80.0% large, Mix 3: 99.9% large
    specs = {
        "small-heavy": (134_217_728, 67_112_960, 201_326_592),
        "balanced": (67_108_864, 268_451_840, 100_663_296),
        "90-10": (368_640, 671_129_600, 552_960),
    }
    for mix in MIXES:
        small_b, large_b, recl_b = specs[mix]
        for frac in FRACTIONS:
            for offset, sec in ((-10_000, 1.0), (0, 2.5), (500_000, 10.0)):
                rows.append(
                    make_run(
                        mix,
                        frac,
                        small_bytes_copied=small_b + offset,
                        large_bytes_copied=large_b,
                        bytes_reclaimed=recl_b + offset,
                        seconds=sec,
                    )
                )
    return rows


class ReportTests(unittest.TestCase):
    def test_report_from_given_numbers_uses_median_of_three_runs(self):
        rows = full_dataset()
        code, text = report(rows, mixes=MIXES, delete_fractions=FRACTIONS)
        self.assertEqual(code, 0, text)
        self.assertIn("small-heavy", text)
        self.assertIn("balanced", text)
        self.assertIn("90-10", text)
        # Median of (1.0, 2.5, 10.0) seconds is 2.500 s, not mean 4.500 s.
        self.assertIn("2.500", text)
        self.assertNotIn("4.500", text)
        # Check large share formatting (99.95% for 90-10, 80.00% for balanced, 33.33% for small-heavy)
        self.assertIn("99.95%", text)
        self.assertIn("80.00%", text)
        self.assertIn("33.33%", text)

    def test_zero_reclaimed_reads_reclaimed_nothing_and_fails(self):
        rows = full_dataset()
        rows[-1] = make_run(
            "90-10",
            0.90,
            small_bytes_copied=368_640,
            large_bytes_copied=671_129_600,
            bytes_reclaimed=0,
            seconds=1.5,
        )
        code, text = report(rows, mixes=MIXES, delete_fractions=FRACTIONS)
        self.assertNotEqual(code, 0)
        self.assertIn("reclaimed nothing", text.lower())

    def test_volume_not_removed_reads_reclaimed_nothing_and_fails(self):
        rows = full_dataset()
        rows[0] = make_run(
            "small-heavy",
            0.50,
            bytes_reclaimed=100_000,
            volume_removed=False,
        )
        code, text = report(rows, mixes=MIXES, delete_fractions=FRACTIONS)
        self.assertNotEqual(code, 0)
        self.assertIn("reclaimed nothing", text.lower())

    def test_busy_volume_reads_reclaimed_nothing_and_fails(self):
        rows = full_dataset()
        rows[0] = make_run(
            "small-heavy",
            0.50,
            bytes_reclaimed=100_000,
            volume_removed=True,
            busy=True,
        )
        code, text = report(rows, mixes=MIXES, delete_fractions=FRACTIONS)
        self.assertNotEqual(code, 0)
        self.assertIn("reclaimed nothing", text.lower())

    def test_default_data_dir_prefers_runner_temp(self):
        import compaction_cost
        from unittest import mock

        with mock.patch.dict(os.environ, {"RUNNER_TEMP": "/runner/fast-tmp"}):
            self.assertEqual(compaction_cost.default_data_dir(), "/runner/fast-tmp")

    def test_missing_mix_or_fraction_fails_with_exit_2(self):
        rows = [r for r in full_dataset() if r["mix"] != "90-10"]
        code, text = report(rows, mixes=MIXES, delete_fractions=FRACTIONS)
        self.assertEqual(code, 2)
        self.assertIn("90-10", text)

    def test_small_only_baseline_comparison_renders_when_present(self):
        rows = full_dataset()
        for mix in MIXES:
            for frac in FRACTIONS:
                for sec in (0.10, 0.20, 0.30):
                    rows.append(
                        make_run(
                            mix,
                            frac,
                            kind="small-only",
                            small_bytes_copied=67_108_864,
                            large_bytes_copied=0,
                            bytes_reclaimed=100_663_296,
                            seconds=sec,
                        )
                    )
        code, text = report(rows, mixes=MIXES, delete_fractions=FRACTIONS)
        self.assertEqual(code, 0, text)
        self.assertIn("without large objects", text.lower())

    def test_load_ignores_non_json_lines(self):
        sample = make_run("90-10", 0.5)
        loaded = load(["Compiling awanfs...", "", json.dumps(sample)])
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["mix"], "90-10")

    def test_main_report_only_writes_command_on_line_1(self):
        rows = full_dataset()
        with tempfile.TemporaryDirectory() as d:
            res_path = os.path.join(d, "results.jsonl")
            out_path = os.path.join(d, "report.md")
            with open(res_path, "w", encoding="utf-8") as fh:
                for r in rows:
                    fh.write(json.dumps(r) + "\n")
            code = main(["--report-only", "--results", res_path, "--output", out_path])
            self.assertEqual(code, 0)
            with open(out_path, encoding="utf-8") as fh:
                lines = fh.read().splitlines()
            self.assertTrue(lines[0].startswith("Command: "), lines[0])


if __name__ == "__main__":
    unittest.main()
