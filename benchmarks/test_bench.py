"""Self-tests for bench.py: where reports go and how tools are looked up."""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench  # noqa: E402


class BenchTests(unittest.TestCase):
    def test_report_path_is_dated_and_host_safe(self):
        with mock.patch.object(bench.platform, "node", return_value="my box/01"):
            p = bench.report_path(0)
        self.assertTrue(p.endswith(os.path.join("measurements", "bench_1970-01-01_my-box-01.md")), p)

    def test_output_in_the_current_directory_needs_no_folder(self):
        bench.ensure_parent("bench_smoke.md")  # CI run 1: makedirs('') raised FileNotFoundError

    def test_no_docker_outside_linux(self):
        with mock.patch.object(bench.sys, "platform", "win32"):
            self.assertFalse(bench.from_image("quay.io/minio/minio:x", "/usr/bin/minio", "out"))

    def test_no_source_build_without_go(self):
        with mock.patch.object(bench.shutil, "which", return_value=None):
            self.assertFalse(bench.from_source("https://github.com/minio/minio", "tag", "out"))

    def test_missing_tools_stop_with_instructions(self):
        with mock.patch.object(bench, "TOOLS", "/nonexistent-awanfs-tools"), \
             mock.patch.object(bench.os, "makedirs"), \
             mock.patch.object(bench, "from_image", return_value=False), \
             mock.patch.object(bench, "from_source", return_value=False):
            with self.assertRaises(SystemExit) as e:
                bench.ensure_minio()
        self.assertIn("--awanfs-only", str(e.exception))


class AwanfsOnly(unittest.TestCase):
    def test_reads_throughput_and_peak_rss_from_measure(self):
        import tempfile
        orig = bench.ratio_check.measure
        bench.ratio_check.measure = lambda start, duration: ({n: 10.0 for n in bench.ratio_check.WORKLOADS}, 64.0)
        try:
            with tempfile.TemporaryDirectory() as d:
                out = os.path.join(d, "r.md")
                self.assertEqual(bench.awanfs_only("unused", 1, 0.1, out, "cmd"), 0)
                with open(out, encoding="utf-8") as fh:
                    text = fh.read()
        finally:
            bench.ratio_check.measure = orig
        self.assertIn("| 10.0 |", text)
        self.assertIn("Peak RSS over all workloads: 64.0 MiB", text)


if __name__ == "__main__":
    unittest.main()
