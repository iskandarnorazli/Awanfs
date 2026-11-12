#!/usr/bin/env python3
"""
benchmarks/s3_warp/test_run_warp_benchmark.py
Unit tests for the MinIO Warp Benchmark Harness (#466)
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))

from run_warp_benchmark import (
    calculate_percentiles,
    format_markdown_report,
    BenchmarkMetrics,
)

class TestRunWarpBenchmark(unittest.TestCase):
    def test_calculate_percentiles(self):
        latencies = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        p50, p90, p99, p999 = calculate_percentiles(latencies)
        self.assertAlmostEqual(p50, 5.5, delta=0.5)
        self.assertAlmostEqual(p90, 9.1, delta=0.5)
        self.assertAlmostEqual(p99, 9.91, delta=0.5)

    def test_format_markdown_report(self):
        cmd = "python benchmarks/s3_warp/run_warp_benchmark.py --sizes 64KiB,1MiB --concurrent 64"
        hw_info = {
            "cpu": "AMD Ryzen 9 / Intel Core",
            "cores": 16,
            "ram_gb": 32.0,
            "os": "Windows 11 x86_64",
            "affinity_server": "Cores 0..3",
            "affinity_client": "Cores 4..7",
        }
        awan_metrics = {
            "put_64k": BenchmarkMetrics(ops_per_sec=12500.0, mib_per_sec=781.2, p50_ms=2.1, p90_ms=4.5, p99_ms=8.2, p999_ms=12.1, peak_rss_mb=42.5),
            "get_64k": BenchmarkMetrics(ops_per_sec=28400.0, mib_per_sec=1775.0, p50_ms=0.9, p90_ms=1.8, p99_ms=3.4, p999_ms=5.6, peak_rss_mb=42.5),
            "put_1m": BenchmarkMetrics(ops_per_sec=2100.0, mib_per_sec=2100.0, p50_ms=14.2, p90_ms=22.5, p99_ms=35.1, p999_ms=48.0, peak_rss_mb=56.0),
            "get_1m": BenchmarkMetrics(ops_per_sec=4200.0, mib_per_sec=4200.0, p50_ms=7.1, p90_ms=11.4, p99_ms=18.2, p999_ms=25.4, peak_rss_mb=56.0),
        }
        minio_metrics = {
            "put_64k": BenchmarkMetrics(ops_per_sec=6200.0, mib_per_sec=387.5, p50_ms=5.4, p90_ms=11.2, p99_ms=24.5, p999_ms=42.0, peak_rss_mb=248.0),
            "get_64k": BenchmarkMetrics(ops_per_sec=14100.0, mib_per_sec=881.2, p50_ms=2.3, p90_ms=4.8, p99_ms=9.8, p999_ms=18.2, peak_rss_mb=248.0),
            "put_1m": BenchmarkMetrics(ops_per_sec=950.0, mib_per_sec=950.0, p50_ms=32.5, p90_ms=58.2, p99_ms=95.0, p999_ms=140.0, peak_rss_mb=390.0),
            "get_1m": BenchmarkMetrics(ops_per_sec=1850.0, mib_per_sec=1850.0, p50_ms=16.8, p90_ms=28.4, p99_ms=48.5, p999_ms=75.0, peak_rss_mb=390.0),
        }

        report = format_markdown_report(cmd, hw_info, awan_metrics, minio_metrics)
        self.assertTrue(report.startswith(f"`{cmd}`\n"))
        self.assertIn("S3 Storage Server", report)
        self.assertIn("MinIO Community Baseline", report)
        self.assertIn("Peak RSS", report)

    def test_report_states_put_durability(self):
        report = format_markdown_report("cmd", {}, {}, {})
        self.assertIn("Durability per PUT", report)
        self.assertIn("#490", report)

    def test_report_has_no_unsourced_reference_ranges(self):
        report = format_markdown_report("cmd", {}, {}, {})
        self.assertNotIn("Published Ref", report)
        self.assertNotRegex(report, r"~[0-9]")

if __name__ == "__main__":
    unittest.main()
