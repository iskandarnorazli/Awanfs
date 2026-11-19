#!/usr/bin/env python3
"""Unit tests for benchmarks/market_claims.py (#58)."""

import http.server
import json
import os
import socketserver
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import market_claims as mc  # noqa: E402
from run_warp_benchmark import BenchmarkMetrics  # noqa: E402


class _MockS3Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    store = {}
    lock = threading.Lock()
    op_counts = {"PUT": 0, "GET": 0, "HEAD": 0, "DELETE": 0, "LIST": 0}

    def log_message(self, format, *args):
        pass

    def do_PUT(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length > 0 else b""
        key = self.path.split("?")[0]
        with self.lock:
            self.store[key] = body
            self.op_counts["PUT"] += 1
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        key = self.path.split("?")[0]
        with self.lock:
            self.op_counts["HEAD"] += 1
            data = self.store.get(key)
        if data is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()

    def do_GET(self):
        if self.path == "/_awanfs/metrics":
            body = (
                b"awanfs_payload_cache_hits_total 2\n"
                b"awanfs_payload_cache_misses_total 50\n"
            )
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if "list-type=2" in self.path:
            with self.lock:
                self.op_counts["LIST"] += 1
                n = len(self.store)
            xml = f"<ListBucketResult><KeyCount>{n}</KeyCount></ListBucketResult>".encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(xml)))
            self.end_headers()
            self.wfile.write(xml)
            return
        key = self.path.split("?")[0]
        with self.lock:
            self.op_counts["GET"] += 1
            data = self.store.get(key)
        if data is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_DELETE(self):
        key = self.path.split("?")[0]
        with self.lock:
            self.op_counts["DELETE"] += 1
            existed = key in self.store
            self.store.pop(key, None)
        self.send_response(204 if existed else 404)
        self.send_header("Content-Length", "0")
        self.end_headers()


class _ThreadedS3(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class TestDurabilityAndSetup(unittest.TestCase):
    def test_durability_mapping_and_server_args(self):
        self.assertEqual(mc.normalize_durability("group")[:2], ("group", "group"))
        self.assertEqual(mc.normalize_durability("strict")[:2], ("strict", "strict"))
        canon, flag, label, _ = mc.normalize_durability("none")
        self.assertEqual((canon, flag), ("none", "fast"))
        self.assertIn("NO-SYNC", label)

        args = mc.build_awanfs_server_args("s3-server", "/tmp/d", 9000, durability="none", memory_budget="16MiB")
        self.assertIn("--durability", args)
        self.assertEqual(args[args.index("--durability") + 1], "fast")
        self.assertIn("--memory-budget", args)
        self.assertEqual(args[args.index("--memory-budget") + 1], "16MiB")

    def test_minio_4_drive_erasure_setup(self):
        with tempfile.TemporaryDirectory() as d:
            cmd, dirs = mc.build_minio_server_args("/usr/bin/minio", d, 9000, 9001, drives=4)
            self.assertEqual(len(dirs), 4)
            for sub in dirs:
                self.assertTrue(os.path.isdir(sub))
            self.assertEqual(cmd[:2], ["/usr/bin/minio", "server"])
            self.assertEqual(cmd[2:6], dirs)
            self.assertIn("--address", cmd)

    def test_cli_defaults_match_market_claims(self):
        args = mc.build_parser().parse_args([])
        self.assertEqual(args.runs, 3)
        self.assertEqual(mc.parse_duration_seconds(args.duration), 30.0)
        self.assertEqual(args.minio_drives, 4)
        self.assertEqual(args.durabilities, "group,strict,none")


class TestSpreadAndPayloads(unittest.TestCase):
    def test_spread_formatting(self):
        med, lo, hi = mc.summarize_spread([12.0, 30.0, 18.0])
        self.assertEqual((med, lo, hi), (18.0, 12.0, 30.0))
        self.assertEqual(mc.format_spread([12.0, 30.0, 18.0]), "18.0 (12.0..30.0)")
        self.assertEqual(mc.format_spread([]), "-")

    def test_community_payload_is_800kb_and_compressible(self):
        data = mc.make_compressible_payload(mc.COMMUNITY_DOC_BYTES, doc_id=42)
        self.assertEqual(len(data), 800 * 1024)
        ratio = mc.payload_compression_ratio(data)
        self.assertGreater(ratio, 3.0, f"expected compressible payload, got ratio {ratio:.2f}")

    def test_prometheus_counter_parse(self):
        text = "# HELP foo bar\nawanfs_payload_cache_hits_total 12\nawanfs_payload_cache_misses_total 99\n"
        self.assertEqual(mc.parse_prometheus_counter(text, "awanfs_payload_cache_hits_total"), 12)
        self.assertEqual(mc.parse_prometheus_counter(text, "awanfs_payload_cache_misses_total"), 99)
        self.assertIsNone(mc.parse_prometheus_counter(text, "missing_metric"))


class TestWarpIntegration(unittest.TestCase):
    def test_build_warp_command_and_parse_json(self):
        cmd = mc.build_warp_command("warp", "mixed", "127.0.0.1:9000", "ak", "sk", "bkt", 32, 30.0, "64KiB")
        self.assertEqual(cmd[:4], ["warp", "mixed", "--host", "127.0.0.1:9000"])
        self.assertIn("--json", cmd)
        self.assertIn("--noclear", cmd)
        self.assertIn("--obj.size", cmd)

        list_cmd = mc.build_warp_command("warp", "list", "127.0.0.1:9000", "ak", "sk", "bkt", 8, 10.0, "64KiB")
        self.assertIn("--objects", list_cmd)
        self.assertIn("1000", list_cmd)

        sample = json.dumps({
            "total": {
                "throughput": {"bytes_per_sec": 100 * 1024 * 1024},
                "operations": {"ops_per_sec": 1600.0},
                "latencies": {"p50_ms": 4.2, "p90_ms": 8.1, "p99_ms": 15.3, "p999_ms": 22.0},
            }
        })
        m = mc.parse_warp_json("warp: running...\n" + sample + "\n")
        self.assertTrue(m.measured)
        self.assertAlmostEqual(m.mib_per_sec, 100.0, places=2)
        self.assertAlmostEqual(m.ops_per_sec, 1600.0, places=2)
        self.assertAlmostEqual(m.p99_ms, 15.3, places=2)

        bad = mc.parse_warp_json("not json")
        self.assertFalse(bad.measured)

        # Real `minio/warp` `aggregate.Aggregated` multi-line JSON (single op and mixed)
        real_single = "Saving benchmark data\n" + json.dumps({
            "type": "single",
            "mixed": False,
            "operations": [{
                "type": "PUT",
                "errors": 0,
                "n": 3000,
                "skipped": False,
                "throughput": {
                    "measure_duration_millis": 10000,
                    "bytes": 196608000.0,
                    "objects": 3000.0,
                    "ops": 3000,
                    "errors": 0,
                },
                "single_sized_requests": {
                    "requests": 3000,
                    "dur_median_millis": 3.5,
                    "dur_90_millis": 7.1,
                    "dur_99_millis": 14.2,
                },
            }],
        }, indent=2)
        m_real = mc.parse_warp_json(real_single)
        self.assertTrue(m_real.measured, m_real.note)
        self.assertAlmostEqual(m_real.ops_per_sec, 300.0, places=2)
        self.assertAlmostEqual(m_real.mib_per_sec, 18.75, places=2)
        self.assertAlmostEqual(m_real.p50_ms, 3.5, places=2)
        self.assertAlmostEqual(m_real.p99_ms, 14.2, places=2)

        real_mixed = json.dumps({
            "type": "mixed",
            "mixed": True,
            "mixed_server_stats": {
                "measure_duration_millis": 10000,
                "bytes": 104857600.0,
                "ops": 1600,
                "errors": 0,
            },
            "operations": [
                {"type": "GET", "skipped": False, "single_sized_requests": {"requests": 800, "dur_median_millis": 2.0, "dur_90_millis": 4.0, "dur_99_millis": 8.0}},
                {"type": "PUT", "skipped": False, "single_sized_requests": {"requests": 200, "dur_median_millis": 6.0, "dur_90_millis": 10.0, "dur_99_millis": 20.0}},
            ],
        }, indent=2)
        m_mix = mc.parse_warp_json(real_mixed)
        self.assertTrue(m_mix.measured, m_mix.note)
        self.assertAlmostEqual(m_mix.mib_per_sec, 10.0, places=2)
        self.assertAlmostEqual(m_mix.ops_per_sec, 160.0, places=2)
        self.assertAlmostEqual(m_mix.p50_ms, 2.8, places=2)
        self.assertAlmostEqual(m_mix.p99_ms, 10.4, places=2)

        # Real `minio/warp --json` v2 (`aggregate.Realtime`) JSON output
        real_v2 = json.dumps({
            "v": 2,
            "final": True,
            "total": {
                "total_requests": 2000,
                "total_errors": 0,
                "total_bytes": 131072000,
                "throughput": {"measure_duration_millis": 0, "bytes": 0, "ops": 0},
                "throughput_by_host": {
                    "http://127.0.0.1:9000": {
                        "measure_duration_millis": 2000,
                        "bytes": 131072000,
                        "ops": 2000,
                        "errors": 0,
                    }
                },
                "requests_by_client": {
                    "host-1": [{
                        "single_sized_requests": {
                            "dur_median_millis": 2.17,
                            "dur_90_millis": 10.19,
                            "dur_99_millis": 14.12,
                            "requests": 2000,
                        }
                    }]
                },
            },
        }, indent=2)
        m_v2 = mc.parse_warp_json(real_v2)
        self.assertTrue(m_v2.measured, m_v2.note)
        self.assertAlmostEqual(m_v2.ops_per_sec, 1000.0, places=2)
        self.assertAlmostEqual(m_v2.mib_per_sec, 62.5, places=2)
        self.assertAlmostEqual(m_v2.p50_ms, 2.17, places=2)
        self.assertAlmostEqual(m_v2.p99_ms, 14.12, places=2)


class TestWorkloadsAgainstMockS3(unittest.TestCase):
    def setUp(self):
        _MockS3Handler.store = {}
        _MockS3Handler.op_counts = {"PUT": 0, "GET": 0, "HEAD": 0, "DELETE": 0, "LIST": 0}
        self.srv = _ThreadedS3(("127.0.0.1", 0), _MockS3Handler)
        self.port = self.srv.server_address[1]
        self.thread = threading.Thread(target=self.srv.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()

    def test_cold_get_list_head_delete_and_mixed_3r2w(self):
        cold_m, hits_d, misses_d = mc.run_cold_get_phase(
            "127.0.0.1", self.port, "bkt", 4096, 2, 0.15, cold_set_bytes=32768
        )
        self.assertTrue(cold_m.measured, cold_m.note)
        self.assertGreater(cold_m.ops_per_sec, 0.0)
        self.assertEqual((hits_d, misses_d), (0, 0))

        list_m = mc.run_list_phase("127.0.0.1", self.port, "bkt", num_keys=25, concurrency=2, duration_sec=0.15)
        self.assertTrue(list_m.measured, list_m.note)
        self.assertGreater(_MockS3Handler.op_counts["LIST"], 0)

        head_m = mc.run_head_phase("127.0.0.1", self.port, "bkt", 1024, 2, 0.15)
        self.assertTrue(head_m.measured, head_m.note)
        self.assertGreater(_MockS3Handler.op_counts["HEAD"], 0)

        del_m = mc.run_delete_phase("127.0.0.1", self.port, "bkt", 1024, 2, 0.1, batch_size=16)
        self.assertTrue(del_m.measured, del_m.note)
        self.assertGreater(_MockS3Handler.op_counts["DELETE"], 0)

        mix_m = mc.run_asymmetric_mixed_phase("127.0.0.1", self.port, "bkt", b"a" * 1024, readers=3, writers=2, duration_sec=0.15)
        self.assertTrue(mix_m.measured, mix_m.note)
        self.assertGreater(mix_m.mib_per_sec, 0.0)

    def test_failed_requests_are_not_measured(self):
        # Negative control: clearing the store right after warm-up causes 404s, which must set measured=False.
        orig_pop = mc._populate_keys
        mc._populate_keys = lambda *a, **kw: (32, 0, "")
        try:
            m, _, _ = mc.run_cold_get_phase("127.0.0.1", self.port, "bkt", 1024, 2, 0.1, cold_set_bytes=4096)
        finally:
            mc._populate_keys = orig_pop
        self.assertFalse(m.measured)
        self.assertIn("HTTP 404", m.note)


class TestReportFormatting(unittest.TestCase):
    def test_report_starts_with_command_and_includes_all_sections(self):
        cmd = "python benchmarks/market_claims.py --runs 3 --duration 30"
        hw = {
            "cpu": "Test CPU",
            "cores": "16",
            "ram_gb": "32.0",
            "os": "Linux 6.8",
            "affinity_server": "Cores 0..3",
            "affinity_client": "Cores 4..7",
            "minio_status": "Measured Live (RELEASE.2025-09-07T16-13-09Z)",
            "rustfs_status": "Measured Live (1.0.5)",
            "warp_status": "Measured (v1.0.0)",
        }
        m1 = mc.RunMeasurement(BenchmarkMetrics(1000.0, 100.0, 1.0, 2.0, 3.0, 4.0, 50.0, server_cpu_s=1.2), disk_mib=64.0, cache_hits=0, cache_misses=500)
        m2 = mc.RunMeasurement(BenchmarkMetrics(1200.0, 120.0, 0.8, 1.8, 2.8, 3.8, 52.0, server_cpu_s=1.4), disk_mib=64.0, cache_hits=0, cache_misses=600)
        m3 = mc.RunMeasurement(BenchmarkMetrics(1100.0, 110.0, 0.9, 1.9, 2.9, 3.9, 51.0, server_cpu_s=1.3), disk_mib=64.0, cache_hits=0, cache_misses=550)
        results = {
            "AwanFS (--durability=group, default)": {"get_64k_cold": [m1, m2, m3]},
            "MinIO (4-dir erasure)": {"get_64k_cold": [m1, m1, m1]},
        }
        warp_results = {
            "AwanFS (--durability=group, default)": {"get_64k_cold": [BenchmarkMetrics(950.0, 95.0, 1.1, 2.1, 3.1, 4.1, 50.0)]},
        }
        report = mc.format_market_claims_report(
            cmd=cmd,
            hw_info=hw,
            results=results,
            workloads=[("get_64k_cold", "64 KiB Cold GET")],
            durabilities=["group", "strict", "none"],
            runs=3,
            duration_sec=30.0,
            profile="full",
            minio_drives=4,
            warp_results=warp_results,
        )
        self.assertTrue(report.startswith(f"Command: `{cmd}`\n"))
        self.assertIn("Durability & Setup Table per Server", report)
        self.assertIn("--durability fast", report)
        self.assertIn("4-dir erasure", report)
        self.assertIn("110.0 (100.0..120.0)", report)
        self.assertIn("`warp`", report)
        self.assertIn("Cold-Read Payload Cache Verification", report)

    def test_no_subprocess_pipe_in_market_claims(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "market_claims.py")
        with open(path, encoding="utf-8") as fh:
            src = fh.read()
        self.assertNotIn("stdout=subprocess.PIPE", src)
        self.assertNotIn("stderr=subprocess.PIPE", src)


if __name__ == "__main__":
    unittest.main()
