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
        self.assertIn("#29", report)  # AwanFS issue number (was awan-lang #490 before the split)

    def test_report_durability_matches_the_server_default(self):
        # The benchmark starts s3-server without --durability, so it runs with group commit, the
        # default since #29 (ARCHITECTURE.md decision 1): a 200 means the bytes are on stable
        # storage. The report said "no fsync on the write path" until 2026-11-14.
        report = format_markdown_report("cmd", {}, {}, {})
        self.assertNotIn("no fsync on the write path", report)
        self.assertIn("group commit", report)

    def test_the_minio_note_says_what_minio_does_on_each_platform(self):
        from run_warp_benchmark import minio_durability_note
        self.assertIn("syncs each PUT", minio_durability_note("posix"))
        self.assertIn("does not flush", minio_durability_note("nt"))
        self.assertIn("not like for like", minio_durability_note("nt"))

    def test_report_has_no_unsourced_reference_ranges(self):
        report = format_markdown_report("cmd", {}, {}, {})
        self.assertNotIn("Published Ref", report)
        self.assertNotRegex(report, r"~[0-9]")

class TestMachineFacts(unittest.TestCase):
    def test_report_names_what_ran_not_the_original_laptop(self):
        hw = {"os": "Linux 6.8 (x86_64)", "cores": "2", "ram_gb": "7.8",
              "minio_status": "Measured Live (minio version RELEASE.2025-09-07T16-13-09Z)",
              "rustfs_status": "not measured", "affinity_server": "not pinned (2 logical cores; pinning needs 8)"}
        report = format_markdown_report("cmd", hw, {}, {})
        self.assertNotIn("windows/amd64", report)
        self.assertNotIn("32.0 GB", report)
        self.assertNotIn("MinIO and RustFS fsync", report)
        self.assertIn("minio version RELEASE.2025-09-07T16-13-09Z", report)
        self.assertIn("not pinned (2 logical cores", report)

    def test_ram_and_pinning_come_from_the_machine(self):
        import run_warp_benchmark as r
        self.assertNotEqual(r.host_ram_gb(), "32.0" if (os.cpu_count() or 0) < 8 else "never")
        server, client = r.affinity_labels()
        if (os.cpu_count() or 0) < 8:
            self.assertTrue(server.startswith("not pinned"))



class BoundedJoin(unittest.TestCase):
    """#135: a phase worker stuck in a request must not hang the phase, the job or the run."""

    def test_workers_that_finish_are_all_joined_and_none_is_reported_stuck(self):
        import threading
        import run_warp_benchmark as r
        done = threading.Event()
        threads = [threading.Thread(target=done.wait, args=(0.01,), daemon=True) for _ in range(8)]
        for t in threads:
            t.start()
        self.assertEqual(r.join_all(threads, "quick", timeout_sec=10.0), 0)
        self.assertFalse(any(t.is_alive() for t in threads))

    def test_a_stuck_worker_is_counted_and_the_wait_is_bounded(self):
        import threading
        import time
        import run_warp_benchmark as r
        release = threading.Event()  # never set: these threads stand in for a stuck request
        threads = [threading.Thread(target=release.wait, daemon=True) for _ in range(3)]
        quick = [threading.Thread(target=time.sleep, args=(0.01,), daemon=True) for _ in range(2)]
        for t in threads + quick:
            t.start()
        t0 = time.perf_counter()
        stuck = r.join_all(threads + quick, "stuck", timeout_sec=0.5)
        waited = time.perf_counter() - t0
        self.assertEqual(stuck, 3, "the three that never return are counted")
        self.assertLess(waited, 5.0, f"the wait is bounded, took {waited:.2f}s")
        release.set()

    def test_the_phase_workers_are_daemons_so_a_stuck_one_cannot_hold_the_process(self):
        import inspect
        import run_warp_benchmark as r
        src = inspect.getsource(r)
        self.assertEqual(src.count("threading.Thread(target=worker"), 3)
        for call in ("threading.Thread(target=worker, args=(i,), daemon=True)",
                     "threading.Thread(target=worker, daemon=True)",
                     "threading.Thread(target=upload_worker, daemon=True)",
                     "threading.Thread(target=range_get_worker, daemon=True)"):
            self.assertIn(call, src, f"{call} is not a daemon thread")
        # Counted, not asserted on the source, so a failure does not print the whole module.
        self.assertEqual(src.count("        t.join()\n"), 0, "an unbounded join is left in a phase")

    def test_every_phase_joins_through_the_helper(self):
        import inspect
        import run_warp_benchmark as r
        self.assertEqual(inspect.getsource(r).count("join_all(threads, phase_label)"), 5)



class BoundedWarmup(unittest.TestCase):
    """#135: the sequential warm-up gets its own bound and says what happened.

    Runs the real phase against a server that accepts connections and never answers, which is
    what a wedged server looks like to the client.
    """

    def test_a_server_that_never_answers_does_not_hang_the_warm_up(self):
        import socket
        import threading
        import time
        import run_warp_benchmark as r

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(64)
        port = listener.getsockname()[1]
        held = []
        stop = threading.Event()

        def accept_and_ignore():
            listener.settimeout(0.2)
            while not stop.is_set():
                try:
                    c, _ = listener.accept()
                    held.append(c)  # accepted, never answered
                except OSError:
                    pass

        t = threading.Thread(target=accept_and_ignore, daemon=True)
        t.start()
        try:
            t0 = time.perf_counter()
            m = r.run_workload_phase("127.0.0.1", port, "bkt", 1024, 2, 0.2, "put",
                                     server_pid=None, warmup_timeout_sec=2.0)
            took = time.perf_counter() - t0
        finally:
            stop.set()
            for c in held:
                try:
                    c.close()
                except OSError:
                    pass
            listener.close()

        self.assertLess(took, 60.0, f"the phase has to come back; it took {took:.0f}s")
        self.assertFalse(m.measured, "a phase whose warm-up failed is not a measurement")
        self.assertIn("warm-up", m.note, m.note)

    def test_the_warm_up_timeout_is_its_own_bound(self):
        import run_warp_benchmark as r
        self.assertGreater(r.WARMUP_TIMEOUT_SEC, 0)
        self.assertIn("if time.perf_counter() > warm_deadline:", __import__("inspect").getsource(r))



class ServerOutputIsNotAPipe(unittest.TestCase):
    """#135's cause: AwanFS logs a line per request to stderr, and the harness piped stderr
    without reading it. The pipe filled after about 271 requests on Linux and 16 on Windows,
    and the server stopped answering while it held its port."""

    def test_a_child_that_writes_more_than_a_pipe_holds_is_not_blocked(self):
        import os
        import subprocess
        import sys
        import tempfile
        import time
        import run_warp_benchmark as r

        n = 300_000  # several times a 64 KiB Linux pipe, many times a 4 KiB Windows one
        code = f"import sys; sys.stderr.write('x' * {n}); sys.stderr.flush(); print('done')"
        log = os.path.join(tempfile.mkdtemp(prefix="startlogged_"), "server.log")
        p = r.start_logged([sys.executable, "-c", code], log)
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
            self.fail("the child blocked writing its output: a pipe nobody reads")
        self.assertEqual(p.returncode, 0)
        with open(log, "rb") as fh:
            data = fh.read()
        self.assertGreaterEqual(data.count(b"x"), n, "everything it wrote is in the log")
        self.assertIn(b"done", data, "stdout goes to the same log")

    def test_no_benchmark_script_pipes_a_server_it_does_not_read(self):
        import glob
        import os
        here = os.path.dirname(os.path.abspath(__file__))
        for path in sorted(glob.glob(os.path.join(here, "*.py"))):
            if os.path.basename(path).startswith("test_"):
                continue
            with open(path, encoding="utf-8") as fh:
                src = fh.read()
            for bad in ("stdout=subprocess.PIPE", "stderr=subprocess.PIPE"):
                self.assertEqual(src.count(bad), 0, f"{os.path.basename(path)} uses {bad}")



class FailedResponsesAreErrors(unittest.TestCase):
    """A run in which every request answered 404 was reported as 532 ops/s at 0.00 MiB/s and
    passed a memory check (#127, runs 43504508701 and 43504519493). A failed response is an
    error, not an operation."""

    def serve(self, get_status: int):
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_PUT(self):
                n = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(n)
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_GET(self):
                body = b"x" * 1024 if get_status == 200 else b"<Error/>"
                self.send_response(get_status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        return srv

    def run_get_phase(self, status):
        import run_warp_benchmark as r
        srv = self.serve(status)
        try:
            return r.run_workload_phase("127.0.0.1", srv.server_address[1], "bkt", 1024, 2, 0.5, "get",
                                        server_pid=None, warmup_timeout_sec=10.0)
        finally:
            srv.shutdown()
            srv.server_close()

    def test_a_phase_whose_requests_all_fail_reports_no_operations_and_is_not_measured(self):
        m = self.run_get_phase(404)
        self.assertEqual(m.ops_per_sec, 0.0, "a 404 is not an operation")
        self.assertFalse(m.measured)
        self.assertIn("HTTP 404", m.note)

    def test_a_phase_whose_requests_succeed_is_measured(self):
        m = self.run_get_phase(200)
        self.assertGreater(m.ops_per_sec, 0.0)
        self.assertGreater(m.mib_per_sec, 0.0)
        self.assertTrue(m.measured, m.note)




class CpuSamplerTests(unittest.TestCase):
    def test_get_process_cpu_times_returns_non_negative_pair(self):
        from run_warp_benchmark import get_process_cpu_times
        u, s = get_process_cpu_times(os.getpid())
        self.assertGreaterEqual(u, 0.0)
        self.assertGreaterEqual(s, 0.0)

    def test_get_system_cpu_times_returns_valid_busy_and_total(self):
        from run_warp_benchmark import get_system_cpu_times
        busy, total = get_system_cpu_times()
        self.assertGreaterEqual(busy, 0.0)
        self.assertGreaterEqual(total, busy)

    def test_cpu_sampler_measures_client_and_system_cpu_pct(self):
        from run_warp_benchmark import CpuSampler
        with CpuSampler(os.getpid(), every=0.02) as sampler:
            x = 0
            for i in range(400_000):
                x += i * i
        self.assertGreaterEqual(sampler.server_cpu_s, 0.0)
        self.assertGreaterEqual(sampler.client_cpu_s, 0.0)
        self.assertGreaterEqual(sampler.system_cpu_s, 0.0)
        self.assertGreaterEqual(sampler.server_cpu_pct, 0.0)
        self.assertGreaterEqual(sampler.client_cpu_pct, 0.0)
        self.assertGreaterEqual(sampler.system_cpu_pct, 0.0)
        self.assertLessEqual(sampler.system_cpu_pct, 100.0)

    def test_cpu_sampler_without_server_pid_leaves_server_zero(self):
        from run_warp_benchmark import CpuSampler
        with CpuSampler(None, every=0.02) as sampler:
            sum(range(100_000))
        self.assertEqual(sampler.server_cpu_s, 0.0)
        self.assertEqual(sampler.server_cpu_pct, 0.0)

    def test_report_includes_cpu_breakdown_with_percentages(self):
        awan = {
            "get_64k": BenchmarkMetrics(
                ops_per_sec=5000.0, mib_per_sec=312.5,
                p50_ms=1.0, p90_ms=2.0, p99_ms=3.0, p999_ms=4.0,
                peak_rss_mb=40.0,
                server_cpu_s=1.2, client_cpu_s=2.4, system_cpu_s=1.5,
                server_cpu_pct=60.0, client_cpu_pct=120.0, system_cpu_pct=45.0,
            ),
        }
        report = format_markdown_report("cmd", {}, awan, {})
        self.assertIn("CPU Breakdown", report)
        self.assertIn("Server CPU %", report)
        self.assertIn("60.0%", report)
        self.assertIn("120.0%", report)
        self.assertIn("45.0%", report)


class AffinitySplittingTests(unittest.TestCase):
    def test_affinity_masks_split_cores_into_disjoint_halves(self):
        from run_warp_benchmark import affinity_masks
        self.assertEqual(affinity_masks(8), (0x0F, 0xF0))
        srv16, cli16 = affinity_masks(16)
        self.assertEqual((srv16, cli16), (0x00FF, 0xFF00))
        self.assertEqual(srv16 & cli16, 0)
        self.assertEqual(srv16 | cli16, (1 << 16) - 1)
        srv20, cli20 = affinity_masks(20)
        self.assertEqual((srv20, cli20), (0x03FF, 0xFFC00))
        self.assertEqual(srv20 & cli20, 0)
        self.assertEqual(srv20 | cli20, (1 << 20) - 1)

    def test_affinity_labels_reflect_core_count(self):
        import run_warp_benchmark as r
        srv4, cli4 = r.affinity_labels(4)
        self.assertIn("not pinned (4 logical cores", srv4)
        self.assertEqual(srv4, cli4)
        if r.can_pin(16):
            srv16, cli16 = r.affinity_labels(16)
            self.assertEqual(srv16, "Cores 0..7 (Affinity Mask 0x00FF)")
            self.assertEqual(cli16, "Cores 8..15 (Affinity Mask 0xFF00)")

    def test_round_runners_do_not_hardcode_0x0f_affinity(self):
        import inspect
        import run_warp_benchmark as r
        for fn in (r.run_awanfs_round, r.run_minio_or_fallback_round, r.run_rustfs_round):
            src = inspect.getsource(fn)
            self.assertNotIn("0x0F", src, f"{fn.__name__} must use affinity_masks(), not 0x0F")


class MultiRoundAndRssCeilingTests(unittest.TestCase):
    """#133: 3-round median + min..max spread, peak inflight bytes, and RSS ceiling enforcement."""

    def _m(self, mib: float, rss: float, inflight: float = None, measured: bool = True) -> BenchmarkMetrics:
        return BenchmarkMetrics(
            ops_per_sec=mib * 10.0,
            mib_per_sec=mib,
            p50_ms=1.0,
            p90_ms=2.0,
            p99_ms=3.0,
            p999_ms=4.0,
            peak_rss_mb=rss,
            measured=measured,
            peak_inflight_mb=inflight,
        )

    def test_aggregate_rounds_computes_median_and_min_max_spread(self):
        from run_warp_benchmark import aggregate_rounds

        rounds = [
            {"put_10g": self._m(100.0, 410.0, inflight=12.0)},
            {"put_10g": self._m(120.0, 390.0, inflight=8.0)},
            {"put_10g": self._m(110.0, 450.0, inflight=10.0)},
        ]
        agg = aggregate_rounds(rounds)
        m = agg["put_10g"]
        self.assertAlmostEqual(m.mib_per_sec, 110.0)
        self.assertAlmostEqual(m.peak_rss_mb, 410.0)
        self.assertAlmostEqual(m.peak_rss_min_mb, 390.0)
        self.assertAlmostEqual(m.peak_rss_max_mb, 450.0)
        self.assertAlmostEqual(m.peak_inflight_mb, 10.0)

        report = format_markdown_report(
            "cmd --runs 3",
            {},
            agg,
            minio_or_baseline=aggregate_rounds([
                {"put_10g": self._m(90.0, 900.0)},
                {"put_10g": self._m(95.0, 950.0)},
                {"put_10g": self._m(92.0, 920.0)},
            ]),
            runs=3,
        )
        self.assertIn("Rounds per Server:** 3", report)
        self.assertIn("410.0 MB (390.0..450.0)", report)
        self.assertIn("920.0 MB (900.0..950.0)", report)
        self.assertIn("Peak In-Flight Request Bytes (AwanFS)", report)
        self.assertIn("| `put_10g` | 10.00 | 410.0 MB (390.0..450.0) |", report)

    def test_aggregate_rounds_preserves_cpu_percentages(self):
        from run_warp_benchmark import aggregate_rounds

        rounds = [
            {"get_64k": BenchmarkMetrics(1000.0, 62.5, 1.0, 2.0, 3.0, 4.0, 40.0, server_cpu_s=1.0, client_cpu_s=2.0, system_cpu_s=0.5, server_cpu_pct=50.0, client_cpu_pct=100.0, system_cpu_pct=20.0)},
            {"get_64k": BenchmarkMetrics(1200.0, 75.0, 1.0, 2.0, 3.0, 4.0, 42.0, server_cpu_s=1.4, client_cpu_s=2.6, system_cpu_s=0.7, server_cpu_pct=70.0, client_cpu_pct=130.0, system_cpu_pct=30.0)},
            {"get_64k": BenchmarkMetrics(1100.0, 68.8, 1.0, 2.0, 3.0, 4.0, 41.0, server_cpu_s=1.2, client_cpu_s=2.2, system_cpu_s=0.6, server_cpu_pct=60.0, client_cpu_pct=110.0, system_cpu_pct=25.0)},
        ]
        m = aggregate_rounds(rounds)["get_64k"]
        self.assertAlmostEqual(m.server_cpu_pct, 60.0)
        self.assertAlmostEqual(m.client_cpu_pct, 110.0)
        self.assertAlmostEqual(m.system_cpu_pct, 25.0)

    def test_check_rss_ceiling_passes_under_and_fails_over_ceiling(self):
        from run_warp_benchmark import check_rss_ceiling

        under = {"mpu_put_1g": self._m(500.0, 320.0), "put_10g": self._m(400.0, 480.0)}
        peak, bad = check_rss_ceiling(under, 500.0)
        self.assertAlmostEqual(peak, 480.0)
        self.assertEqual(bad, [])

        over = {"mpu_put_1g": self._m(500.0, 320.0), "put_10g": self._m(400.0, 610.5)}
        peak_over, bad_over = check_rss_ceiling(over, 500.0)
        self.assertAlmostEqual(peak_over, 610.5)
        self.assertEqual(len(bad_over), 1)
        self.assertIn("610.5", bad_over[0])
        self.assertIn("500.0", bad_over[0])

        _, bad_empty = check_rss_ceiling({}, 500.0)
        self.assertEqual(len(bad_empty), 1)
        self.assertIn("not measured", bad_empty[0])

        ok_md = format_markdown_report("cmd", {}, under, minio_or_baseline={"put_10g": self._m(200.0, 900.0)}, rss_ceiling_mib=500.0)
        self.assertIn("Peak RSS Ceiling Check", ok_md)
        self.assertIn("| AwanFS | 480.0 | 500.0 | ok |", ok_md)

        over_md = format_markdown_report("cmd", {}, over, minio_or_baseline={"put_10g": self._m(200.0, 900.0)}, rss_ceiling_mib=500.0)
        self.assertIn("| AwanFS | 610.5 | 500.0 | OVER THE CEILING |", over_md)

    def test_get_awanfs_peak_inflight_mb_reads_metrics_endpoint(self):
        import threading
        from http.server import BaseHTTPRequestHandler, HTTPServer
        from run_warp_benchmark import get_awanfs_peak_inflight_mb

        body = b"# TYPE awanfs_peak_inflight_bytes gauge\nawanfs_peak_inflight_bytes 8388608\n"

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        srv = HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            got = get_awanfs_peak_inflight_mb("127.0.0.1", srv.server_address[1])
            self.assertAlmostEqual(got, 8.0)
        finally:
            srv.shutdown()
            srv.server_close()

    def test_selfhosted_workflow_passes_runs_and_rss_ceiling(self):
        wf_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", ".github", "workflows", "bench-selfhosted.yml")
        )
        with open(wf_path, encoding="utf-8") as fh:
            wf = fh.read()
        self.assertIn("--runs", wf)
        self.assertIn("--rss-ceiling-mib", wf)


class SmallPartsFlagForMpu1mTests(unittest.TestCase):
    """#111 (owner decision 2026-11-19): mpu_1m passes --allow-small-parts while default flags stay intact elsewhere."""

    def test_default_server_argv_omits_allow_small_parts(self):
        import run_warp_benchmark as r
        argv = r.build_awanfs_server_argv("s3-server", 9000, "/tmp/data")
        self.assertEqual(argv, ["s3-server", "--port", "9000", "--data-dir", "/tmp/data", "--allow-anonymous"])
        self.assertNotIn("--allow-small-parts", argv)
        self.assertNotIn("--durability", argv)

    def test_mpu_1m_server_argv_adds_allow_small_parts(self):
        import run_warp_benchmark as r
        argv = r.build_awanfs_server_argv("s3-server", 9001, "/tmp/mpu", allow_small_parts=True)
        self.assertIn("--allow-small-parts", argv)
        self.assertEqual(
            argv,
            ["s3-server", "--port", "9001", "--data-dir", "/tmp/mpu", "--allow-anonymous", "--allow-small-parts"],
        )

    def test_run_awanfs_mpu_1m_phase_launches_server_with_allow_small_parts(self):
        from unittest import mock
        import run_warp_benchmark as r

        launched_argvs = []
        fake_proc = mock.MagicMock()
        fake_proc.pid = 4242

        class FakeConn:
            def __init__(self, *a, **kw):
                pass
            def request(self, *a, **kw):
                pass
            def getresponse(self):
                resp = mock.MagicMock()
                resp.status = 200
                resp.read.return_value = b""
                return resp
            def close(self):
                pass

        expected_metrics = BenchmarkMetrics(100.0, 100.0, 1.0, 2.0, 3.0, 4.0, 32.0, measured=True)

        with mock.patch.object(r, "start_logged", side_effect=lambda argv, log: launched_argvs.append(argv) or fake_proc), \
             mock.patch.object(r, "wait_for_port", return_value=True), \
             mock.patch.object(r, "set_affinity_mask"), \
             mock.patch.object(r.http.client, "HTTPConnection", FakeConn), \
             mock.patch.object(r, "run_multipart_upload_duration_phase", return_value=expected_metrics) as mock_phase:
            got = r.run_awanfs_mpu_1m_phase("s3-server", "warp-bench-bucket", 2.0, 0x0F)

        self.assertEqual(got, expected_metrics)
        self.assertEqual(len(launched_argvs), 1)
        self.assertIn("--allow-small-parts", launched_argvs[0])
        self.assertEqual(mock_phase.call_args[0][3:5], (256 * 1024, 4))
        fake_proc.terminate.assert_called_once()
        fake_proc.wait.assert_called_once()

    def test_main_uses_default_flags_for_main_server_and_dedicated_helper_for_mpu_1m(self):
        import inspect
        import run_warp_benchmark as r
        # #133 moved the AwanFS run from main() into run_awanfs_round().
        round_src = inspect.getsource(r.run_awanfs_round)
        self.assertIn("build_awanfs_server_argv(server_bin, port, s3_data_dir)", round_src)
        self.assertIn("run_awanfs_mpu_1m_phase(", round_src)
        self.assertIn("skip_mpu_1m=True", round_src)

    def test_multipart_duration_phase_marks_400_errors_as_not_measured(self):
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        import run_warp_benchmark as r

        class RejectCompleteHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def log_message(self, *a):
                pass
            def do_POST(self):
                n = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(n)
                if "?uploads" in self.path:
                    body = b"<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>"
                    self.send_response(200)
                else:
                    body = b"<Error><Code>EntityTooSmall</Code></Error>"
                    self.send_response(400)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def do_PUT(self):
                n = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(n)
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.send_header("ETag", '"abc"')
                self.end_headers()

        srv = ThreadingHTTPServer(("127.0.0.1", 0), RejectCompleteHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            m = r.run_multipart_upload_duration_phase("127.0.0.1", srv.server_address[1], "bkt", 1024, 2, 2, 0.25)
        finally:
            srv.shutdown()
            srv.server_close()
        self.assertFalse(m.measured)
        self.assertEqual(m.mib_per_sec, 0.0)
        self.assertEqual(m.ops_per_sec, 0.0)
        self.assertIn("HTTP 400", m.note)


class SizeFlagDecouplingTests(unittest.TestCase):
    """#57: requesting 1GiB in bench-full runs the 1 GiB multipart workload without 10GiB."""

    def test_one_gib_flag_does_not_enable_ten_gib(self):
        import inspect
        import run_warp_benchmark as r
        # #133 moved the size phases from main() into run_server_suite().
        src = inspect.getsource(r.run_server_suite)
        self.assertIn('if "1GiB" in size_set:', src)
        self.assertIn('if "10GiB" in size_set:', src)
        self.assertNotIn('"1GiB" in sizes or "10GiB" in sizes', src)


class MemorySplitParsingTests(unittest.TestCase):
    """#274: parse Linux /proc/<pid>/status and Windows psutil memory_full_info() splits."""

    AWANFS_PROC_STATUS = (
        "Name:\ts3-server\n"
        "VmHWM:\t 1193984 kB\n"
        "VmRSS:\t 1180000 kB\n"
        "RssAnon:\t   40960 kB\n"
        "RssFile:\t 1139024 kB\n"
        "RssShmem:\t       0 kB\n"
    )
    MINIO_PROC_STATUS = (
        "Name:\tminio\n"
        "VmHWM:\t 1018880 kB\n"
        "VmRSS:\t 1005000 kB\n"
        "RssAnon:\t  950272 kB\n"
        "RssFile:\t   54728 kB\n"
        "RssShmem:\t       0 kB\n"
    )

    def test_parse_proc_status_linux_fixed_samples(self):
        import run_warp_benchmark as r
        awan = r.parse_proc_status(self.AWANFS_PROC_STATUS)
        self.assertAlmostEqual(awan["VmHWM"], 1193984 / 1024.0)
        self.assertAlmostEqual(awan["VmRSS"], 1180000 / 1024.0)
        self.assertAlmostEqual(awan["RssAnon"], 40.0)
        self.assertAlmostEqual(awan["RssFile"], 1139024 / 1024.0)
        self.assertEqual(awan["RssShmem"], 0.0)

        minio = r.parse_proc_status(self.MINIO_PROC_STATUS)
        self.assertAlmostEqual(minio["VmHWM"], 995.0)
        self.assertAlmostEqual(minio["VmRSS"], 1005000 / 1024.0)
        self.assertAlmostEqual(minio["RssAnon"], 928.0)
        self.assertAlmostEqual(minio["RssFile"], 54728 / 1024.0)

        self.assertEqual(r.parse_proc_status("RssAnon:\t12 MB\nRssFile:\tbad kB\n"), {})

    def test_parse_windows_memory_info_fixed_samples(self):
        import types
        import run_warp_benchmark as r
        mib = 1024 * 1024
        awan_info = types.SimpleNamespace(private=48 * mib, wset=268 * mib, peak_wset=270 * mib)
        minio_info = types.SimpleNamespace(private=980 * mib, wset=1050 * mib, peak_wset=1052 * mib)

        got_awan = r.parse_windows_memory_info(awan_info)
        self.assertEqual(got_awan, {"private": 48.0, "wset": 268.0, "peak_wset": 270.0})
        got_minio = r.parse_windows_memory_info(minio_info)
        self.assertEqual(got_minio, {"private": 980.0, "wset": 1050.0, "peak_wset": 1052.0})
        self.assertEqual(r.parse_windows_memory_info(types.SimpleNamespace(private=-1, wset="bad")), {})

    def test_peak_rss_sampler_tracks_linux_peak_across_samples(self):
        import tempfile
        import time
        import run_warp_benchmark as r

        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "status")
            with open(p, "w", encoding="ascii") as fh:
                fh.write("VmHWM:\t 102400 kB\nVmRSS:\t 102400 kB\nRssAnon:\t 65536 kB\nRssFile:\t 36864 kB\n")
            sampler = r.PeakRssSampler(1234, every=0.01, status_path=p)
            with sampler:
                time.sleep(0.05)
                with open(p, "w", encoding="ascii") as fh:
                    fh.write("VmHWM:\t 204800 kB\nVmRSS:\t 196608 kB\nRssAnon:\t 32768 kB\nRssFile:\t 163840 kB\n")
                time.sleep(0.05)
            self.assertAlmostEqual(sampler.peak_rss_mb, 200.0)
            self.assertAlmostEqual(sampler.rss_anon_mb, 64.0)
            self.assertAlmostEqual(sampler.rss_file_mb, 160.0)
            self.assertIsNone(sampler.win_private_mb)
            self.assertIsNone(sampler.win_wset_mb)

    def test_peak_rss_sampler_tracks_windows_psutil_peak_across_samples(self):
        import time
        import types
        from unittest import mock
        import run_warp_benchmark as r

        mib = 1024 * 1024
        samples = iter([
            types.SimpleNamespace(private=64 * mib, wset=120 * mib, peak_wset=120 * mib),
            types.SimpleNamespace(private=40 * mib, wset=200 * mib, peak_wset=205 * mib),
        ])
        last = [None]

        class FakeProc:
            def __init__(self, pid):
                self.pid = pid

            def memory_full_info(self):
                try:
                    last[0] = next(samples)
                except StopIteration:
                    pass
                return last[0]

        fake_psutil = types.SimpleNamespace(Process=FakeProc, NoSuchProcess=RuntimeError, AccessDenied=PermissionError)
        with mock.patch.object(r.sys, "platform", "win32"), \
             mock.patch.dict(sys.modules, {"psutil": fake_psutil}):
            sampler = r.PeakRssSampler(9999, every=0.01)
            with sampler:
                time.sleep(0.05)
        self.assertAlmostEqual(sampler.win_private_mb, 64.0)
        self.assertAlmostEqual(sampler.win_wset_mb, 200.0)
        self.assertAlmostEqual(sampler.peak_rss_mb, 205.0)
        self.assertIsNone(sampler.rss_file_mb)
        self.assertIsNone(sampler.rss_anon_mb)

    def test_missing_or_exited_process_marks_split_not_measured(self):
        import types
        from unittest import mock
        import run_warp_benchmark as r

        missing_linux = r.PeakRssSampler(1234, status_path="/nonexistent/proc/status")
        with missing_linux:
            pass
        self.assertIsNone(missing_linux.rss_file_mb)
        self.assertIsNone(missing_linux.rss_anon_mb)

        class ExitedError(Exception):
            pass

        class ExitedProc:
            def __init__(self, pid):
                raise ExitedError("gone")

        fake_psutil = types.SimpleNamespace(Process=ExitedProc, NoSuchProcess=ExitedError, AccessDenied=PermissionError)
        with mock.patch.object(r.sys, "platform", "win32"), \
             mock.patch.dict(sys.modules, {"psutil": fake_psutil}):
            self.assertEqual(r.sample_windows_memory(999999), {})
            exited_win = r.PeakRssSampler(999999)
            with exited_win:
                pass
        self.assertIsNone(exited_win.win_private_mb)
        self.assertIsNone(exited_win.win_wset_mb)

    @unittest.skipUnless(sys.platform == "win32", "Windows live memory split")
    def test_windows_live_child_measures_private_and_wset(self):
        import subprocess
        import tempfile
        import time
        import run_warp_benchmark as r

        with tempfile.TemporaryDirectory() as d:
            f = os.path.join(d, "mapped.bin")
            with open(f, "wb") as fh:
                fh.write(os.urandom(32 << 20))
            code = (
                "import mmap, sys, time\n"
                "heap = bytearray(48 << 20)\n"
                "for i in range(0, len(heap), 4096): heap[i] = 1\n"
                "fh = open(sys.argv[1], 'rb')\n"
                "m = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)\n"
                "s = sum(m[i] for i in range(0, len(m), 4096))\n"
                "print('ready', s, flush=True); time.sleep(20)\n"
            )
            p = subprocess.Popen([sys.executable, "-c", code, f], stdout=subprocess.PIPE, text=True)
            try:
                self.assertTrue(p.stdout.readline().startswith("ready"))
                with r.PeakRssSampler(p.pid, every=0.02) as sampler:
                    time.sleep(0.1)
                self.assertGreaterEqual(sampler.win_private_mb, 44.0)
                self.assertGreaterEqual(sampler.win_wset_mb, 70.0)
                self.assertGreater(sampler.win_wset_mb, sampler.win_private_mb)
            finally:
                if p.stdout is not None:
                    p.stdout.close()
                p.kill()
                p.wait()


class MemorySplitReportTests(unittest.TestCase):
    """#274: run_warp_benchmark report includes split next to peak RSS and handles missing samples."""

    def test_report_includes_linux_rss_file_and_anon_next_to_total(self):
        awan = {
            "get_1m": BenchmarkMetrics(
                4000.0, 4000.0, 1.0, 2.0, 3.0, 4.0, 1166.0,
                peak_rss_file_mb=1110.0, peak_rss_anon_mb=42.0,
            ),
        }
        minio = {
            "get_1m": BenchmarkMetrics(
                2000.0, 2000.0, 2.0, 4.0, 6.0, 8.0, 995.0,
                peak_rss_file_mb=53.0, peak_rss_anon_mb=928.0,
            ),
        }
        report = format_markdown_report("cmd", {"os": "Linux 6.8 (x86_64)"}, awan, minio)
        self.assertIn("File-Backed (RssFile", report)
        self.assertIn("Anonymous (RssAnon", report)
        self.assertIn("**1166.0 MB** | **1110.0 MB** | **42.0 MB**", report)
        self.assertIn("995.0 MB | 53.0 MB | 928.0 MB", report)
        self.assertIn("RssFile 1110.0 MB, RssAnon 42.0 MB", report)
        self.assertIn("RssFile 53.0 MB, RssAnon 928.0 MB", report)

    def test_report_includes_windows_private_and_wset_labelled_explicitly(self):
        awan = {
            "put_64k": BenchmarkMetrics(
                3700.0, 234.0, 15.0, 26.0, 37.0, 44.0, 88.3,
                peak_win_private_mb=41.2, peak_win_wset_mb=88.0,
            ),
        }
        minio = {
            "put_64k": BenchmarkMetrics(
                520.0, 32.6, 57.0, 330.0, 587.0, 656.0, 500.8,
                peak_win_private_mb=470.5, peak_win_wset_mb=500.1,
            ),
        }
        report = format_markdown_report("cmd", {"os": "Windows 11 (AMD64)"}, awan, minio)
        self.assertIn("Private Bytes (private", report)
        self.assertIn("Working Set (wset", report)
        self.assertIn("**88.3 MB** | **41.2 MB** | **88.0 MB**", report)
        self.assertIn("500.8 MB | 470.5 MB | 500.1 MB", report)
        self.assertIn("private 41.2 MB, wset 88.0 MB", report)
        self.assertIn("private 470.5 MB, wset 500.1 MB", report)

    def test_report_marks_missing_split_samples_not_measured(self):
        awan = {"put_64k": BenchmarkMetrics(1000.0, 62.5, 1.0, 2.0, 3.0, 4.0, 0.0)}
        win_report = format_markdown_report("cmd", {"os": "Windows 11"}, awan, {})
        self.assertIn("Private Bytes (private", win_report)
        self.assertIn("Working Set (wset", win_report)
        self.assertIn("not measured", win_report)

        linux_report = format_markdown_report("cmd", {"os": "Linux 6.8"}, awan, {})
        self.assertIn("File-Backed (RssFile", linux_report)
        self.assertIn("Anonymous (RssAnon", linux_report)
        self.assertIn("not measured", linux_report)


class LargeFixedVolumePhaseTests(unittest.TestCase):
    """#261: fixed-volume phases must bound idle stalls, not total transfer time, and never swallow errors."""

    def test_fixed_volume_phase_completes_when_total_exceeds_join_timeout_while_making_progress(self):
        import threading
        import time
        from http.server import BaseHTTPRequestHandler
        from unittest import mock
        import run_warp_benchmark as r

        class SlowPartHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_PUT(self):
                n = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(n)
                time.sleep(0.25)
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

        srv = r.ThreadedHTTPServer(("127.0.0.1", 0), SlowPartHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            # 8 parts * 0.25s = ~2.0s total (> 0.8s join timeout), while each part completes in 0.25s (< 0.8s stall).
            with mock.patch.object(r, "PHASE_JOIN_TIMEOUT_SEC", 0.8), \
                 mock.patch.object(r.join_all, "__defaults__", (0.8,)):
                m = r.run_fixed_volume_phase(
                    "127.0.0.1", srv.server_address[1], "warp-bench-bucket",
                    8192, 1024, 1, "put", server_pid=None, key_modulus=2,
                )
        finally:
            srv.shutdown()
            srv.server_close()

        self.assertTrue(m.measured, f"phase making steady progress should complete, got note={m.note!r}")
        self.assertGreater(m.ops_per_sec, 0.0)
        self.assertGreater(m.mib_per_sec, 0.0)

    def test_fixed_volume_phase_records_errors_instead_of_swallowing(self):
        import threading
        from http.server import BaseHTTPRequestHandler
        import run_warp_benchmark as r

        count_lock = threading.Lock()
        seen = {"n": 0}

        class FlakyHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_PUT(self):
                n = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(n)
                with count_lock:
                    seen["n"] += 1
                    cur = seen["n"]
                status = 500 if cur == 2 else 200
                body = b"<Error/>" if status != 200 else b""
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if body:
                    self.wfile.write(body)

        srv = r.ThreadedHTTPServer(("127.0.0.1", 0), FlakyHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            m = r.run_fixed_volume_phase(
                "127.0.0.1", srv.server_address[1], "warp-bench-bucket",
                4096, 1024, 2, "put", server_pid=None, key_modulus=2,
            )
        finally:
            srv.shutdown()
            srv.server_close()

        self.assertFalse(m.measured, "a phase with HTTP 500 responses must not report measured=True")
        self.assertIn("500", m.note, m.note)

    def test_fixed_volume_phase_records_connection_exceptions_instead_of_swallowing(self):
        import socket
        import threading
        from http.server import BaseHTTPRequestHandler
        import run_warp_benchmark as r

        count_lock = threading.Lock()
        seen = {"n": 0}

        class DropSecondHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_PUT(self):
                n = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(n)
                with count_lock:
                    seen["n"] += 1
                    cur = seen["n"]
                if cur == 2:
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    self.connection.close()
                    return
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

        srv = r.ThreadedHTTPServer(("127.0.0.1", 0), DropSecondHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            m = r.run_fixed_volume_phase(
                "127.0.0.1", srv.server_address[1], "warp-bench-bucket",
                4096, 1024, 2, "put", server_pid=None, key_modulus=2,
            )
        finally:
            srv.shutdown()
            srv.server_close()

        self.assertFalse(m.measured, "a phase with dropped connections must not report measured=True")
        self.assertTrue(m.note, "expected error details in note")

    def test_fixed_volume_phase_stops_when_stalled_with_no_progress(self):
        import socket
        import threading
        import time
        from unittest import mock
        import run_warp_benchmark as r

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(16)
        port = listener.getsockname()[1]
        held = []
        stop = threading.Event()

        def accept_and_ignore():
            listener.settimeout(0.1)
            while not stop.is_set():
                try:
                    c, _ = listener.accept()
                    held.append(c)
                except OSError:
                    pass

        threading.Thread(target=accept_and_ignore, daemon=True).start()
        try:
            t0 = time.perf_counter()
            req_patch = (
                mock.patch.object(r, "LARGE_PART_REQUEST_TIMEOUT_SEC", 0.5)
                if hasattr(r, "LARGE_PART_REQUEST_TIMEOUT_SEC")
                else mock.MagicMock()
            )
            with mock.patch.object(r, "PHASE_JOIN_TIMEOUT_SEC", 0.4), \
                 mock.patch.object(r.join_all, "__defaults__", (0.4,)), \
                 req_patch:
                m = r.run_fixed_volume_phase(
                    "127.0.0.1", port, "warp-bench-bucket",
                    4096, 1024, 2, "put", server_pid=None, key_modulus=2,
                )
            elapsed = time.perf_counter() - t0
        finally:
            stop.set()
            for c in held:
                try:
                    c.close()
                except OSError:
                    pass
            listener.close()

        self.assertFalse(m.measured)
        self.assertLess(elapsed, 10.0, f"stalled fixed-volume phase took too long: {elapsed:.2f}s")

    def test_large_part_request_timeout_exceeds_30s(self):
        import inspect
        import run_warp_benchmark as r
        self.assertGreaterEqual(r.LARGE_PART_REQUEST_TIMEOUT_SEC, 120.0)
        self.assertNotIn("timeout=30.0", inspect.getsource(r.run_fixed_volume_phase))
        self.assertNotIn("timeout=30.0", inspect.getsource(r.run_large_multipart_object_phase))

    def test_large_multipart_object_phase_records_errors_without_crashing(self):
        import threading
        from http.server import BaseHTTPRequestHandler
        import run_warp_benchmark as r

        class FailPartHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_POST(self):
                n = int(self.headers.get("Content-Length", "0"))
                if n > 0:
                    self.rfile.read(n)
                if "?uploads" in self.path:
                    xml = b"<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>"
                else:
                    xml = b'<CompleteMultipartUploadResult><ETag>"abc-2"</ETag></CompleteMultipartUploadResult>'
                self.send_response(200)
                self.send_header("Content-Length", str(len(xml)))
                self.end_headers()
                self.wfile.write(xml)

            def do_PUT(self):
                n = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(n)
                self.send_response(500)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_GET(self):
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()

        srv = r.ThreadedHTTPServer(("127.0.0.1", 0), FailPartHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            put_m, get_m = r.run_large_multipart_object_phase(
                "127.0.0.1", srv.server_address[1], "warp-bench-bucket",
                "obj.bin", 2048, 1024, 2, server_pid=None,
            )
        finally:
            srv.shutdown()
            srv.server_close()

        self.assertFalse(put_m.measured)
        self.assertIn("500", put_m.note, put_m.note)
        self.assertFalse(get_m.measured)
        self.assertIn("404", get_m.note, get_m.note)


if __name__ == "__main__":
    unittest.main()
