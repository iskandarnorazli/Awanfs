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


if __name__ == "__main__":
    unittest.main()
