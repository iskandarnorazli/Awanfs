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


class HeapAndFilePages(unittest.TestCase):
    """#127: peak RSS split into heap (RssAnon) and pages of mapped files (RssFile)."""

    STATUS = ("Name:\tawanfs\nVmHWM:\t  555000 kB\nVmRSS:\t  540000 kB\n"
              "RssAnon:\t  262144 kB\nRssFile:\t  276480 kB\nRssShmem:\t       0 kB\n")

    def test_the_status_fields_parse_to_mib(self):
        got = br.parse_status(self.STATUS)
        self.assertEqual(got, {"RssAnon": 256.0, "RssFile": 270.0, "RssShmem": 0.0})
        self.assertEqual(br.parse_status("RssAnon:\t12 MB\nRssFile: x kB\n"), {}, "only kB counts are read")

    @unittest.skipUnless(sys.platform.startswith("linux"), "/proc/<pid>/status is Linux")
    def test_a_process_that_maps_a_file_shows_it_as_file_pages_not_heap(self):
        import subprocess
        import tempfile
        import time
        d = tempfile.mkdtemp(prefix="rsssplit_")
        data = os.path.join(d, "f.bin")
        with open(data, "wb") as fh:
            fh.write(os.urandom(48 << 20))
        code = (
            "import mmap,sys,time\n"
            "heap = bytearray(96 << 20)\n"
            "for i in range(0, len(heap), 4096): heap[i] = 1\n"
            "f = open(sys.argv[1], 'rb'); m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)\n"
            "s = 0\n"
            "for i in range(0, len(m), 4096): s += m[i]\n"
            "print('ready', flush=True); time.sleep(30)\n"
        )
        p = subprocess.Popen([sys.executable, "-c", code, data], stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(p.stdout.readline().strip(), "ready")
            with br.RssSplit(p.pid) as split:
                time.sleep(0.3)
            self.assertGreaterEqual(split.anon, 90.0, f"heap: {split.anon}")
            self.assertGreaterEqual(split.file, 44.0, f"file pages: {split.file}")
            self.assertLess(split.file, split.anon, "the 48 MiB mapping is not counted as the 96 MiB heap")
        finally:
            p.kill()
            p.wait()

    def test_without_proc_the_split_is_not_measured(self):
        split = br.RssSplit(2 ** 22 - 7)  # a pid that does not exist
        with split:
            pass
        self.assertIsNone(split.anon)
        self.assertIsNone(split.file)
        self.assertEqual(br.mib_or_nm(None), "not measured")


class CacheShare(unittest.TestCase):
    PAGE = ("# HELP awanfs_payload_cache_bytes Bytes held by the payload cache.\n"
            "# TYPE awanfs_payload_cache_bytes gauge\nawanfs_payload_cache_bytes 134217728\n"
            "awanfs_payload_cache_bytes_total 7\n")

    def test_a_metric_is_read_by_its_exact_name(self):
        self.assertEqual(br.metric(self.PAGE, "awanfs_payload_cache_bytes"), 134217728.0)
        self.assertIsNone(br.metric(self.PAGE, "awanfs_missing"))
        self.assertEqual(br.metric("awanfs_payload_cache_bytes_total 7\n", "awanfs_payload_cache_bytes"), None,
                         "a longer name with the same prefix is not the metric")

    def test_no_server_means_not_measured(self):
        self.assertIsNone(br.cache_mib(1))


class Cli(unittest.TestCase):
    def test_the_parsers_defaults_are_the_workflow_steps_values(self):
        a = br.build_parser().parse_args(["--server-bin", "x"])
        self.assertEqual((a.budget, a.budget_mib, a.concurrency, a.size, a.duration, a.overhead),
                         ("256MiB", 256.0, 64, "1MiB", 10.0, 200.0))

    def test_server_env_is_off_unless_asked_and_refuses_a_malformed_pair(self):
        self.assertEqual(br.build_parser().parse_args(["--server-bin", "x"]).server_env, [])
        a = br.build_parser().parse_args(["--server-bin", "x", "--server-env", "MALLOC_ARENA_MAX=2",
                                          "--server-env", "MALLOC_MMAP_THRESHOLD_=131072"])
        self.assertEqual(br.server_env(a.server_env), {"MALLOC_ARENA_MAX": "2", "MALLOC_MMAP_THRESHOLD_": "131072"})
        for bad in (["MALLOC_ARENA_MAX"], ["=2"]):
            with self.assertRaises(SystemExit):
                br.server_env(bad)

    def test_the_server_gets_the_extra_environment(self):
        import inspect
        src = inspect.getsource(br.start_server)
        self.assertIn("env=env", src)
        self.assertIn("dict(os.environ, **(env_extra or {}))", src)

    @unittest.skipUnless(sys.platform.startswith("linux"), "/proc/<pid>/environ is Linux")
    def test_a_child_started_with_the_extra_environment_has_it(self):
        import subprocess
        p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(10)"],
                             env=dict(os.environ, MALLOC_ARENA_MAX="2"))
        q = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(10)"],
                             env={k: v for k, v in os.environ.items() if k != "MALLOC_ARENA_MAX"})
        try:
            self.assertEqual(br.process_env(p.pid, "MALLOC_ARENA_MAX"), "2")
            self.assertEqual(br.process_env(q.pid, "MALLOC_ARENA_MAX"), "unset")
            self.assertIsNone(br.process_env(2 ** 22 - 7, "MALLOC_ARENA_MAX"))
        finally:
            for x in (p, q):
                x.kill()
                x.wait()

    def test_an_unknown_size_is_refused(self):
        with self.assertRaises(SystemExit):
            br.build_parser().parse_args(["--server-bin", "x", "--size", "7MiB"])

    def test_the_module_states_its_exit_codes(self):
        self.assertIn("Exit codes: 0 under the ceiling, 1 over it", br.__doc__)

    def test_unique_keys_defaults_false_and_can_be_enabled(self):
        self.assertFalse(br.build_parser().parse_args(["--server-bin", "x"]).unique_keys)
        self.assertTrue(br.build_parser().parse_args(["--server-bin", "x", "--unique-keys"]).unique_keys)


class UniqueKeys(unittest.TestCase):
    """#218: --unique-keys writes a new key on every PUT instead of reusing the warm-up pool."""

    def test_without_unique_keys_the_pool_repeats_as_before(self):
        import run_warp_benchmark as rwb
        keys = [rwb.object_key(0, i, 8, unique_keys=False) for i in range(16)]
        self.assertEqual(keys[:8], [f"worker_0_obj_{i}.dat" for i in range(8)])
        self.assertEqual(keys[8:], keys[:8])

    def test_with_unique_keys_no_key_repeats_across_workers(self):
        import run_warp_benchmark as rwb
        seen = []
        for w in range(8):
            for i in range(50):
                seen.append(rwb.object_key(w, i, 8, unique_keys=True))
        self.assertEqual(len(seen), 400)
        self.assertEqual(len(set(seen)), 400)

    def test_run_workload_phase_uses_unique_keys_only_for_put(self):
        import http.server
        import socketserver
        import threading
        import run_warp_benchmark as rwb

        recorded = []
        lock = threading.Lock()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_PUT(self):
                n = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(n)
                with lock:
                    recorded.append(("PUT", self.path))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_GET(self):
                with lock:
                    recorded.append(("GET", self.path))
                body = b"x" * 64
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True

        srv = Server(("127.0.0.1", 0), Handler)
        port = srv.server_address[1]
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            recorded.clear()
            rwb.run_workload_phase("127.0.0.1", port, "budget", 64, 2, 0.25, "put", unique_keys=True)
            puts = [p for m, p in recorded if m == "PUT"]
            warmup_puts = puts[:16]
            phase_puts = puts[16:]
            self.assertGreater(len(phase_puts), 0)
            self.assertEqual(len(phase_puts), len(set(phase_puts)), "unique-keys PUTs must not repeat")
            self.assertTrue(set(phase_puts).isdisjoint(set(warmup_puts)))

            recorded.clear()
            rwb.run_workload_phase("127.0.0.1", port, "budget", 64, 2, 0.25, "get", unique_keys=True)
            gets = [p for m, p in recorded if m == "GET"]
            self.assertGreater(len(gets), 0)
            self.assertTrue(all("_obj_" in p for p in gets), f"GETs should hit pool, got {gets[:5]}")

            recorded.clear()
            rwb.run_workload_phase("127.0.0.1", port, "budget", 64, 2, 0.25, "put", unique_keys=False)
            puts_default = [p for m, p in recorded if m == "PUT"][16:]
            self.assertGreater(len(puts_default), 0)
            self.assertTrue(all("_obj_" in p for p in puts_default))
        finally:
            srv.shutdown()
            srv.server_close()


class WithoutProcTests(unittest.TestCase):
    """Experiment 5 (#175): budget_rss.py runs cleanly when /proc/<pid>/status is absent (Windows)."""

    def test_main_runs_cleanly_when_proc_files_are_absent(self):
        import tempfile
        import shutil
        from unittest import mock
        import http.server
        import socketserver
        import threading
        import subprocess

        class FakeServerHandler(http.server.BaseHTTPRequestHandler):
            def do_PUT(self):
                n = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(n)
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_GET(self):
                if self.path == "/_awanfs/metrics":
                    body = b"awanfs_payload_cache_bytes 1048576\n"
                else:
                    body = b"x" * 65536
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True

        srv = Server(("127.0.0.1", 0), FakeServerHandler)
        port = srv.server_address[1]
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        d = tempfile.mkdtemp(prefix="budget_noproc_")
        report = os.path.join(d, "report.md")
        try:
            class NoProcSplit(br.RssSplit):
                def __init__(self, pid, every=0.05):
                    super().__init__(pid, every=every)
                    self.path = os.path.join(d, "nonexistent_proc_status")

            with mock.patch.object(br, "start_server", return_value=(proc, port)), \
                 mock.patch.object(br, "RssSplit", NoProcSplit), \
                 mock.patch.object(br, "process_env", return_value=None):
                rc = br.main([
                    "--server-bin", "dummy",
                    "--budget", "256MiB", "--budget-mib", "256",
                    "--concurrency", "2", "--size", "64KiB", "--duration", "0.25",
                    "--overhead", "200", "--data-dir", d,
                    "--server-env", "MALLOC_ARENA_MAX=2",
                    "--report", report,
                ])
            self.assertEqual(rc, 0)
            with open(report, encoding="utf-8") as fh:
                text = fh.read()
            self.assertIn("not measured", text)
            self.assertIn("**ok**", text)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            srv.shutdown()
            srv.server_close()
            shutil.rmtree(d, ignore_errors=True)


class WindowsAndReportSplitTests(unittest.TestCase):
    """#274: budget_rss parses Linux /proc and Windows psutil memory_full_info() splits and formats both."""

    MINIO_STATUS = ("Name:\tminio\nVmHWM:\t 1018880 kB\nVmRSS:\t 1005000 kB\n"
                    "RssAnon:\t  950272 kB\nRssFile:\t   54728 kB\nRssShmem:\t       0 kB\n")

    def test_parse_proc_status_reads_vmrss_and_split_for_minio_and_awanfs(self):
        got = br.parse_proc_status(self.MINIO_STATUS)
        self.assertEqual(got["VmHWM"], 995.0)
        self.assertAlmostEqual(got["VmRSS"], 1005000 / 1024.0)
        self.assertEqual(got["RssAnon"], 928.0)
        self.assertAlmostEqual(got["RssFile"], 54728 / 1024.0)

    def test_windows_psutil_memory_full_info_fixed_sample_tracks_peak(self):
        import time
        import types
        from unittest import mock

        mib = 1024 * 1024
        samples = iter([
            types.SimpleNamespace(private=64 * mib, wset=110 * mib, peak_wset=110 * mib),
            types.SimpleNamespace(private=96 * mib, wset=180 * mib, peak_wset=184 * mib),
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
        with mock.patch.object(br.sys, "platform", "win32"), \
             mock.patch.dict(sys.modules, {"psutil": fake_psutil}):
            with br.RssSplit(4321, every=0.01) as split:
                time.sleep(0.05)
        self.assertEqual(split.private, 96.0)
        self.assertEqual(split.wset, 180.0)
        self.assertIsNone(split.anon)
        self.assertIsNone(split.file)

    def test_windows_exited_process_marks_split_not_measured(self):
        import types
        from unittest import mock

        class ExitedError(Exception):
            pass

        class ExitedProc:
            def __init__(self, pid):
                raise ExitedError("exited")

        fake_psutil = types.SimpleNamespace(Process=ExitedProc, NoSuchProcess=ExitedError, AccessDenied=PermissionError)
        with mock.patch.object(br.sys, "platform", "win32"), \
             mock.patch.dict(sys.modules, {"psutil": fake_psutil}):
            with br.RssSplit(4321, every=0.01) as split:
                pass
        self.assertIsNone(split.private)
        self.assertIsNone(split.wset)
        self.assertEqual(br.mib_or_nm(split.private), "not measured")

    def test_format_report_linux_and_windows_splits_and_missing(self):
        linux_rows = [
            ("put", 500.0, 500.0, 10.0, 320.0, 200.0, 115.0, 128.0, None, None),
            ("get", 900.0, 900.0, 5.0, 340.0, None, None, None, None, None),
        ]
        linux_md = br.format_report(
            cmd="python benchmarks/budget_rss.py",
            budget="256MiB",
            budget_mib=256.0,
            overhead=200.0,
            rows=linux_rows,
            peak=340.0,
            seen_env={},
            size_bytes=1 << 20,
            split_kind="linux",
        )
        self.assertIn("peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB", linux_md)
        self.assertIn("| put | 500.00 | 500.0 | 10.00 | 320.0 | 200.0 | 115.0 | 128.0 | 72.0 |", linux_md)
        self.assertIn("| get | 900.00 | 900.0 | 5.00 | 340.0 | not measured | not measured | not measured | not measured |", linux_md)
        self.assertIn("ceiling of 456.0 MiB", linux_md)

        win_rows = [
            ("put", 400.0, 400.0, 12.0, 300.0, None, None, 128.0, 210.0, 295.0),
            ("get", 800.0, 800.0, 6.0, 350.0, None, None, 128.0, None, None),
        ]
        win_md = br.format_report(
            cmd="python benchmarks/budget_rss.py",
            budget="256MiB",
            budget_mib=256.0,
            overhead=200.0,
            rows=win_rows,
            peak=350.0,
            seen_env={},
            size_bytes=1 << 20,
            split_kind="windows",
        )
        self.assertIn("peak RSS MiB | peak private bytes (private) MiB | peak working set (wset) MiB", win_md)
        self.assertIn("private less cache MiB", win_md)
        self.assertIn("| put | 400.00 | 400.0 | 12.00 | 300.0 | 210.0 | 295.0 | 128.0 | 82.0 |", win_md)
        self.assertIn("| get | 800.00 | 800.0 | 6.00 | 350.0 | not measured | not measured | 128.0 | not measured |", win_md)
        self.assertIn("ceiling of 456.0 MiB", win_md)


if __name__ == "__main__":
    unittest.main()
