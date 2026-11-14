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

    STATUS = ("Name:\ts3-server\nVmHWM:\t  555000 kB\nVmRSS:\t  540000 kB\n"
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


if __name__ == "__main__":
    unittest.main()
