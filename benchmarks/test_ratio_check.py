"""Self-tests for ratio_check.py: how ratios are formed and when a run fails."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ratio_check as rc  # noqa: E402

BASE = {"tolerance": 0.20, "ratios": {"get_64k": 1.5, "put_64k": 10.0}}


class RatioTests(unittest.TestCase):
    def test_ratio_is_median_over_median(self):
        r = rc.median_ratios({"get_64k": [300, 100, 200]}, {"get_64k": [100, 400, 50]})
        self.assertAlmostEqual(r["get_64k"], 2.0)

    def test_minio_without_bytes_has_no_ratio_and_fails(self):
        r = rc.median_ratios({"get_64k": [100]}, {"get_64k": [0, 0, 0]})
        self.assertIsNone(r["get_64k"])
        _, bad = rc.compare(r, BASE)
        self.assertEqual(len(bad), 1)


class CompareTests(unittest.TestCase):
    def test_within_tolerance_passes(self):
        rows, bad = rc.compare({"get_64k": 1.25, "put_64k": 9.0}, BASE)
        self.assertEqual(bad, [])
        self.assertTrue(all(v == "ok" for *_, v in rows), rows)

    def test_drop_past_tolerance_fails(self):
        rows, bad = rc.compare({"get_64k": 1.19, "put_64k": 10.0}, BASE)
        self.assertEqual(len(bad), 1)
        self.assertIn("get_64k", bad[0])

    def test_no_baseline_reports_only(self):
        rows, bad = rc.compare({"get_64k": 0.1}, None)
        self.assertEqual(bad, [])
        self.assertEqual(rows[0][3], "no baseline")

    def test_much_faster_asks_to_raise_the_baseline(self):
        rows, _ = rc.compare({"get_64k": 2.0}, BASE)
        self.assertIn("raise the baseline", rows[0][3])

    def test_report_starts_with_its_command(self):
        rows, _ = rc.compare({"get_64k": 1.5}, BASE)
        md = rc.render(rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "python benchmarks/ratio_check.py --runs 5", 5)
        self.assertTrue(md.startswith("Command: `python benchmarks/ratio_check.py --runs 5`"))


class RssTable(unittest.TestCase):
    def test_peak_rss_is_reported_when_both_servers_have_it(self):
        rows = [("get_64k", 1.5, 1.4, "ok")]
        md = rc.render(rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "cmd", 1, {"awanfs": [80.0, 100.0, 90.0], "minio": [300.0]})
        self.assertIn("| AwanFS | 90.0 |", md)
        self.assertIn("| MinIO | 300.0 |", md)

    def test_no_rss_table_without_numbers(self):
        rows = [("get_64k", 1.5, 1.4, "ok")]
        md = rc.render(rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "cmd", 1, {"awanfs": [], "minio": [300.0]})
        self.assertNotIn("peak RSS", md)



class RssCeiling(unittest.TestCase):
    """The ceiling the nightly ratio run enforces. The numbers are the measured ones: 117.0 MiB
    on github ubuntu-latest (run 43400978818), and 2258.9 MiB with the PUT path caching views of
    the volume mapping (#126, reverted; run 43413585215)."""

    CEIL = {"tolerance": 0.20, "rss_ceiling_mib": 250, "ratios": {"get_64k": 1.5}}

    def test_measured_rss_is_under_the_ceiling(self):
        med, ceiling, bad = rc.check_rss({"awanfs": [117.0, 117.9], "minio": [559.8]}, self.CEIL)
        self.assertEqual(bad, [])
        self.assertEqual(ceiling, 250)
        self.assertAlmostEqual(med, 117.45)

    def test_the_126_regression_fails_the_run(self):
        _, _, bad = rc.check_rss({"awanfs": [2258.9], "minio": [569.5]}, self.CEIL)
        self.assertEqual(len(bad), 1)
        self.assertIn("2258.9", bad[0])
        self.assertIn("250.0", bad[0])

    def test_a_baseline_without_a_ceiling_only_reports(self):
        med, ceiling, bad = rc.check_rss({"awanfs": [9999.0], "minio": [1.0]}, {"tolerance": 0.2})
        self.assertEqual((ceiling, bad), (None, []))
        self.assertEqual(med, 9999.0)

    def test_the_report_names_the_verdict_and_the_ceiling(self):
        rows = [("get_64k", 1.5, 1.4, "ok")]
        over = rc.render(rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "cmd", 1,
                         {"awanfs": [2258.9], "minio": [569.5]}, 250)
        self.assertIn("| AwanFS | 2258.9 | OVER THE CEILING |", over)
        under = rc.render(rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "cmd", 1,
                          {"awanfs": [117.0], "minio": [559.8]}, 250)
        self.assertIn("| AwanFS | 117.0 | ok |", under)
        self.assertIn("Ceiling 250.0 MiB", under)

    def test_the_committed_baseline_carries_a_ceiling(self):
        import json
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "ratio_baseline.json"), encoding="utf-8") as fh:
            base = json.load(fh)
        self.assertIn("rss_ceiling_mib", base)
        _, _, bad = rc.check_rss({"awanfs": [2258.9], "minio": [1.0]}, base)
        self.assertEqual(len(bad), 1, "the committed ceiling must catch the measured regression")


class AffinityTests(unittest.TestCase):
    def test_ratio_check_pins_client_and_servers_when_pinnable(self):
        import inspect
        for fn in (rc.start_awanfs, rc.start_minio, rc.main):
            self.assertIn("set_affinity_mask", inspect.getsource(fn))

    def test_render_includes_pin_labels_when_provided(self):
        rows = [("get_64k", 1.5, 1.4, "ok")]
        md = rc.render(
            rows, {"get_64k": [150.0]}, {"get_64k": [100.0]}, "cmd", 1,
            pin_labels=("Cores 0..7 (Affinity Mask 0x00FF)", "Cores 8..15 (Affinity Mask 0xFF00)"),
        )
        self.assertIn("Cores 0..7 (Affinity Mask 0x00FF)", md)
        self.assertIn("Cores 8..15 (Affinity Mask 0xFF00)", md)


class CpuPerOpTests(unittest.TestCase):
    def test_process_cpu_seconds_sums_user_system_and_children(self):
        import types
        from unittest import mock

        class NoSuchProcess(Exception):
            pass

        class AccessDenied(Exception):
            pass

        class FakeChild:
            def __init__(self, user, system, fail=False):
                self._u = user
                self._s = system
                self._fail = fail

            def cpu_times(self):
                if self._fail:
                    raise NoSuchProcess("child exited")
                return types.SimpleNamespace(user=self._u, system=self._s)

        class FakeProc:
            def __init__(self, pid):
                self.pid = pid

            def status(self):
                return "running"

            def cpu_times(self):
                return types.SimpleNamespace(user=1.25, system=0.75)

            def children(self, recursive=True):
                return [
                    FakeChild(0.30, 0.20),
                    FakeChild(0.0, 0.0, fail=True),
                    FakeChild(0.40, 0.10),
                ]

        fake_psutil = types.SimpleNamespace(
            Process=FakeProc,
            NoSuchProcess=NoSuchProcess,
            AccessDenied=AccessDenied,
            STATUS_ZOMBIE="zombie",
        )
        with mock.patch.dict(sys.modules, {"psutil": fake_psutil}):
            total = rc.process_cpu_seconds(4321)
        self.assertAlmostEqual(total, 3.0)

    def test_computes_cpu_per_1k_ops_and_ratio_in_report(self):
        a_cpu = rc.cpu_seconds_per_1k_ops(1.0, 2.5, 5000)   # 1.5s / 5k = 0.300
        m_cpu = rc.cpu_seconds_per_1k_ops(4.0, 7.0, 4000)   # 3.0s / 4k = 0.750
        self.assertAlmostEqual(a_cpu, 0.300)
        self.assertAlmostEqual(m_cpu, 0.750)

        rows, bad = rc.compare({"get_64k": 1.5}, BASE)
        self.assertEqual(bad, [])
        md = rc.render(
            rows,
            {"get_64k": [150.0]},
            {"get_64k": [100.0]},
            "cmd",
            1,
            awanfs_cpu={"get_64k": [a_cpu]},
            minio_cpu={"get_64k": [m_cpu]},
        )
        self.assertIn("AwanFS CPU s / 1k ops", md)
        self.assertIn("MinIO CPU s / 1k ops", md)
        self.assertIn("CPU ratio", md)
        self.assertIn("| `get_64k` | 150.00 | 100.00 | 1.50 | 1.50 | ok | 0.300 | 0.750 | 0.40 |", md)

    def test_measure_run_computes_cpu_from_before_after_and_ops(self):
        import tempfile
        import types
        from unittest import mock

        class NoSuchProcess(Exception):
            pass

        class AccessDenied(Exception):
            pass

        snapshots = {
            "put_64k": [(1.0, 0.5, 0.1, 0.0), (2.2, 0.8, 0.4, 0.2)],
        }
        calls = {"count": 0}

        class FakeChild:
            def __init__(self, u, s):
                self._u = u
                self._s = s

            def cpu_times(self):
                return types.SimpleNamespace(user=self._u, system=self._s)

        class FakeProc:
            def __init__(self, pid):
                self.pid = pid

            def status(self):
                return "running"

            def cpu_times(self):
                idx = min(calls["count"], 1)
                u, s, _, _ = snapshots["put_64k"][idx]
                return types.SimpleNamespace(user=u, system=s)

            def children(self, recursive=True):
                idx = min(calls["count"], 1)
                _, _, cu, cs = snapshots["put_64k"][idx]
                calls["count"] += 1
                return [FakeChild(cu, cs)]

        class FakePopen:
            pid = 9999

            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                pass

            def kill(self):
                pass

        fake_psutil = types.SimpleNamespace(
            Process=FakeProc,
            NoSuchProcess=NoSuchProcess,
            AccessDenied=AccessDenied,
            STATUS_ZOMBIE="zombie",
        )
        fake_metrics = rc.rwb.BenchmarkMetrics(
            ops_per_sec=2000.0, mib_per_sec=125.0,
            p50_ms=1.0, p90_ms=2.0, p99_ms=3.0, p999_ms=4.0, peak_rss_mb=50.0,
        )
        workloads = {"put_64k": (65536, 16, "put")}
        tmp = tempfile.mkdtemp()
        with mock.patch.dict(sys.modules, {"psutil": fake_psutil}), \
             mock.patch.object(rc, "WORKLOADS", workloads), \
             mock.patch.object(rc, "run_workload_phase", return_value=fake_metrics), \
             mock.patch.object(rc, "get_process_peak_rss_mb", return_value=50.0):
            mib, peak, cpu = rc.measure_run(lambda: (FakePopen(), 9000, tmp), duration=2.0)

        # before = 1.0 + 0.5 + 0.1 + 0.0 = 1.6; after = 2.2 + 0.8 + 0.4 + 0.2 = 3.6; delta = 2.0s
        # ops = 2000.0 * 2.0 = 4000; 2.0 / 4000 * 1000 = 0.500
        self.assertAlmostEqual(mib["put_64k"], 125.0)
        self.assertAlmostEqual(peak, 50.0)
        self.assertAlmostEqual(cpu["put_64k"], 0.500)

    def test_exited_process_reads_not_measured_without_crashing(self):
        import tempfile
        import types
        from unittest import mock

        class NoSuchProcess(Exception):
            pass

        class AccessDenied(Exception):
            pass

        calls = {"count": 0}

        class FakeProc:
            def __init__(self, pid):
                if calls["count"] > 0:
                    raise NoSuchProcess("process exited")
                calls["count"] += 1

            def status(self):
                return "running"

            def cpu_times(self):
                return types.SimpleNamespace(user=1.0, system=0.5)

            def children(self, recursive=True):
                return []

        class FakePopen:
            pid = 8888

            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                pass

            def kill(self):
                pass

        fake_psutil = types.SimpleNamespace(
            Process=FakeProc,
            NoSuchProcess=NoSuchProcess,
            AccessDenied=AccessDenied,
            STATUS_ZOMBIE="zombie",
        )
        fake_metrics = rc.rwb.BenchmarkMetrics(
            ops_per_sec=1000.0, mib_per_sec=62.5,
            p50_ms=1.0, p90_ms=2.0, p99_ms=3.0, p999_ms=4.0, peak_rss_mb=0.0,
        )
        workloads = {"put_64k": (65536, 16, "put")}
        tmp = tempfile.mkdtemp()
        with mock.patch.dict(sys.modules, {"psutil": fake_psutil}), \
             mock.patch.object(rc, "WORKLOADS", workloads), \
             mock.patch.object(rc, "run_workload_phase", return_value=fake_metrics), \
             mock.patch.object(rc, "get_process_peak_rss_mb", return_value=0.0):
            _, _, cpu = rc.measure_run(lambda: (FakePopen(), 9000, tmp), duration=2.0)

        self.assertIsNone(cpu["put_64k"])
        rows, bad = rc.compare({"put_64k": 2.0}, {"tolerance": 0.4, "ratios": {"put_64k": 2.0}})
        self.assertEqual(bad, [])
        md = rc.render(
            rows,
            {"put_64k": [62.5]},
            {"put_64k": [31.25]},
            "cmd",
            1,
            awanfs_cpu={"put_64k": [cpu["put_64k"]]},
            minio_cpu={"put_64k": [0.800]},
        )
        self.assertIn("| `put_64k` | 62.50 | 31.25 | 2.00 | 2.00 | ok | not measured | 0.800 | - |", md)

    def test_stopped_popen_process_reads_not_measured_even_without_psutil_error(self):
        import tempfile
        import types
        from unittest import mock

        class NoSuchProcess(Exception):
            pass

        class AccessDenied(Exception):
            pass

        class FakeProc:
            def __init__(self, pid):
                pass

            def status(self):
                return "running"

            def cpu_times(self):
                return types.SimpleNamespace(user=1.0, system=0.5)

            def children(self, recursive=True):
                return []

        class ExitedPopen:
            pid = 7777
            _calls = 0

            def poll(self):
                self._calls += 1
                return None if self._calls == 1 else 1

            def terminate(self):
                pass

            def wait(self, timeout=None):
                pass

            def kill(self):
                pass

        fake_psutil = types.SimpleNamespace(
            Process=FakeProc,
            NoSuchProcess=NoSuchProcess,
            AccessDenied=AccessDenied,
            STATUS_ZOMBIE="zombie",
        )
        fake_metrics = rc.rwb.BenchmarkMetrics(
            ops_per_sec=1000.0, mib_per_sec=62.5,
            p50_ms=1.0, p90_ms=2.0, p99_ms=3.0, p999_ms=4.0, peak_rss_mb=0.0,
        )
        workloads = {"put_64k": (65536, 16, "put")}
        tmp = tempfile.mkdtemp()
        with mock.patch.dict(sys.modules, {"psutil": fake_psutil}), \
             mock.patch.object(rc, "WORKLOADS", workloads), \
             mock.patch.object(rc, "run_workload_phase", return_value=fake_metrics), \
             mock.patch.object(rc, "get_process_peak_rss_mb", return_value=0.0):
            _, _, cpu = rc.measure_run(lambda: (ExitedPopen(), 9000, tmp), duration=2.0)

        self.assertIsNone(cpu["put_64k"])

    def test_zombie_and_missing_psutil_return_none(self):
        import types
        from unittest import mock

        class FakeZombie:
            def __init__(self, pid):
                pass

            def status(self):
                return "zombie"

            def cpu_times(self):
                return types.SimpleNamespace(user=1.0, system=0.5)

        fake_psutil = types.SimpleNamespace(
            Process=FakeZombie,
            NoSuchProcess=Exception,
            AccessDenied=Exception,
            STATUS_ZOMBIE="zombie",
        )
        with mock.patch.dict(sys.modules, {"psutil": fake_psutil}):
            self.assertIsNone(rc.process_cpu_seconds(1234))

        with mock.patch.dict(sys.modules, {"psutil": None}):
            self.assertIsNone(rc.process_cpu_seconds(1234))

    def test_measure_phase_uses_cpusampler_hook_on_real_workload_phase(self):
        import threading
        import types
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from unittest import mock

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

        srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()

        class NoSuchProcess(Exception):
            pass

        class AccessDenied(Exception):
            pass

        caller = threading.current_thread()
        server_calls = {"n": 0}

        class FakeProc:
            def __init__(self, pid):
                self.pid = pid

            def status(self):
                return "running"

            def cpu_times(self):
                if self.pid != 5555:
                    return types.SimpleNamespace(user=0.0, system=0.0)
                if threading.current_thread() is not caller:
                    return types.SimpleNamespace(user=1.5, system=0.4)
                server_calls["n"] += 1
                if server_calls["n"] == 1:
                    return types.SimpleNamespace(user=1.0, system=0.2)
                return types.SimpleNamespace(user=2.5, system=0.7)  # delta = 2.0s

            def children(self, recursive=True):
                return []

        class FakePopen:
            pid = 5555

            def poll(self):
                return None

        fake_psutil = types.SimpleNamespace(
            Process=FakeProc,
            NoSuchProcess=NoSuchProcess,
            AccessDenied=AccessDenied,
            STATUS_ZOMBIE="zombie",
        )
        try:
            with mock.patch.dict(sys.modules, {"psutil": fake_psutil}):
                mib, cpu_per_1k = rc._measure_phase(FakePopen(), srv.server_address[1], 1024, 2, 0.3, "put")
        finally:
            srv.shutdown()
            srv.server_close()

        self.assertEqual(server_calls["n"], 2)
        self.assertGreater(mib, 0.0)
        self.assertIsNotNone(cpu_per_1k)
        self.assertGreater(cpu_per_1k, 0.0)


if __name__ == "__main__":
    unittest.main()
