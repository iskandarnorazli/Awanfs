"""Tests for large_volume_probe.py's read-after-write split (#204), against a stub server.

    python benchmarks/test_large_volume_probe.py
"""
import http.server
import inspect
import os
import sys
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import large_volume_probe as lvp  # noqa: E402


class Store(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    objects = {}
    connections = 0

    def setup(self):
        type(self).connections += 1
        super().setup()

    def do_PUT(self):
        n = int(self.headers.get("Content-Length", "0"))
        self.objects[self.path] = self.rfile.read(n)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        body = self.objects.get(self.path)
        if body is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class SplitTest(unittest.TestCase):
    def setUp(self):
        Store.objects = {}
        Store.connections = 0
        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Store)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.port = self.srv.server_address[1]

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()

    def test_every_row_is_measured_and_reads_what_was_written(self):
        small = os.urandom(lvp.SMALL)
        old = ["/probe/old-0", "/probe/old-1"]
        for k in old:
            lvp.put(self.port, k, small)
        before = Store.connections
        out = lvp.read_after_write_split(self.port, small, old, rounds=3)
        self.assertEqual(set(out), {"connect", "open_conn", "pause", "keep_alive", "old"})
        for k, (p50, p99) in out.items():
            self.assertGreaterEqual(p99, p50, k)
            self.assertGreaterEqual(p50, 0.0, k)
        # Per round: PUT, GET (open), PUT, GET (pause), PUT+GET on one connection, GET (old).
        self.assertEqual(Store.connections - before, 3 * 6, "the keep-alive row used one connection")

    def test_a_wrong_length_is_not_measured(self):
        lvp.put(self.port, "/probe/old-0", b"short")
        with self.assertRaises(lvp.NotMeasured):
            lvp.read_after_write_split(self.port, os.urandom(lvp.SMALL), ["/probe/old-0"], rounds=1)

    def test_the_report_has_the_split_rows(self):
        r = {"volume_gib": 1.0, "fill_mibs": 100.0, "rss": 50.0, "read_after_write": (14.8, 20.0)}
        for m in ("put", "get", "mixed"):
            r[m] = (1000.0, 1.0, 2.0)
        r["split"] = {k: (1.0, 2.0) for k in ("connect", "open_conn", "pause", "keep_alive", "old")}
        text = lvp.render([("growing (default), round 1", r)], 1.0, "cmd")
        for needle in ("opening the connection", "connection already open", "100 ms after the PUT", "on one connection", "written before the fill"):
            self.assertIn(needle, text)


class ConnectProbeTest(unittest.TestCase):
    def setUp(self):
        Store.objects = {}
        Store.connections = 0
        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Store)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.port = self.srv.server_address[1]

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()

    def test_connect_only_measures_connect_and_open_get(self):
        before = Store.connections
        out = lvp.measure_connect(self.port, rounds=4)
        self.assertEqual(set(out), {"connect", "open_conn"})
        for k, (p50, p99) in out.items():
            self.assertGreaterEqual(p99, p50, k)
            self.assertGreaterEqual(p50, 0.0, k)
        # 1 PUT to seed the object + 4 fresh connections (connect + GET each).
        self.assertEqual(Store.connections - before, 1 + 4)

    def test_connect_only_wrong_bytes_raises_not_measured(self):
        Store.objects["/probe/connect-probe"] = b"short"
        with mock.patch.object(lvp, "put"):
            with self.assertRaises(lvp.NotMeasured):
                lvp.measure_connect(self.port, rounds=1)

    def test_render_and_parse_connect_comparison(self):
        results = [
            ("AwanFS", 1, {"connect": (11.20, 24.10), "open_conn": (0.34, 0.50)}),
            ("MinIO", 1, {"connect": (11.00, 23.00), "open_conn": (0.42, 0.60)}),
            ("MinIO", 2, {"connect": (11.40, 25.00), "open_conn": (0.46, 0.70)}),
            ("AwanFS", 2, {"connect": (11.60, 25.90), "open_conn": (0.36, 0.54)}),
        ]
        cmd = "python benchmarks/large_volume_probe.py --server-bin awanfs --minio-bin minio --mc-bin mc --connect-only"
        text = lvp.render_connect(results, 300, cmd)
        self.assertTrue(text.startswith(f"Command: `{cmd}`"))
        for needle in ("opening the connection (connect)", "AwanFS, round 1", "MinIO, round 1", "AwanFS", "MinIO"):
            self.assertIn(needle, text)
        parsed = lvp.parse_connect_summary(text)
        self.assertAlmostEqual(parsed["AwanFS"]["connect_p50_ms"], 11.40, places=2)
        self.assertAlmostEqual(parsed["AwanFS"]["connect_p99_ms"], 25.00, places=2)
        self.assertAlmostEqual(parsed["AwanFS"]["open_conn_p50_ms"], 0.35, places=2)
        self.assertAlmostEqual(parsed["MinIO"]["connect_p50_ms"], 11.20, places=2)
        self.assertAlmostEqual(parsed["MinIO"]["connect_p99_ms"], 24.00, places=2)
        self.assertAlmostEqual(parsed["MinIO"]["open_conn_p50_ms"], 0.44, places=2)
        self.assertAlmostEqual(parsed["ratio"], 11.40 / 11.20, places=2)

    def test_render_connect_without_minio_marks_not_measured(self):
        results = [
            ("AwanFS", 1, {"connect": (11.20, 24.10), "open_conn": (0.34, 0.50)}),
            ("AwanFS", 2, {"connect": (11.60, 25.90), "open_conn": (0.36, 0.54)}),
        ]
        text = lvp.render_connect(results, 300, "cmd")
        self.assertIn("| MinIO | not measured | not measured | not measured |", text)
        self.assertIn("not measured", text)
        parsed = lvp.parse_connect_summary(text)
        self.assertIsNotNone(parsed["AwanFS"])
        self.assertIsNone(parsed["MinIO"])
        self.assertIsNone(parsed["ratio"])

    def test_server_starters_log_to_files_not_pipes(self):
        import inspect
        for fn in (lvp.start_awanfs, lvp.start_minio):
            src = inspect.getsource(fn)
            self.assertNotIn("PIPE", src)
            self.assertIn(".log", src)

    def test_connect_comparison_alternates_order_and_stops_servers_on_error(self):
        calls = []
        stopped = []

        def fake_start_awanfs(bin_path):
            calls.append(("start", "AwanFS"))
            return ("proc-a", 1001, "dir-a")

        def fake_start_minio(bin_path, mc_path):
            calls.append(("start", "MinIO", mc_path))
            return ("proc-m", 1002, "dir-m")

        def fake_measure(port, rounds):
            calls.append(("measure", port, rounds))
            return {"connect": (1.0, 2.0), "open_conn": (0.3, 0.5)}

        def fake_stop(proc, data):
            stopped.append((proc, data))

        with mock.patch.object(lvp, "start_awanfs", side_effect=fake_start_awanfs), \
             mock.patch.object(lvp, "start_minio", side_effect=fake_start_minio), \
             mock.patch.object(lvp, "measure_connect", side_effect=fake_measure), \
             mock.patch.object(lvp, "stop_server", side_effect=fake_stop):
            res = lvp.run_connect_comparison("awanfs", "minio", "mc", rounds=5)
        self.assertEqual([(s, r) for s, r, _ in res], [("AwanFS", 1), ("MinIO", 1), ("MinIO", 2), ("AwanFS", 2)])
        self.assertEqual(len(stopped), 4)

        stopped.clear()
        with mock.patch.object(lvp, "start_awanfs", side_effect=fake_start_awanfs), \
             mock.patch.object(lvp, "start_minio", side_effect=fake_start_minio), \
             mock.patch.object(lvp, "measure_connect", side_effect=lvp.NotMeasured("bad")), \
             mock.patch.object(lvp, "stop_server", side_effect=fake_stop):
            with self.assertRaises(lvp.NotMeasured):
                lvp.run_connect_comparison("awanfs", "minio", "mc", rounds=1)
        self.assertEqual(stopped, [("proc-a", "dir-a")])


class FourListenerConnectTest(unittest.TestCase):
    def setUp(self):
        Store.objects = {}
        Store.connections = 0
        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Store)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.port = self.srv.server_address[1]

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()

    def test_start_bare_listener_logs_to_file_and_cleans_up_on_timeout(self):
        src = inspect.getsource(lvp.start_bare_listener)
        self.assertNotIn("PIPE", src)
        self.assertIn(".log", src)

        with self.assertRaises(ValueError):
            lvp.start_bare_listener("bare_listeners", "invalid")

        fake_proc = mock.MagicMock()
        created_dirs = []
        orig_mkdtemp = tempfile.mkdtemp

        def track_mkdtemp(*args, **kwargs):
            d = orig_mkdtemp(*args, **kwargs)
            created_dirs.append(d)
            return d

        with mock.patch.object(lvp.tempfile, "mkdtemp", side_effect=track_mkdtemp), \
             mock.patch.object(lvp, "get_free_port", return_value=43210), \
             mock.patch.object(lvp.subprocess, "Popen", return_value=fake_proc) as popen, \
             mock.patch.object(lvp, "wait_port", return_value=False):
            with self.assertRaises(SystemExit):
                lvp.start_bare_listener("bare_listeners", "std")
        popen_args = popen.call_args[0][0]
        self.assertEqual(popen_args, ["bare_listeners", "--mode", "std", "--port", "43210"])
        fake_proc.kill.assert_called_once()
        fake_proc.wait.assert_called_once()
        self.assertEqual(len(created_dirs), 1)
        self.assertFalse(os.path.exists(created_dirs[0]))

    def test_four_listener_comparison_runs_three_alternating_rounds_and_stops_on_error(self):
        started = []
        stopped = []

        def fake_bare(bin_path, mode):
            started.append(("bare", mode, bin_path))
            return (f"proc-{mode}", 1100 + len(started), f"dir-{mode}")

        def fake_awanfs(bin_path):
            started.append(("awanfs", bin_path))
            return ("proc-s3", 1200 + len(started), "dir-s3")

        def fake_minio(bin_path, mc_path):
            started.append(("MinIO", bin_path, mc_path))
            return ("proc-minio", 1300 + len(started), "dir-minio")

        def fake_measure(port, rounds):
            return {"connect": (0.25, 0.50), "open_conn": (0.35, 0.70)}

        def fake_stop(proc, data):
            stopped.append((proc, data))

        with mock.patch.object(lvp, "start_bare_listener", side_effect=fake_bare), \
             mock.patch.object(lvp, "start_awanfs", side_effect=fake_awanfs), \
             mock.patch.object(lvp, "start_minio", side_effect=fake_minio), \
             mock.patch.object(lvp, "measure_connect", side_effect=fake_measure), \
             mock.patch.object(lvp, "stop_server", side_effect=fake_stop):
            res = lvp.run_connect_comparison(
                "awanfs", "minio", "mc", rounds=5, bare_bin="bare_listeners", connect_rounds=3
            )

        self.assertEqual(len(res), 12)
        self.assertEqual(len(stopped), 12)
        for rnd in (1, 2, 3):
            names = [name for name, r, _ in res if r == rnd]
            self.assertEqual(set(names), {"bare std", "bare tokio", "awanfs", "MinIO"})
        r1 = [name for name, r, _ in res if r == 1]
        r2 = [name for name, r, _ in res if r == 2]
        self.assertEqual(r1, ["bare std", "bare tokio", "awanfs", "MinIO"])
        self.assertEqual(r2, list(reversed(r1)))
        self.assertIn(("bare", "std", "bare_listeners"), started)
        self.assertIn(("bare", "tokio", "bare_listeners"), started)

        stopped.clear()
        with mock.patch.object(lvp, "start_bare_listener", side_effect=fake_bare), \
             mock.patch.object(lvp, "start_awanfs", side_effect=fake_awanfs), \
             mock.patch.object(lvp, "start_minio", side_effect=fake_minio), \
             mock.patch.object(lvp, "measure_connect", side_effect=lvp.NotMeasured("boom")), \
             mock.patch.object(lvp, "stop_server", side_effect=fake_stop):
            with self.assertRaises(lvp.NotMeasured):
                lvp.run_connect_comparison(
                    "awanfs", "minio", "mc", rounds=1, bare_bin="bare_listeners", connect_rounds=3
                )
        self.assertEqual(stopped, [("proc-std", "dir-std")])

    def test_summarize_render_and_parse_four_listeners_with_spread(self):
        results = [
            ("bare std", 1, {"connect": (0.20, 0.40), "open_conn": (0.30, 0.55)}),
            ("bare tokio", 1, {"connect": (15.00, 22.00), "open_conn": (0.32, 0.60)}),
            ("awanfs", 1, {"connect": (15.30, 24.00), "open_conn": (0.38, 0.70)}),
            ("MinIO", 1, {"connect": (0.30, 25.00), "open_conn": (0.95, 1.40)}),
            ("MinIO", 2, {"connect": (0.34, 26.00), "open_conn": (0.99, 1.50)}),
            ("awanfs", 2, {"connect": (15.50, 26.00), "open_conn": (0.40, 0.74)}),
            ("bare tokio", 2, {"connect": (15.50, 24.00), "open_conn": (0.36, 0.64)}),
            ("bare std", 2, {"connect": (0.30, 0.60), "open_conn": (0.34, 0.65)}),
            ("bare tokio", 3, {"connect": (15.20, 23.00), "open_conn": (0.34, 0.62)}),
            ("awanfs", 3, {"connect": (15.40, 25.00), "open_conn": (0.39, 0.72)}),
            ("MinIO", 3, {"connect": (0.31, 24.00), "open_conn": (0.97, 1.45)}),
            ("bare std", 3, {"connect": (0.25, 0.50), "open_conn": (0.32, 0.60)}),
        ]
        summary = lvp.summarize_connect(results)
        self.assertAlmostEqual(summary["bare std"]["connect_p50_ms"], 0.25, places=2)
        self.assertAlmostEqual(summary["bare std"]["connect_p50_min_ms"], 0.20, places=2)
        self.assertAlmostEqual(summary["bare std"]["connect_p50_max_ms"], 0.30, places=2)
        self.assertAlmostEqual(summary["bare tokio"]["connect_p50_ms"], 15.20, places=2)
        self.assertAlmostEqual(summary["awanfs"]["connect_p50_ms"], 15.40, places=2)
        self.assertAlmostEqual(summary["MinIO"]["connect_p50_ms"], 0.31, places=2)
        self.assertAlmostEqual(summary["ratios"]["bare_tokio_vs_bare_std"], 15.20 / 0.25, places=2)
        self.assertAlmostEqual(summary["ratios"]["s3_server_vs_bare_tokio"], 15.40 / 15.20, places=2)
        self.assertAlmostEqual(summary["ratios"]["s3_server_vs_minio"], 15.40 / 0.31, places=2)

        cmd = "python benchmarks/large_volume_probe.py --server-bin awanfs --bare-bin bare_listeners --minio-bin minio --mc-bin mc --connect-only --connect-rounds 3"
        text = lvp.render_connect(results, 300, cmd)
        self.assertTrue(text.startswith(f"Command: `{cmd}`"))
        for needle in (
            "| bare std | 0.25 (0.20..0.30) | 0.50 (0.40..0.60) | 0.32 (0.30..0.34) |",
            "| bare tokio | 15.20 (15.00..15.50) | 23.00 (22.00..24.00) | 0.34 (0.32..0.36) |",
            "| awanfs | 15.40 (15.30..15.50) | 25.00 (24.00..26.00) | 0.39 (0.38..0.40) |",
            "| MinIO | 0.31 (0.30..0.34) | 25.00 (24.00..26.00) | 0.97 (0.95..0.99) |",
        ):
            self.assertIn(needle, text)

        parsed = lvp.parse_connect_summary(text)
        self.assertAlmostEqual(parsed["bare std"]["connect_p50_ms"], 0.25, places=2)
        self.assertAlmostEqual(parsed["bare std"]["connect_p50_min_ms"], 0.20, places=2)
        self.assertAlmostEqual(parsed["bare std"]["connect_p50_max_ms"], 0.30, places=2)
        self.assertAlmostEqual(parsed["bare tokio"]["connect_p50_ms"], 15.20, places=2)
        self.assertAlmostEqual(parsed["awanfs"]["connect_p99_ms"], 25.00, places=2)
        self.assertAlmostEqual(parsed["MinIO"]["open_conn_p50_ms"], 0.97, places=2)
        self.assertAlmostEqual(parsed["ratios"]["bare_tokio_vs_bare_std"], 15.20 / 0.25, places=2)
        self.assertAlmostEqual(parsed["ratios"]["s3_server_vs_bare_tokio"], 15.40 / 15.20, places=2)
        self.assertAlmostEqual(parsed["ratios"]["s3_server_vs_minio"], 15.40 / 0.31, places=2)

    def test_render_four_listeners_without_minio_marks_not_measured(self):
        results = [
            ("bare std", 1, {"connect": (0.20, 0.40), "open_conn": (0.30, 0.55)}),
            ("bare tokio", 1, {"connect": (0.25, 0.45), "open_conn": (0.31, 0.56)}),
            ("awanfs", 1, {"connect": (15.20, 22.00), "open_conn": (0.38, 0.70)}),
        ]
        text = lvp.render_connect(results, 300, "cmd")
        self.assertIn("| MinIO | not measured | not measured | not measured |", text)
        parsed = lvp.parse_connect_summary(text)
        self.assertIsNotNone(parsed["bare std"])
        self.assertIsNotNone(parsed["bare tokio"])
        self.assertIsNotNone(parsed["awanfs"])
        self.assertIsNone(parsed["MinIO"])
        self.assertIsNone(parsed["ratios"]["s3_server_vs_minio"])

    def test_cli_connect_only_with_bare_bin_writes_four_listener_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            report_path = os.path.join(tmp, "connect.md")
            with mock.patch.object(lvp, "start_bare_listener", side_effect=lambda _b, mode: (f"proc-{mode}", self.port, f"dir-{mode}")), \
                 mock.patch.object(lvp, "start_awanfs", side_effect=lambda _b: ("proc-s3", self.port, "dir-s3")), \
                 mock.patch.object(lvp, "stop_server"):
                rc = lvp.main([
                    "--server-bin", "awanfs",
                    "--bare-bin", "bare_listeners",
                    "--connect-only",
                    "--rounds", "2",
                    "--connect-rounds", "3",
                    "--report", report_path,
                ])
            self.assertEqual(rc, 0)
            with open(report_path, encoding="utf-8") as fh:
                report = fh.read()
        self.assertIn("| bare std |", report)
        self.assertIn("| bare tokio |", report)
        self.assertIn("| awanfs |", report)
        self.assertIn("| MinIO | not measured | not measured | not measured |", report)


if __name__ == "__main__":
    unittest.main()
