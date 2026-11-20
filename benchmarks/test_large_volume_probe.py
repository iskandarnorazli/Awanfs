"""Tests for large_volume_probe.py's read-after-write split (#204), against a stub server.

    python benchmarks/test_large_volume_probe.py
"""
import http.server
import os
import sys
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
        cmd = "python benchmarks/large_volume_probe.py --server-bin s3-server --minio-bin minio --mc-bin mc --connect-only"
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
            res = lvp.run_connect_comparison("s3-server", "minio", "mc", rounds=5)
        self.assertEqual([(s, r) for s, r, _ in res], [("AwanFS", 1), ("MinIO", 1), ("MinIO", 2), ("AwanFS", 2)])
        self.assertEqual(len(stopped), 4)

        stopped.clear()
        with mock.patch.object(lvp, "start_awanfs", side_effect=fake_start_awanfs), \
             mock.patch.object(lvp, "start_minio", side_effect=fake_start_minio), \
             mock.patch.object(lvp, "measure_connect", side_effect=lvp.NotMeasured("bad")), \
             mock.patch.object(lvp, "stop_server", side_effect=fake_stop):
            with self.assertRaises(lvp.NotMeasured):
                lvp.run_connect_comparison("s3-server", "minio", "mc", rounds=1)
        self.assertEqual(stopped, [("proc-a", "dir-a")])


if __name__ == "__main__":
    unittest.main()
