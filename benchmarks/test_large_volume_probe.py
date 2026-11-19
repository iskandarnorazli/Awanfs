"""Tests for large_volume_probe.py's read-after-write split (#204), against a stub server.

    python benchmarks/test_large_volume_probe.py
"""
import http.server
import os
import sys
import threading
import unittest

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


if __name__ == "__main__":
    unittest.main()
