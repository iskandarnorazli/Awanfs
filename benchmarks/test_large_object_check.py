"""Self-tests for large_object_check.py against a small in-process S3 stand-in.

    python benchmarks/test_large_object_check.py

The stand-in can be told to fail in the ways a real server has (wrong status, wrong bytes,
short body), and each must come out as NotMeasured rather than as a number: an earlier
benchmark here reported 404s as throughput (budget_rss.py, 2026-11-13).
"""
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import large_object_check as loc  # noqa: E402


class FakeS3(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, fault=""):
        super().__init__(("127.0.0.1", 0), Handler)
        self.objects = {}
        self.uploads = {}
        self.fault = fault
        self.lock = threading.Lock()
        self.completed = 0


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def answer(self, status, body=b"", headers=None):
        self.send_response(status)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", "0")))

    def do_PUT(self):
        u = urlsplit(self.path)
        q = parse_qs(u.query)
        data = self.body()
        if self.server.fault == "put_404":
            return self.answer(404)
        with self.server.lock:
            if "uploadId" in q:
                self.server.uploads[q["uploadId"][0]][int(q["partNumber"][0])] = data
                return self.answer(200, headers={"ETag": f'"p{q["partNumber"][0]}"'})
            self.server.objects[u.path] = data
        self.answer(200)

    def do_POST(self):
        u = urlsplit(self.path)
        q = parse_qs(u.query, keep_blank_values=True)
        self.body()
        with self.server.lock:
            if "uploads" in q:
                uid = f"u{len(self.server.uploads)}"
                self.server.uploads[uid] = {}
                return self.answer(200, f"<R><UploadId>{uid}</UploadId></R>".encode())
            parts = self.server.uploads[q["uploadId"][0]]
            self.server.objects[u.path] = b"".join(parts[n] for n in sorted(parts))
            self.server.completed += 1
        self.answer(200, b"<CompleteMultipartUploadResult/>")

    def do_GET(self):
        u = urlsplit(self.path)
        data = self.server.objects.get(u.path)
        if data is None:
            return self.answer(404)
        rng = self.headers.get("Range")
        if rng:
            a, b = rng.split("=")[1].split("-")
            piece = data[int(a):int(b) + 1]
            if self.server.fault == "range_wrong_bytes":
                piece = bytes(len(piece))
            if self.server.fault == "range_200":
                return self.answer(200, piece)
            return self.answer(206, piece)
        if self.server.fault == "get_short" and len(data) > loc.SMALL:
            data = data[:-1]
        self.answer(200, data)


class WithServer(unittest.TestCase):
    fault = ""

    def setUp(self):
        self.srv = FakeS3(self.fault)
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.size = 2 * loc.MIB
        self.body = os.urandom(self.size)

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()

    def measure(self):
        return loc.measure_size(self.port, self.size, self.body, 8 * loc.MIB, 0.2, 7)


class Healthy(WithServer):
    def test_every_metric_has_a_number(self):
        m = self.measure()
        self.assertEqual(sorted(m), sorted(k for k, _, _ in loc.METRICS))
        for k, v in m.items():
            self.assertGreater(v, 0.0, k)

    def test_it_stored_what_it_says_it_moved(self):
        self.measure()
        stored = self.srv.objects
        for i in range(4):
            self.assertEqual(stored[f"/{loc.BUCKET}/s2-a-{i}"], self.body)
            self.assertEqual(stored[f"/{loc.BUCKET}/s2-b-{i}"], self.body)
        self.assertEqual(stored[f"/{loc.BUCKET}/s2-mpu"], self.body)
        self.assertEqual(self.srv.completed, 1)

    def test_multipart_sends_16_mib_parts(self):
        body = os.urandom(loc.PART * 2 + 5)
        loc.multipart_put(self.port, f"/{loc.BUCKET}/m", memoryview(body), 4)
        self.assertEqual(sorted(len(p) for p in self.srv.uploads["u0"].values()), [5, loc.PART, loc.PART])
        self.assertEqual(self.srv.objects[f"/{loc.BUCKET}/m"], body)


class PutFails(WithServer):
    fault = "put_404"

    def test_a_refused_put_is_not_a_number(self):
        with self.assertRaises(loc.NotMeasured):
            self.measure()


class RangeWrongBytes(WithServer):
    fault = "range_wrong_bytes"

    def test_wrong_range_bytes_are_not_a_number(self):
        with self.assertRaises(loc.NotMeasured):
            self.measure()


class RangeIgnored(WithServer):
    fault = "range_200"

    def test_a_range_answered_200_is_not_a_number(self):
        with self.assertRaises(loc.NotMeasured):
            self.measure()


class GetShort(WithServer):
    fault = "get_short"

    def test_a_short_body_is_not_a_number(self):
        with self.assertRaises(loc.NotMeasured):
            self.measure()


class Pure(unittest.TestCase):
    def test_count_for(self):
        self.assertEqual(loc.count_for(16 * loc.MIB, 512 * loc.MIB), 32)
        self.assertEqual(loc.count_for(64 * loc.MIB, 512 * loc.MIB), 8)
        self.assertEqual(loc.count_for(256 * loc.MIB, 512 * loc.MIB), 4)
        self.assertEqual(loc.count_for(1 * loc.MIB, 512 * loc.MIB), 32)

    def test_better_is_above_one_when_awanfs_is_ahead(self):
        self.assertEqual(loc.better(200.0, 100.0, True), 2.0)
        self.assertEqual(loc.better(5.0, 10.0, False), 2.0)
        self.assertIsNone(loc.better(0.0, 10.0, True))

    def test_in_parallel_raises_the_failure(self):
        def f(x):
            if x == 3:
                raise loc.NotMeasured("x")
            return x * 2
        with self.assertRaises(loc.NotMeasured):
            loc.in_parallel(f, list(range(8)), 4)
        self.assertEqual(loc.in_parallel(lambda x: x * 2, [1, 2, 3], 2), [2, 4, 6])

    def test_render_says_not_measured_for_a_missing_server(self):
        results = {"16MiB": {"awanfs": {"put_c1_mibs": [100.0, 300.0, 200.0]}, "minio": {}}}
        text = loc.render(results, {"awanfs": {"peak": 10.0}}, "cmd", 3)
        self.assertIn("| PUT, 1 client, MiB/s | 200.0 | not measured | not measured |", text)
        self.assertTrue(text.startswith("Command: `cmd`"))

    def test_render_ratio(self):
        results = {"16MiB": {"awanfs": {"range_1m_p99_ms": [4.0]}, "minio": {"range_1m_p99_ms": [8.0]}}}
        self.assertIn("| 1 MiB range, P99 ms | 4.0 | 8.0 | 2.00x |", loc.render(results, {}, "cmd", 1))


if __name__ == "__main__":
    unittest.main()
