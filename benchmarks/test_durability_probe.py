"""Tests for benchmarks/durability_probe.py (#334).

    python benchmarks/test_durability_probe.py
"""
import http.server
import os
import sys
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import durability_probe as dp  # noqa: E402


class StraceParsingTests(unittest.TestCase):
    def test_parse_strace_c_summary_counts_sync_syscalls(self):
        sample = """\
% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 94.12    0.048100         240       200           fdatasync
  4.90    0.002504         250        10           fsync
  0.98    0.000501         250         2         1 sync_file_range
  0.00    0.000010          10         1           syncfs
  0.00    0.000005           5         1           msync
------ ----------- ----------- --------- --------- ----------------
100.00    0.051120         238       214         1 total
"""
        self.assertEqual(dp.parse_strace_summary(sample), 214)

    def test_parse_strace_c_summary_handles_zero_and_empty(self):
        zero_table = """\
% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
100.00    0.000000           0         0           total
"""
        self.assertEqual(dp.parse_strace_summary(zero_table), 0)
        self.assertEqual(dp.parse_strace_summary(""), 0)
        self.assertEqual(dp.parse_strace_summary("   \n\t "), 0)

    def test_parse_strace_c_summary_rejects_non_summary_error_text(self):
        with self.assertRaises(ValueError):
            dp.parse_strace_summary("strace: attach: ptrace(PTRACE_SEIZE, 123): Operation not permitted\n")

    def test_parse_openat_counts_o_dsync_and_o_sync_only_on_success(self):
        trace = """\
[pid 101] openat(AT_FDCWD, "/tmp/minio/bkt/k1/xl.meta", O_WRONLY|O_CREAT|O_TRUNC|O_DSYNC|O_CLOEXEC, 0644) = 12
[pid 101] openat(AT_FDCWD, "/tmp/minio/bkt/k1/part.1", O_WRONLY|O_CREAT|O_SYNC, 0666) = 13
openat(AT_FDCWD, "/tmp/plain/k2", O_WRONLY|O_CREAT|O_CLOEXEC, 0644) = 14
[pid 102] openat(AT_FDCWD, "/tmp/O_DSYNC_in_path/O_SYNC", O_RDONLY|O_CLOEXEC) = 15
[pid 102] openat(AT_FDCWD, "/tmp/missing", O_WRONLY|O_DSYNC) = -1 ENOENT (No such file or directory)
[pid 103] openat(AT_FDCWD, "/tmp/split1", O_WRONLY|O_DSYNC <unfinished ...>
[pid 104] openat(AT_FDCWD, "/tmp/split2", O_WRONLY <unfinished ...>
[pid 103] <... openat resumed>) = 16
[pid 104] <... openat resumed>|O_SYNC, 0644) = 17
--- SIGCHLD {si_signo=SIGCHLD} ---
+++ exited with 0 +++
"""
        self.assertEqual(dp.parse_openat_sync_opens(trace), 4)


class DurabilityRuleAndReportTests(unittest.TestCase):
    def test_durable_rule_requires_at_least_one_sync_or_sync_open_per_put(self):
        self.assertEqual(dp.classify_durability(200, 200, 0), (1.0, 0.0, "durable"))
        self.assertEqual(dp.classify_durability(200, 0, 200), (0.0, 1.0, "durable"))
        self.assertEqual(dp.classify_durability(200, 100, 100), (0.5, 0.5, "durable"))
        self.assertEqual(dp.classify_durability(200, 400, 0), (2.0, 0.0, "durable"))

        self.assertEqual(dp.classify_durability(200, 199, 0), (0.995, 0.0, "not durable"))
        self.assertEqual(dp.classify_durability(200, 0, 199), (0.0, 0.995, "not durable"))
        self.assertEqual(dp.classify_durability(200, 10, 0), (0.05, 0.0, "not durable"))
        self.assertEqual(dp.classify_durability(200, 0, 0), (0.0, 0.0, "not durable"))

    def test_default_servers_match_three_default_rows_owner_decision(self):
        names = [s.name for s in dp.default_servers()]
        self.assertEqual(len(names), 3)
        self.assertTrue(any("AwanFS" in n and "group" in n for n in names))
        self.assertTrue(any("MinIO" in n for n in names))
        self.assertTrue(any("RustFS" in n for n in names))
        self.assertFalse(any("fast" in n.lower() for n in names))
        self.assertFalse(any("strict" in n.lower() for n in names))

    def test_render_report_includes_table_run_id_and_client_cap_note(self):
        rows = [
            dp.build_probe_row("AwanFS (--durability group)", 200, 200, 0, 2600.0),
            dp.build_probe_row("MinIO", 200, 0, 400, 680.0),
            dp.build_probe_row("RustFS (release defaults)", 200, 0, 0, 900.0),
        ]
        md = dp.render_report(
            rows,
            cmd="python3 benchmarks/durability_probe.py",
            run_id="44750000001",
            machine_info="Linux x86_64 (4 CPUs)",
        )
        self.assertTrue(md.startswith("Command: `python3 benchmarks/durability_probe.py`"))
        self.assertIn("44750000001", md)
        self.assertIn("5,000", md)
        self.assertIn("#262", md)
        self.assertIn("| AwanFS (--durability group) | 1.00 | 0.00 | durable | 2,600.0 |", md)
        self.assertIn("| MinIO | 0.00 | 2.00 | durable | 680.0 |", md)
        self.assertIn("| RustFS (release defaults) | 0.00 | 0.00 | not durable | 900.0 |", md)


class ConcurrentPutAndStraceErrorTests(unittest.TestCase):
    def test_send_concurrent_puts_sends_exact_count_and_raises_on_http_error(self):
        seen = []
        seen_lock = threading.Lock()
        fail_on = set()

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_PUT(self):
                n = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(n)
                with seen_lock:
                    seen.append((self.path, len(body)))
                status = 500 if self.path in fail_on else 200
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        port = srv.server_address[1]
        try:
            dp._send_concurrent_puts("127.0.0.1", port, dp.BUCKET, count=32, concurrency=8, size=1024, prefix="ok")
            self.assertEqual(len(seen), 32)
            self.assertTrue(all(sz == 1024 for _, sz in seen))
            self.assertEqual(len({p for p, _ in seen}), 32)

            fail_on.add(f"/{dp.BUCKET}/err_0.dat")
            with self.assertRaises(RuntimeError) as ctx:
                dp._send_concurrent_puts("127.0.0.1", port, dp.BUCKET, count=4, concurrency=2, size=64, prefix="err")
            self.assertIn("HTTP 500", str(ctx.exception))
        finally:
            srv.shutdown()
            srv.server_close()

    def test_wait_strace_attached_raises_strace_attach_error_with_printed_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = os.path.join(tmp, "strace.log")
            with open(log_path, "w", encoding="utf-8") as fh:
                fh.write("strace: attach: ptrace(PTRACE_SEIZE, 999): Operation not permitted\n")

            fake_strace = mock.MagicMock()
            fake_strace.poll.return_value = 1
            fake_strace.returncode = 1
            fake_srv = mock.MagicMock()
            fake_srv.poll.return_value = None

            with self.assertRaises(dp.StraceAttachError) as ctx:
                dp._wait_strace_attached(fake_strace, log_path, fake_srv, timeout=1.0)
            self.assertIn("Operation not permitted", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
