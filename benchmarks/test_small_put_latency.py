"""Unit and stub-server tests for benchmarks/small_put_latency.py (#362).

    python benchmarks/test_small_put_latency.py
"""
import hashlib
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import run_warp_benchmark as rwb  # noqa: E402
import small_put_latency as spl  # noqa: E402


class CliAndParsingTests(unittest.TestCase):
    def test_parse_size_bytes(self):
        self.assertEqual(spl.parse_size_bytes("4KiB"), 4 * 1024)
        self.assertEqual(spl.parse_size_bytes("16KiB"), 16 * 1024)
        self.assertEqual(spl.parse_size_bytes("64KiB"), 64 * 1024)
        self.assertEqual(spl.parse_size_bytes("1MiB"), 1024 * 1024)
        self.assertEqual(spl.parse_size_bytes("4096"), 4096)
        with self.assertRaises(ValueError):
            spl.parse_size_bytes("0")
        with self.assertRaises(ValueError):
            spl.parse_size_bytes("bogus")

    def test_parse_durabilities_defaults_to_group_and_validates(self):
        self.assertEqual(spl.parse_durabilities("group"), ["group"])
        self.assertEqual(spl.parse_durabilities("group,strict,group"), ["group", "strict"])
        self.assertEqual(spl.parse_durabilities("strict"), ["strict"])
        with self.assertRaises(ValueError):
            spl.parse_durabilities("")
        with self.assertRaises(ValueError):
            spl.parse_durabilities("bogus")

    def test_parse_concurrencies(self):
        self.assertEqual(spl.parse_concurrencies("1,16"), [1, 16])
        with self.assertRaises(ValueError):
            spl.parse_concurrencies("0,16")
        with self.assertRaises(ValueError):
            spl.parse_concurrencies("")

    def test_parser_defaults_keep_group_only(self):
        ap = spl.build_parser()
        args = ap.parse_args([])
        self.assertEqual(args.durabilities, "group")
        self.assertEqual(args.sizes, "4KiB,16KiB,64KiB")
        self.assertEqual(args.concurrencies, "1,16")
        self.assertEqual(args.cond_size, "4KiB")
        self.assertTrue(args.include_conditional)

    def test_build_awanfs_server_argv_omits_durability_for_default_group(self):
        argv_default = spl.build_awanfs_server_argv("s3-server", 9000, "/tmp/d", durability="group")
        self.assertEqual(argv_default, ["s3-server", "--port", "9000", "--data-dir", "/tmp/d", "--allow-anonymous"])
        self.assertNotIn("--durability", argv_default)

        argv_strict = spl.build_awanfs_server_argv("s3-server", 9001, "/tmp/d", durability="strict")
        self.assertEqual(
            argv_strict,
            ["s3-server", "--port", "9001", "--data-dir", "/tmp/d", "--allow-anonymous", "--durability", "strict"],
        )

    def test_build_workloads_covers_sizes_conditions_and_concurrencies(self):
        specs = spl.build_workloads([4096, 16384, 65536], [1, 16], cond_size=4096, include_conditional=True)
        keys = [s.key for s in specs]
        self.assertEqual(
            keys,
            [
                "put_4kib_c1",
                "put_4kib_c16",
                "put_16kib_c1",
                "put_16kib_c16",
                "put_64kib_c1",
                "put_64kib_c16",
                "put_4kib_if_none_match_c1",
                "put_4kib_if_none_match_c16",
                "put_4kib_if_match_c1",
                "put_4kib_if_match_c16",
            ],
        )


class AggregationAndFormattingTests(unittest.TestCase):
    def _sample(self, key: str, p50: float, p99: float, ops: float = 1000.0, size: int = 4096, conc: int = 1, cond: str = "none"):
        return spl.SmallPutMetrics(
            workload_key=key,
            label=f"{size // 1024} KiB PUT",
            size_bytes=size,
            concurrency=conc,
            condition=cond,
            ops=500,
            ops_per_sec=ops,
            mib_per_sec=(ops * size) / (1024.0 * 1024.0),
            p50_ms=p50,
            p90_ms=p99 * 0.8,
            p99_ms=p99,
            p999_ms=p99 * 1.2,
            measured=True,
        )

    def test_aggregate_rounds_computes_median_and_spread(self):
        r1 = {"put_4kib_c1": self._sample("put_4kib_c1", 0.200, 0.500, ops=4000.0)}
        r2 = {"put_4kib_c1": self._sample("put_4kib_c1", 0.300, 0.700, ops=3000.0)}
        r3 = {"put_4kib_c1": self._sample("put_4kib_c1", 0.250, 0.600, ops=3500.0)}
        agg = spl.aggregate_rounds([r1, r2, r3])["put_4kib_c1"]
        self.assertTrue(agg.measured)
        self.assertAlmostEqual(agg.p50_ms, 0.250)
        self.assertAlmostEqual(agg.p99_ms, 0.600)
        self.assertAlmostEqual(agg.ops_per_sec, 3500.0)
        self.assertAlmostEqual(agg.p50_min_ms, 0.200)
        self.assertAlmostEqual(agg.p50_max_ms, 0.300)
        self.assertAlmostEqual(agg.p99_min_ms, 0.500)
        self.assertAlmostEqual(agg.p99_max_ms, 0.700)

    def test_format_markdown_report_starts_with_command_and_renders_ms_and_us(self):
        specs = spl.build_workloads([4096], [1, 16], cond_size=4096, include_conditional=True)
        group_results = {
            "put_4kib_c1": self._sample("put_4kib_c1", 0.412, 1.250, ops=2400.0, size=4096, conc=1, cond="none"),
            "put_4kib_c16": self._sample("put_4kib_c16", 1.500, 4.200, ops=9600.0, size=4096, conc=16, cond="none"),
            "put_4kib_if_none_match_c1": self._sample("put_4kib_if_none_match_c1", 0.450, 1.300, ops=2200.0, size=4096, conc=1, cond="if_none_match_star"),
            "put_4kib_if_none_match_c16": self._sample("put_4kib_if_none_match_c16", 1.600, 4.500, ops=9000.0, size=4096, conc=16, cond="if_none_match_star"),
            "put_4kib_if_match_c1": self._sample("put_4kib_if_match_c1", 0.480, 1.400, ops=2050.0, size=4096, conc=1, cond="if_match"),
            "put_4kib_if_match_c16": spl.SmallPutMetrics(
                workload_key="put_4kib_if_match_c16",
                label="4 KiB PUT (If-Match)",
                size_bytes=4096,
                concurrency=16,
                condition="if_match",
                ops=0,
                ops_per_sec=0.0,
                mib_per_sec=0.0,
                p50_ms=0.0,
                p90_ms=0.0,
                p99_ms=0.0,
                p999_ms=0.0,
                measured=False,
                note="10 requests failed (HTTP 501 x10)",
            ),
        }
        cmd = "python benchmarks/small_put_latency.py --runs 3 --duration 2s"
        md = spl.format_markdown_report(
            cmd=cmd,
            hw_info={"os": "Linux 6.8", "cores": "4", "ram_gb": "16.0"},
            workloads=specs,
            target_results=[("AwanFS (--durability=group, default)", group_results)],
            runs=3,
            duration_sec=2.0,
            durability_notes=["AwanFS (`--durability=group`, default): shared group commit sync before 200 OK."],
        )
        self.assertTrue(md.startswith(f"Command: `{cmd}`\n"))
        self.assertIn("P50 (ms)", md)
        self.assertIn("P50 (us)", md)
        self.assertIn("P99 (ms)", md)
        self.assertIn("P99 (us)", md)
        self.assertIn("| 0.412 | 412.0 | 1.250 | 1250.0 |", md)
        self.assertIn("If-None-Match: *", md)
        self.assertIn("If-Match", md)
        self.assertIn("not measured (10 requests failed (HTTP 501 x10))", md)

    def test_format_markdown_report_names_mc_version_when_provided(self):
        specs = spl.build_workloads([4096], [1], include_conditional=False)
        results = {"put_4kib_c1": self._sample("put_4kib_c1", 0.4, 1.0)}
        md = spl.format_markdown_report(
            cmd="python benchmarks/small_put_latency.py --runs 1 --duration 1s",
            hw_info={"os": "Windows", "cores": "4", "ram_gb": "16.0"},
            workloads=specs,
            target_results=[("RustFS", results)],
            runs=1,
            duration_sec=1.0,
            durability_notes=[],
            mc_version="mc version RELEASE.2025-08-13T08-35-41Z",
        )
        self.assertIn("- **mc:** mc version RELEASE.2025-08-13T08-35-41Z", md)


class ConditionalPutStubPhaseTests(unittest.TestCase):
    def _start_conditional_stub(self, reject_if_match: bool = False):
        lock = threading.Lock()
        objects: Dict[str, str] = {}
        seen_headers: List[Dict[str, str]] = []

        class StubHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_PUT(self):
                n = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(n)
                if_none_match = self.headers.get("If-None-Match")
                if_match = self.headers.get("If-Match")
                etag = f'"{hashlib.md5(body).hexdigest()}"'
                with lock:
                    seen_headers.append(
                        {
                            "path": self.path,
                            "if_none_match": if_none_match,
                            "if_match": if_match,
                        }
                    )
                    if if_none_match == "*" and self.path in objects:
                        self.send_response(412)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    if if_match is not None:
                        if reject_if_match:
                            self.send_response(501)
                            self.send_header("Content-Length", "0")
                            self.end_headers()
                            return
                        cur = objects.get(self.path)
                        if cur is None:
                            self.send_response(404)
                            self.send_header("Content-Length", "0")
                            self.end_headers()
                            return
                        if cur != if_match:
                            self.send_response(412)
                            self.send_header("Content-Length", "0")
                            self.end_headers()
                            return
                    objects[self.path] = etag

                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.send_header("ETag", etag)
                self.end_headers()

        srv = rwb.ThreadedHTTPServer(("127.0.0.1", 0), StubHandler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        return srv, seen_headers, objects

    def test_unconditional_and_conditional_phases_succeed_against_precondition_stub(self):
        srv, seen, objects = self._start_conditional_stub()
        try:
            port = srv.server_address[1]
            m_put = spl.run_small_put_phase(
                "127.0.0.1", port, "small-put-bench",
                spl.WorkloadSpec("put_4kib_c1", "4 KiB PUT", 4096, 1, "none"),
                duration_sec=0.15, phase_tag="t1",
            )
            m_inm = spl.run_small_put_phase(
                "127.0.0.1", port, "small-put-bench",
                spl.WorkloadSpec("put_4kib_if_none_match_c2", "4 KiB PUT (If-None-Match: *)", 4096, 2, "if_none_match_star"),
                duration_sec=0.15, phase_tag="t2",
            )
            m_im = spl.run_small_put_phase(
                "127.0.0.1", port, "small-put-bench",
                spl.WorkloadSpec("put_4kib_if_match_c2", "4 KiB PUT (If-Match)", 4096, 2, "if_match"),
                duration_sec=0.15, phase_tag="t3",
            )
        finally:
            srv.shutdown()
            srv.server_close()

        self.assertTrue(m_put.measured, m_put.note)
        self.assertGreater(m_put.ops, 0)
        self.assertGreaterEqual(m_put.p99_ms, m_put.p50_ms)

        self.assertTrue(m_inm.measured, m_inm.note)
        self.assertGreater(m_inm.ops, 0)
        inm_reqs = [h for h in seen if h["if_none_match"] == "*"]
        self.assertGreaterEqual(len(inm_reqs), m_inm.ops)

        self.assertTrue(m_im.measured, m_im.note)
        self.assertGreater(m_im.ops, 0)
        im_reqs = [h for h in seen if h["if_match"] is not None]
        self.assertGreaterEqual(len(im_reqs), m_im.ops)
        self.assertTrue(all(h["if_match"].startswith('"') for h in im_reqs))
        self.assertGreater(len(objects), 0)

    def test_unsupported_conditional_put_marks_not_measured_with_status(self):
        srv, _seen, _objects = self._start_conditional_stub(reject_if_match=True)
        try:
            m = spl.run_small_put_phase(
                "127.0.0.1", srv.server_address[1], "small-put-bench",
                spl.WorkloadSpec("put_4kib_if_match_c1", "4 KiB PUT (If-Match)", 4096, 1, "if_match"),
                duration_sec=0.15, phase_tag="t4",
            )
        finally:
            srv.shutdown()
            srv.server_close()

        self.assertFalse(m.measured)
        self.assertIn("501", m.note)


class WorkflowWiringTests(unittest.TestCase):
    def test_nightly_bench_full_wires_small_put_latency_with_default_group_only(self):
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        bench_full_path = os.path.join(repo_root, ".github", "workflows", "bench-full.yml")
        with open(bench_full_path, encoding="utf-8") as fh:
            wf = fh.read()
        self.assertIn("benchmarks/small_put_latency.py", wf)
        self.assertIn("benchmarks/test_small_put_latency.py", wf)
        self.assertNotIn("--durability strict", wf)
        self.assertNotIn("--durability=strict", wf)
        self.assertNotIn("--durabilities strict", wf)
        self.assertNotIn("group,strict", wf)

    def test_ci_workflow_runs_small_put_latency_self_tests(self):
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        ci_path = os.path.join(repo_root, ".github", "workflows", "ci.yml")
        with open(ci_path, encoding="utf-8") as fh:
            wf = fh.read()
        self.assertIn("benchmarks/test_small_put_latency.py", wf)

    def test_all_workflows_pin_mc_release_and_cache_key(self):
        import glob
        import re

        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        wf_dir = os.path.join(repo_root, ".github", "workflows")
        paths = sorted(glob.glob(os.path.join(wf_dir, "*.yml")))
        self.assertTrue(paths)
        mc_clone_re = re.compile(r"git clone\b[^\n]*--branch\b[^\n]*MC_RELEASE[^\n]*github\.com/minio/mc")
        for p in paths:
            name = os.path.basename(p)
            with open(p, encoding="utf-8") as fh:
                wf = fh.read()
            self.assertNotIn("quay.io/minio/mc:latest", wf, f"{name} must pin quay.io/minio/mc image tag")
            if "github.com/minio/mc" in wf:
                self.assertIn("MC_RELEASE: RELEASE.2025-08-13T08-35-41Z", wf, f"{name} missing MC_RELEASE")
                self.assertRegex(wf, mc_clone_re, f"{name} must clone minio/mc at MC_RELEASE")
                self.assertIn("github.com/minio/mc/cmd.Version=", wf, f"{name} must set cmd.Version via -ldflags")
                self.assertIn("github.com/minio/mc/cmd.ReleaseTag=", wf, f"{name} must set cmd.ReleaseTag via -ldflags")
                if "uses: actions/cache@" in wf:
                    self.assertIn("env.MC_RELEASE", wf, f"{name} tool cache key must include env.MC_RELEASE")


if __name__ == "__main__":
    unittest.main()
