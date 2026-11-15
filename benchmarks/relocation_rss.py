"""Peak RSS of the server while a streamed PUT is relocated (#185).

A streamed PUT (above 4 MiB) whose key is written while its body arrives is copied to the end of
the log before it is indexed (decision 51). The copy used to read the object through the volume
mapping, which left every page of it resident: RSS grew by up to the object's size, outside
--memory-budget. This drives that case against a server binary and reports its peak RSS.

    python benchmarks/relocation_rss.py --server-bin server/target/release/s3-server \
        --size 64MiB --budget 32MiB --overhead-mib 16 --report relocation_rss.md

Exit 0: peak RSS at or under the budget plus the overhead. 1: over it. 2: not measured (the
server never relocated, so the run says nothing about the copy).
"""

import argparse
import http.client
import os
import shutil
import socket
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from budget_rss import RssSplit, start_server  # noqa: E402
from run_warp_benchmark import get_process_peak_rss_mb  # noqa: E402

SIZES = {"16MiB": 16 << 20, "64MiB": 64 << 20, "256MiB": 256 << 20}
BUCKET = "reloc-rss"


def verdict(peak_mib: float, budget_mib: float, overhead_mib: float, relocations: int) -> int:
    """The exit code for a run: 2 when nothing was relocated, else 1 over the ceiling, else 0."""
    if relocations < 1:
        return 2
    return 1 if peak_mib > budget_mib + overhead_mib else 0


def metric(port: int, name: str) -> int:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request("GET", "/_awanfs/metrics")
    text = conn.getresponse().read().decode()
    conn.close()
    for line in text.splitlines():
        if line.startswith(name + " "):
            return int(line.split()[1])
    raise SystemExit(f"{name} is not in the server's metrics")


def request(port: int, method: str, path: str, body: bytes = b"") -> int:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    conn.request(method, path, body=body)
    status = conn.getresponse().status
    conn.close()
    return status


def run(server_bin: str, size: int, budget: str) -> dict:
    data_dir = tempfile.mkdtemp(prefix="reloc_rss_")
    proc, port = start_server(server_bin, budget, data_dir)
    try:
        assert request(port, "PUT", f"/{BUCKET}") == 200
        body = os.urandom(size)
        idle = get_process_peak_rss_mb(proc.pid)
        with RssSplit(proc.pid) as split:
            s = socket.create_connection(("127.0.0.1", port), timeout=120)
            head = f"PUT /{BUCKET}/k HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n"
            s.sendall(head.encode())
            half = size // 2
            s.sendall(body[:half])
            # Let the server reserve the streamed needle before the overwrite: the relocation
            # count below says whether it did, so a slow start shows as "not measured".
            time.sleep(1.0)
            assert request(port, "PUT", f"/{BUCKET}/k", b"x" * 1000) == 200
            s.sendall(body[half:])
            answer = b""
            while True:
                part = s.recv(65536)
                if not part:
                    break
                answer += part
            s.close()
            status = answer.split(b" ", 2)[1].decode() if answer.startswith(b"HTTP/1.1") else "none"
        peak = get_process_peak_rss_mb(proc.pid)
        return {
            "status": status,
            "relocations": metric(port, "awanfs_stream_relocations_total"),
            "idle_mib": idle,
            "peak_mib": peak,
            "peak_anon_mib": split.anon,
            "peak_file_mib": split.file,
        }
    finally:
        proc.kill()
        proc.wait(timeout=30)
        shutil.rmtree(data_dir, ignore_errors=True)


def fmt(v) -> str:
    return "not measured" if v is None else f"{v:.1f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--size", default="64MiB", choices=sorted(SIZES))
    ap.add_argument("--budget", default="32MiB")
    ap.add_argument("--budget-mib", type=float, default=32.0)
    ap.add_argument("--overhead-mib", type=float, default=16.0)
    ap.add_argument("--report")
    args = ap.parse_args()

    r = run(args.server_bin, SIZES[args.size], args.budget)
    code = verdict(r["peak_mib"], args.budget_mib, args.overhead_mib, r["relocations"])
    lines = [
        f"Relocation of a {args.size} streamed PUT under --memory-budget {args.budget} (#185)",
        "",
        "| | value |",
        "|---|---|",
        f"| streamed PUT answer | {r['status']} |",
        f"| relocations (awanfs_stream_relocations_total) | {r['relocations']} |",
        f"| RSS before, MiB | {fmt(r['idle_mib'])} |",
        f"| peak RSS, MiB | {fmt(r['peak_mib'])} |",
        f"| peak heap (RssAnon), MiB | {fmt(r['peak_anon_mib'])} |",
        f"| peak file pages (RssFile), MiB | {fmt(r['peak_file_mib'])} |",
        f"| ceiling (budget + overhead), MiB | {args.budget_mib + args.overhead_mib:.1f} |",
        "",
        {0: "ok: under the ceiling", 1: "FAILED: over the ceiling", 2: "not measured: the server did not relocate"}[code],
    ]
    text = "\n".join(lines) + "\n"
    print(text)
    if args.report:
        with open(args.report, "w", encoding="utf-8") as fh:
            fh.write(text)
    return code


if __name__ == "__main__":
    sys.exit(main())
