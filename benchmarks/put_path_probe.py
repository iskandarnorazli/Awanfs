"""Where does a small PUT's time go on this machine? AwanFS only.

    python benchmarks/put_path_probe.py --server-bin server/target/release/s3-server

Runs 64 KiB PUTs at 1, 8 and 64 clients against the server started four ways, and reports
ops/s with P50 and P90 ms for each:

- default: group commit, a volume file that grows as it is written.
- no sync (`AWANFS_DURABILITY=fast`): the same without waiting for the flushes. The gap to
  "default" is what the commit costs.
- sparse volume (`AWAN_SPARSE_VOLUME=1`): the volume is one 32 GiB sparse file mapped once,
  so the file never grows and the mapping is never redone. The gap to "default" is what
  growing costs.
- sparse, no sync: neither.

Use it with benchmarks/sync_probe.py, which says what the disk itself costs.
"""
import argparse
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from budget_rss import create_bucket  # noqa: E402
from ratio_check import wait_port  # noqa: E402
from run_warp_benchmark import get_free_port, run_workload_phase  # noqa: E402

CONFIGS = [
    ("default", {}),
    ("no sync", {"AWANFS_DURABILITY": "fast"}),
    ("sparse volume", {"AWAN_SPARSE_VOLUME": "1"}),
    ("sparse, no sync", {"AWAN_SPARSE_VOLUME": "1", "AWANFS_DURABILITY": "fast"}),
]


def measure(server_bin: str, env_extra: dict, clients, size: int, seconds: float):
    """[(clients, ops/s, p50 ms, p90 ms)] for one server configuration."""
    data = tempfile.mkdtemp(prefix="put_probe_")
    port = get_free_port()
    with open(os.path.join(data, "server.log"), "ab") as log:
        proc = subprocess.Popen([server_bin, "--port", str(port), "--data-dir", data, "--allow-anonymous", "--auto-compaction", "off"],
                                stdout=log, stderr=subprocess.STDOUT, env=dict(os.environ, **env_extra))
    try:
        if not wait_port(port, timeout=40.0):
            raise SystemExit(f"the server did not answer on port {port} with {env_extra}")
        create_bucket(port, "probe")
        out = []
        for c in clients:
            m = run_workload_phase("127.0.0.1", port, "probe", size, c, seconds, "put", server_pid=proc.pid)
            if m.ops_per_sec * size < size:
                raise SystemExit(f"not measured: {c} clients moved nothing with {env_extra}")
            out.append((c, m.ops_per_sec, m.p50_ms, m.p90_ms))
        return out
    finally:
        proc.kill()
        proc.wait()
        shutil.rmtree(data, ignore_errors=True)


def render(results, clients, size, cmd):
    lines = [
        f"Command: `{cmd}`",
        "",
        f"# Where a {size // 1024} KiB PUT's time goes ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs). Data under {tempfile.gettempdir()}. AwanFS only.",
        "",
        "| server started with | " + " | ".join(f"{c} client{'s' if c > 1 else ''}: ops/s (P50, P90 ms)" for c in clients) + " |",
        "|---|" + "---|" * len(clients),
    ]
    for label, rows in results:
        lines.append(f"| {label} | " + " | ".join(f"{ops:.0f} ({p50:.2f}, {p90:.2f})" for _, ops, p50, p90 in rows) + " |")
    lines.append("")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--clients", default="1,8,64")
    ap.add_argument("--size-kib", type=int, default=64)
    ap.add_argument("--seconds", type=float, default=3.0)
    ap.add_argument("--report", default="")
    a = ap.parse_args(argv)
    clients = [int(c) for c in a.clients.split(",")]
    results = []
    for label, env in CONFIGS:
        print(f"{label} ...", flush=True)
        results.append((label, measure(a.server_bin, env, clients, a.size_kib * 1024, a.seconds)))
    cmd = "python benchmarks/put_path_probe.py " + " ".join(argv if argv is not None else sys.argv[1:])
    text = render(results, clients, a.size_kib * 1024, cmd)
    print(text)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as fh:
            fh.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
