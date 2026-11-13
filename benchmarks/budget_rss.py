"""Does `--memory-budget` hold? (#127 gate line 2)

Starts the server with one budget, drives `--concurrency` clients doing PUTs and then GETs of
one object size, and reports its peak RSS against the budget plus a stated overhead.

    python benchmarks/budget_rss.py --server-bin server/target/release/s3-server \
        --budget 256MiB --concurrency 64 --size 1MiB --duration 10

Exit codes: 0 under the ceiling, 1 over it, 2 the server did not start.

The ceiling is the budget plus `--overhead` MiB. The overhead is the binary, the runtime, the
volume mapping's resident pages and the index; it is not part of the budget, which bounds the
payload cache and the in-flight request bodies. State the overhead you measured, do not raise it
to make a run pass.
"""
import argparse
import os
import platform
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_warp_benchmark import get_free_port, get_process_peak_rss_mb, run_workload_phase  # noqa: E402

from ratio_check import wait_port  # noqa: E402

SIZES = {"4KiB": 4096, "64KiB": 65536, "1MiB": 1 << 20, "4MiB": 4 << 20, "16MiB": 16 << 20}


def start_server(server_bin: str, budget: str, data_dir: str) -> "tuple[subprocess.Popen, int]":
    port = get_free_port()
    proc = subprocess.Popen(
        [server_bin, "--port", str(port), "--data-dir", data_dir, "--allow-anonymous",
         "--memory-budget", budget, "--auto-compaction", "off"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    if not wait_port(port, timeout=40.0):
        proc.kill()
        sys.exit(f"the server did not answer on port {port}")
    return proc, port


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--budget", default="256MiB")
    ap.add_argument("--budget-mib", type=float, default=256.0, help="the same budget in MiB, for the arithmetic")
    ap.add_argument("--concurrency", type=int, default=64)
    ap.add_argument("--size", default="1MiB", choices=sorted(SIZES))
    ap.add_argument("--duration", type=float, default=10.0)
    ap.add_argument("--overhead", type=float, default=200.0, help="MiB outside the budget; state what you measured")
    ap.add_argument("--data-dir", default=os.path.join(".tmp", "budget_rss"))
    ap.add_argument("--report", default="")
    return ap


def ceiling_mib(budget_mib: float, overhead: float) -> float:
    """What peak RSS may reach: the budget plus the overhead outside it."""
    return budget_mib + overhead


def over_ceiling(peak_mib: float, budget_mib: float, overhead: float) -> bool:
    return peak_mib > ceiling_mib(budget_mib, overhead)


def main(argv=None) -> int:
    a = build_parser().parse_args(argv)

    os.makedirs(a.data_dir, exist_ok=True)
    proc, port = start_server(a.server_bin, a.budget, a.data_dir)
    rows, peak = [], 0.0
    try:
        for mode in ("put", "get"):
            m = run_workload_phase("127.0.0.1", port, "budget", SIZES[a.size], a.concurrency,
                                   a.duration, mode, server_pid=proc.pid)
            peak = max(peak, get_process_peak_rss_mb(proc.pid))
            rows.append((mode, m.mib_per_sec, m.ops_per_sec, m.p99_ms, peak))
    finally:
        proc.kill()
        proc.wait()

    ceiling = ceiling_mib(a.budget_mib, a.overhead)
    over = over_ceiling(peak, a.budget_mib, a.overhead)
    lines = [
        f"Command: `python benchmarks/budget_rss.py --budget {a.budget} --concurrency {a.concurrency} "
        f"--size {a.size} --duration {a.duration} --overhead {a.overhead}`",
        "",
        f"# Does --memory-budget {a.budget} hold? ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs).",
        "",
        "| phase | MiB/s | ops/s | P99 ms | peak RSS MiB |",
        "|---|---|---|---|---|",
    ]
    for mode, mib, ops, p99, rss in rows:
        lines.append(f"| {mode} | {mib:.2f} | {ops:.1f} | {p99:.2f} | {rss:.1f} |")
    lines += [
        "",
        f"Peak RSS {peak:.1f} MiB against a ceiling of {ceiling:.1f} MiB "
        f"(budget {a.budget_mib:.0f} + overhead {a.overhead:.0f}): **{'OVER' if over else 'ok'}**.",
        "",
        "The budget bounds the payload cache and the bytes in-flight request bodies hold. The "
        "overhead above it is the binary, the runtime, the resident pages of the volume mapping "
        "and the index.",
        "",
    ]
    report = "\n".join(lines)
    print(report)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as fh:
            fh.write(report)
    if over:
        print(f"\nbudget check failed: peak RSS {peak:.1f} MiB, ceiling {ceiling:.1f} MiB")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
