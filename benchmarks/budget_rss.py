"""Does `--memory-budget` hold? (#127 gate line 2)

Starts the server with one budget, drives `--concurrency` clients doing PUTs and then GETs of
one object size, and reports its peak RSS against the budget plus a stated overhead.

    python benchmarks/budget_rss.py --server-bin server/target/release/s3-server \
        --budget 256MiB --concurrency 64 --size 1MiB --duration 10

Exit codes: 0 under the ceiling, 1 over it or not measured (a phase that moved no bytes),
2 the server did not start.

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
    # The server's output goes to a file in its data directory, never a pipe: it writes one JSON
    # line per request to stderr, and a pipe nobody reads fills and stops the server (#135).
    # The first two runs of this script hung exactly that way (43433893855, 43433917911).
    log_path = os.path.join(data_dir, "server.log")
    with open(log_path, "ab") as log:
        proc = subprocess.Popen(
            [server_bin, "--port", str(port), "--data-dir", data_dir, "--allow-anonymous",
             "--memory-budget", budget, "--auto-compaction", "off"],
            stdout=log, stderr=subprocess.STDOUT,
        )
    print(f"server log: {log_path}", flush=True)
    if not wait_port(port, timeout=40.0):
        proc.kill()
        sys.exit(f"the server did not answer on port {port}")
    return proc, port


def parse_status(text: str) -> dict:
    """RssAnon, RssFile and RssShmem in MiB from the text of /proc/<pid>/status (Linux)."""
    out = {}
    for line in text.splitlines():
        name, _, rest = line.partition(":")
        if name in ("RssAnon", "RssFile", "RssShmem"):
            fields = rest.split()
            if len(fields) == 2 and fields[1] == "kB" and fields[0].isdigit():
                out[name] = int(fields[0]) / 1024.0
    return out


class RssSplit:
    """The highest RssAnon (heap) and RssFile (pages of mapped files, here the volume mapping)
    seen while a phase runs. Peak RSS (VmHWM) has no per-kind peak, so this samples. On a
    platform without /proc both stay None and the report says "not measured" (#127: the owner
    asked for this split before deciding what the budget covers)."""

    def __init__(self, pid: int, every: float = 0.05):
        self.path = f"/proc/{pid}/status"
        self.every = every
        self.anon = None
        self.file = None
        self._stop = None
        self._thread = None

    def sample(self) -> None:
        try:
            with open(self.path, encoding="ascii") as fh:
                got = parse_status(fh.read())
        except OSError:
            return
        if "RssAnon" in got:
            self.anon = max(self.anon or 0.0, got["RssAnon"])
        if "RssFile" in got:
            self.file = max(self.file or 0.0, got["RssFile"])

    def __enter__(self):
        import threading
        self.sample()
        self._stop = threading.Event()

        def loop():
            while not self._stop.wait(self.every):
                self.sample()

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=5)
        self.sample()
        return False


def mib_or_nm(x) -> str:
    return "not measured" if x is None else f"{x:.1f}"


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


def verdict(rows, peak_mib: float, budget_mib: float, overhead: float, size_bytes: int) -> "tuple[str, list]":
    """("ok" | "OVER" | "NOT MEASURED", reasons). A phase that moved fewer bytes than one object
    did not measure anything: the first runs of this script answered every request 404
    (no bucket) and reported 532 ops/s, 0.00 MiB/s and an idle server's 17 MiB as "ok"."""
    reasons = []
    for mode, mib, ops, _p99, _rss, *_ in rows:
        if mib * (1 << 20) < size_bytes:
            reasons.append(f"{mode} moved {mib:.2f} MiB/s at {ops:.1f} ops/s: the requests failed")
    if reasons:
        return "NOT MEASURED", reasons
    if over_ceiling(peak_mib, budget_mib, overhead):
        return "OVER", [f"peak RSS {peak_mib:.1f} MiB over {ceiling_mib(budget_mib, overhead):.1f} MiB"]
    return "ok", []


def metric(text: str, name: str):
    """The value of `name` in a Prometheus text page, or None."""
    for line in text.splitlines():
        if line.startswith(name + " "):
            try:
                return float(line.split()[1])
            except (IndexError, ValueError):
                return None
    return None


def cache_mib(port: int):
    """awanfs_payload_cache_bytes in MiB, or None when the page cannot be read. Read at the end of
    a phase: the cache only grows during one, up to its cap, so that is its peak."""
    import http.client
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10.0)
        conn.request("GET", "/_awanfs/metrics")
        resp = conn.getresponse()
        text = resp.read().decode("utf-8", "replace")
        conn.close()
    except OSError:
        return None
    v = metric(text, "awanfs_payload_cache_bytes")
    return None if v is None else v / (1 << 20)


def create_bucket(port: int, bucket: str) -> None:
    """The phases write into `bucket`, so it has to exist, or every request answers 404."""
    import http.client
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10.0)
    conn.request("PUT", f"/{bucket}")
    resp = conn.getresponse()
    resp.read()
    conn.close()
    if resp.status != 200:
        sys.exit(f"could not create bucket {bucket}: HTTP {resp.status}")


def main(argv=None) -> int:
    a = build_parser().parse_args(argv)

    os.makedirs(a.data_dir, exist_ok=True)
    proc, port = start_server(a.server_bin, a.budget, a.data_dir)
    rows, peak = [], 0.0
    try:
        create_bucket(port, "budget")
        for mode in ("put", "get"):
            with RssSplit(proc.pid) as split:
                m = run_workload_phase("127.0.0.1", port, "budget", SIZES[a.size], a.concurrency,
                                       a.duration, mode, server_pid=proc.pid)
            peak = max(peak, get_process_peak_rss_mb(proc.pid))
            rows.append((mode, m.mib_per_sec, m.ops_per_sec, m.p99_ms, peak, split.anon, split.file, cache_mib(port)))
    finally:
        proc.kill()
        proc.wait()

    ceiling = ceiling_mib(a.budget_mib, a.overhead)
    result, reasons = verdict(rows, peak, a.budget_mib, a.overhead, SIZES[a.size])
    lines = [
        f"Command: `python benchmarks/budget_rss.py --budget {a.budget} --concurrency {a.concurrency} "
        f"--size {a.size} --duration {a.duration} --overhead {a.overhead}`",
        "",
        f"# Does --memory-budget {a.budget} hold? ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs).",
        "",
        "| phase | MiB/s | ops/s | P99 ms | peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB | payload cache MiB | heap less cache MiB |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for mode, mib, ops, p99, rss, anon, filep, cache in rows:
        rest = None if anon is None or cache is None else anon - cache
        lines.append(f"| {mode} | {mib:.2f} | {ops:.1f} | {p99:.2f} | {rss:.1f} | {mib_or_nm(anon)} | {mib_or_nm(filep)} "
                     f"| {mib_or_nm(cache)} | {mib_or_nm(rest)} |")
    lines += [
        "",
        f"Peak RSS {peak:.1f} MiB against a ceiling of {ceiling:.1f} MiB "
        f"(budget {a.budget_mib:.0f} + overhead {a.overhead:.0f}): **{result}**."
        + ("".join(f"\n- {r}" for r in reasons)),
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
    if result != "ok":
        print(f"\nbudget check {result}: " + "; ".join(reasons))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
