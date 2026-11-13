"""Layer 2 performance regression check: AwanFS throughput relative to MinIO on the same machine.

Both servers run on the same runner, one after the other, alternating which goes first, for
`--runs` rounds. Each workload's ratio is median(AwanFS MiB/s) / median(MinIO MiB/s). Machine
speed mostly cancels out of the ratio, so it can be compared across runs on shared CI runners,
which raw MiB/s cannot.

A workload fails when its ratio drops more than `tolerance` (default 20%) below the baseline in
`--baseline`. Without a baseline the script only reports, and prints the JSON to commit as one.

Throughput counts bytes from successful responses only (`BenchmarkMetrics.mib_per_sec`); the
harness's ops/s also counts failed requests.

Both servers sync before acknowledging a PUT: AwanFS in its default `group` durability mode
(#29), MinIO per object. Baselines recorded before #29 compared a non-durable AwanFS PUT and
must be re-recorded for the PUT rows.

Usage:
  python benchmarks/ratio_check.py --server-bin server/target/release/s3-server \\
      --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc [--runs 5] [--duration 3] \\
      [--baseline benchmarks/ratio_baseline.json] [--report ratio_report.md]

Exit codes: 0 ok or no baseline, 1 regression, 2 setup failure (a server did not start).
"""
import argparse
import json
import os
import platform
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_warp_benchmark import get_free_port, get_process_peak_rss_mb, run_workload_phase  # noqa: E402

KIB = 1024
MIB = 1024 * 1024

# name -> (payload size, concurrency, mode)
WORKLOADS: Dict[str, Tuple[int, int, str]] = {
    "put_64k": (64 * KIB, 16, "put"),
    "get_64k": (64 * KIB, 16, "get"),
    "range_get_4k": (64 * KIB, 16, "range_get_4k"),
    "mixed_64k": (64 * KIB, 16, "mixed"),
    "put_1m": (1 * MIB, 8, "put"),
    "get_1m": (1 * MIB, 8, "get"),
}
BUCKET = "ratio-bench"


def median_ratios(awanfs: Dict[str, List[float]], minio: Dict[str, List[float]]) -> Dict[str, Optional[float]]:
    """Per workload: median(awanfs) / median(minio), or None when MinIO moved no bytes."""
    out: Dict[str, Optional[float]] = {}
    for name in awanfs:
        m = statistics.median(minio.get(name) or [0.0])
        out[name] = statistics.median(awanfs[name]) / m if m > 0 else None
    return out


def compare(ratios: Dict[str, Optional[float]], baseline: Optional[dict]) -> Tuple[List[tuple], List[str]]:
    """(rows, failures). rows: (workload, ratio, baseline ratio, verdict)."""
    rows, bad = [], []
    tol = float((baseline or {}).get("tolerance", 0.20))
    base_ratios = (baseline or {}).get("ratios", {})
    for name, r in ratios.items():
        b = base_ratios.get(name)
        if r is None:
            rows.append((name, None, b, "MinIO moved no bytes"))
            bad.append(f"{name}: MinIO moved no bytes, so there is no ratio")
        elif b is None:
            rows.append((name, r, None, "no baseline"))
        elif r < b * (1 - tol):
            rows.append((name, r, b, "REGRESSION"))
            bad.append(f"{name}: ratio {r:.2f}, baseline {b:.2f} (allowed down to {b * (1 - tol):.2f})")
        elif r > b * (1 + tol):
            rows.append((name, r, b, "faster: raise the baseline"))
        else:
            rows.append((name, r, b, "ok"))
    return rows, bad


def render(rows: List[tuple], awanfs: Dict[str, List[float]], minio: Dict[str, List[float]], cmd: str, runs: int,
           rss: Optional[Dict[str, List[float]]] = None) -> str:
    def f(x):
        return "-" if x is None else f"{x:.2f}"

    lines = [
        f"Command: `{cmd}`",
        "",
        f"# AwanFS / MinIO throughput ratio ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs). {runs} runs per server, "
        "order alternating; medians of MiB/s from successful responses.",
        "",
        "| workload | AwanFS MiB/s (median) | MinIO MiB/s (median) | ratio | baseline | verdict |",
        "|---|---|---|---|---|---|",
    ]
    for name, r, b, verdict in rows:
        a_med = statistics.median(awanfs[name]) if awanfs.get(name) else None
        m_med = statistics.median(minio[name]) if minio.get(name) else None
        lines.append(f"| `{name}` | {f(a_med)} | {f(m_med)} | {f(r)} | {f(b)} | {verdict} |")
    if rss and all(rss.get(k) for k in ("awanfs", "minio")):
        a_rss, m_rss = statistics.median(rss["awanfs"]), statistics.median(rss["minio"])
        lines += ["", "| server | peak RSS over all workloads, MiB (median of runs) |", "|---|---|",
                  f"| AwanFS | {a_rss:.1f} |", f"| MinIO | {m_rss:.1f} |",
                  "", "Peak RSS is recorded, not checked: it includes file pages of mapped volumes, which the OS can reclaim."]
    lines += ["", "AwanFS runs in its default `group` durability mode: PUTs are synced before the answer, as in MinIO.", ""]
    return "\n".join(lines)


def wait_port(port: int, timeout: float = 20.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def start_awanfs(server_bin: str) -> Tuple[subprocess.Popen, int, str]:
    data = tempfile.mkdtemp(prefix="ratio_awanfs_")
    port = get_free_port()
    proc = subprocess.Popen([server_bin, "--port", str(port), "--data-dir", data, "--allow-anonymous"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_port(port):
        proc.kill()
        raise RuntimeError("s3-server did not start")
    import http.client
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5.0)
    conn.request("PUT", f"/{BUCKET}")
    conn.getresponse().read()
    conn.close()
    return proc, port, data


def start_minio(minio_bin: str, mc_bin: str) -> Tuple[subprocess.Popen, int, str]:
    data = tempfile.mkdtemp(prefix="ratio_minio_")
    port, console = get_free_port(), get_free_port()
    env = dict(os.environ, MINIO_ROOT_USER="minioadmin", MINIO_ROOT_PASSWORD="minioadmin",
               MINIO_UPDATE="off", MINIO_BROWSER="off")
    proc = subprocess.Popen([minio_bin, "server", data, "--address", f"127.0.0.1:{port}",
                             "--console-address", f"127.0.0.1:{console}"],
                            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_port(port):
        proc.kill()
        raise RuntimeError("minio did not start")
    alias = f"ratio{port}"
    for args in (["alias", "set", alias, f"http://127.0.0.1:{port}", "minioadmin", "minioadmin"],
                 ["mb", f"{alias}/{BUCKET}"],
                 ["anonymous", "set", "public", f"{alias}/{BUCKET}"]):
        for _ in range(20):
            if subprocess.run([mc_bin, *args], capture_output=True).returncode == 0:
                break
            time.sleep(0.5)
        else:
            proc.kill()
            raise RuntimeError(f"mc {' '.join(args[:1])} failed")
    return proc, port, data


def measure(start, duration: float) -> Tuple[Dict[str, float], float]:
    """MiB/s per workload, and the server's peak RSS in MiB after all of them (0 if unknown)."""
    proc, port, data = start()
    try:
        mib = {name: run_workload_phase("127.0.0.1", port, BUCKET, size, conc, duration, mode, proc.pid).mib_per_sec
               for name, (size, conc, mode) in WORKLOADS.items()}
        return mib, get_process_peak_rss_mb(proc.pid)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(data, ignore_errors=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--minio-bin", required=True)
    ap.add_argument("--mc-bin", required=True)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--duration", type=float, default=3.0)
    ap.add_argument("--baseline", default=os.path.join(os.path.dirname(__file__), "ratio_baseline.json"))
    ap.add_argument("--report", help="write the markdown report here")
    a = ap.parse_args(argv)
    cmd = "python benchmarks/ratio_check.py " + " ".join(argv if argv is not None else sys.argv[1:])

    awanfs: Dict[str, List[float]] = {n: [] for n in WORKLOADS}
    minio: Dict[str, List[float]] = {n: [] for n in WORKLOADS}
    rss: Dict[str, List[float]] = {"awanfs": [], "minio": []}
    starters = [("awanfs", lambda: start_awanfs(a.server_bin), awanfs),
                ("minio", lambda: start_minio(a.minio_bin, a.mc_bin), minio)]
    try:
        for run in range(a.runs):
            order = starters if run % 2 == 0 else starters[::-1]
            for label, start, sink in order:
                print(f"run {run + 1}/{a.runs}: {label}", flush=True)
                results, peak = measure(start, a.duration)
                for name, mib in results.items():
                    sink[name].append(mib)
                if peak > 0:
                    rss[label].append(peak)
    except RuntimeError as e:
        print(f"setup failed: {e}")
        return 2

    baseline = None
    if os.path.exists(a.baseline):
        with open(a.baseline, encoding="utf-8") as fh:
            baseline = json.load(fh)
    ratios = median_ratios(awanfs, minio)
    rows, bad = compare(ratios, baseline)
    report = render(rows, awanfs, minio, cmd, a.runs, rss)
    if baseline is None:
        suggestion = {"tolerance": 0.20, "_machine": "github ubuntu-latest",
                      "ratios": {k: round(v, 3) for k, v in ratios.items() if v is not None}}
        report += "\nNo baseline yet. After review, commit this as `benchmarks/ratio_baseline.json`:\n\n```json\n" \
                  + json.dumps(suggestion, indent=2) + "\n```\n"
    print(report)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as fh:
            fh.write(report)
    if bad:
        print("\nratio check failed:\n" + "\n".join(bad))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
