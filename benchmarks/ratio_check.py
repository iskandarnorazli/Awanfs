"""Layer 2 performance regression check: AwanFS throughput relative to MinIO on the same machine.

Both servers run on the same runner, one after the other, alternating which goes first, for
`--runs` rounds. Each workload's ratio is median(AwanFS MiB/s) / median(MinIO MiB/s). Machine
speed mostly cancels out of the ratio, so it can be compared across runs on shared CI runners,
which raw MiB/s cannot.

A workload fails when its ratio drops more than `tolerance` (default 20%) below the baseline in
`--baseline`, and the run fails when AwanFS's median peak RSS is over that file's
`rss_ceiling_mib`. Without a baseline the script only reports, and prints the JSON to commit as one.

Throughput counts bytes from successful responses only (`BenchmarkMetrics.mib_per_sec`); the
harness's ops/s also counts failed requests.

Both servers sync before acknowledging a PUT: AwanFS in its default `group` durability mode
(#29), MinIO per object. Baselines recorded before #29 compared a non-durable AwanFS PUT and
must be re-recorded for the PUT rows.

Usage:
  python benchmarks/ratio_check.py --server-bin server/target/release/awanfs \\
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
import threading
import time
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run_warp_benchmark as rwb  # noqa: E402
from run_warp_benchmark import (  # noqa: E402
    affinity_labels,
    affinity_masks,
    can_pin,
    get_free_port,
    get_process_peak_rss_mb,
    run_workload_phase,
    set_affinity_mask,
)

_ORIG_RUN_WORKLOAD_PHASE = rwb.run_workload_phase

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


def check_rss(rss: Optional[Dict[str, List[float]]], baseline: Optional[dict]) -> Tuple[Optional[float], Optional[float], List[str]]:
    """(AwanFS median peak RSS, ceiling, failures). A ceiling of None only reports.

    Peak RSS counts resident file pages of the mapped volumes, so a read that faults a payload
    in shows up here. That is the point: a container's memory limit counts those pages too.
    """
    med = statistics.median(rss["awanfs"]) if rss and rss.get("awanfs") else None
    ceiling = (baseline or {}).get("rss_ceiling_mib")
    if med is None or ceiling is None:
        return med, ceiling, []
    if med > float(ceiling):
        return med, ceiling, [f"AwanFS peak RSS {med:.1f} MiB, ceiling {float(ceiling):.1f} MiB"]
    return med, ceiling, []


def _ct_user_system(ct) -> Tuple[float, float]:
    return (
        float(ct.user) + float(getattr(ct, "children_user", 0.0) or 0.0),
        float(ct.system) + float(getattr(ct, "children_system", 0.0) or 0.0),
    )


def _is_zombie(proc, zombie_status) -> bool:
    status_fn = getattr(proc, "status", None)
    return callable(status_fn) and status_fn() == zombie_status


def process_cpu_times(pid: Optional[int]) -> Optional[Tuple[float, float]]:
    """Return `(user_s, system_s)` for `pid` and recursive children via psutil, or None."""
    if not pid or pid <= 0:
        return None
    try:
        import psutil  # type: ignore
    except ImportError:
        return None
    no_such = getattr(psutil, "NoSuchProcess", Exception)
    access_denied = getattr(psutil, "AccessDenied", Exception)
    zombie_status = getattr(psutil, "STATUS_ZOMBIE", "zombie")
    try:
        proc = psutil.Process(pid)
        if _is_zombie(proc, zombie_status):
            return None
        user_s, sys_s = _ct_user_system(proc.cpu_times())
        children_fn = getattr(proc, "children", None)
        if callable(children_fn):
            for child in children_fn(recursive=True):
                try:
                    if _is_zombie(child, zombie_status):
                        continue
                    cu, cs = _ct_user_system(child.cpu_times())
                    user_s += cu
                    sys_s += cs
                except (no_such, access_denied, OSError):
                    pass
        return user_s, sys_s
    except (no_such, access_denied, OSError):
        return None


def process_cpu_seconds(pid: Optional[int]) -> Optional[float]:
    """Return total CPU seconds (user + system, children included) for `pid`, or None."""
    ct = process_cpu_times(pid)
    return (ct[0] + ct[1]) if ct is not None else None


def cpu_seconds_per_1k_ops(
    cpu_before: Optional[float],
    cpu_after: Optional[float],
    ops: float,
) -> Optional[float]:
    """Server CPU seconds per 1,000 operations from before/after CPU seconds and op count."""
    if cpu_before is None or cpu_after is None or ops <= 0:
        return None
    return (max(0.0, cpu_after - cpu_before) / ops) * 1000.0


def median_cpu_per_1k(samples: Optional[Dict[str, List[Optional[float]]]], name: str) -> Optional[float]:
    """Median measured CPU s / 1k ops for workload `name`, or None if unmeasured."""
    if not samples:
        return None
    vals = [v for v in samples.get(name, []) if v is not None]
    return statistics.median(vals) if vals else None


def render(rows: List[tuple], awanfs: Dict[str, List[float]], minio: Dict[str, List[float]], cmd: str, runs: int,
           rss: Optional[Dict[str, List[float]]] = None, rss_ceiling: Optional[float] = None,
           pin_labels: Optional[Tuple[str, str]] = None,
           awanfs_cpu: Optional[Dict[str, List[Optional[float]]]] = None,
           minio_cpu: Optional[Dict[str, List[Optional[float]]]] = None) -> str:
    def f(x):
        return "-" if x is None else f"{x:.2f}"

    def f_cpu(x):
        return "not measured" if x is None else f"{x:.3f}"

    lines = [
        f"Command: `{cmd}`",
        "",
        f"# AwanFS / MinIO throughput ratio ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs). {runs} runs per server, "
        "order alternating; medians of MiB/s from successful responses.",
    ]
    if pin_labels:
        lines.append(f"Core pinning: server `{pin_labels[0]}`, client `{pin_labels[1]}`.")
    lines += [
        "",
        "| workload | AwanFS MiB/s (median) | MinIO MiB/s (median) | ratio | baseline | verdict | AwanFS CPU s / 1k ops | MinIO CPU s / 1k ops | CPU ratio |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for name, r, b, verdict in rows:
        a_med = statistics.median(awanfs[name]) if awanfs.get(name) else None
        m_med = statistics.median(minio[name]) if minio.get(name) else None
        a_cpu = median_cpu_per_1k(awanfs_cpu, name)
        m_cpu = median_cpu_per_1k(minio_cpu, name)
        c_ratio = (a_cpu / m_cpu) if (a_cpu is not None and m_cpu is not None and m_cpu > 0) else None
        lines.append(
            f"| `{name}` | {f(a_med)} | {f(m_med)} | {f(r)} | {f(b)} | {verdict} "
            f"| {f_cpu(a_cpu)} | {f_cpu(m_cpu)} | {f(c_ratio)} |"
        )
    if rss and all(rss.get(k) for k in ("awanfs", "minio")):
        a_rss, m_rss = statistics.median(rss["awanfs"]), statistics.median(rss["minio"])
        if rss_ceiling is None:
            verdict, note = "no ceiling", "No RSS ceiling in the baseline, so peak RSS is only reported."
        elif a_rss > float(rss_ceiling):
            verdict = "OVER THE CEILING"
            note = f"AwanFS peak RSS is over the ceiling of {float(rss_ceiling):.1f} MiB. This check fails the run."
        else:
            verdict = "ok"
            note = f"Ceiling {float(rss_ceiling):.1f} MiB. Peak RSS counts resident file pages of the mapped " \
                   "volumes, which a container's memory limit counts too."
        lines += ["", "| server | peak RSS over all workloads, MiB (median of runs) | verdict |", "|---|---|---|",
                  f"| AwanFS | {a_rss:.1f} | {verdict} |", f"| MinIO | {m_rss:.1f} | - |", "", note]
    lines += [
        "",
        "AwanFS runs in its default `group` durability mode: PUTs are synced before the answer, as in MinIO. "
        "CPU s / 1k ops is server process CPU seconds (user + system, children included) per 1,000 successful operations (reported only).",
        "",
    ]
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
    if can_pin():
        srv_mask, _ = affinity_masks()
        set_affinity_mask(proc.pid, srv_mask)
    if not wait_port(port):
        proc.kill()
        raise RuntimeError("awanfs did not start")
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
    if can_pin():
        srv_mask, _ = affinity_masks()
        set_affinity_mask(proc.pid, srv_mask)
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


def _proc_cpu_seconds_if_running(proc: subprocess.Popen) -> Optional[float]:
    if proc.poll() is not None:
        return None
    return process_cpu_seconds(proc.pid)


def _measure_phase(
    proc: subprocess.Popen,
    port: int,
    size: int,
    conc: int,
    duration: float,
    mode: str,
) -> Tuple[float, Optional[float]]:
    """Run one workload phase and return `(mib_per_sec, cpu_s_per_1k_ops)`."""
    caller_thread = threading.current_thread()
    phase_samples: List[Tuple[float, Optional[float]]] = []
    orig_cpu_fn = rwb.get_process_cpu_times
    use_hook = run_workload_phase is _ORIG_RUN_WORKLOAD_PHASE

    def hooked_cpu(pid: int) -> Tuple[float, float]:
        if pid == proc.pid and threading.current_thread() is caller_thread:
            ct = None if proc.poll() is not None else process_cpu_times(pid)
            phase_samples.append((time.perf_counter(), (ct[0] + ct[1]) if ct is not None else None))
            return ct if ct is not None else (0.0, 0.0)
        return orig_cpu_fn(pid)

    cpu_before = None if use_hook else _proc_cpu_seconds_if_running(proc)
    rwb.get_process_cpu_times = hooked_cpu
    try:
        metrics = run_workload_phase("127.0.0.1", port, BUCKET, size, conc, duration, mode, proc.pid)
    finally:
        rwb.get_process_cpu_times = orig_cpu_fn
    cpu_after = None if use_hook else _proc_cpu_seconds_if_running(proc)

    if proc.poll() is not None or not metrics.measured or metrics.ops_per_sec <= 0:
        return metrics.mib_per_sec, None

    if len(phase_samples) >= 2:
        t0, c0 = phase_samples[0]
        t1, c1 = phase_samples[-1]
        elapsed = t1 - t0
        ops = metrics.ops_per_sec * elapsed if elapsed > 0 else 0.0
        return metrics.mib_per_sec, cpu_seconds_per_1k_ops(c0, c1, ops)

    ops = metrics.ops_per_sec * duration if duration > 0 else 0.0
    return metrics.mib_per_sec, cpu_seconds_per_1k_ops(cpu_before, cpu_after, ops)


def measure_run(start, duration: float) -> Tuple[Dict[str, float], float, Dict[str, Optional[float]]]:
    """MiB/s per workload, server peak RSS in MiB, and server CPU s / 1k ops per workload."""
    proc, port, data = start()
    try:
        mib: Dict[str, float] = {}
        cpu_per_1k: Dict[str, Optional[float]] = {}
        for name, (size, conc, mode) in WORKLOADS.items():
            m_val, c_val = _measure_phase(proc, port, size, conc, duration, mode)
            mib[name] = m_val
            cpu_per_1k[name] = c_val
        return mib, get_process_peak_rss_mb(proc.pid), cpu_per_1k
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(data, ignore_errors=True)


def measure(start, duration: float) -> Tuple[Dict[str, float], float]:
    """MiB/s per workload, and the server's peak RSS in MiB after all of them (0 if unknown)."""
    mib, peak, _ = measure_run(start, duration)
    return mib, peak


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

    try:
        import psutil  # type: ignore  # noqa: F401
    except ImportError:
        print("note: psutil is not installed; CPU s / 1k ops will be not measured", flush=True)

    pin_labels = None
    if can_pin():
        _, cli_mask = affinity_masks()
        set_affinity_mask(None, cli_mask)
        pin_labels = affinity_labels()

    awanfs: Dict[str, List[float]] = {n: [] for n in WORKLOADS}
    minio: Dict[str, List[float]] = {n: [] for n in WORKLOADS}
    awanfs_cpu: Dict[str, List[Optional[float]]] = {n: [] for n in WORKLOADS}
    minio_cpu: Dict[str, List[Optional[float]]] = {n: [] for n in WORKLOADS}
    rss: Dict[str, List[float]] = {"awanfs": [], "minio": []}
    starters = [("awanfs", lambda: start_awanfs(a.server_bin), awanfs, awanfs_cpu),
                ("minio", lambda: start_minio(a.minio_bin, a.mc_bin), minio, minio_cpu)]
    try:
        for run in range(a.runs):
            order = starters if run % 2 == 0 else starters[::-1]
            for label, start, sink, cpu_sink in order:
                print(f"run {run + 1}/{a.runs}: {label}", flush=True)
                results, peak, cpu_results = measure_run(start, a.duration)
                for name, mib in results.items():
                    sink[name].append(mib)
                for name, cpu_val in cpu_results.items():
                    cpu_sink[name].append(cpu_val)
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
    _, ceiling, rss_bad = check_rss(rss, baseline)
    bad += rss_bad
    report = render(rows, awanfs, minio, cmd, a.runs, rss, ceiling, pin_labels=pin_labels,
                    awanfs_cpu=awanfs_cpu, minio_cpu=minio_cpu)
    if baseline is None:
        suggestion = {"tolerance": 0.20, "_machine": "github ubuntu-latest",
                      "rss_ceiling_mib": round(2 * statistics.median(rss["awanfs"])) if rss.get("awanfs") else None,
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
