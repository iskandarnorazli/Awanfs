"""CPU core scaling benchmark: PUT and GET throughput against cores, AwanFS vs MinIO vs RustFS.

    python benchmarks/core_scaling.py [--cores 1,2,4] [--runs 2] [--duration 3] [--output core_scaling.md]

Measures how 4 KiB and 64 KiB PUT and GET ops/s, P50/P99 latency, server CPU %, and ops/s per
busy core scale with N pinned server cores (1, 2, 4, ...). Writes a markdown report starting with
its command and SeaStore-style 4-panel PNG and SVG charts next to the report.
"""
import argparse
import http.client
import math
import os
import platform
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import run_warp_benchmark as rwb  # noqa: E402
import sync_probe  # noqa: E402

KIB = 1024
BUCKET = "core-scaling-bench"
SERVERS: Tuple[str, ...] = ("AwanFS", "MinIO", "RustFS")

# (key, display label, mode, payload_size)
WORKLOADS: List[Tuple[str, str, str, int]] = [
    ("put_4k", "PUT 4 KiB", "put", 4 * KIB),
    ("put_64k", "PUT 64 KiB", "put", 64 * KIB),
    ("get_4k", "GET 4 KiB", "get", 4 * KIB),
    ("get_64k", "GET 64 KiB", "get", 64 * KIB),
]

SERVER_STYLES: Dict[str, Dict[str, str]] = {
    "AwanFS": {"linestyle": "-", "marker": "o", "color": "#1f77b4"},
    "MinIO": {"linestyle": "--", "marker": "s", "color": "#d62728"},
    "RustFS": {"linestyle": "-.", "marker": "^", "color": "#2ca02c"},
}


@dataclass
class PointResult:
    ops_per_sec: Optional[float] = None
    p50_ms: Optional[float] = None
    p99_ms: Optional[float] = None
    cpu_pct: Optional[float] = None
    ops_per_busy_core: Optional[float] = None
    reason: str = ""
    shares_cores: bool = False

    @property
    def measured(self) -> bool:
        return self.ops_per_sec is not None


def default_cores(total_cores: Optional[int] = None) -> List[int]:
    total = max(1, total_cores if total_cores is not None else (os.cpu_count() or 1))
    cores = []
    n = 1
    while n <= total:
        cores.append(n)
        n *= 2
    return cores or [1]


def parse_cores(spec: str, total_cores: Optional[int] = None) -> List[int]:
    if not spec or not spec.strip():
        return default_cores(total_cores)
    vals = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        v = int(part)
        if v < 1:
            raise ValueError(f"core count must be >= 1, got {v}")
        if v not in vals:
            vals.append(v)
    if not vals:
        return default_cores(total_cores)
    return sorted(vals)


def pinning_command(argv: Sequence[str], n_cores: int, platform_name: str = sys.platform) -> List[str]:
    """Wraps `argv` with OS-level pinning before exec on Linux; leaves argv untouched on Windows."""
    if n_cores < 1:
        raise ValueError(f"n_cores must be >= 1, got {n_cores}")
    if platform_name.startswith("linux"):
        cpu_list = "0" if n_cores == 1 else f"0-{n_cores - 1}"
        return ["taskset", "-c", cpu_list, *argv]
    return list(argv)


def pin_process(pid: int, cores: Sequence[int]) -> bool:
    """Pins `pid` to `cores` via psutil (preferred) or OS fallback. Returns True on success."""
    core_list = [int(c) for c in cores]
    if not core_list:
        return False
    try:
        import psutil  # type: ignore
        psutil.Process(pid).cpu_affinity(core_list)
        return True
    except ImportError:
        pass
    except Exception:
        return False

    if sys.platform == "win32":
        import ctypes
        mask = 0
        for c in core_list:
            mask |= 1 << c
        try:
            PROCESS_SET_INFORMATION = 0x0200
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_SET_INFORMATION, False, pid)
            if handle:
                try:
                    return bool(ctypes.windll.kernel32.SetProcessAffinityMask(handle, mask))
                finally:
                    ctypes.windll.kernel32.CloseHandle(handle)
        except Exception:
            return False
    elif hasattr(os, "sched_setaffinity"):
        try:
            os.sched_setaffinity(pid, set(core_list))
            return True
        except Exception:
            return False
    return False


def get_process_affinity(pid: int) -> Optional[List[int]]:
    try:
        import psutil  # type: ignore
        return list(psutil.Process(pid).cpu_affinity())
    except Exception:
        if hasattr(os, "sched_getaffinity"):
            try:
                return sorted(os.sched_getaffinity(pid))
            except Exception:
                return None
        return None


def client_cores_for(n_cores: int, total_cores: int) -> Tuple[List[int], bool]:
    """Returns `(client_core_ids, shares_cores)` for a server pinned to `0..n_cores-1`."""
    total = max(1, total_cores)
    if total > n_cores:
        return list(range(n_cores, total)), False
    return list(range(total)), True


def process_cpu_times_total(pid: int) -> Tuple[float, float]:
    """Returns `(user_s, system_s)` for `pid` including recursive children via psutil."""
    try:
        import psutil  # type: ignore
        proc = psutil.Process(pid)
        ct = proc.cpu_times()
        user_s = float(ct.user)
        sys_s = float(ct.system)
        for child in proc.children(recursive=True):
            try:
                cct = child.cpu_times()
                user_s += float(cct.user)
                sys_s += float(cct.system)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return user_s, sys_s
    except ImportError:
        return rwb.get_process_cpu_times(pid)
    except Exception:
        return 0.0, 0.0


def cpu_percent(before: Tuple[float, float], after: Tuple[float, float], wall_sec: float) -> Optional[float]:
    """Server CPU % over `wall_sec`, where 100.0% = 1 full core busy."""
    if wall_sec <= 0:
        return None
    delta = max(0.0, (after[0] + after[1]) - (before[0] + before[1]))
    return (delta / wall_sec) * 100.0


def ops_per_busy_core(ops_per_sec: Optional[float], cpu_pct: Optional[float]) -> Optional[float]:
    if ops_per_sec is None or cpu_pct is None or cpu_pct <= 0:
        return None
    return ops_per_sec / (cpu_pct / 100.0)


def server_order_for_run(run_idx: int, servers: Sequence[str] = SERVERS) -> List[str]:
    seq = list(servers)
    return seq if run_idx % 2 == 0 else seq[::-1]


def aggregate_runs(run_points: Sequence[PointResult]) -> PointResult:
    """Computes per-metric medians across measured runs, or returns the failure reason."""
    measured = [p for p in run_points if p.measured]
    if not measured:
        reason = next((p.reason for p in run_points if p.reason), "measurement failed")
        shares = any(p.shares_cores for p in run_points)
        return PointResult(reason=reason, shares_cores=shares)

    ops_med = statistics.median(p.ops_per_sec for p in measured if p.ops_per_sec is not None)
    p50_med = statistics.median(p.p50_ms for p in measured if p.p50_ms is not None)
    p99_med = statistics.median(p.p99_ms for p in measured if p.p99_ms is not None)
    cpu_vals = [p.cpu_pct for p in measured if p.cpu_pct is not None]
    cpu_med = statistics.median(cpu_vals) if cpu_vals else None
    eff_vals = [p.ops_per_busy_core for p in measured if p.ops_per_busy_core is not None]
    eff_med = statistics.median(eff_vals) if eff_vals else ops_per_busy_core(ops_med, cpu_med)
    return PointResult(
        ops_per_sec=ops_med,
        p50_ms=p50_med,
        p99_ms=p99_med,
        cpu_pct=cpu_med,
        ops_per_busy_core=eff_med,
        shares_cores=any(p.shares_cores for p in measured),
    )


def compute_scaling(points_by_core: Dict[int, PointResult], cores: Sequence[int]) -> Optional[float]:
    if not cores:
        return None
    base_n = 1 if 1 in cores else min(cores)
    max_n = max(cores)
    base = points_by_core.get(base_n)
    top = points_by_core.get(max_n)
    if not base or not top or not base.measured or not top.measured:
        return None
    if not base.ops_per_sec or base.ops_per_sec <= 0 or top.ops_per_sec is None:
        return None
    return top.ops_per_sec / base.ops_per_sec


def _tail_log(log_path: str, max_chars: int = 200) -> str:
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as fh:
            lines = [ln.strip() for ln in fh.read().splitlines() if ln.strip()]
        if not lines:
            return ""
        tail = " | ".join(lines[-3:])
        return tail[-max_chars:]
    except OSError:
        return ""


def _wait_port(proc: subprocess.Popen, port: int, timeout: float = 20.0) -> Optional[str]:
    end = time.time() + timeout
    while time.time() < end:
        code = proc.poll()
        if code is not None:
            return f"exited with code {code}"
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                return None
        except OSError:
            time.sleep(0.15)
    return f"port {port} did not open within {timeout:.0f}s"


def _stop_proc(proc: Optional[subprocess.Popen]) -> None:
    if proc is None:
        return
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _spawn_pinned(argv: List[str], n_cores: int, log_path: str, env: Optional[dict] = None) -> subprocess.Popen:
    cmd = pinning_command(argv, n_cores)
    use_taskset = sys.platform.startswith("linux") and cmd[0] == "taskset" and shutil.which("taskset") is not None
    launch_argv = cmd if use_taskset else argv
    log = open(log_path, "ab")
    try:
        proc = subprocess.Popen(launch_argv, env=env, stdout=log, stderr=subprocess.STDOUT)
    finally:
        log.close()
    pin_process(proc.pid, list(range(n_cores)))
    return proc


def _setup_mc_bucket(mc_bin: str, alias: str, port: int, access_key: str, secret_key: str) -> None:
    cmds = (
        ["alias", "set", alias, f"http://127.0.0.1:{port}", access_key, secret_key],
        ["mb", f"{alias}/{BUCKET}"],
        ["anonymous", "set", "public", f"{alias}/{BUCKET}"],
    )
    for args in cmds:
        last_err = ""
        for _ in range(20):
            res = subprocess.run([mc_bin, *args], capture_output=True, text=True)
            if res.returncode == 0:
                break
            last_err = (res.stderr or res.stdout or f"exit {res.returncode}").strip()
            time.sleep(0.4)
        else:
            raise RuntimeError(f"mc {args[0]} failed: {last_err[:120]}")


def _start_awanfs(server_bin: Optional[str], n_cores: int, data_root: str) -> Tuple[subprocess.Popen, int, str, str]:
    if not server_bin or not os.path.isfile(server_bin):
        raise RuntimeError("s3-server binary not found")
    data = tempfile.mkdtemp(prefix="scaling_awanfs_", dir=data_root)
    port = rwb.get_free_port()
    log_path = os.path.join(data_root, f"scaling_awanfs_{port}.log")
    proc = _spawn_pinned([server_bin, "--port", str(port), "--data-dir", data, "--allow-anonymous"], n_cores, log_path)
    err = _wait_port(proc, port)
    if err:
        _stop_proc(proc)
        tail = _tail_log(log_path)
        shutil.rmtree(data, ignore_errors=True)
        raise RuntimeError(f"s3-server {err}" + (f": {tail}" if tail else ""))
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5.0)
        conn.request("PUT", f"/{BUCKET}")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        if resp.status not in (200, 409):
            raise RuntimeError(f"bucket PUT returned HTTP {resp.status}")
    except Exception as e:
        _stop_proc(proc)
        shutil.rmtree(data, ignore_errors=True)
        raise RuntimeError(f"s3-server bucket setup failed: {e}") from e
    return proc, port, data, log_path


def _start_minio(minio_bin: Optional[str], mc_bin: Optional[str], n_cores: int, data_root: str) -> Tuple[subprocess.Popen, int, str, str]:
    if not minio_bin or not os.path.isfile(minio_bin):
        raise RuntimeError("minio binary not found")
    if not mc_bin or not os.path.isfile(mc_bin):
        raise RuntimeError("mc binary not found")
    data = tempfile.mkdtemp(prefix="scaling_minio_", dir=data_root)
    port, console = rwb.get_free_port(), rwb.get_free_port()
    log_path = os.path.join(data_root, f"scaling_minio_{port}.log")
    env = dict(
        os.environ,
        MINIO_ROOT_USER="minioadmin",
        MINIO_ROOT_PASSWORD="minioadmin",
        MINIO_UPDATE="off",
        MINIO_BROWSER="off",
    )
    proc = _spawn_pinned(
        [minio_bin, "server", data, "--address", f"127.0.0.1:{port}", "--console-address", f"127.0.0.1:{console}"],
        n_cores,
        log_path,
        env=env,
    )
    err = _wait_port(proc, port)
    if err:
        _stop_proc(proc)
        tail = _tail_log(log_path)
        shutil.rmtree(data, ignore_errors=True)
        raise RuntimeError(f"minio {err}" + (f": {tail}" if tail else ""))
    try:
        _setup_mc_bucket(mc_bin, f"scale_minio_{port}", port, "minioadmin", "minioadmin")
        if not rwb.anonymous_put_ok("127.0.0.1", port, BUCKET):
            raise RuntimeError("anonymous PUT probe failed")
    except Exception as e:
        _stop_proc(proc)
        shutil.rmtree(data, ignore_errors=True)
        raise RuntimeError(f"minio setup failed: {e}") from e
    return proc, port, data, log_path


def _start_rustfs(rustfs_bin: Optional[str], mc_bin: Optional[str], n_cores: int, data_root: str) -> Tuple[subprocess.Popen, int, str, str]:
    if not rustfs_bin or not os.path.isfile(rustfs_bin):
        raise RuntimeError("rustfs binary not found")
    if not mc_bin or not os.path.isfile(mc_bin):
        raise RuntimeError("mc binary not found for rustfs setup")
    data = tempfile.mkdtemp(prefix="scaling_rustfs_", dir=data_root)
    port = rwb.get_free_port()
    log_path = os.path.join(data_root, f"scaling_rustfs_{port}.log")
    env = dict(os.environ, RUSTFS_ACCESS_KEY="rustfsadmin", RUSTFS_SECRET_KEY="rustfsadmin")
    proc = _spawn_pinned(
        [rustfs_bin, "server", "--address", f"127.0.0.1:{port}", "--access-key", "rustfsadmin", "--secret-key", "rustfsadmin", data],
        n_cores,
        log_path,
        env=env,
    )
    err = _wait_port(proc, port)
    if err:
        _stop_proc(proc)
        tail = _tail_log(log_path)
        shutil.rmtree(data, ignore_errors=True)
        raise RuntimeError(f"rustfs {err}" + (f": {tail}" if tail else ""))
    try:
        _setup_mc_bucket(mc_bin, f"scale_rustfs_{port}", port, "rustfsadmin", "rustfsadmin")
        if not rwb.anonymous_put_ok("127.0.0.1", port, BUCKET):
            raise RuntimeError("anonymous PUT probe failed")
    except Exception as e:
        _stop_proc(proc)
        shutil.rmtree(data, ignore_errors=True)
        raise RuntimeError(f"rustfs setup failed: {e}") from e
    return proc, port, data, log_path


def _run_phase_with_cpu(
    host: str,
    port: int,
    bucket: str,
    size: int,
    concurrency: int,
    duration_sec: float,
    mode: str,
    server_pid: int,
    shares_cores: bool,
) -> PointResult:
    """Runs `run_workload_phase` and measures server CPU time (children included) over its timed window."""
    samples: List[Tuple[float, Tuple[float, float]]] = []
    orig_cpu_fn = rwb.get_process_cpu_times

    def hooked_cpu(pid: int) -> Tuple[float, float]:
        if pid == server_pid:
            ct = process_cpu_times_total(pid)
            samples.append((time.perf_counter(), ct))
            return ct
        return orig_cpu_fn(pid)

    rwb.get_process_cpu_times = hooked_cpu
    try:
        metrics = rwb.run_workload_phase(host, port, bucket, size, concurrency, duration_sec, mode, server_pid)
    finally:
        rwb.get_process_cpu_times = orig_cpu_fn

    if not metrics.measured:
        return PointResult(reason=metrics.note or "phase did not complete", shares_cores=shares_cores)

    if len(samples) >= 2:
        t0, cpu0 = samples[0]
        t1, cpu1 = samples[-1]
        pct = cpu_percent(cpu0, cpu1, t1 - t0)
    else:
        pct = (metrics.server_cpu_s / duration_sec) * 100.0 if duration_sec > 0 else None

    eff = ops_per_busy_core(metrics.ops_per_sec, pct)
    return PointResult(
        ops_per_sec=metrics.ops_per_sec,
        p50_ms=metrics.p50_ms,
        p99_ms=metrics.p99_ms,
        cpu_pct=pct,
        ops_per_busy_core=eff,
        shares_cores=shares_cores,
    )


def measure_server_for_cores(
    server_name: str,
    starter: Callable[[], Tuple[subprocess.Popen, int, str, str]],
    n_cores: int,
    cli_cores: Sequence[int],
    shares_cores: bool,
    duration_sec: float,
    workloads: Sequence[Tuple[str, str, str, int]] = WORKLOADS,
) -> Dict[str, PointResult]:
    """Starts one server, pins the load generator to `cli_cores`, runs all workloads, cleans up."""
    try:
        proc, port, data_dir, log_path = starter()
    except Exception as e:
        reason = str(e).strip() or f"{server_name} failed to start"
        return {wkey: PointResult(reason=reason, shares_cores=shares_cores) for wkey, *_ in workloads}

    concurrency = 16 * n_cores
    out: Dict[str, PointResult] = {}
    try:
        pin_process(os.getpid(), cli_cores)
        for wkey, label, mode, size in workloads:
            print(f"    {server_name} N={n_cores} (c={concurrency}): {label}...", flush=True)
            out[wkey] = _run_phase_with_cpu(
                "127.0.0.1", port, BUCKET, size, concurrency, duration_sec, mode, proc.pid, shares_cores
            )
    finally:
        _stop_proc(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
        try:
            os.remove(log_path)
        except OSError:
            pass
    return out


def probe_disk_max_put_ops(data_root: str, sizes: Sequence[int] = (4 * KIB, 64 * KIB), count: int = 100) -> Dict[int, Optional[float]]:
    """Uses benchmarks/sync_probe.py to measure single-writer durable append ops/s for each PUT size."""
    out: Dict[int, Optional[float]] = {}
    work = tempfile.mkdtemp(prefix="scaling_sync_probe_", dir=data_root)
    orig_block = sync_probe.BLOCK
    try:
        for sz in sizes:
            sync_probe.BLOCK = sz
            use_unbuf = os.name == "nt"
            lats = sync_probe.run_variant(work, f"max_{sz}", count, flushes=2, preallocate=False, unbuffered=use_unbuf)
            if lats and sum(lats) > 0:
                out[sz] = len(lats) / (sum(lats) / 1000.0)
            else:
                out[sz] = None
    except Exception:
        for sz in sizes:
            out.setdefault(sz, None)
    finally:
        sync_probe.BLOCK = orig_block
        shutil.rmtree(work, ignore_errors=True)
    return out


def write_charts(
    results: Dict[str, Dict[str, Dict[int, PointResult]]],
    cores: Sequence[int],
    report_path: str,
    max_put_ops: Optional[Dict[int, Optional[float]]] = None,
) -> List[str]:
    """Generates 4 SeaStore-style 4-panel figures + 1 ops/s-per-busy-core figure as PNG and SVG."""
    import matplotlib  # type: ignore
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # type: ignore

    report_abs = os.path.abspath(report_path)
    out_dir = os.path.dirname(report_abs)
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.splitext(os.path.basename(report_abs))[0]
    written: List[str] = []
    x = list(cores)
    max_put_ops = max_put_ops or {}

    def _series(wkey: str, srv: str, attr: str, scale: float = 1.0) -> List[float]:
        srv_map = results.get(wkey, {}).get(srv, {})
        vals: List[float] = []
        for n in x:
            pt = srv_map.get(n)
            v = getattr(pt, attr, None) if pt and pt.measured else None
            vals.append(float(v) * scale if v is not None else math.nan)
        return vals

    def _style_axes(ax, ylabel: str, title: str) -> None:
        ax.set_xscale("log", base=2)
        ax.set_xticks(x)
        ax.set_xticklabels([str(c) for c in x])
        ax.set_xlabel("CPU cores")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(True, alpha=0.3)

    # Figures 1..4: one 4-panel figure per workload
    for wkey, wlabel, mode, size in WORKLOADS:
        fig, axes = plt.subplots(1, 4, figsize=(16, 4.2))
        panels = [
            ("(a) Throughput", "K ops/s", "ops_per_sec", 1e-3),
            ("(b) CPU usage", "CPU usage (%)", "cpu_pct", 1.0),
            ("(c) P50 latency", "P50 latency (ms)", "p50_ms", 1.0),
            ("(d) P99 latency", "P99 latency (ms)", "p99_ms", 1.0),
        ]
        for ax, (ptitle, ylabel, attr, scale) in zip(axes, panels):
            for srv in SERVERS:
                st = SERVER_STYLES[srv]
                ax.plot(
                    x,
                    _series(wkey, srv, attr, scale),
                    linestyle=st["linestyle"],
                    marker=st["marker"],
                    color=st["color"],
                    linewidth=1.8,
                    markersize=5.5,
                    label=srv,
                )
            if attr == "ops_per_sec" and mode == "put":
                max_ops = max_put_ops.get(size)
                if max_ops is not None and max_ops > 0:
                    ax.axhline(max_ops / 1000.0, linestyle=":", color="black", linewidth=1.6, label="MAX")
            _style_axes(ax, ylabel, ptitle)

        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, bbox_to_anchor=(0.5, 0.99))
        fig.suptitle(f"{wlabel} — Core Scaling (concurrency = 16 × N)", y=1.06)
        fig.tight_layout(rect=[0, 0, 1, 0.92])

        for ext in ("png", "svg"):
            p = os.path.join(out_dir, f"{stem}_{wkey}.{ext}")
            fig.savefig(p, dpi=150, bbox_inches="tight")
            written.append(p)
        plt.close(fig)

    # Figure 5: ops/s per busy core across workloads
    fig5, axes5 = plt.subplots(1, len(WORKLOADS), figsize=(16, 4.2))
    for ax, (wkey, wlabel, _mode, _size) in zip(axes5, WORKLOADS):
        for srv in SERVERS:
            st = SERVER_STYLES[srv]
            ax.plot(
                x,
                _series(wkey, srv, "ops_per_busy_core", 1.0),
                linestyle=st["linestyle"],
                marker=st["marker"],
                color=st["color"],
                linewidth=1.8,
                markersize=5.5,
                label=srv,
            )
        _style_axes(ax, "ops/s per busy core", wlabel)

    handles5, labels5 = axes5[0].get_legend_handles_labels()
    fig5.legend(handles5, labels5, loc="upper center", ncol=len(labels5), frameon=False, bbox_to_anchor=(0.5, 0.99))
    fig5.suptitle("Throughput Efficiency: ops/s per busy core", y=1.06)
    fig5.tight_layout(rect=[0, 0, 1, 0.92])

    for ext in ("png", "svg"):
        p = os.path.join(out_dir, f"{stem}_ops_per_busy_core.{ext}")
        fig5.savefig(p, dpi=150, bbox_inches="tight")
        written.append(p)
    plt.close(fig5)

    return written


def render_report(
    cmd: str,
    cores: Sequence[int],
    results: Dict[str, Dict[str, Dict[int, PointResult]]],
    total_cores: int,
    runs: int,
    duration_sec: float,
    max_put_ops: Optional[Dict[int, Optional[float]]] = None,
    chart_paths: Optional[Sequence[str]] = None,
) -> str:
    max_n = max(cores)
    shared_ns = [n for n in cores if n >= total_cores]
    if shared_ns:
        sharing_note = (
            f"Load generator runs on remaining cores (`N..{total_cores - 1}`) when `N < {total_cores}`, "
            f"and **shares cores** with the server (`0..{total_cores - 1}`) at `N = {', '.join(str(n) for n in shared_ns)}`."
        )
    else:
        sharing_note = (
            f"Load generator is pinned to remaining cores (`N..{total_cores - 1}`) at all tested core counts "
            f"(never shares cores with the server)."
        )

    max_put_ops = max_put_ops or {}
    probe_4k = max_put_ops.get(4 * KIB)
    probe_64k = max_put_ops.get(64 * KIB)
    probe_note = (
        f"Disk `sync_probe` durable append ceiling (`MAX` line on PUT charts): "
        f"4 KiB = {probe_4k:,.0f} ops/s" if probe_4k else "4 KiB = not measured"
    ) + "; " + (
        f"64 KiB = {probe_64k:,.0f} ops/s." if probe_64k else "64 KiB = not measured."
    )

    lines = [
        f"Command: `{cmd}`",
        "",
        f"# Core Scaling Benchmark: AwanFS vs MinIO vs RustFS ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"- **Machine:** {platform.node()} ({platform.platform()}, {total_cores} logical cores, {rwb.host_ram_gb()} GB RAM, CPU: {rwb.cpu_model()})",
        f"- **Methodology:** `cores = {', '.join(str(n) for n in cores)}`, `concurrency = 16 × N`, {runs} alternating runs per point (median reported), {duration_sec:.1f}s per phase.",
        f"- **Core pinning:** {sharing_note}",
        f"- **Durability:** AwanFS group commit (#29). {rwb.minio_durability_note()} {probe_note}",
        "",
        "## Throughput and Latency (`ops/s (P50, P99 ms)`)",
        "",
    ]

    core_headers = [f"{n} core" if n == 1 else f"{n} cores" for n in cores]
    lines.append("| Workload | Size | Server | " + " | ".join(core_headers) + f" | Scaling ({max_n}/1) |")
    lines.append("|---|---|---|" + "|".join(["---"] * (len(core_headers) + 1)) + "|")

    for wkey, wlabel, _mode, size in WORKLOADS:
        size_label = f"{size // KIB} KiB"
        for srv in SERVERS:
            pts = results.get(wkey, {}).get(srv, {})
            cells = []
            for n in cores:
                pt = pts.get(n)
                if pt and pt.measured:
                    cells.append(f"{pt.ops_per_sec:,.0f} ops/s ({pt.p50_ms:.2f}, {pt.p99_ms:.2f} ms)")
                else:
                    reason = pt.reason if pt and pt.reason else "not measured"
                    cells.append(f"not measured ({reason})")
            sc = compute_scaling(pts, cores)
            sc_str = f"{sc:.2f}x" if sc is not None else "-"
            lines.append(f"| {wlabel} | {size_label} | {srv} | " + " | ".join(cells) + f" | {sc_str} |")

    lines.extend([
        "",
        "## Server CPU Usage and Efficiency (`CPU %` where 100% = 1 core busy; `ops/s per busy core`)",
        "",
        "| Workload | Size | Server | " + " | ".join(core_headers) + " |",
        "|---|---|---|" + "|".join(["---"] * len(core_headers)) + "|",
    ])
    for wkey, wlabel, _mode, size in WORKLOADS:
        size_label = f"{size // KIB} KiB"
        for srv in SERVERS:
            pts = results.get(wkey, {}).get(srv, {})
            cells = []
            for n in cores:
                pt = pts.get(n)
                if pt and pt.measured:
                    cpu_s = f"{pt.cpu_pct:.1f}%" if pt.cpu_pct is not None else "-"
                    eff_s = f"{pt.ops_per_busy_core:,.0f} ops/s/core" if pt.ops_per_busy_core is not None else "-"
                    cells.append(f"{cpu_s} ({eff_s})")
                else:
                    reason = pt.reason if pt and pt.reason else "not measured"
                    cells.append(f"not measured ({reason})")
            lines.append(f"| {wlabel} | {size_label} | {srv} | " + " | ".join(cells) + " |")

    if chart_paths:
        lines.extend(["", "## Charts", ""])
        for p in chart_paths:
            lines.append(f"- `{os.path.basename(p)}`")

    lines.append("")
    return "\n".join(lines)


def _default_awanfs_bin() -> Optional[str]:
    exe = ".exe" if sys.platform == "win32" else ""
    p = os.path.join(REPO_ROOT, "server", "target", "release", f"s3-server{exe}")
    return p if os.path.isfile(p) else None


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cores", default="", help="Comma-separated core counts (e.g. 1,2,4). Default: powers of 2 up to CPU count")
    ap.add_argument("--runs", type=int, default=2, help="Alternating rounds per core count (default: 2)")
    ap.add_argument("--duration", type=float, default=3.0, help="Seconds per workload phase (default: 3.0)")
    ap.add_argument("--server-bin", default="", help="Path to AwanFS release s3-server binary")
    ap.add_argument("--minio-bin", default="", help="Path to MinIO binary")
    ap.add_argument("--mc-bin", default="", help="Path to MinIO mc binary")
    ap.add_argument("--rustfs-bin", default="", help="Path to RustFS binary")
    ap.add_argument("--data-dir", default="", help="Directory for temporary server data and sync_probe (defaults to temp dir)")
    ap.add_argument("--skip-sync-probe", action="store_true", help="Skip disk sync_probe MAX baseline")
    ap.add_argument("--output", "--report", dest="output", default="core_scaling.md",
                    help="Markdown report path (PNG and SVG charts are written alongside it)")
    a = ap.parse_args(argv)

    raw_args = list(argv) if argv is not None else sys.argv[1:]
    cmd = "python benchmarks/core_scaling.py" + (" " + " ".join(raw_args) if raw_args else "")

    total_cores = os.cpu_count() or 1
    cores = parse_cores(a.cores, total_cores)
    data_root = os.path.abspath(a.data_dir) if a.data_dir else tempfile.gettempdir()
    os.makedirs(data_root, exist_ok=True)

    awanfs_bin = a.server_bin or _default_awanfs_bin()
    auto_minio, auto_mc = rwb.find_minio_binaries()
    minio_bin = a.minio_bin or auto_minio
    mc_bin = a.mc_bin or auto_mc
    rustfs_bin = a.rustfs_bin or rwb.find_rustfs_binary()

    orig_affinity = get_process_affinity(os.getpid())

    if a.skip_sync_probe:
        max_put_ops: Dict[int, Optional[float]] = {}
    else:
        print(f"Running sync_probe on {data_root} for 4 KiB and 64 KiB...", flush=True)
        max_put_ops = probe_disk_max_put_ops(data_root)

    # raw_runs[wkey][srv][n] -> list of PointResult across runs
    raw_runs: Dict[str, Dict[str, Dict[int, List[PointResult]]]] = {
        wkey: {srv: {n: [] for n in cores} for srv in SERVERS}
        for wkey, *_ in WORKLOADS
    }

    try:
        for n in cores:
            if n > total_cores:
                reason = f"requested {n} cores > {total_cores} available"
                for wkey, *_ in WORKLOADS:
                    for srv in SERVERS:
                        raw_runs[wkey][srv][n].append(PointResult(reason=reason))
                continue

            cli_cores, shares = client_cores_for(n, total_cores)
            for run_idx in range(a.runs):
                order = server_order_for_run(run_idx, SERVERS)
                for srv in order:
                    print(f"[N={n}, run {run_idx + 1}/{a.runs}] Starting {srv} (client cores={cli_cores}, shares={shares})...", flush=True)
                    if srv == "AwanFS":
                        starter = lambda n=n: _start_awanfs(awanfs_bin, n, data_root)
                    elif srv == "MinIO":
                        starter = lambda n=n: _start_minio(minio_bin, mc_bin, n, data_root)
                    else:
                        starter = lambda n=n: _start_rustfs(rustfs_bin, mc_bin, n, data_root)

                    phase_map = measure_server_for_cores(srv, starter, n, cli_cores, shares, a.duration)
                    for wkey, pt in phase_map.items():
                        raw_runs[wkey][srv][n].append(pt)
    finally:
        if orig_affinity:
            pin_process(os.getpid(), orig_affinity)

    results: Dict[str, Dict[str, Dict[int, PointResult]]] = {
        wkey: {
            srv: {n: aggregate_runs(raw_runs[wkey][srv][n]) for n in cores}
            for srv in SERVERS
        }
        for wkey, *_ in WORKLOADS
    }

    try:
        chart_paths = write_charts(results, cores, a.output, max_put_ops=max_put_ops)
    except ImportError as e:
        print(f"warning: matplotlib unavailable, skipping charts ({e})", file=sys.stderr)
        chart_paths = []

    report = render_report(
        cmd=cmd,
        cores=cores,
        results=results,
        total_cores=total_cores,
        runs=a.runs,
        duration_sec=a.duration,
        max_put_ops=max_put_ops,
        chart_paths=chart_paths,
    )
    out_abs = os.path.abspath(a.output)
    os.makedirs(os.path.dirname(out_abs), exist_ok=True)
    with open(out_abs, "w", encoding="utf-8") as fh:
        fh.write(report)
    print(report)
    print(f"Report written to {out_abs}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
