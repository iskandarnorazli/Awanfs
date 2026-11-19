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
import random
import re
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import run_warp_benchmark as rwb  # noqa: E402
import sync_probe  # noqa: E402

KIB = 1024
MIB = 1024 * 1024
BUCKET = "core-scaling-bench"
SERVERS: Tuple[str, ...] = ("AwanFS", "MinIO", "RustFS")
PATTERNS: Tuple[str, ...] = ("seq-write", "rand-write", "seq-read", "rand-read")

DEFAULT_SIZES = "4KiB,1MiB"
DEFAULT_CONCURRENCIES = "1,4,16,64,256"
DEFAULT_SEED = 42
DEFAULT_MEMORY_BUDGET = "32MiB"
DEFAULT_PREFILL_SIZE = "48MiB"

# (key, display label, mode, payload_size)
WORKLOADS: List[Tuple[str, str, str, int]] = [
    ("put_4k", "PUT 4 KiB", "put", 4 * KIB),
    ("put_64k", "PUT 64 KiB", "put", 64 * KIB),
    ("get_4k", "GET 4 KiB", "get", 4 * KIB),
    ("get_64k", "GET 64 KiB", "get", 64 * KIB),
]

SERVER_STYLES: Dict[str, Dict[str, str]] = {
    "AwanFS": {"linestyle": "-", "marker": "o", "color": "#1f77b4", "hatch": ""},
    "MinIO": {"linestyle": "--", "marker": "s", "color": "#d62728", "hatch": "//"},
    "RustFS": {"linestyle": "-.", "marker": "^", "color": "#2ca02c", "hatch": ".."},
}

CORE_STYLES: Tuple[Dict[str, str], ...] = (
    {"linestyle": "-", "marker": "o", "color": "#1f77b4"},
    {"linestyle": "--", "marker": "s", "color": "#ff7f0e"},
    {"linestyle": "-.", "marker": "^", "color": "#2ca02c"},
    {"linestyle": ":", "marker": "D", "color": "#d62728"},
    {"linestyle": "-", "marker": "v", "color": "#9467bd"},
    {"linestyle": "--", "marker": "P", "color": "#8c564b"},
)


@dataclass
class PointResult:
    ops_per_sec: Optional[float] = None
    p50_ms: Optional[float] = None
    p99_ms: Optional[float] = None
    cpu_pct: Optional[float] = None
    ops_per_busy_core: Optional[float] = None
    reason: str = ""
    shares_cores: bool = False
    mib_per_sec: Optional[float] = None

    @property
    def measured(self) -> bool:
        return self.ops_per_sec is not None


@dataclass(frozen=True)
class RequestSpec:
    method: str
    key: str
    range_start: Optional[int] = None
    range_end: Optional[int] = None


_SIZE_RE = re.compile(r"^\s*(\d+)\s*([a-zA-Z]*)\s*$")
_SIZE_UNITS = {
    "": 1,
    "b": 1,
    "k": KIB,
    "kb": KIB,
    "kib": KIB,
    "m": MIB,
    "mb": MIB,
    "mib": MIB,
    "g": 1024 * MIB,
    "gb": 1024 * MIB,
    "gib": 1024 * MIB,
}


def parse_size(spec: str) -> int:
    m = _SIZE_RE.match(spec)
    if not m:
        raise ValueError(f"invalid size {spec!r}")
    num = int(m.group(1))
    unit = m.group(2).lower()
    if unit not in _SIZE_UNITS:
        raise ValueError(f"unknown size unit in {spec!r}")
    val = num * _SIZE_UNITS[unit]
    if val < 1:
        raise ValueError(f"size must be >= 1 byte, got {spec!r}")
    return val


def format_size(n: int) -> str:
    if n >= MIB and n % MIB == 0:
        return f"{n // MIB} MiB"
    if n >= KIB and n % KIB == 0:
        return f"{n // KIB} KiB"
    return f"{n} B"


def parse_sizes(spec: str) -> List[int]:
    if not spec or not spec.strip():
        return [4 * KIB, MIB]
    vals: List[int] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        sz = parse_size(part)
        if sz not in vals:
            vals.append(sz)
    if not vals:
        raise ValueError("sizes list cannot be empty")
    return vals


def parse_patterns(spec: str) -> List[str]:
    if not spec or not spec.strip():
        return list(PATTERNS)
    vals: List[str] = []
    for part in spec.split(","):
        p = part.strip()
        if not p:
            continue
        if p not in PATTERNS:
            raise ValueError(f"unknown pattern {p!r}; expected one of {', '.join(PATTERNS)}")
        if p not in vals:
            vals.append(p)
    if not vals:
        raise ValueError("patterns list cannot be empty")
    return vals


def parse_concurrencies(spec: str) -> List[int]:
    if not spec or not spec.strip():
        return [1, 4, 16, 64, 256]
    vals: List[int] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        v = int(part)
        if v < 1:
            raise ValueError(f"concurrency must be >= 1, got {v}")
        if v not in vals:
            vals.append(v)
    if not vals:
        raise ValueError("concurrencies list cannot be empty")
    return sorted(vals)


def stated_cache_bytes(memory_budget_bytes: int) -> int:
    """AwanFS splits `--memory-budget` in half between payload cache and in-flight bodies."""
    return memory_budget_bytes // 2


def validate_prefill_exceeds_cache(prefill_bytes: int, cache_bytes: int) -> None:
    if prefill_bytes <= cache_bytes:
        raise ValueError(
            f"prefill size ({format_size(prefill_bytes)}) must exceed stated cache size ({format_size(cache_bytes)})"
        )


def deterministic_bytes(size: int, seed: int) -> bytes:
    """Deterministic incompressible byte payload identical across all servers for a given seed."""
    rng = random.Random(seed)
    if hasattr(rng, "randbytes"):
        return rng.randbytes(size)
    return bytes(rng.getrandbits(8) for _ in range(size))


def rand_write_key_space_for(size: int) -> int:
    return 16 if size >= MIB else 256


def build_request_spec(
    pattern: str,
    size: int,
    step_idx: int,
    rng: random.Random,
    prefill_count: int,
    large_obj_size: int,
    rand_write_key_space: Optional[int] = None,
) -> RequestSpec:
    ks = rand_write_key_space if rand_write_key_space is not None else rand_write_key_space_for(size)
    if pattern == "seq-write":
        return RequestSpec("PUT", f"seq/{step_idx:06d}")
    if pattern == "rand-write":
        idx = rng.randrange(max(1, ks))
        return RequestSpec("PUT", f"rand/{idx:06d}")
    if pattern == "seq-read":
        if size < MIB:
            idx = step_idx % max(1, prefill_count)
            return RequestSpec("GET", f"prefill_4k/{idx:06d}")
        chunks = max(1, large_obj_size // size)
        start = (step_idx % chunks) * size
        end = min(large_obj_size - 1, start + size - 1)
        return RequestSpec("GET", "prefill_large/seq_obj.bin", start, end)
    if pattern == "rand-read":
        if size < MIB:
            idx = rng.randrange(max(1, prefill_count))
            return RequestSpec("GET", f"prefill_4k/{idx:06d}")
        range_len = 4 * KIB
        blocks = max(1, large_obj_size // range_len)
        start = rng.randrange(blocks) * range_len
        end = min(large_obj_size - 1, start + range_len - 1)
        return RequestSpec("GET", "prefill_large/seq_obj.bin", start, end)
    raise ValueError(f"unknown pattern {pattern!r}")


def generate_request_sequence(
    pattern: str,
    size: int,
    count: int,
    seed: int,
    prefill_count: int,
    large_obj_size: int,
    rand_write_key_space: Optional[int] = None,
) -> List[RequestSpec]:
    rng = random.Random(seed)
    return [
        build_request_spec(
            pattern=pattern,
            size=size,
            step_idx=i,
            rng=rng,
            prefill_count=prefill_count,
            large_obj_size=large_obj_size,
            rand_write_key_space=rand_write_key_space,
        )
        for i in range(count)
    ]


class PatternRequestSource:
    """Thread-safe deterministic request stream for concurrent load workers."""

    def __init__(
        self,
        pattern: str,
        size: int,
        seed: int,
        prefill_count: int,
        large_obj_size: int,
        rand_write_key_space: Optional[int] = None,
    ):
        self.pattern = pattern
        self.size = size
        self.rng = random.Random(seed)
        self.prefill_count = prefill_count
        self.large_obj_size = large_obj_size
        self.rand_write_key_space = rand_write_key_space
        self._idx = 0
        self._lock = threading.Lock()

    def next_request(self) -> RequestSpec:
        with self._lock:
            idx = self._idx
            self._idx += 1
            return build_request_spec(
                pattern=self.pattern,
                size=self.size,
                step_idx=idx,
                rng=self.rng,
                prefill_count=self.prefill_count,
                large_obj_size=self.large_obj_size,
                rand_write_key_space=self.rand_write_key_space,
            )


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
    mib_vals = [p.mib_per_sec for p in measured if p.mib_per_sec is not None]
    mib_med = statistics.median(mib_vals) if mib_vals else None
    p50_med = statistics.median(p.p50_ms for p in measured if p.p50_ms is not None)
    p99_med = statistics.median(p.p99_ms for p in measured if p.p99_ms is not None)
    cpu_vals = [p.cpu_pct for p in measured if p.cpu_pct is not None]
    cpu_med = statistics.median(cpu_vals) if cpu_vals else None
    eff_vals = [p.ops_per_busy_core for p in measured if p.ops_per_busy_core is not None]
    eff_med = statistics.median(eff_vals) if eff_vals else ops_per_busy_core(ops_med, cpu_med)
    return PointResult(
        ops_per_sec=ops_med,
        mib_per_sec=mib_med,
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


def _start_awanfs(
    server_bin: Optional[str],
    n_cores: int,
    data_root: str,
    memory_budget: Optional[str] = None,
) -> Tuple[subprocess.Popen, int, str, str]:
    if not server_bin or not os.path.isfile(server_bin):
        raise RuntimeError("s3-server binary not found")
    data = tempfile.mkdtemp(prefix="scaling_awanfs_", dir=data_root)
    port = rwb.get_free_port()
    log_path = os.path.join(data_root, f"scaling_awanfs_{port}.log")
    argv = [server_bin, "--port", str(port), "--data-dir", data, "--allow-anonymous"]
    if memory_budget:
        argv.extend(["--memory-budget", memory_budget])
    proc = _spawn_pinned(argv, n_cores, log_path)
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


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _prefill_server(
    host: str,
    port: int,
    bucket: str,
    seed: int,
    prefill_bytes: int,
    patterns: Sequence[str],
    sizes: Sequence[int],
    timeout_sec: float = 120.0,
) -> Optional[str]:
    """Writes identical deterministic prefill data before timing read/overwrite phases."""
    need_4k = any(p in ("seq-read", "rand-read") for p in patterns) and any(s < MIB for s in sizes)
    need_large = any(p in ("seq-read", "rand-read") for p in patterns) and any(s >= MIB for s in sizes)
    need_rand_write = "rand-write" in patterns
    deadline = time.perf_counter() + timeout_sec

    if need_4k:
        prefill_count = max(1, prefill_bytes // (4 * KIB))
        payload_4k = deterministic_bytes(4 * KIB, seed)
        workers = min(16, prefill_count)
        next_idx = 0
        idx_lock = threading.Lock()
        errors: List[str] = []

        def prefill_worker():
            nonlocal next_idx
            conn = http.client.HTTPConnection(host, port, timeout=15.0)
            headers = {"Content-Length": str(len(payload_4k)), "Connection": "keep-alive"}
            try:
                while time.perf_counter() < deadline:
                    with idx_lock:
                        if errors or next_idx >= prefill_count:
                            return
                        i = next_idx
                        next_idx += 1
                    try:
                        conn.request("PUT", f"/{bucket}/prefill_4k/{i:06d}", body=payload_4k, headers=headers)
                        resp = conn.getresponse()
                        resp.read()
                        if resp.status != 200:
                            with idx_lock:
                                errors.append(f"prefill 4 KiB HTTP {resp.status}")
                            return
                    except Exception as e:
                        with idx_lock:
                            errors.append(f"prefill 4 KiB {type(e).__name__}: {e}")
                        return
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        threads = [threading.Thread(target=prefill_worker, daemon=True) for _ in range(workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=max(1.0, deadline - time.perf_counter()))
        if errors:
            return errors[0]
        if next_idx < prefill_count:
            return f"prefill 4 KiB timed out ({next_idx}/{prefill_count})"

    if need_large:
        large_payload = deterministic_bytes(prefill_bytes, seed + 1)
        conn = http.client.HTTPConnection(host, port, timeout=max(30.0, timeout_sec))
        try:
            conn.request(
                "PUT",
                f"/{bucket}/prefill_large/seq_obj.bin",
                body=large_payload,
                headers={"Content-Length": str(len(large_payload)), "Connection": "close"},
            )
            resp = conn.getresponse()
            resp.read()
            if resp.status != 200:
                return f"prefill large object HTTP {resp.status}"
        except Exception as e:
            return f"prefill large object {type(e).__name__}: {e}"
        finally:
            conn.close()

    if need_rand_write:
        for sz in sorted(set(sizes)):
            ks = rand_write_key_space_for(sz)
            payload = deterministic_bytes(sz, seed + 2)
            conn = http.client.HTTPConnection(host, port, timeout=30.0)
            headers = {"Content-Length": str(sz), "Connection": "keep-alive"}
            try:
                for i in range(ks):
                    if time.perf_counter() > deadline:
                        return f"prefill rand-write timed out ({i}/{ks})"
                    conn.request("PUT", f"/{bucket}/rand/{i:06d}", body=payload, headers=headers)
                    resp = conn.getresponse()
                    resp.read()
                    if resp.status != 200:
                        return f"prefill rand-write HTTP {resp.status}"
            except Exception as e:
                return f"prefill rand-write {type(e).__name__}: {e}"
            finally:
                conn.close()

    return None


def _run_pattern_phase_with_cpu(
    host: str,
    port: int,
    bucket: str,
    pattern: str,
    size: int,
    concurrency: int,
    duration_sec: float,
    seed: int,
    prefill_bytes: int,
    server_pid: int,
    shares_cores: bool,
) -> PointResult:
    """Runs one `(pattern, size, concurrency)` phase and measures server CPU % over its timed window."""
    prefill_count = max(1, prefill_bytes // (4 * KIB))
    source = PatternRequestSource(
        pattern=pattern,
        size=size,
        seed=seed,
        prefill_count=prefill_count,
        large_obj_size=prefill_bytes,
    )
    put_payload = deterministic_bytes(size, seed + 3) if pattern in ("seq-write", "rand-write") else b""
    stop_event = threading.Event()
    latencies: List[float] = []
    lat_lock = threading.Lock()
    total_bytes = 0
    errors = 0
    error_notes: Dict[str, int] = {}
    metrics_lock = threading.Lock()

    def worker():
        nonlocal total_bytes, errors
        local_lats: List[float] = []
        local_bytes = 0
        local_errs: Dict[str, int] = {}
        conn = http.client.HTTPConnection(host, port, timeout=15.0)
        put_headers = {"Content-Length": str(size), "Connection": "keep-alive"}
        get_headers = {"Connection": "keep-alive"}

        while not stop_event.is_set():
            spec = source.next_request()
            path = f"/{bucket}/{spec.key}"
            t0 = time.perf_counter()
            try:
                if spec.method == "PUT":
                    conn.request("PUT", path, body=put_payload, headers=put_headers)
                    resp = conn.getresponse()
                    resp.read()
                    ok = resp.status == 200
                    if ok:
                        local_bytes += size
                    else:
                        msg = f"HTTP {resp.status}"
                        local_errs[msg] = local_errs.get(msg, 0) + 1
                elif spec.range_start is not None and spec.range_end is not None:
                    hdrs = {"Connection": "keep-alive", "Range": f"bytes={spec.range_start}-{spec.range_end}"}
                    conn.request("GET", path, headers=hdrs)
                    resp = conn.getresponse()
                    data = resp.read()
                    want_len = spec.range_end - spec.range_start + 1
                    if resp.status == 206 and len(data) == want_len:
                        ok = True
                        local_bytes += len(data)
                    elif resp.status == 200:
                        ok = False
                        msg = "range GET unsupported (HTTP 200)"
                        local_errs[msg] = local_errs.get(msg, 0) + 1
                    elif resp.status == 206:
                        ok = False
                        msg = f"range GET short read ({len(data)}/{want_len})"
                        local_errs[msg] = local_errs.get(msg, 0) + 1
                    else:
                        ok = False
                        msg = f"HTTP {resp.status}"
                        local_errs[msg] = local_errs.get(msg, 0) + 1
                else:
                    conn.request("GET", path, headers=get_headers)
                    resp = conn.getresponse()
                    data = resp.read()
                    ok = resp.status == 200
                    if ok:
                        local_bytes += len(data)
                    else:
                        msg = f"HTTP {resp.status}"
                        local_errs[msg] = local_errs.get(msg, 0) + 1
                t1 = time.perf_counter()
                if ok:
                    local_lats.append((t1 - t0) * 1000.0)
            except Exception as e:
                msg = type(e).__name__
                local_errs[msg] = local_errs.get(msg, 0) + 1
                try:
                    conn.close()
                except Exception:
                    pass
                conn = http.client.HTTPConnection(host, port, timeout=15.0)

        try:
            conn.close()
        except Exception:
            pass

        with lat_lock:
            latencies.extend(local_lats)
        with metrics_lock:
            total_bytes += local_bytes
            for m, cnt in local_errs.items():
                errors += cnt
                error_notes[m] = error_notes.get(m, 0) + cnt

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    cpu_0 = process_cpu_times_total(server_pid)
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(duration_sec)
    stop_event.set()
    stuck = rwb.join_all(threads, f"{pattern} {format_size(size)} c={concurrency}", timeout_sec=max(20.0, duration_sec * 10.0))
    t_total = time.perf_counter() - t_start
    cpu_1 = process_cpu_times_total(server_pid)

    if stuck or errors > 0 or not latencies or t_total <= 0:
        reasons = []
        if errors > 0:
            summary = ", ".join(f"{k} x{v}" for k, v in sorted(error_notes.items()))
            reasons.append(f"{errors} failed ({summary})")
        if stuck:
            reasons.append(f"{stuck} stuck workers")
        if not latencies and not reasons:
            reasons.append("no requests completed")
        return PointResult(reason="; ".join(reasons), shares_cores=shares_cores)

    ops_per_sec = len(latencies) / t_total
    mib_per_sec = (total_bytes / float(MIB)) / t_total
    p50, _p90, p99, _p999 = rwb.calculate_percentiles(latencies)
    pct = cpu_percent(cpu_0, cpu_1, t_total)
    eff = ops_per_busy_core(ops_per_sec, pct)
    return PointResult(
        ops_per_sec=ops_per_sec,
        mib_per_sec=mib_per_sec,
        p50_ms=p50,
        p99_ms=p99,
        cpu_pct=pct,
        ops_per_busy_core=eff,
        shares_cores=shares_cores,
    )


def measure_server_matrix_for_cores(
    server_name: str,
    starter: Callable[[], Tuple[subprocess.Popen, int, str, str]],
    n_cores: int,
    cli_cores: Sequence[int],
    shares_cores: bool,
    patterns: Sequence[str],
    sizes: Sequence[int],
    concurrencies: Sequence[int],
    duration_sec: float,
    seed: int,
    prefill_bytes: int,
) -> Dict[Tuple[str, int, int], PointResult]:
    """Starts one server, prefills reads once, runs the `(pattern, size, concurrency)` sweep, cleans up."""
    all_keys = [(pat, sz, c) for pat in patterns for sz in sizes for c in concurrencies]
    try:
        proc, port, data_dir, log_path = starter()
    except Exception as e:
        reason = str(e).strip() or f"{server_name} failed to start"
        return {k: PointResult(reason=reason, shares_cores=shares_cores) for k in all_keys}

    out: Dict[Tuple[str, int, int], PointResult] = {}
    try:
        pin_process(os.getpid(), cli_cores)
        print(f"    {server_name} N={n_cores}: prefilling {format_size(prefill_bytes)}...", flush=True)
        prefill_err = _prefill_server("127.0.0.1", port, BUCKET, seed, prefill_bytes, patterns, sizes)
        if prefill_err:
            print(f"    {server_name} N={n_cores}: prefill failed: {prefill_err}", flush=True)
            for k in all_keys:
                out[k] = PointResult(reason=prefill_err, shares_cores=shares_cores)
            return out

        for pat in patterns:
            for sz in sizes:
                for c in concurrencies:
                    print(f"    {server_name} N={n_cores} {pat} {format_size(sz)} c={c}...", flush=True)
                    out[(pat, sz, c)] = _run_pattern_phase_with_cpu(
                        "127.0.0.1",
                        port,
                        BUCKET,
                        pat,
                        sz,
                        c,
                        duration_sec,
                        seed,
                        prefill_bytes,
                        proc.pid,
                        shares_cores,
                    )
    finally:
        _stop_proc(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
        try:
            os.remove(log_path)
        except OSError:
            pass
    return out


def _pick_target(values: Sequence[int], preferred: int) -> int:
    if preferred in values:
        return preferred
    return min(values, key=lambda v: (abs(v - preferred), -v))


def write_matrix_charts(
    results: Dict[str, Dict[int, Dict[str, Dict[int, Dict[int, PointResult]]]]],
    patterns: Sequence[str],
    sizes: Sequence[int],
    cores: Sequence[int],
    concurrencies: Sequence[int],
    report_path: str,
) -> List[str]:
    """Generates all 4 chart families for #251 as PNG and SVG alongside `report_path`."""
    import matplotlib  # type: ignore
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # type: ignore

    report_abs = os.path.abspath(report_path)
    out_dir = os.path.dirname(report_abs)
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.splitext(os.path.basename(report_abs))[0]
    written: List[str] = []

    def _save_both(fig, suffix: str) -> None:
        for ext in ("png", "svg"):
            p = os.path.join(out_dir, f"{stem}_{suffix}.{ext}")
            fig.savefig(p, dpi=150, bbox_inches="tight")
            written.append(p)
        plt.close(fig)

    fixed_conc = _pick_target(concurrencies, 64)
    core_list = list(cores)
    conc_list = list(concurrencies)

    # 1. Per pattern and size: 4 panels (K ops/s, CPU %, P50, P99) against cores at concurrency = 64
    for pat in patterns:
        for sz in sizes:
            sz_lbl = format_size(sz)
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
                    vals = []
                    for n in core_list:
                        pt = results.get(pat, {}).get(sz, {}).get(srv, {}).get(n, {}).get(fixed_conc)
                        v = getattr(pt, attr, None) if pt and pt.measured else None
                        vals.append(float(v) * scale if v is not None else math.nan)
                    ax.plot(
                        core_list,
                        vals,
                        linestyle=st["linestyle"],
                        marker=st["marker"],
                        color=st["color"],
                        linewidth=1.8,
                        markersize=5.5,
                        label=srv,
                    )
                ax.set_xscale("log", base=2)
                ax.set_xticks(core_list)
                ax.set_xticklabels([str(c) for c in core_list])
                ax.set_xlabel("CPU cores")
                ax.set_ylabel(ylabel)
                ax.set_title(ptitle)
                ax.grid(True, alpha=0.3)

            handles, labels = axes[0].get_legend_handles_labels()
            fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, bbox_to_anchor=(0.5, 0.99))
            fig.suptitle(f"{pat} ({sz_lbl}) — Core Scaling (concurrency = {fixed_conc})", y=1.06)
            fig.tight_layout(rect=[0, 0, 1, 0.92])
            _save_both(fig, f"cores_{_slug(pat)}_{_slug(sz_lbl)}")

    # 2. Per pattern and size: ops/s and P99 against concurrency (log2 x), one line per core count, one panel per server, shared y scale
    for pat in patterns:
        for sz in sizes:
            sz_lbl = format_size(sz)
            fig, axes = plt.subplots(2, len(SERVERS), figsize=(15, 7.6))
            row_metrics = [
                ("ops/s", "ops_per_sec"),
                ("P99 latency (ms)", "p99_ms"),
            ]
            for r_idx, (ylabel, attr) in enumerate(row_metrics):
                all_vals: List[float] = []
                for c_idx, srv in enumerate(SERVERS):
                    ax = axes[r_idx][c_idx]
                    for n_idx, n in enumerate(core_list):
                        st = CORE_STYLES[n_idx % len(CORE_STYLES)]
                        vals = []
                        for conc in conc_list:
                            pt = results.get(pat, {}).get(sz, {}).get(srv, {}).get(n, {}).get(conc)
                            v = getattr(pt, attr, None) if pt and pt.measured else None
                            if v is not None and not math.isnan(float(v)):
                                fv = float(v)
                                vals.append(fv)
                                all_vals.append(fv)
                            else:
                                vals.append(math.nan)
                        ax.plot(
                            conc_list,
                            vals,
                            linestyle=st["linestyle"],
                            marker=st["marker"],
                            color=st["color"],
                            linewidth=1.8,
                            markersize=5.0,
                            label=f"{n} core" if n == 1 else f"{n} cores",
                        )
                    ax.set_xscale("log", base=2)
                    ax.set_xticks(conc_list)
                    ax.set_xticklabels([str(c) for c in conc_list])
                    ax.set_xlabel("Concurrency")
                    ax.set_ylabel(ylabel)
                    ax.set_title(f"{srv} — {ylabel}")
                    ax.grid(True, alpha=0.3)

                ymax = max(all_vals) * 1.1 if all_vals and max(all_vals) > 0 else 1.0
                for c_idx in range(len(SERVERS)):
                    axes[r_idx][c_idx].set_ylim(0.0, ymax)

            handles, labels = axes[0][0].get_legend_handles_labels()
            fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, bbox_to_anchor=(0.5, 0.99))
            fig.suptitle(f"{pat} ({sz_lbl}) — Concurrency Sweep across Core Counts", y=1.03)
            fig.tight_layout(rect=[0, 0, 1, 0.94])
            _save_both(fig, f"conc_{_slug(pat)}_{_slug(sz_lbl)}")

    # 3. Per server: heatmap of ops/s over cores x concurrency, one per pattern at 4 KiB
    hm_size = 4 * KIB if (4 * KIB) in sizes else sizes[0]
    hm_sz_lbl = format_size(hm_size)
    for srv in SERVERS:
        fig, axes = plt.subplots(1, len(patterns), figsize=(4.4 * len(patterns), 4.0))
        if len(patterns) == 1:
            axes = [axes]
        srv_vals = [
            float(results[pat][hm_size][srv][n][c].ops_per_sec)
            for pat in patterns
            for n in core_list
            for c in conc_list
            if results.get(pat, {}).get(hm_size, {}).get(srv, {}).get(n, {}).get(c)
            and results[pat][hm_size][srv][n][c].measured
            and results[pat][hm_size][srv][n][c].ops_per_sec is not None
        ]
        vmax = max(srv_vals) if srv_vals and max(srv_vals) > 0 else 1.0
        cmap = plt.get_cmap("viridis").copy()
        cmap.set_bad(color="lightgray")
        last_im = None
        for ax, pat in zip(axes, patterns):
            mat: List[List[float]] = []
            for n in core_list:
                row: List[float] = []
                for c in conc_list:
                    pt = results.get(pat, {}).get(hm_size, {}).get(srv, {}).get(n, {}).get(c)
                    v = pt.ops_per_sec if pt and pt.measured else None
                    row.append(float(v) if v is not None else math.nan)
                mat.append(row)
            im = ax.imshow(mat, aspect="auto", origin="lower", cmap=cmap, vmin=0.0, vmax=vmax)
            last_im = im
            ax.set_xticks(list(range(len(conc_list))))
            ax.set_xticklabels([str(c) for c in conc_list])
            ax.set_yticks(list(range(len(core_list))))
            ax.set_yticklabels([str(n) for n in core_list])
            ax.set_xlabel("Concurrency")
            ax.set_ylabel("CPU cores")
            ax.set_title(pat)
            for r_i, row in enumerate(mat):
                for c_i, val in enumerate(row):
                    txt = "N/A" if math.isnan(val) else (f"{val / 1000.0:.1f}k" if val >= 1000.0 else f"{val:.0f}")
                    ax.text(c_i, r_i, txt, ha="center", va="center", fontsize=7.5, color="black" if math.isnan(val) or val > vmax * 0.6 else "white")
        if last_im is not None:
            fig.colorbar(last_im, ax=axes, shrink=0.85, label="ops/s")
        fig.suptitle(f"{srv} — {hm_sz_lbl} Throughput Heatmap (ops/s)", y=1.04)
        _save_both(fig, f"heatmap_{_slug(srv)}_{_slug(hm_sz_lbl)}")

    # 4. Summary figure: random vs sequential, read and write, at 4 cores and concurrency 64, grouped bars per server
    sum_cores = _pick_target(core_list, 4)
    sum_conc = _pick_target(conc_list, 64)
    fig_s, axes_s = plt.subplots(1, len(sizes), figsize=(6.5 * len(sizes), 4.6))
    if len(sizes) == 1:
        axes_s = [axes_s]
    x_pos = list(range(len(patterns)))
    bar_w = 0.24
    for ax, sz in zip(axes_s, sizes):
        for s_idx, srv in enumerate(SERVERS):
            st = SERVER_STYLES[srv]
            offsets = [xp + (s_idx - (len(SERVERS) - 1) / 2.0) * bar_w for xp in x_pos]
            heights = []
            for pat in patterns:
                pt = results.get(pat, {}).get(sz, {}).get(srv, {}).get(sum_cores, {}).get(sum_conc)
                v = pt.ops_per_sec if pt and pt.measured else None
                heights.append(float(v) if v is not None else math.nan)
            ax.bar(
                offsets,
                heights,
                width=bar_w,
                color=st["color"],
                hatch=st.get("hatch", ""),
                edgecolor="black",
                linewidth=0.6,
                label=srv,
            )
            for ox, h in zip(offsets, heights):
                if math.isnan(h):
                    ax.text(ox, 0.0, "N/A", ha="center", va="bottom", fontsize=7, rotation=90)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(list(patterns))
        ax.set_xlabel("Access pattern")
        ax.set_ylabel("ops/s")
        ax.set_title(f"{format_size(sz)}")
        ax.grid(True, axis="y", alpha=0.3)

    handles_s, labels_s = axes_s[0].get_legend_handles_labels()
    if handles_s:
        fig_s.legend(handles_s, labels_s, loc="upper center", ncol=len(labels_s), frameon=False, bbox_to_anchor=(0.5, 0.99))
    else:
        axes_s[0].legend(loc="upper right", frameon=False)
    fig_s.suptitle(f"Random vs Sequential Summary ({sum_cores} cores, concurrency = {sum_conc})", y=1.05)
    fig_s.tight_layout(rect=[0, 0, 1, 0.91])
    _save_both(fig_s, f"summary_{sum_cores}c_c{sum_conc}")

    return written


def render_matrix_report(
    cmd: str,
    patterns: Sequence[str],
    sizes: Sequence[int],
    cores: Sequence[int],
    concurrencies: Sequence[int],
    results: Dict[str, Dict[int, Dict[str, Dict[int, Dict[int, PointResult]]]]],
    total_cores: int,
    runs: int,
    duration_sec: float,
    seed: int,
    memory_budget: str,
    prefill_bytes: int,
    chart_paths: Optional[Sequence[str]] = None,
) -> str:
    budget_bytes = parse_size(memory_budget)
    cache_bytes = stated_cache_bytes(budget_bytes)
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

    lines = [
        f"Command: `{cmd}`",
        "",
        f"# Pattern & Concurrency Scaling Benchmark: AwanFS vs MinIO vs RustFS ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"- **Machine:** {platform.node()} ({platform.platform()}, {total_cores} logical cores, {rwb.host_ram_gb()} GB RAM, CPU: {rwb.cpu_model()})",
        f"- **Methodology:** `patterns = {', '.join(patterns)}`, `sizes = {', '.join(format_size(s) for s in sizes)}`, `cores = {', '.join(str(n) for n in cores)}`, `concurrencies = {', '.join(str(c) for c in concurrencies)}`, {runs} alternating runs per point (median reported), {duration_sec:.1f}s per phase, seed = `{seed}`.",
        f"- **Cache & Prefill:** AwanFS `--memory-budget {format_size(budget_bytes)}` (stated cache half = {format_size(cache_bytes)}). Read prefill size = {format_size(prefill_bytes)} ({prefill_bytes // (4 * KIB):,} x 4 KiB objects + 1 x {format_size(prefill_bytes)} large object for range GETs), identical deterministic bytes for every server, written before timing.",
        f"- **Core pinning:** {sharing_note}",
        f"- **Durability:** AwanFS group commit (#29). {rwb.minio_durability_note()}",
        "",
        "## All Measured Points",
        "",
        "| Server | Pattern | Size | Cores | Concurrency | ops/s | MiB/s | P50 (ms) | P99 (ms) | CPU % |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]

    for srv in SERVERS:
        for pat in patterns:
            for sz in sizes:
                sz_lbl = format_size(sz)
                for n in cores:
                    for c in concurrencies:
                        pt = results.get(pat, {}).get(sz, {}).get(srv, {}).get(n, {}).get(c)
                        if pt and pt.measured:
                            mib_s = f"{pt.mib_per_sec:.2f}" if pt.mib_per_sec is not None else "-"
                            cpu_s = f"{pt.cpu_pct:.1f}%" if pt.cpu_pct is not None else "-"
                            lines.append(
                                f"| {srv} | {pat} | {sz_lbl} | {n} | {c} | {pt.ops_per_sec:,.0f} | {mib_s} | {pt.p50_ms:.2f} | {pt.p99_ms:.2f} | {cpu_s} |"
                            )
                        else:
                            reason = pt.reason if pt and pt.reason else "unknown"
                            lines.append(
                                f"| {srv} | {pat} | {sz_lbl} | {n} | {c} | not measured ({reason}) | - | - | - | - |"
                            )

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


def _run_matrix_mode(
    a: argparse.Namespace,
    cmd: str,
    cores: List[int],
    total_cores: int,
    data_root: str,
    awanfs_bin: Optional[str],
    minio_bin: Optional[str],
    mc_bin: Optional[str],
    rustfs_bin: Optional[str],
) -> int:
    patterns = parse_patterns(a.pattern)
    sizes = parse_sizes(a.sizes)
    concurrencies = parse_concurrencies(a.concurrencies)
    budget_bytes = parse_size(a.memory_budget)
    cache_bytes = stated_cache_bytes(budget_bytes)
    prefill_bytes = parse_size(a.prefill_size)
    validate_prefill_exceeds_cache(prefill_bytes, cache_bytes)

    orig_affinity = get_process_affinity(os.getpid())
    raw_runs: Dict[str, Dict[int, Dict[str, Dict[int, Dict[int, List[PointResult]]]]]] = {
        pat: {
            sz: {
                srv: {n: {c: [] for c in concurrencies} for n in cores}
                for srv in SERVERS
            }
            for sz in sizes
        }
        for pat in patterns
    }

    try:
        for n in cores:
            if n > total_cores:
                reason = f"requested {n} cores > {total_cores} available"
                for pat in patterns:
                    for sz in sizes:
                        for srv in SERVERS:
                            for c in concurrencies:
                                raw_runs[pat][sz][srv][n][c].append(PointResult(reason=reason))
                continue

            cli_cores, shares = client_cores_for(n, total_cores)
            for run_idx in range(a.runs):
                order = server_order_for_run(run_idx, SERVERS)
                for srv in order:
                    print(
                        f"[N={n}, run {run_idx + 1}/{a.runs}] Starting {srv} "
                        f"(client cores={cli_cores}, shares={shares})...",
                        flush=True,
                    )
                    if srv == "AwanFS":
                        starter = lambda n=n: _start_awanfs(awanfs_bin, n, data_root, memory_budget=a.memory_budget)
                    elif srv == "MinIO":
                        starter = lambda n=n: _start_minio(minio_bin, mc_bin, n, data_root)
                    else:
                        starter = lambda n=n: _start_rustfs(rustfs_bin, mc_bin, n, data_root)

                    phase_map = measure_server_matrix_for_cores(
                        server_name=srv,
                        starter=starter,
                        n_cores=n,
                        cli_cores=cli_cores,
                        shares_cores=shares,
                        patterns=patterns,
                        sizes=sizes,
                        concurrencies=concurrencies,
                        duration_sec=a.duration,
                        seed=a.seed,
                        prefill_bytes=prefill_bytes,
                    )
                    for (pat, sz, c), pt in phase_map.items():
                        raw_runs[pat][sz][srv][n][c].append(pt)
    finally:
        if orig_affinity:
            pin_process(os.getpid(), orig_affinity)

    results: Dict[str, Dict[int, Dict[str, Dict[int, Dict[int, PointResult]]]]] = {
        pat: {
            sz: {
                srv: {
                    n: {c: aggregate_runs(raw_runs[pat][sz][srv][n][c]) for c in concurrencies}
                    for n in cores
                }
                for srv in SERVERS
            }
            for sz in sizes
        }
        for pat in patterns
    }

    try:
        chart_paths = write_matrix_charts(results, patterns, sizes, cores, concurrencies, a.output)
    except ImportError as e:
        print(f"warning: matplotlib unavailable, skipping charts ({e})", file=sys.stderr)
        chart_paths = []

    report = render_matrix_report(
        cmd=cmd,
        patterns=patterns,
        sizes=sizes,
        cores=cores,
        concurrencies=concurrencies,
        results=results,
        total_cores=total_cores,
        runs=a.runs,
        duration_sec=a.duration,
        seed=a.seed,
        memory_budget=a.memory_budget,
        prefill_bytes=prefill_bytes,
        chart_paths=chart_paths,
    )
    out_abs = os.path.abspath(a.output)
    os.makedirs(os.path.dirname(out_abs), exist_ok=True)
    with open(out_abs, "w", encoding="utf-8") as fh:
        fh.write(report)
    print(report)
    print(f"Report written to {out_abs}")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cores", default="", help="Comma-separated core counts (e.g. 1,2,4). Default: powers of 2 up to CPU count")
    ap.add_argument("--runs", type=int, default=2, help="Alternating rounds per core count (default: 2)")
    ap.add_argument("--duration", type=float, default=3.0, help="Seconds per workload phase (default: 3.0)")
    ap.add_argument("--matrix", action="store_true", help="Run pattern x size x concurrency x cores matrix (#251)")
    ap.add_argument("--pattern", default="", help="Comma-separated patterns: seq-write,rand-write,seq-read,rand-read")
    ap.add_argument("--sizes", default="", help="Comma-separated object sizes (e.g. 4KiB,1MiB or 4096,1048576)")
    ap.add_argument("--concurrencies", default="", help="Comma-separated concurrencies (e.g. 1,4,16,64,256)")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED, help=f"RNG seed for random patterns and prefill (default: {DEFAULT_SEED})")
    ap.add_argument("--memory-budget", default=DEFAULT_MEMORY_BUDGET, help=f"AwanFS --memory-budget (default: {DEFAULT_MEMORY_BUDGET})")
    ap.add_argument("--prefill-size", default=DEFAULT_PREFILL_SIZE, help=f"Total prefill size per read set, > cache (default: {DEFAULT_PREFILL_SIZE})")
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

    if a.matrix or a.pattern or a.sizes or a.concurrencies:
        return _run_matrix_mode(
            a=a,
            cmd=cmd,
            cores=cores,
            total_cores=total_cores,
            data_root=data_root,
            awanfs_bin=awanfs_bin,
            minio_bin=minio_bin,
            mc_bin=mc_bin,
            rustfs_bin=rustfs_bin,
        )

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
