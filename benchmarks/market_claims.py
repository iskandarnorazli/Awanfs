#!/usr/bin/env python3
"""Market-claims benchmark methodology harness (#58).

Addresses the six methodological requirements for comparing AwanFS against
MinIO and RustFS on 8+ core NVMe hardware:

1. Synced writes: measures AwanFS at its default, `--durability=group`,
   alongside MinIO and RustFS at theirs, with an explicit per-server durability
   table. Other AwanFS modes run only when `--durabilities` names them.
2. Standard load tool: runs MinIO `warp` (when available) alongside the
   persistent HTTP/1.1 keep-alive harness (`run_warp_benchmark.py`) on the
   same workloads.
3. Realistic data: supports cold-read GETs (`--cache-bytes 0`, implemented via
   `--memory-budget 16MiB` with a working set larger than the payload cache
   and verified via `/_awanfs/metrics`), `LIST` (1,000 keys), `HEAD`, `DELETE`,
   and compressible ~800 KB documents (`--profile community`).
4. Longer runs & spread: `--runs` rounds (default 3) of `--duration` seconds
   (default 30) in rotating server order, reporting `median (min..max)`.
5. 8+ core pinning: pins server and client processes to separate core masks on
   8+ core hosts.
6. Fair competitor setups: starts MinIO in a 4-directory erasure set
   (`minio server dir1 dir2 dir3 dir4`) by default and records each server's
   exact setup and durability semantics.
"""

import argparse
import http.client
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import zlib
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ratio_check import wait_port  # noqa: E402
from run_warp_benchmark import (  # noqa: E402
    BenchmarkMetrics,
    WARMUP_TIMEOUT_SEC,
    affinity_labels,
    anonymous_put_ok,
    binary_version,
    calculate_percentiles,
    cpu_model,
    find_minio_binaries,
    find_rustfs_binary,
    get_free_port,
    get_process_cpu_times,
    get_process_peak_rss_mb,
    host_ram_gb,
    join_all,
    minio_durability_note,
    run_workload_phase,
    set_affinity_mask,
)

KIB = 1024
MIB = 1024 * 1024
COMMUNITY_DOC_BYTES = 800 * KIB
BUCKET = "market-claims-bench"
DEFAULT_ACCESS_KEY = "awanadmin"
DEFAULT_SECRET_KEY = "awanadminsecret"
MINIO_USER = "minioadmin"
MINIO_PASS = "minioadmin"
RUSTFS_USER = "rustfsadmin"
RUSTFS_PASS = "rustfsadmin"

DURABILITY_MODES: Dict[str, Tuple[str, str, str]] = {
    # requested_name: (s3_server_flag, display_label, durable_on_power_loss)
    "group": ("group", "AwanFS (--durability=group, default)", "Yes (shared group commit sync)"),
    "strict": ("strict", "AwanFS (--durability=strict)", "Yes (per-write sync)"),
    "none": ("fast", "AwanFS (--durability=none/fast, NO-SYNC)", "No (~1s background flush window)"),
    "fast": ("fast", "AwanFS (--durability=none/fast, NO-SYNC)", "No (~1s background flush window)"),
}


@dataclass
class RunMeasurement:
    metrics: BenchmarkMetrics
    disk_mib: float = 0.0
    cache_hits: Optional[int] = None
    cache_misses: Optional[int] = None


def parse_duration_seconds(raw: str) -> float:
    text = str(raw).strip().lower()
    if text.endswith("s"):
        text = text[:-1]
    val = float(text)
    if val <= 0:
        raise ValueError(f"duration must be > 0, got {raw!r}")
    return val


def normalize_durability(name: str) -> Tuple[str, str, str, str]:
    """Maps a requested durability label ('group', 'strict', 'none'/'fast') to
    `(canonical_key, server_flag, display_label, durability_summary)`."""
    key = name.strip().lower()
    if key not in DURABILITY_MODES:
        raise ValueError(f"unsupported durability {name!r}; expected group, strict, none, or fast")
    canonical = "none" if key == "fast" else key
    flag, label, summary = DURABILITY_MODES[key]
    return canonical, flag, label, summary


def summarize_spread(values: Sequence[float]) -> Tuple[float, float, float]:
    """Returns `(median, min, max)` for `values` (or `(0.0, 0.0, 0.0)` if empty)."""
    if not values:
        return 0.0, 0.0, 0.0
    return statistics.median(values), min(values), max(values)


def format_spread(values: Sequence[float], decimals: int = 1) -> str:
    """Formats `values` as `median (min..max)`."""
    if not values:
        return "-"
    med, lo, hi = summarize_spread(values)
    return f"{med:.{decimals}f} ({lo:.{decimals}f}..{hi:.{decimals}f})"


def make_compressible_payload(size_bytes: int = COMMUNITY_DOC_BYTES, doc_id: int = 0) -> bytes:
    """Generates a deterministic, realistic, compressible document (~3-5x zlib ratio)."""
    header = (
        f'{{"schema":"awanfs-market-bench-v1","doc_id":{doc_id},'
        f'"service":"storage-benchmark","region":"ap-southeast-1","records":['
    ).encode("ascii")
    record = (
        b'{"ts":"2026-11-18T00:00:00Z","level":"INFO","component":"volume_writer",'
        b'"op":"put_object","status":200,"latency_us":412,"bytes":819200},'
    )
    footer = b"]}"
    if size_bytes <= len(header) + len(footer):
        return (header + footer)[:size_bytes]
    body_len = size_bytes - len(header) - len(footer)
    reps = (body_len // len(record)) + 1
    body = (record * reps)[:body_len]
    return header + body + footer


def payload_compression_ratio(data: bytes) -> float:
    """Uncompressed size / zlib-compressed size."""
    if not data:
        return 1.0
    comp = zlib.compress(data, level=6)
    return len(data) / max(1, len(comp))


def dir_size_bytes(path: str) -> int:
    """Total bytes of regular files under `path`."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def parse_prometheus_counter(text: str, metric_name: str) -> Optional[int]:
    """Extracts an integer metric value from Prometheus exposition text."""
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[0] == metric_name:
            try:
                return int(float(parts[1]))
            except ValueError:
                return None
    return None


def fetch_awanfs_cache_counters(host: str, port: int) -> Tuple[Optional[int], Optional[int]]:
    """Reads `(hits, misses)` from `GET /_awanfs/metrics`, or `(None, None)` if unavailable."""
    try:
        conn = http.client.HTTPConnection(host, port, timeout=5.0)
        conn.request("GET", "/_awanfs/metrics")
        resp = conn.getresponse()
        body = resp.read().decode("utf-8", errors="replace")
        conn.close()
        if resp.status != 200:
            return None, None
        return (
            parse_prometheus_counter(body, "awanfs_payload_cache_hits_total"),
            parse_prometheus_counter(body, "awanfs_payload_cache_misses_total"),
        )
    except Exception:
        return None, None


def find_server_binary() -> Optional[str]:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    exe = ".exe" if sys.platform == "win32" else ""
    candidates = [
        os.path.join(repo_root, "server", "target", "release", f"s3-server{exe}"),
        shutil.which(f"s3-server{exe}"),
    ]
    return next((p for p in candidates if p and os.path.isfile(p)), None)


def find_warp_binary() -> Optional[str]:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    exe = ".exe" if sys.platform == "win32" else ""
    candidates = [
        os.path.join(repo_root, ".tmp", "warp", f"warp{exe}"),
        shutil.which(f"warp{exe}"),
        shutil.which("warp"),
    ]
    return next((p for p in candidates if p and os.path.isfile(p)), None)


def ensure_warp_binary() -> Optional[str]:
    """Finds `warp`, or builds it into `.tmp/warp` via `go install` if Go is available."""
    found = find_warp_binary()
    if found:
        return found
    if not shutil.which("go"):
        return None
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dest_dir = os.path.join(repo_root, ".tmp", "warp")
    os.makedirs(dest_dir, exist_ok=True)
    env = dict(os.environ, GOBIN=dest_dir)
    res = subprocess.run(
        ["go", "install", "github.com/minio/warp@latest"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if res.returncode == 0:
        return find_warp_binary()
    return None


def build_minio_server_args(
    minio_bin: str,
    data_root: str,
    port: int,
    console_port: int,
    drives: int = 4,
) -> Tuple[List[str], List[str]]:
    """Builds the MinIO command line and creates its data directories.

    With `drives > 1` (default 4), creates `dir1..dirN` under `data_root` and passes
    them as a multi-drive erasure set (`minio server dir1 dir2 dir3 dir4`).
    """
    if drives <= 1:
        dirs = [data_root]
    else:
        dirs = [os.path.join(data_root, f"dir{i}") for i in range(1, drives + 1)]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
    cmd = [
        minio_bin,
        "server",
        *dirs,
        "--address",
        f"127.0.0.1:{port}",
        "--console-address",
        f"127.0.0.1:{console_port}",
    ]
    return cmd, dirs


def build_awanfs_server_args(
    server_bin: str,
    data_dir: str,
    port: int,
    durability: str = "group",
    memory_budget: Optional[str] = None,
) -> List[str]:
    _, flag, _, _ = normalize_durability(durability)
    cmd = [
        server_bin,
        "--port",
        str(port),
        "--data-dir",
        data_dir,
        "--durability",
        flag,
        "--allow-anonymous",
    ]
    if memory_budget:
        cmd.extend(["--memory-budget", memory_budget])
    return cmd


def start_awanfs(
    server_bin: str,
    durability: str = "group",
    memory_budget: Optional[str] = None,
    affinity_mask: Optional[int] = None,
) -> Tuple[subprocess.Popen, int, str]:
    data_dir = tempfile.mkdtemp(prefix="mc_awanfs_")
    port = get_free_port()
    log_path = os.path.join(data_dir, "server.log")
    cmd = build_awanfs_server_args(server_bin, data_dir, port, durability=durability, memory_budget=memory_budget)
    env = dict(os.environ, AWANFS_ACCESS_KEY=DEFAULT_ACCESS_KEY, AWANFS_SECRET_KEY=DEFAULT_SECRET_KEY)
    with open(log_path, "ab") as log:
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
    if affinity_mask:
        set_affinity_mask(proc.pid, affinity_mask)
    if not wait_port(port, timeout=30.0):
        proc.kill()
        proc.wait()
        raise RuntimeError(f"s3-server ({durability}) did not start")
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5.0)
    conn.request("PUT", f"/{BUCKET}")
    conn.getresponse().read()
    conn.close()
    return proc, port, data_dir


def start_minio(
    minio_bin: str,
    mc_bin: str,
    drives: int = 4,
    affinity_mask: Optional[int] = None,
) -> Tuple[subprocess.Popen, int, str]:
    data_dir = tempfile.mkdtemp(prefix="mc_minio_")
    port = get_free_port()
    console_port = get_free_port()
    log_path = os.path.join(data_dir, "minio.log")
    cmd, _ = build_minio_server_args(minio_bin, data_dir, port, console_port, drives=drives)
    env = dict(
        os.environ,
        MINIO_ROOT_USER=MINIO_USER,
        MINIO_ROOT_PASSWORD=MINIO_PASS,
        MINIO_UPDATE="off",
        MINIO_BROWSER="off",
    )
    with open(log_path, "ab") as log:
        proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT)
    if affinity_mask:
        set_affinity_mask(proc.pid, affinity_mask)
    if not wait_port(port, timeout=30.0):
        proc.kill()
        proc.wait()
        raise RuntimeError("minio did not start")
    alias = f"mc_bench_{port}"
    for args in (
        ["alias", "set", alias, f"http://127.0.0.1:{port}", MINIO_USER, MINIO_PASS],
        ["mb", f"{alias}/{BUCKET}"],
        ["anonymous", "set", "public", f"{alias}/{BUCKET}"],
    ):
        for _ in range(20):
            if subprocess.run([mc_bin, *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
                break
            time.sleep(0.5)
        else:
            proc.kill()
            proc.wait()
            raise RuntimeError(f"mc {args[0]} failed")
    return proc, port, data_dir


def start_rustfs(
    rustfs_bin: str,
    mc_bin: str,
    affinity_mask: Optional[int] = None,
) -> Tuple[subprocess.Popen, int, str]:
    data_dir = tempfile.mkdtemp(prefix="mc_rustfs_")
    port = get_free_port()
    log_path = os.path.join(data_dir, "rustfs.log")
    env = dict(os.environ, RUSTFS_ACCESS_KEY=RUSTFS_USER, RUSTFS_SECRET_KEY=RUSTFS_PASS)
    cmd = [
        rustfs_bin,
        "server",
        "--address",
        f"127.0.0.1:{port}",
        "--access-key",
        RUSTFS_USER,
        "--secret-key",
        RUSTFS_PASS,
        data_dir,
    ]
    with open(log_path, "ab") as log:
        proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT)
    if affinity_mask:
        set_affinity_mask(proc.pid, affinity_mask)
    if not wait_port(port, timeout=30.0):
        proc.kill()
        proc.wait()
        raise RuntimeError("rustfs did not start")
    alias = f"rf_bench_{port}"
    for args in (
        ["alias", "set", alias, f"http://127.0.0.1:{port}", RUSTFS_USER, RUSTFS_PASS],
        ["mb", f"{alias}/{BUCKET}"],
        ["anonymous", "set", "public", f"{alias}/{BUCKET}"],
    ):
        for _ in range(20):
            if subprocess.run([mc_bin, *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
                break
            time.sleep(0.5)
        else:
            proc.kill()
            proc.wait()
            raise RuntimeError(f"mc {args[0]} for rustfs failed")
    return proc, port, data_dir


def stop_server(proc: subprocess.Popen, data_dir: str) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
    shutil.rmtree(data_dir, ignore_errors=True)


def _populate_keys(
    host: str,
    port: int,
    bucket: str,
    prefix: str,
    count: int,
    payload: bytes,
    timeout_sec: float = WARMUP_TIMEOUT_SEC,
) -> Tuple[int, int, str]:
    ok, fail, last_err = 0, 0, ""
    deadline = time.perf_counter() + timeout_sec
    conn = http.client.HTTPConnection(host, port, timeout=10.0)
    headers = {"Content-Length": str(len(payload)), "Connection": "keep-alive"}
    try:
        for i in range(count):
            if time.perf_counter() > deadline:
                break
            path = f"/{bucket}/{prefix}_{i:05d}.dat"
            try:
                conn.request("PUT", path, body=payload, headers=headers)
                resp = conn.getresponse()
                resp.read()
                if resp.status == 200:
                    ok += 1
                else:
                    fail += 1
                    last_err = f"HTTP {resp.status}"
            except Exception as exc:
                fail += 1
                last_err = f"{type(exc).__name__}: {exc}"
                conn.close()
                conn = http.client.HTTPConnection(host, port, timeout=10.0)
    finally:
        conn.close()
    return ok, fail, last_err


def _populate_keys_parallel(
    host: str,
    port: int,
    bucket: str,
    prefix: str,
    count: int,
    payload: bytes,
    workers: int = 8,
    timeout_sec: float = WARMUP_TIMEOUT_SEC,
) -> Tuple[int, int, str]:
    """Populates `count` keys across `workers` threads for faster DELETE batch setup."""
    if workers <= 1 or count <= 32:
        return _populate_keys(host, port, bucket, prefix, count, payload, timeout_sec=timeout_sec)

    deadline = time.perf_counter() + timeout_sec
    next_idx = 0
    idx_lock = threading.Lock()
    ok, fail, last_err = 0, 0, ""
    headers = {"Content-Length": str(len(payload)), "Connection": "keep-alive"}

    def worker() -> None:
        nonlocal next_idx, ok, fail, last_err
        l_ok, l_fail, l_err = 0, 0, ""
        conn = http.client.HTTPConnection(host, port, timeout=10.0)
        try:
            while time.perf_counter() <= deadline:
                with idx_lock:
                    if next_idx >= count:
                        break
                    my_idx = next_idx
                    next_idx += 1
                path = f"/{bucket}/{prefix}_{my_idx:05d}.dat"
                try:
                    conn.request("PUT", path, body=payload, headers=headers)
                    resp = conn.getresponse()
                    resp.read()
                    if resp.status == 200:
                        l_ok += 1
                    else:
                        l_fail += 1
                        l_err = f"HTTP {resp.status}"
                except Exception as exc:
                    l_fail += 1
                    l_err = f"{type(exc).__name__}: {exc}"
                    conn.close()
                    conn = http.client.HTTPConnection(host, port, timeout=10.0)
        finally:
            conn.close()
        with idx_lock:
            ok += l_ok
            fail += l_fail
            if l_err:
                last_err = l_err

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(min(workers, count))]
    for t in threads:
        t.start()
    join_all(threads, f"populate_{prefix}")
    return ok, fail, last_err


def _build_metrics(
    latencies: List[float],
    total_bytes: int,
    elapsed: float,
    errors: int,
    error_statuses: Dict[int, int],
    stuck: int,
    server_pid: Optional[int],
    srv_cpu_0: Tuple[float, float],
    cli_cpu_0: Tuple[float, float],
    extra_note: str = "",
) -> BenchmarkMetrics:
    srv_cpu_1 = get_process_cpu_times(server_pid) if server_pid else (0.0, 0.0)
    cli_cpu_1 = get_process_cpu_times(os.getpid())
    srv_cpu_s = max(0.0, (srv_cpu_1[0] - srv_cpu_0[0]) + (srv_cpu_1[1] - srv_cpu_0[1])) if server_pid else 0.0
    cli_cpu_s = max(0.0, (cli_cpu_1[0] - cli_cpu_0[0]) + (cli_cpu_1[1] - cli_cpu_0[1]))
    sys_cpu_s = (max(0.0, srv_cpu_1[1] - srv_cpu_0[1]) if server_pid else 0.0) + max(0.0, cli_cpu_1[1] - cli_cpu_0[1])

    ops = len(latencies)
    ops_per_sec = ops / elapsed if elapsed > 0 else 0.0
    mib_per_sec = (total_bytes / float(MIB)) / elapsed if elapsed > 0 else 0.0
    p50, p90, p99, p999 = calculate_percentiles(latencies)
    peak_rss = get_process_peak_rss_mb(server_pid) if server_pid else 0.0

    note_parts = []
    if errors:
        detail = ", ".join(f"HTTP {k} x{v}" for k, v in sorted(error_statuses.items()))
        note_parts.append(f"{errors} requests failed ({detail})")
    if stuck:
        note_parts.append(f"{stuck} workers did not finish")
    if extra_note:
        note_parts.append(extra_note)

    return BenchmarkMetrics(
        ops_per_sec=ops_per_sec,
        mib_per_sec=mib_per_sec,
        p50_ms=p50,
        p90_ms=p90,
        p99_ms=p99,
        p999_ms=p999,
        peak_rss_mb=peak_rss,
        server_cpu_s=srv_cpu_s,
        client_cpu_s=cli_cpu_s,
        system_cpu_s=sys_cpu_s,
        measured=(not stuck and errors == 0 and ops > 0),
        note=", ".join(note_parts),
    )


def run_cold_get_phase(
    host: str,
    port: int,
    bucket: str,
    payload_size: int,
    concurrency: int,
    duration_sec: float,
    cold_set_bytes: int,
    server_pid: Optional[int] = None,
    payload: Optional[bytes] = None,
) -> Tuple[BenchmarkMetrics, Optional[int], Optional[int]]:
    """Cold-read GETs sweeping unique keys totaling at least `cold_set_bytes`."""
    data = payload if payload is not None else os.urandom(payload_size)
    num_keys = max(concurrency * 4, (cold_set_bytes + payload_size - 1) // payload_size)
    prefix = f"cold_{payload_size}"
    warm_ok, warm_fail, last_err = _populate_keys(host, port, bucket, prefix, num_keys, data)
    if warm_ok < num_keys or warm_fail > 0:
        return (
            BenchmarkMetrics(0, 0, 0, 0, 0, 0, get_process_peak_rss_mb(server_pid) if server_pid else 0.0,
                             measured=False, note=f"cold warm-up wrote {warm_ok}/{num_keys} ({last_err})"),
            None,
            None,
        )

    hits_0, misses_0 = fetch_awanfs_cache_counters(host, port)
    stop_event = threading.Event()
    latencies: List[float] = []
    total_bytes = 0
    errors = 0
    error_statuses: Dict[int, int] = {}
    lock = threading.Lock()

    def worker(wid: int) -> None:
        nonlocal total_bytes, errors
        local_lats: List[float] = []
        local_bytes = 0
        local_errs: Dict[int, int] = {}
        conn = http.client.HTTPConnection(host, port, timeout=15.0)
        idx = wid
        while not stop_event.is_set():
            key_idx = idx % num_keys
            idx += concurrency
            path = f"/{bucket}/{prefix}_{key_idx:05d}.dat"
            t0 = time.perf_counter()
            try:
                conn.request("GET", path, headers={"Connection": "keep-alive"})
                resp = conn.getresponse()
                body = resp.read()
                t1 = time.perf_counter()
                if resp.status == 200:
                    local_lats.append((t1 - t0) * 1000.0)
                    local_bytes += len(body)
                else:
                    local_errs[resp.status] = local_errs.get(resp.status, 0) + 1
            except Exception:
                local_errs[0] = local_errs.get(0, 0) + 1
                conn.close()
                conn = http.client.HTTPConnection(host, port, timeout=15.0)
        conn.close()
        with lock:
            latencies.extend(local_lats)
            total_bytes += local_bytes
            for k, v in local_errs.items():
                errors += v
                error_statuses[k] = error_statuses.get(k, 0) + v

    srv_cpu_0 = get_process_cpu_times(server_pid) if server_pid else (0.0, 0.0)
    cli_cpu_0 = get_process_cpu_times(os.getpid())
    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(duration_sec)
    stop_event.set()
    stuck = join_all(threads, f"cold_get_{payload_size}")
    elapsed = time.perf_counter() - t_start

    hits_1, misses_1 = fetch_awanfs_cache_counters(host, port)
    hits_d = (hits_1 - hits_0) if (hits_0 is not None and hits_1 is not None) else None
    misses_d = (misses_1 - misses_0) if (misses_0 is not None and misses_1 is not None) else None

    m = _build_metrics(latencies, total_bytes, elapsed, errors, error_statuses, stuck, server_pid, srv_cpu_0, cli_cpu_0)
    return m, hits_d, misses_d


def run_list_phase(
    host: str,
    port: int,
    bucket: str,
    num_keys: int = 1000,
    concurrency: int = 8,
    duration_sec: float = 10.0,
    server_pid: Optional[int] = None,
) -> BenchmarkMetrics:
    """Benchmarks `ListObjectsV2` over `num_keys` (default 1,000) objects."""
    prefix = "list_1000"
    warm_ok, warm_fail, last_err = _populate_keys(host, port, bucket, prefix, num_keys, b"x" * 64)
    if warm_ok < num_keys or warm_fail > 0:
        return BenchmarkMetrics(
            0, 0, 0, 0, 0, 0, get_process_peak_rss_mb(server_pid) if server_pid else 0.0,
            measured=False, note=f"list warm-up wrote {warm_ok}/{num_keys} ({last_err})",
        )

    stop_event = threading.Event()
    latencies: List[float] = []
    total_bytes = 0
    errors = 0
    error_statuses: Dict[int, int] = {}
    lock = threading.Lock()
    query_path = f"/{bucket}?list-type=2&prefix={prefix}_&max-keys={num_keys}"

    def worker() -> None:
        nonlocal total_bytes, errors
        local_lats: List[float] = []
        local_bytes = 0
        local_errs: Dict[int, int] = {}
        conn = http.client.HTTPConnection(host, port, timeout=15.0)
        while not stop_event.is_set():
            t0 = time.perf_counter()
            try:
                conn.request("GET", query_path, headers={"Connection": "keep-alive"})
                resp = conn.getresponse()
                body = resp.read()
                t1 = time.perf_counter()
                if resp.status == 200:
                    local_lats.append((t1 - t0) * 1000.0)
                    local_bytes += len(body)
                else:
                    local_errs[resp.status] = local_errs.get(resp.status, 0) + 1
            except Exception:
                local_errs[0] = local_errs.get(0, 0) + 1
                conn.close()
                conn = http.client.HTTPConnection(host, port, timeout=15.0)
        conn.close()
        with lock:
            latencies.extend(local_lats)
            total_bytes += local_bytes
            for k, v in local_errs.items():
                errors += v
                error_statuses[k] = error_statuses.get(k, 0) + v

    srv_cpu_0 = get_process_cpu_times(server_pid) if server_pid else (0.0, 0.0)
    cli_cpu_0 = get_process_cpu_times(os.getpid())
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(duration_sec)
    stop_event.set()
    stuck = join_all(threads, f"list_{num_keys}")
    elapsed = time.perf_counter() - t_start
    return _build_metrics(latencies, total_bytes, elapsed, errors, error_statuses, stuck, server_pid, srv_cpu_0, cli_cpu_0)


def run_head_phase(
    host: str,
    port: int,
    bucket: str,
    payload_size: int,
    concurrency: int,
    duration_sec: float,
    server_pid: Optional[int] = None,
    payload: Optional[bytes] = None,
) -> BenchmarkMetrics:
    """Benchmarks `HEAD /bucket/key` across `concurrency` workers."""
    data = payload if payload is not None else os.urandom(payload_size)
    num_keys = max(concurrency * 4, 32)
    prefix = f"head_{payload_size}"
    warm_ok, warm_fail, last_err = _populate_keys(host, port, bucket, prefix, num_keys, data)
    if warm_ok < num_keys or warm_fail > 0:
        return BenchmarkMetrics(
            0, 0, 0, 0, 0, 0, get_process_peak_rss_mb(server_pid) if server_pid else 0.0,
            measured=False, note=f"head warm-up wrote {warm_ok}/{num_keys} ({last_err})",
        )

    stop_event = threading.Event()
    latencies: List[float] = []
    errors = 0
    error_statuses: Dict[int, int] = {}
    lock = threading.Lock()

    def worker(wid: int) -> None:
        nonlocal errors
        local_lats: List[float] = []
        local_errs: Dict[int, int] = {}
        conn = http.client.HTTPConnection(host, port, timeout=10.0)
        idx = wid
        while not stop_event.is_set():
            key_idx = idx % num_keys
            idx += concurrency
            path = f"/{bucket}/{prefix}_{key_idx:05d}.dat"
            t0 = time.perf_counter()
            try:
                conn.request("HEAD", path, headers={"Connection": "keep-alive"})
                resp = conn.getresponse()
                resp.read()
                t1 = time.perf_counter()
                if resp.status == 200:
                    local_lats.append((t1 - t0) * 1000.0)
                else:
                    local_errs[resp.status] = local_errs.get(resp.status, 0) + 1
            except Exception:
                local_errs[0] = local_errs.get(0, 0) + 1
                conn.close()
                conn = http.client.HTTPConnection(host, port, timeout=10.0)
        conn.close()
        with lock:
            latencies.extend(local_lats)
            for k, v in local_errs.items():
                errors += v
                error_statuses[k] = error_statuses.get(k, 0) + v

    srv_cpu_0 = get_process_cpu_times(server_pid) if server_pid else (0.0, 0.0)
    cli_cpu_0 = get_process_cpu_times(os.getpid())
    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(duration_sec)
    stop_event.set()
    stuck = join_all(threads, f"head_{payload_size}")
    elapsed = time.perf_counter() - t_start
    return _build_metrics(latencies, 0, elapsed, errors, error_statuses, stuck, server_pid, srv_cpu_0, cli_cpu_0)


def run_delete_phase(
    host: str,
    port: int,
    bucket: str,
    payload_size: int,
    concurrency: int,
    duration_sec: float,
    server_pid: Optional[int] = None,
    batch_size: int = 256,
    payload: Optional[bytes] = None,
) -> BenchmarkMetrics:
    """Benchmarks `DELETE /bucket/key` against real pre-populated objects in timed batches."""
    data = payload if payload is not None else os.urandom(payload_size)
    latencies: List[float] = []
    errors = 0
    error_statuses: Dict[int, int] = {}
    lock = threading.Lock()
    active_elapsed = 0.0
    batch_num = 0
    srv_cpu_0 = get_process_cpu_times(server_pid) if server_pid else (0.0, 0.0)
    cli_cpu_0 = get_process_cpu_times(os.getpid())
    stuck_total = 0
    effective_batch = max(batch_size, concurrency * 16) if duration_sec >= 1.0 else batch_size
    wall_deadline = time.perf_counter() + max(duration_sec * 2.5, duration_sec + 30.0)

    while active_elapsed < duration_sec and (batch_num == 0 or time.perf_counter() < wall_deadline):
        prefix = f"del_{payload_size}_b{batch_num}"
        batch_num += 1
        warm_ok, warm_fail, last_err = _populate_keys_parallel(
            host, port, bucket, prefix, effective_batch, data, workers=min(concurrency, 16)
        )
        if warm_ok == 0:
            return BenchmarkMetrics(
                0, 0, 0, 0, 0, 0, get_process_peak_rss_mb(server_pid) if server_pid else 0.0,
                measured=False, note=f"delete warm-up failed ({last_err})",
            )

        next_idx = 0
        idx_lock = threading.Lock()

        def worker() -> None:
            nonlocal next_idx, errors
            local_lats: List[float] = []
            local_errs: Dict[int, int] = {}
            conn = http.client.HTTPConnection(host, port, timeout=10.0)
            while True:
                with idx_lock:
                    if next_idx >= warm_ok:
                        break
                    my_idx = next_idx
                    next_idx += 1
                path = f"/{bucket}/{prefix}_{my_idx:05d}.dat"
                t0 = time.perf_counter()
                try:
                    conn.request("DELETE", path, headers={"Connection": "keep-alive"})
                    resp = conn.getresponse()
                    resp.read()
                    t1 = time.perf_counter()
                    if resp.status in (200, 204):
                        local_lats.append((t1 - t0) * 1000.0)
                    else:
                        local_errs[resp.status] = local_errs.get(resp.status, 0) + 1
                except Exception:
                    local_errs[0] = local_errs.get(0, 0) + 1
                    conn.close()
                    conn = http.client.HTTPConnection(host, port, timeout=10.0)
            conn.close()
            with lock:
                latencies.extend(local_lats)
                for k, v in local_errs.items():
                    errors += v
                    error_statuses[k] = error_statuses.get(k, 0) + v

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
        t0 = time.perf_counter()
        for t in threads:
            t.start()
        stuck_total += join_all(threads, f"delete_{payload_size}")
        active_elapsed += time.perf_counter() - t0
        if warm_fail > 0:
            break

    return _build_metrics(latencies, 0, active_elapsed, errors, error_statuses, stuck_total, server_pid, srv_cpu_0, cli_cpu_0)


def run_asymmetric_mixed_phase(
    host: str,
    port: int,
    bucket: str,
    payload: bytes,
    readers: int = 3,
    writers: int = 2,
    duration_sec: float = 30.0,
    server_pid: Optional[int] = None,
) -> BenchmarkMetrics:
    """Community mixed workload: `readers` GET workers + `writers` PUT workers concurrently."""
    num_keys = max((readers + writers) * 4, 32)
    prefix = f"mix_{readers}r_{writers}w"
    warm_ok, warm_fail, last_err = _populate_keys(host, port, bucket, prefix, num_keys, payload)
    if warm_ok < num_keys or warm_fail > 0:
        return BenchmarkMetrics(
            0, 0, 0, 0, 0, 0, get_process_peak_rss_mb(server_pid) if server_pid else 0.0,
            measured=False, note=f"mixed warm-up wrote {warm_ok}/{num_keys} ({last_err})",
        )

    stop_event = threading.Event()
    latencies: List[float] = []
    total_bytes = 0
    errors = 0
    error_statuses: Dict[int, int] = {}
    lock = threading.Lock()
    payload_len = len(payload)

    def worker(wid: int, is_writer: bool) -> None:
        nonlocal total_bytes, errors
        local_lats: List[float] = []
        local_bytes = 0
        local_errs: Dict[int, int] = {}
        conn = http.client.HTTPConnection(host, port, timeout=15.0)
        put_hdrs = {"Content-Length": str(payload_len), "Connection": "keep-alive"}
        get_hdrs = {"Connection": "keep-alive"}
        idx = wid
        while not stop_event.is_set():
            key_idx = idx % num_keys
            idx += readers + writers
            path = f"/{bucket}/{prefix}_{key_idx:05d}.dat"
            t0 = time.perf_counter()
            try:
                if is_writer:
                    conn.request("PUT", path, body=payload, headers=put_hdrs)
                    resp = conn.getresponse()
                    resp.read()
                    t1 = time.perf_counter()
                    if resp.status == 200:
                        local_lats.append((t1 - t0) * 1000.0)
                        local_bytes += payload_len
                    else:
                        local_errs[resp.status] = local_errs.get(resp.status, 0) + 1
                else:
                    conn.request("GET", path, headers=get_hdrs)
                    resp = conn.getresponse()
                    body = resp.read()
                    t1 = time.perf_counter()
                    if resp.status == 200:
                        local_lats.append((t1 - t0) * 1000.0)
                        local_bytes += len(body)
                    else:
                        local_errs[resp.status] = local_errs.get(resp.status, 0) + 1
            except Exception:
                local_errs[0] = local_errs.get(0, 0) + 1
                conn.close()
                conn = http.client.HTTPConnection(host, port, timeout=15.0)
        conn.close()
        with lock:
            latencies.extend(local_lats)
            total_bytes += local_bytes
            for k, v in local_errs.items():
                errors += v
                error_statuses[k] = error_statuses.get(k, 0) + v

    srv_cpu_0 = get_process_cpu_times(server_pid) if server_pid else (0.0, 0.0)
    cli_cpu_0 = get_process_cpu_times(os.getpid())
    threads = (
        [threading.Thread(target=worker, args=(i, False), daemon=True) for i in range(readers)]
        + [threading.Thread(target=worker, args=(readers + i, True), daemon=True) for i in range(writers)]
    )
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(duration_sec)
    stop_event.set()
    stuck = join_all(threads, f"mixed_{readers}r_{writers}w")
    elapsed = time.perf_counter() - t_start
    return _build_metrics(latencies, total_bytes, elapsed, errors, error_statuses, stuck, server_pid, srv_cpu_0, cli_cpu_0)


def run_payload_phase(
    host: str,
    port: int,
    bucket: str,
    payload: bytes,
    concurrency: int,
    duration_sec: float,
    mode: str,
    server_pid: Optional[int] = None,
) -> BenchmarkMetrics:
    """Runs PUT or GET with a caller-supplied payload (e.g. compressible 800 KB docs)."""
    num_keys = max(concurrency * 4, 16)
    prefix = f"doc_{len(payload)}_{mode}"
    warm_ok, warm_fail, last_err = _populate_keys(host, port, bucket, prefix, num_keys, payload)
    if warm_ok < num_keys or warm_fail > 0:
        return BenchmarkMetrics(
            0, 0, 0, 0, 0, 0, get_process_peak_rss_mb(server_pid) if server_pid else 0.0,
            measured=False, note=f"{mode} warm-up wrote {warm_ok}/{num_keys} ({last_err})",
        )

    stop_event = threading.Event()
    latencies: List[float] = []
    total_bytes = 0
    errors = 0
    error_statuses: Dict[int, int] = {}
    lock = threading.Lock()
    payload_len = len(payload)

    def worker(wid: int) -> None:
        nonlocal total_bytes, errors
        local_lats: List[float] = []
        local_bytes = 0
        local_errs: Dict[int, int] = {}
        conn = http.client.HTTPConnection(host, port, timeout=15.0)
        put_hdrs = {"Content-Length": str(payload_len), "Connection": "keep-alive"}
        get_hdrs = {"Connection": "keep-alive"}
        idx = wid
        while not stop_event.is_set():
            key_idx = idx % num_keys
            idx += concurrency
            path = f"/{bucket}/{prefix}_{key_idx:05d}.dat"
            t0 = time.perf_counter()
            try:
                if mode == "put":
                    conn.request("PUT", path, body=payload, headers=put_hdrs)
                    resp = conn.getresponse()
                    resp.read()
                    t1 = time.perf_counter()
                    if resp.status == 200:
                        local_lats.append((t1 - t0) * 1000.0)
                        local_bytes += payload_len
                    else:
                        local_errs[resp.status] = local_errs.get(resp.status, 0) + 1
                else:
                    conn.request("GET", path, headers=get_hdrs)
                    resp = conn.getresponse()
                    body = resp.read()
                    t1 = time.perf_counter()
                    if resp.status == 200:
                        local_lats.append((t1 - t0) * 1000.0)
                        local_bytes += len(body)
                    else:
                        local_errs[resp.status] = local_errs.get(resp.status, 0) + 1
            except Exception:
                local_errs[0] = local_errs.get(0, 0) + 1
                conn.close()
                conn = http.client.HTTPConnection(host, port, timeout=15.0)
        conn.close()
        with lock:
            latencies.extend(local_lats)
            total_bytes += local_bytes
            for k, v in local_errs.items():
                errors += v
                error_statuses[k] = error_statuses.get(k, 0) + v

    srv_cpu_0 = get_process_cpu_times(server_pid) if server_pid else (0.0, 0.0)
    cli_cpu_0 = get_process_cpu_times(os.getpid())
    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(duration_sec)
    stop_event.set()
    stuck = join_all(threads, f"{mode}_{payload_len}B_c{concurrency}")
    elapsed = time.perf_counter() - t_start
    return _build_metrics(latencies, total_bytes, elapsed, errors, error_statuses, stuck, server_pid, srv_cpu_0, cli_cpu_0)


def build_warp_command(
    warp_bin: str,
    op: str,
    host_port: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    concurrent: int,
    duration_sec: float,
    obj_size: str,
    benchdata_path: Optional[str] = None,
) -> List[str]:
    dur = f"{max(1, int(round(duration_sec)))}s"
    cmd = [
        warp_bin,
        op,
        "--host",
        host_port,
        "--access-key",
        access_key,
        "--secret-key",
        secret_key,
        "--bucket",
        bucket,
        "--concurrent",
        str(concurrent),
        "--duration",
        dur,
        "--noclear",
        "--quiet",
        "--no-color",
        "--json",
    ]
    if benchdata_path:
        cmd.extend(["--benchdata", benchdata_path])
    if op in ("put", "get", "mixed", "delete", "stat"):
        cmd.extend(["--obj.size", obj_size])
    if op in ("get", "mixed"):
        cmd.extend(["--objects", str(max(256, concurrent * 4))])
    if op == "mixed":
        cmd.extend(["--get-distrib", "80", "--put-distrib", "20", "--stat-distrib", "0", "--delete-distrib", "0"])
    elif op == "list":
        cmd.extend(["--objects", "1000"])
    return cmd


def _extract_json_payload(raw_text: str) -> Optional[dict]:
    """Extracts the JSON object from `warp --json` output (supports multi-line JSON)."""
    stripped = raw_text.strip()
    if not stripped:
        return None
    try:
        obj = json.loads(stripped)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(raw_text):
        if ch == "{":
            try:
                obj, _ = decoder.raw_decode(raw_text, idx)
                if isinstance(obj, dict) and (
                    "operations" in obj
                    or "total" in obj
                    or "mixed_server_stats" in obj
                    or "by_op_type" in obj
                    or "error" in obj
                ):
                    return obj
            except json.JSONDecodeError:
                continue
    return None


def parse_warp_json(raw_text: str, server_pid: Optional[int] = None) -> BenchmarkMetrics:
    """Parses MinIO `warp --json` output (`v: 2` Realtime, `Aggregated`, or summary JSON)."""
    payload = _extract_json_payload(raw_text)
    if payload is None:
        return BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0, measured=False, note="invalid warp JSON")

    if payload.get("status") == "error" and isinstance(payload.get("error"), dict):
        msg = payload["error"].get("message") or "warp error"
        cause = payload["error"].get("cause", {})
        if isinstance(cause, dict) and cause.get("message"):
            msg = f"{msg}: {cause['message']}"
        return BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0, measured=False, note=str(msg))

    rss = get_process_peak_rss_mb(server_pid) if server_pid else 0.0

    # Real `minio/warp --json` (`v: 2` `aggregate.Realtime`): `total.throughput_by_host` + `requests_by_client`.
    total_v2 = payload.get("total")
    if isinstance(total_v2, dict) and (
        "throughput_by_host" in total_v2 or "throughput_by_client" in total_v2 or "total_requests" in total_v2
    ):
        by_host = total_v2.get("throughput_by_host") or total_v2.get("throughput_by_client") or {}
        dur_ms_vals = [
            float(v.get("measure_duration_millis", 0) or 0)
            for v in by_host.values()
            if isinstance(v, dict)
        ]
        dur_s = (max(dur_ms_vals) / 1000.0) if dur_ms_vals and max(dur_ms_vals) > 0 else 0.0
        total_bytes = sum(float(v.get("bytes", 0.0) or 0.0) for v in by_host.values() if isinstance(v, dict))
        total_ops = sum(float(v.get("ops", 0) or 0) for v in by_host.values() if isinstance(v, dict))
        errors = int(total_v2.get("total_errors", 0) or 0)
        if total_ops == 0 and total_v2.get("total_requests", 0):
            total_ops = float(total_v2.get("total_requests", 0) or 0)
            total_bytes = float(total_v2.get("total_bytes", 0.0) or 0.0)

        ops_per_sec = (total_ops / dur_s) if dur_s > 0 else 0.0
        mib_per_sec = ((total_bytes / float(MIB)) / dur_s) if dur_s > 0 else 0.0

        weight_sum = 0
        p50_sum = p90_sum = p99_sum = 0.0
        req_by_cli = total_v2.get("requests_by_client") or {}
        if isinstance(req_by_cli, dict):
            for entries in req_by_cli.values():
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    reqs = entry.get("single_sized_requests")
                    if isinstance(reqs, dict) and reqs.get("requests", 0) > 0:
                        w = int(reqs["requests"])
                        weight_sum += w
                        p50_sum += float(reqs.get("dur_median_millis", 0.0) or 0.0) * w
                        p90_sum += float(reqs.get("dur_90_millis", 0.0) or 0.0) * w
                        p99_sum += float(reqs.get("dur_99_millis", 0.0) or 0.0) * w
        p50 = p50_sum / weight_sum if weight_sum > 0 else 0.0
        p90 = p90_sum / weight_sum if weight_sum > 0 else 0.0
        p99 = p99_sum / weight_sum if weight_sum > 0 else 0.0
        measured = (errors == 0) and (ops_per_sec > 0 or mib_per_sec > 0)
        note = f"{errors} warp errors" if errors > 0 else ""
        return BenchmarkMetrics(ops_per_sec, mib_per_sec, p50, p90, p99, 0.0, rss, measured=measured, note=note)

    # `minio/warp` `aggregate.Aggregated` schema: `operations` + optional `mixed_server_stats`.
    ops_list = payload.get("operations")
    if isinstance(ops_list, list) and ops_list:
        mixed_stats = payload.get("mixed_server_stats") if payload.get("mixed") else None
        if isinstance(mixed_stats, dict) and mixed_stats.get("measure_duration_millis", 0) > 0:
            dur_s = float(mixed_stats["measure_duration_millis"]) / 1000.0
            total_bytes = float(mixed_stats.get("bytes", 0.0) or 0.0)
            total_ops = float(mixed_stats.get("ops", 0) or 0)
            errors = int(mixed_stats.get("errors", 0) or 0)
        else:
            first_tp = ops_list[0].get("throughput", {}) if isinstance(ops_list[0], dict) else {}
            dur_s = float(first_tp.get("measure_duration_millis", 0) or 0) / 1000.0
            total_bytes = float(first_tp.get("bytes", 0.0) or 0.0)
            total_ops = float(first_tp.get("ops", 0) or 0)
            errors = int(ops_list[0].get("errors", 0) or first_tp.get("errors", 0) or 0)

        ops_per_sec = (total_ops / dur_s) if dur_s > 0 else 0.0
        mib_per_sec = ((total_bytes / float(MIB)) / dur_s) if dur_s > 0 else 0.0

        weight_sum = 0
        p50_sum = p90_sum = p99_sum = 0.0
        for op_entry in ops_list:
            if not isinstance(op_entry, dict) or op_entry.get("skipped"):
                continue
            reqs = op_entry.get("single_sized_requests")
            if isinstance(reqs, dict) and reqs.get("requests", 0) > 0:
                w = int(reqs["requests"])
                weight_sum += w
                p50_sum += float(reqs.get("dur_median_millis", 0.0) or 0.0) * w
                p90_sum += float(reqs.get("dur_90_millis", 0.0) or 0.0) * w
                p99_sum += float(reqs.get("dur_99_millis", 0.0) or 0.0) * w
        p50 = p50_sum / weight_sum if weight_sum > 0 else 0.0
        p90 = p90_sum / weight_sum if weight_sum > 0 else 0.0
        p99 = p99_sum / weight_sum if weight_sum > 0 else 0.0
        measured = (errors == 0) and (ops_per_sec > 0 or mib_per_sec > 0)
        note = f"{errors} warp errors" if errors > 0 else ""
        return BenchmarkMetrics(ops_per_sec, mib_per_sec, p50, p90, p99, 0.0, rss, measured=measured, note=note)

    # Legacy/simplified summary schema fallback
    total = payload.get("total", payload)
    tp = total.get("throughput", {}) if isinstance(total.get("throughput"), dict) else {}
    ops_obj = total.get("operations", {}) if isinstance(total.get("operations"), dict) else {}
    lat = total.get("latencies", {}) if isinstance(total.get("latencies"), dict) else {}

    bps = float(tp.get("bytes_per_sec", total.get("throughput_bps", 0.0)) or 0.0)
    mib_per_sec = bps / float(MIB) if bps > 0 else float(total.get("mib_per_sec", 0.0) or 0.0)
    ops_per_sec = float(
        ops_obj.get("ops_per_sec", total.get("ops_per_sec", total.get("requests_per_sec", 0.0))) or 0.0
    )
    p50 = float(lat.get("p50_ms", lat.get("median_ms", total.get("p50_ms", 0.0))) or 0.0)
    p90 = float(lat.get("p90_ms", total.get("p90_ms", 0.0)) or 0.0)
    p99 = float(lat.get("p99_ms", total.get("p99_ms", 0.0)) or 0.0)
    p999 = float(lat.get("p999_ms", total.get("p999_ms", 0.0)) or 0.0)
    measured = ops_per_sec > 0 or mib_per_sec > 0
    return BenchmarkMetrics(ops_per_sec, mib_per_sec, p50, p90, p99, p999, rss, measured=measured)


def run_warp_workload(
    warp_bin: str,
    op: str,
    host_port: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    concurrent: int,
    duration_sec: float,
    obj_size: str,
    server_pid: Optional[int] = None,
    timeout_sec: float = 300.0,
) -> BenchmarkMetrics:
    """Runs MinIO `warp` writing output to temp files (never a pipe)."""
    with tempfile.NamedTemporaryFile(prefix="warp_out_", suffix=".json", delete=False) as tf:
        out_path = tf.name
    with tempfile.NamedTemporaryFile(prefix="warp_err_", suffix=".log", delete=False) as ef:
        err_path = ef.name
    bench_prefix = out_path[:-5] + "_data"
    cmd = build_warp_command(
        warp_bin, op, host_port, access_key, secret_key, bucket, concurrent, duration_sec, obj_size,
        benchdata_path=bench_prefix,
    )
    try:
        with open(out_path, "wb") as sink, open(err_path, "wb") as err_sink:
            proc = subprocess.Popen(cmd, stdout=sink, stderr=err_sink)
            try:
                rc = proc.wait(timeout=timeout_sec)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                return BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0, measured=False, note="warp timed out")
        with open(out_path, "r", encoding="utf-8", errors="replace") as fh:
            raw = fh.read()
        with open(err_path, "r", encoding="utf-8", errors="replace") as fh:
            err_raw = fh.read().strip()
        m = parse_warp_json(raw, server_pid=server_pid)
        if not m.measured:
            err_tail = err_raw.splitlines()[-1].strip() if err_raw else ""
            if rc != 0 and not m.note:
                m.note = f"warp exited {rc}"
            if err_tail:
                m.note = f"{m.note}: {err_tail}" if m.note else err_tail
        return m
    finally:
        for p in (out_path, err_path, bench_prefix + ".csv.zst", bench_prefix + ".profiles.zip"):
            try:
                os.remove(p)
            except OSError:
                pass


def default_workloads(profile: str) -> List[Tuple[str, str]]:
    if profile == "community":
        return [
            ("seq_put_800k", "Sequential Write 800 KiB compressible (c=1)"),
            ("seq_get_800k", "Sequential Read 800 KiB compressible (c=1)"),
            ("conc_put_800k", "Concurrent Write 800 KiB compressible (c=5)"),
            ("conc_get_800k", "Concurrent Read 800 KiB compressible (c=5)"),
            ("mixed_3r2w_800k", "Mixed 3 Readers + 2 Writers 800 KiB compressible"),
            ("list_1000", "LIST 1,000 keys (ListObjectsV2)"),
            ("head_800k", "HEAD 800 KiB object (c=5)"),
            ("delete_800k", "DELETE 800 KiB object (c=5)"),
        ]
    return [
        ("put_64k", "64 KiB PUT (c=C)"),
        ("get_64k_default", "64 KiB GET (default cache)"),
        ("get_64k_cold", "64 KiB Cold GET (--cache-bytes 0 / >cache set)"),
        ("mixed_64k", "64 KiB Mixed 80/20"),
        ("range_get_4k", "64 KiB Range GET (4 KiB)"),
        ("put_1m", "1 MiB PUT"),
        ("get_1m_default", "1 MiB GET (default cache)"),
        ("get_1m_cold", "1 MiB Cold GET (--cache-bytes 0 / >cache set)"),
        ("list_1000", "LIST 1,000 keys (ListObjectsV2)"),
        ("head_64k", "HEAD 64 KiB object"),
        ("delete_64k", "DELETE 64 KiB object"),
    ]


def build_durability_table(
    durabilities: Sequence[str],
    minio_drives: int,
    minio_status: str,
    rustfs_status: str,
) -> List[str]:
    lines = [
        "## 2. Durability & Setup Table per Server",
        "",
        "| Server / Mode | Invocation / Setup | What a PUT Waits For | Durable on Power Cut? |",
        "| :--- | :--- | :--- | :--- |",
    ]
    seen = set()
    for d in durabilities:
        canonical, flag, label, summary = normalize_durability(d)
        if canonical in seen:
            continue
        seen.add(canonical)
        if canonical == "group":
            waits = "Shared group commit (`fsync`/`FlushFileBuffers` of volume + journal before 200)"
        elif canonical == "strict":
            waits = "Per-write `fsync`/`FlushFileBuffers` of volume + journal before 200 (no batching)"
        else:
            waits = "Acknowledged in memory before flush; background thread syncs every ~1s"
        lines.append(f"| {label} | `s3-server --durability {flag}` | {waits} | {summary} |")

    minio_setup = f"`minio server dir1..dir{minio_drives}` ({minio_drives}-dir erasure)" if minio_drives > 1 else "`minio server dir` (single-drive)"
    minio_note = minio_durability_note()
    minio_durable = "No (>4 KiB on Windows)" if os.name == "nt" else "Yes (`O_DSYNC` per PUT)"
    lines.append(f"| MinIO ({minio_status}) | {minio_setup} | {minio_note} | {minio_durable} |")
    lines.append(f"| RustFS ({rustfs_status}) | `rustfs server <data_dir>` | Single-drive release default (sync not independently probed here) | Unverified here |")
    return lines


def format_market_claims_report(
    cmd: str,
    hw_info: Dict[str, str],
    results: Dict[str, Dict[str, List[RunMeasurement]]],
    workloads: Sequence[Tuple[str, str]],
    durabilities: Sequence[str],
    runs: int,
    duration_sec: float,
    profile: str,
    minio_drives: int,
    warp_results: Optional[Dict[str, Dict[str, List[BenchmarkMetrics]]]] = None,
    compression_ratio: Optional[float] = None,
) -> str:
    """Renders the Markdown report starting with `Command: `...`` on line 1."""
    lines = [
        f"Command: `{cmd}`",
        "",
        f"# Market-Claims S3 Benchmark Report ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        "## 1. Hardware, Pinning & Execution Context",
        f"- **Profile:** `{profile}`",
        f"- **CPU Model:** {hw_info.get('cpu', 'unknown')}",
        f"- **Logical Cores:** {hw_info.get('cores', 'unknown')}",
        f"- **Host RAM:** {hw_info.get('ram_gb', 'unknown')} GB",
        f"- **Operating System:** {hw_info.get('os', platform.platform())}",
        f"- **Server Affinity Pin:** {hw_info.get('affinity_server', 'not recorded')}",
        f"- **Client Affinity Pin:** {hw_info.get('affinity_client', 'not recorded')}",
        f"- **Runs / Phase Duration:** {runs} rounds (alternating server order), {duration_sec:.1f}s per phase",
        f"- **MinIO Setup:** {minio_drives}-directory erasure set (`{hw_info.get('minio_status', 'not measured')}`)",
        f"- **RustFS Setup:** `{hw_info.get('rustfs_status', 'not measured')}`",
        f"- **MinIO `warp` Client:** {hw_info.get('warp_status', 'not measured')}",
        "- **Cold-Read Setup (`--cache-bytes 0`):** `s3-server` exposes `--memory-budget` (minimum `16MiB`, yielding a 6 MiB payload cache) rather than `--cache-bytes 0`; cold-read phases start AwanFS with `--memory-budget 16MiB` and sweep a unique-key working set larger than the cache.",
    ]
    if compression_ratio is not None:
        lines.append(f"- **Community Document Payload:** {COMMUNITY_DOC_BYTES // KIB} KiB structured log/JSON (`zlib` compression ratio {compression_ratio:.2f}x)")
    lines.append("")
    lines.extend(
        build_durability_table(
            durabilities,
            minio_drives,
            hw_info.get("minio_status", "not measured"),
            hw_info.get("rustfs_status", "not measured"),
        )
    )
    lines.extend([
        "",
        "## 3. Throughput, Latency & Resource Spread (`median (min..max)`)",
        "",
        "| Workload | Server / Mode | Client | Throughput MiB/s (min..max) | Ops/sec (min..max) | P50 ms (min..max) | P99 ms (min..max) | Peak RSS MB (min..max) | Disk Used MiB | Server CPU s |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ])

    cold_verification_rows: List[str] = []
    for key, label in workloads:
        for server_label, by_wl in results.items():
            runs_list = by_wl.get(key, [])
            if not runs_list:
                continue
            measured_runs = [r for r in runs_list if r.metrics.measured]
            if not measured_runs:
                note = runs_list[0].metrics.note or "did not finish"
                lines.append(f"| **{label}** | {server_label} | http | not measured ({note}) | - | - | - | - | - | - |")
                continue
            mibs = [r.metrics.mib_per_sec for r in measured_runs]
            ops = [r.metrics.ops_per_sec for r in measured_runs]
            p50s = [r.metrics.p50_ms for r in measured_runs]
            p99s = [r.metrics.p99_ms for r in measured_runs]
            rsss = [r.metrics.peak_rss_mb for r in measured_runs if r.metrics.peak_rss_mb > 0]
            disks = [r.disk_mib for r in measured_runs if r.disk_mib > 0]
            cpus = [r.metrics.server_cpu_s for r in measured_runs if r.metrics.server_cpu_s > 0]
            lines.append(
                f"| **{label}** | {server_label} | http | {format_spread(mibs)} | {format_spread(ops)} "
                f"| {format_spread(p50s, 2)} | {format_spread(p99s, 2)} | {format_spread(rsss)} "
                f"| {format_spread(disks)} | {format_spread(cpus, 2)} |"
            )
            for idx, r in enumerate(measured_runs, 1):
                if r.cache_hits is not None or r.cache_misses is not None:
                    cold_verification_rows.append(
                        f"| {label} | {server_label} | run {idx} | {r.cache_hits or 0} | {r.cache_misses or 0} |"
                    )

        if warp_results:
            for server_label, by_wl in warp_results.items():
                all_w = by_wl.get(key, [])
                if not all_w:
                    continue
                w_runs = [m for m in all_w if m.measured]
                if not w_runs:
                    note = all_w[0].note or "did not finish"
                    lines.append(f"| **{label}** | {server_label} | `warp` | not measured ({note}) | - | - | - | - | - | - |")
                    continue
                lines.append(
                    f"| **{label}** | {server_label} | `warp` | {format_spread([m.mib_per_sec for m in w_runs])} "
                    f"| {format_spread([m.ops_per_sec for m in w_runs])} | {format_spread([m.p50_ms for m in w_runs], 2)} "
                    f"| {format_spread([m.p99_ms for m in w_runs], 2)} | {format_spread([m.peak_rss_mb for m in w_runs if m.peak_rss_mb > 0])} "
                    f"| - | - |"
                )

    if cold_verification_rows:
        lines.extend([
            "",
            "## 4. Cold-Read Payload Cache Verification (`/_awanfs/metrics`)",
            "",
            "| Workload | Server / Mode | Round | Cache Hits (delta) | Cache Misses (delta) |",
            "| :--- | :--- | :--- | :--- | :--- |",
            *cold_verification_rows,
        ])

    return "\n".join(lines) + "\n"


def run_server_suite(
    label: str,
    starter: Callable[[], Tuple[subprocess.Popen, int, str]],
    profile: str,
    concurrent: int,
    duration_sec: float,
    cold_set_mib: int,
    cache_bytes: Optional[int],
    is_awanfs_cold_capable: bool = False,
    cold_starter: Optional[Callable[[], Tuple[subprocess.Popen, int, str]]] = None,
    warp_bin: Optional[str] = None,
    warp_creds: Tuple[str, str] = (DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY),
) -> Tuple[Dict[str, List[RunMeasurement]], Dict[str, List[BenchmarkMetrics]]]:
    """Executes one complete round of `profile` against a freshly started server."""
    out: Dict[str, List[RunMeasurement]] = {}
    warp_out: Dict[str, List[BenchmarkMetrics]] = {}
    host = "127.0.0.1"
    cold_bytes = max(1, cold_set_mib) * MIB

    def record(wl: str, m: BenchmarkMetrics, data_dir: str, hits: Optional[int] = None, misses: Optional[int] = None):
        status = f"{m.mib_per_sec:.1f} MiB/s ({m.ops_per_sec:.0f} ops/s)" if m.measured else f"not measured ({m.note})"
        print(f"    [{label}] {wl}: {status}", flush=True)
        out.setdefault(wl, []).append(
            RunMeasurement(metrics=m, disk_mib=dir_size_bytes(data_dir) / float(MIB), cache_hits=hits, cache_misses=misses)
        )

    def maybe_warp(wl: str, op: str, port: int, pid: int, w_conc: int, obj_size: str):
        if not warp_bin:
            return
        wm = run_warp_workload(
            warp_bin, op, f"{host}:{port}", warp_creds[0], warp_creds[1], BUCKET, w_conc, duration_sec, obj_size, server_pid=pid
        )
        status = f"{wm.mib_per_sec:.1f} MiB/s ({wm.ops_per_sec:.0f} ops/s)" if wm.measured else f"not measured ({wm.note})"
        print(f"    [{label}] warp:{wl}: {status}", flush=True)
        warp_out.setdefault(wl, []).append(wm)

    if profile == "community":
        doc = make_compressible_payload(COMMUNITY_DOC_BYTES, doc_id=1)
        proc, port, data_dir = starter()
        try:
            record("seq_put_800k", run_payload_phase(host, port, BUCKET, doc, 1, duration_sec, "put", proc.pid), data_dir)
            record("seq_get_800k", run_payload_phase(host, port, BUCKET, doc, 1, duration_sec, "get", proc.pid), data_dir)
            record("conc_put_800k", run_payload_phase(host, port, BUCKET, doc, 5, duration_sec, "put", proc.pid), data_dir)
            record("conc_get_800k", run_payload_phase(host, port, BUCKET, doc, 5, duration_sec, "get", proc.pid), data_dir)
            record("mixed_3r2w_800k", run_asymmetric_mixed_phase(host, port, BUCKET, doc, 3, 2, duration_sec, proc.pid), data_dir)
            record("list_1000", run_list_phase(host, port, BUCKET, 1000, 5, duration_sec, proc.pid), data_dir)
            record("head_800k", run_head_phase(host, port, BUCKET, COMMUNITY_DOC_BYTES, 5, duration_sec, proc.pid, payload=doc), data_dir)
            record("delete_800k", run_delete_phase(host, port, BUCKET, COMMUNITY_DOC_BYTES, 5, duration_sec, proc.pid, batch_size=64, payload=doc), data_dir)
            maybe_warp("conc_put_800k", "put", port, proc.pid, 5, "800KiB")
            maybe_warp("conc_get_800k", "get", port, proc.pid, 5, "800KiB")
        finally:
            stop_server(proc, data_dir)
        return out, warp_out

    # Full profile: default cache phases
    run_default = cache_bytes is None or cache_bytes > 0
    run_cold = cache_bytes is None or cache_bytes == 0
    if run_default:
        proc, port, data_dir = starter()
        try:
            c1m = min(concurrent, 32)
            record("put_64k", run_workload_phase(host, port, BUCKET, 64 * KIB, concurrent, duration_sec, "put", proc.pid), data_dir)
            record("get_64k_default", run_workload_phase(host, port, BUCKET, 64 * KIB, concurrent, duration_sec, "get", proc.pid), data_dir)
            record("mixed_64k", run_workload_phase(host, port, BUCKET, 64 * KIB, concurrent, duration_sec, "mixed", proc.pid), data_dir)
            record("range_get_4k", run_workload_phase(host, port, BUCKET, 64 * KIB, concurrent, duration_sec, "range_get_4k", proc.pid), data_dir)
            record("put_1m", run_workload_phase(host, port, BUCKET, 1 * MIB, c1m, duration_sec, "put", proc.pid), data_dir)
            record("get_1m_default", run_workload_phase(host, port, BUCKET, 1 * MIB, c1m, duration_sec, "get", proc.pid), data_dir)
            record("list_1000", run_list_phase(host, port, BUCKET, 1000, min(concurrent, 16), duration_sec, proc.pid), data_dir)
            record("head_64k", run_head_phase(host, port, BUCKET, 64 * KIB, concurrent, duration_sec, proc.pid), data_dir)
            record("delete_64k", run_delete_phase(host, port, BUCKET, 64 * KIB, concurrent, duration_sec, proc.pid), data_dir)
            maybe_warp("put_64k", "put", port, proc.pid, concurrent, "64KiB")
            maybe_warp("get_64k_default", "get", port, proc.pid, concurrent, "64KiB")
            maybe_warp("mixed_64k", "mixed", port, proc.pid, concurrent, "64KiB")
            maybe_warp("list_1000", "list", port, proc.pid, min(concurrent, 16), "64KiB")
        finally:
            stop_server(proc, data_dir)

    if run_cold:
        use_starter = cold_starter if (is_awanfs_cold_capable and cold_starter is not None) else starter
        proc, port, data_dir = use_starter()
        try:
            m64, h64, ms64 = run_cold_get_phase(host, port, BUCKET, 64 * KIB, concurrent, duration_sec, cold_bytes, proc.pid)
            record("get_64k_cold", m64, data_dir, hits=h64, misses=ms64)
            m1m, h1m, ms1m = run_cold_get_phase(host, port, BUCKET, 1 * MIB, min(concurrent, 32), duration_sec, cold_bytes, proc.pid)
            record("get_1m_cold", m1m, data_dir, hits=h1m, misses=ms1m)
        finally:
            stop_server(proc, data_dir)

    return out, warp_out


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Market-claims S3 benchmark harness (#58)")
    ap.add_argument("--server-bin", help="Path to s3-server binary (auto-detected if omitted)")
    ap.add_argument("--build", action="store_true", help="Build s3-server (--release) before running")
    ap.add_argument("--minio-bin", help="Path to minio binary")
    ap.add_argument("--mc-bin", help="Path to mc binary")
    ap.add_argument("--rustfs-bin", help="Path to rustfs binary")
    ap.add_argument("--warp-bin", help="Path to MinIO warp binary")
    ap.add_argument("--fetch-warp", action="store_true", help="Attempt `go install github.com/minio/warp@latest` if missing")
    ap.add_argument("--awanfs-only", action="store_true", help="Run AwanFS modes only (skip MinIO and RustFS)")
    # Benchmarks run every server at its defaults (owner, 2026-11-20): AwanFS in group only,
    # unless another mode is asked for by name.
    ap.add_argument("--durabilities", default="group", help="Comma-separated AwanFS durability modes (default: group)")
    ap.add_argument("--profile", choices=["full", "community"], default="full", help="Benchmark profile: full or community")
    ap.add_argument("--runs", type=int, default=3, help="Number of alternating rounds per server (default: 3)")
    ap.add_argument("--duration", default="30", help="Seconds per phase (default: 30)")
    ap.add_argument("--concurrent", type=int, default=64, help="Client concurrency for full profile (default: 64)")
    ap.add_argument("--cold-set-mib", type=int, default=32, help="Cold-read working set size in MiB (default: 32)")
    ap.add_argument("--cache-bytes", type=int, default=None, help="Set to 0 to run cold-read GETs only")
    ap.add_argument("--minio-drives", type=int, default=4, help="Number of MinIO erasure directories (default: 4)")
    ap.add_argument("--server-cores", default="0x0F", help="Hex CPU affinity mask for server on 8+ core hosts")
    ap.add_argument("--client-cores", default="0xF0", help="Hex CPU affinity mask for client on 8+ core hosts")
    ap.add_argument("--output", default="market_claims_report.md", help="Output markdown report path")
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = build_parser()
    a = ap.parse_args(argv)
    cmd = "python benchmarks/market_claims.py " + " ".join(argv if argv is not None else sys.argv[1:])
    duration_sec = parse_duration_seconds(a.duration)
    durabilities = [d.strip() for d in a.durabilities.split(",") if d.strip()]
    server_mask = int(a.server_cores, 16)
    client_mask = int(a.client_cores, 16)
    set_affinity_mask(None, client_mask)

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if a.build:
        subprocess.run(
            ["cargo", "build", "--locked", "--release", "--bin", "s3-server",
             "--manifest-path", os.path.join(repo_root, "server", "Cargo.toml")],
            check=True,
        )
    server_bin = a.server_bin or find_server_binary()
    if not server_bin or not os.path.isfile(server_bin):
        print("error: s3-server binary not found (pass --server-bin or --build)", file=sys.stderr)
        return 2

    minio_bin, mc_bin = (a.minio_bin, a.mc_bin) if (a.minio_bin and a.mc_bin) else find_minio_binaries()
    rustfs_bin = a.rustfs_bin or find_rustfs_binary()
    warp_bin = a.warp_bin or (ensure_warp_binary() if a.fetch_warp else find_warp_binary())

    targets = []
    seen_dur = set()
    for d in durabilities:
        canonical, _flag, label, _summary = normalize_durability(d)
        if canonical in seen_dur:
            continue
        seen_dur.add(canonical)
        targets.append((
            label,
            lambda dur=canonical: start_awanfs(server_bin, durability=dur, affinity_mask=server_mask),
            lambda dur=canonical: start_awanfs(server_bin, durability=dur, memory_budget="16MiB", affinity_mask=server_mask),
            True,
            (DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY),
        ))

    minio_status = "not measured"
    if not a.awanfs_only and minio_bin and mc_bin:
        minio_status = f"Measured Live ({binary_version(minio_bin)})"
        minio_label = f"MinIO ({a.minio_drives}-dir erasure)" if a.minio_drives > 1 else "MinIO (single-drive)"
        targets.append((
            minio_label,
            lambda: start_minio(minio_bin, mc_bin, drives=a.minio_drives, affinity_mask=server_mask),
            None,
            False,
            (MINIO_USER, MINIO_PASS),
        ))

    rustfs_status = "not measured"
    if not a.awanfs_only and rustfs_bin and mc_bin:
        rustfs_status = f"Measured Live ({binary_version(rustfs_bin)})"
        targets.append((
            "RustFS",
            lambda: start_rustfs(rustfs_bin, mc_bin, affinity_mask=server_mask),
            None,
            False,
            (RUSTFS_USER, RUSTFS_PASS),
        ))

    all_results: Dict[str, Dict[str, List[RunMeasurement]]] = {t[0]: {} for t in targets}
    all_warp: Dict[str, Dict[str, List[BenchmarkMetrics]]] = {t[0]: {} for t in targets}

    for r_idx in range(a.runs):
        shift = r_idx % len(targets)
        order = targets[shift:] + targets[:shift]
        for label, starter, cold_starter, is_awanfs, creds in order:
            print(f"[round {r_idx + 1}/{a.runs}] running {label} ({a.profile})...", flush=True)
            res, w_res = run_server_suite(
                label=label,
                starter=starter,
                profile=a.profile,
                concurrent=a.concurrent,
                duration_sec=duration_sec,
                cold_set_mib=a.cold_set_mib,
                cache_bytes=a.cache_bytes,
                is_awanfs_cold_capable=is_awanfs,
                cold_starter=cold_starter,
                warp_bin=warp_bin,
                warp_creds=creds,
            )
            for wl, items in res.items():
                all_results[label].setdefault(wl, []).extend(items)
            for wl, items in w_res.items():
                all_warp[label].setdefault(wl, []).extend(items)

    pin_srv, pin_cli = affinity_labels()
    hw_info = {
        "cpu": cpu_model(),
        "cores": str(os.cpu_count() or "unknown"),
        "ram_gb": host_ram_gb(),
        "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
        "affinity_server": pin_srv,
        "affinity_client": pin_cli,
        "minio_status": minio_status,
        "rustfs_status": rustfs_status,
        "warp_status": f"Measured ({binary_version(warp_bin)})" if warp_bin else "not measured (warp binary not installed)",
    }
    comp_ratio = payload_compression_ratio(make_compressible_payload(COMMUNITY_DOC_BYTES, 1)) if a.profile == "community" else None
    report = format_market_claims_report(
        cmd=cmd,
        hw_info=hw_info,
        results=all_results,
        workloads=default_workloads(a.profile),
        durabilities=durabilities,
        runs=a.runs,
        duration_sec=duration_sec,
        profile=a.profile,
        minio_drives=a.minio_drives,
        warp_results=all_warp if warp_bin else None,
        compression_ratio=comp_ratio,
    )
    out_dir = os.path.dirname(os.path.abspath(a.output))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(a.output, "w", encoding="utf-8") as fh:
        fh.write(report)
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
