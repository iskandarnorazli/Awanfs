#!/usr/bin/env python3
"""Small-PUT P50/P99 latency benchmark for AwanFS, MinIO, and RustFS (#362).

Measures P50 and P99 latency (in ms and us) for 4 KiB, 16 KiB, and 64 KiB PUTs and
conditional PUTs (`If-None-Match: *` and `If-Match`) at concurrency 1 and 16.

By default (`--durabilities group`), AwanFS runs only at its default group-commit
durability alongside MinIO and RustFS at their release defaults (`AGENTS.md`).
"""
import argparse
from dataclasses import dataclass
import http.client
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
from typing import Callable, Dict, List, Optional, Sequence, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import run_warp_benchmark as rwb  # noqa: E402

KIB = 1024
MIB = 1024 * 1024
BUCKET = "small-put-bench"
VALID_DURABILITIES: Tuple[str, ...] = ("group", "strict", "fast")
CONDITION_LABELS: Dict[str, str] = {
    "none": "none",
    "if_none_match_star": "If-None-Match: *",
    "if_match": "If-Match",
}


@dataclass(frozen=True)
class WorkloadSpec:
    key: str
    label: str
    size_bytes: int
    concurrency: int
    condition: str = "none"


@dataclass
class SmallPutMetrics:
    workload_key: str
    label: str
    size_bytes: int
    concurrency: int
    condition: str
    ops: int
    ops_per_sec: float
    mib_per_sec: float
    p50_ms: float
    p90_ms: float
    p99_ms: float
    p999_ms: float
    measured: bool = True
    note: str = ""
    p50_min_ms: Optional[float] = None
    p50_max_ms: Optional[float] = None
    p99_min_ms: Optional[float] = None
    p99_max_ms: Optional[float] = None
    server_cpu_s: float = 0.0
    peak_rss_mb: float = 0.0


def parse_size_bytes(raw: str) -> int:
    text = str(raw).strip()
    if not text:
        raise ValueError("size must not be empty")
    lower = text.lower()
    units = (("kib", KIB), ("mib", MIB), ("kb", 1000), ("mb", 1000 * 1000), ("k", KIB), ("m", MIB), ("b", 1))
    val = None
    for suffix, mult in units:
        if lower.endswith(suffix):
            num = lower[: -len(suffix)].strip()
            if not num.isdigit():
                raise ValueError(f"invalid size {raw!r}")
            val = int(num) * mult
            break
    if val is None:
        if not lower.isdigit():
            raise ValueError(f"invalid size {raw!r}")
        val = int(lower)
    if val <= 0:
        raise ValueError(f"size must be positive, got {raw!r}")
    return val


def format_size_slug(size_bytes: int) -> str:
    if size_bytes % MIB == 0:
        return f"{size_bytes // MIB}mib"
    if size_bytes % KIB == 0:
        return f"{size_bytes // KIB}kib"
    return f"{size_bytes}b"


def format_size_label(size_bytes: int) -> str:
    if size_bytes % MIB == 0:
        return f"{size_bytes // MIB} MiB"
    if size_bytes % KIB == 0:
        return f"{size_bytes // KIB} KiB"
    return f"{size_bytes} B"


def parse_duration_seconds(raw: str) -> float:
    text = str(raw).strip().lower()
    if text.endswith("s"):
        text = text[:-1]
    val = float(text)
    if val <= 0:
        raise ValueError(f"duration must be > 0, got {raw!r}")
    return val


def parse_concurrencies(raw: str) -> List[int]:
    items = [p.strip() for p in str(raw).split(",") if p.strip()]
    if not items:
        raise ValueError("concurrencies must not be empty")
    out: List[int] = []
    for item in items:
        if not item.isdigit() or int(item) <= 0:
            raise ValueError(f"invalid concurrency {item!r}")
        val = int(item)
        if val not in out:
            out.append(val)
    return out


def parse_durabilities(raw: str) -> List[str]:
    items = [p.strip().lower() for p in str(raw).split(",") if p.strip()]
    if not items:
        raise ValueError("durabilities must not be empty")
    out: List[str] = []
    for item in items:
        if item not in VALID_DURABILITIES:
            raise ValueError(f"invalid durability {item!r}; expected one of {', '.join(VALID_DURABILITIES)}")
        if item not in out:
            out.append(item)
    return out


def build_workloads(
    sizes: Sequence[int],
    concurrencies: Sequence[int],
    cond_size: int = 4 * KIB,
    include_conditional: bool = True,
) -> List[WorkloadSpec]:
    specs: List[WorkloadSpec] = []
    for size in sizes:
        slug = format_size_slug(size)
        sz_lbl = format_size_label(size)
        for conc in concurrencies:
            specs.append(
                WorkloadSpec(
                    key=f"put_{slug}_c{conc}",
                    label=f"{sz_lbl} PUT",
                    size_bytes=size,
                    concurrency=conc,
                    condition="none",
                )
            )
    if include_conditional:
        cond_slug = format_size_slug(cond_size)
        cond_lbl = format_size_label(cond_size)
        for cond_key, cond_tag in (("if_none_match_star", "if_none_match"), ("if_match", "if_match")):
            cond_disp = CONDITION_LABELS[cond_key]
            for conc in concurrencies:
                specs.append(
                    WorkloadSpec(
                        key=f"put_{cond_slug}_{cond_tag}_c{conc}",
                        label=f"{cond_lbl} PUT ({cond_disp})",
                        size_bytes=cond_size,
                        concurrency=conc,
                        condition=cond_key,
                    )
                )
    return specs


def build_awanfs_server_argv(
    server_bin: str,
    port: int,
    data_dir: str,
    durability: str = "group",
) -> List[str]:
    if durability not in VALID_DURABILITIES:
        raise ValueError(f"invalid durability {durability!r}")
    argv = [server_bin, "--port", str(port), "--data-dir", data_dir, "--allow-anonymous"]
    if durability != "group":
        argv.extend(["--durability", durability])
    return argv


def _connect_http(host: str, port: int, timeout: float = 10.0) -> http.client.HTTPConnection:
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.connect()
        if conn.sock is not None:
            conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception:
        pass
    return conn


def _make_payload(size_bytes: int, first_byte: int) -> bytes:
    if size_bytes <= 1:
        return bytes([first_byte & 0xFF]) * size_bytes
    return bytes([first_byte & 0xFF]) + os.urandom(size_bytes - 1)


def run_small_put_phase(
    host: str,
    port: int,
    bucket: str,
    spec: WorkloadSpec,
    duration_sec: float,
    server_pid: Optional[int] = None,
    phase_tag: str = "p0",
) -> SmallPutMetrics:
    """Runs one small-PUT latency phase and returns P50/P99 latency and throughput."""
    payload_a = _make_payload(spec.size_bytes, 0x41)
    payload_b = _make_payload(spec.size_bytes, 0x42)
    stop_event = threading.Event()
    latencies: List[float] = []
    lat_lock = threading.Lock()
    total_bytes = 0
    errors = 0
    error_statuses: Dict[int, int] = {}
    exc_counts: Dict[str, int] = {}
    last_error = ""
    warm_fail = 0
    warm_etags: Dict[int, str] = {}

    if spec.condition == "none":
        pool_keys = 8
        for w in range(spec.concurrency):
            conn = _connect_http(host, port, timeout=10.0)
            try:
                for k in range(pool_keys):
                    path = f"/{bucket}/put_{phase_tag}_w{w}_k{k}.dat"
                    conn.request("PUT", path, body=payload_a, headers={"Content-Length": str(spec.size_bytes), "Connection": "keep-alive"})
                    resp = conn.getresponse()
                    resp.read()
                    if resp.status != 200:
                        warm_fail += 1
                        last_error = f"HTTP {resp.status}"
            except Exception as exc:
                warm_fail += 1
                last_error = f"{type(exc).__name__}: {exc}"
            finally:
                conn.close()
    elif spec.condition == "if_none_match_star":
        conn = _connect_http(host, port, timeout=10.0)
        try:
            path = f"/{bucket}/inm_{phase_tag}_warmup.dat"
            conn.request("PUT", path, body=payload_a, headers={"Content-Length": str(spec.size_bytes), "Connection": "keep-alive"})
            resp = conn.getresponse()
            resp.read()
            if resp.status != 200:
                warm_fail += 1
                last_error = f"HTTP {resp.status}"
        except Exception as exc:
            warm_fail += 1
            last_error = f"{type(exc).__name__}: {exc}"
        finally:
            conn.close()
    elif spec.condition == "if_match":
        for w in range(spec.concurrency):
            conn = _connect_http(host, port, timeout=10.0)
            try:
                path = f"/{bucket}/im_{phase_tag}_w{w}.dat"
                conn.request("PUT", path, body=payload_a, headers={"Content-Length": str(spec.size_bytes), "Connection": "keep-alive"})
                resp = conn.getresponse()
                resp.read()
                etag = resp.getheader("ETag")
                if resp.status != 200 or not etag:
                    warm_fail += 1
                    last_error = f"HTTP {resp.status}" if resp.status != 200 else "missing ETag on warmup PUT"
                else:
                    warm_etags[w] = etag
            except Exception as exc:
                warm_fail += 1
                last_error = f"{type(exc).__name__}: {exc}"
            finally:
                conn.close()
    else:
        raise ValueError(f"unsupported condition {spec.condition!r}")

    def worker(worker_id: int) -> None:
        nonlocal total_bytes, errors, last_error
        local_lats: List[float] = []
        local_bytes = 0
        local_errors: Dict[int, int] = {}
        local_excs: Dict[str, int] = {}
        local_last_err = ""
        req_count = 0
        current_etag = warm_etags.get(worker_id, "")
        conn = _connect_http(host, port, timeout=10.0)

        while not stop_event.is_set():
            if spec.condition == "none":
                path = f"/{bucket}/put_{phase_tag}_w{worker_id}_k{req_count % 8}.dat"
                body = payload_a
                headers = {"Content-Length": str(spec.size_bytes), "Connection": "keep-alive"}
            elif spec.condition == "if_none_match_star":
                path = f"/{bucket}/inm_{phase_tag}_w{worker_id}_{req_count}.dat"
                body = payload_a
                headers = {"Content-Length": str(spec.size_bytes), "Connection": "keep-alive", "If-None-Match": "*"}
            else:
                path = f"/{bucket}/im_{phase_tag}_w{worker_id}.dat"
                body = payload_b if (req_count % 2 == 0) else payload_a
                headers = {"Content-Length": str(spec.size_bytes), "Connection": "keep-alive", "If-Match": current_etag}
            req_count += 1

            t0 = time.perf_counter()
            try:
                conn.request("PUT", path, body=body, headers=headers)
                resp = conn.getresponse()
                resp.read()
                t1 = time.perf_counter()
                if resp.status == 200:
                    local_lats.append((t1 - t0) * 1000.0)
                    local_bytes += spec.size_bytes
                    if spec.condition == "if_match":
                        new_etag = resp.getheader("ETag")
                        if new_etag:
                            current_etag = new_etag
                else:
                    local_errors[resp.status] = local_errors.get(resp.status, 0) + 1
                    local_last_err = f"HTTP {resp.status}"
            except Exception as exc:
                name = type(exc).__name__
                local_excs[name] = local_excs.get(name, 0) + 1
                local_last_err = f"{name}: {exc}"
                try:
                    conn.close()
                except Exception:
                    pass
                conn = _connect_http(host, port, timeout=10.0)
                if spec.condition == "if_match":
                    try:
                        conn.request("HEAD", path, headers={"Connection": "keep-alive"})
                        h_resp = conn.getresponse()
                        h_resp.read()
                        refreshed = h_resp.getheader("ETag")
                        if h_resp.status == 200 and refreshed:
                            current_etag = refreshed
                    except Exception:
                        pass

        try:
            conn.close()
        except Exception:
            pass

        with lat_lock:
            latencies.extend(local_lats)
            total_bytes += local_bytes
            for st, n in local_errors.items():
                errors += n
                error_statuses[st] = error_statuses.get(st, 0) + n
            for k, n in local_excs.items():
                errors += n
                exc_counts[k] = exc_counts.get(k, 0) + n
            if local_last_err:
                last_error = local_last_err

    if warm_fail > 0:
        return SmallPutMetrics(
            workload_key=spec.key,
            label=spec.label,
            size_bytes=spec.size_bytes,
            concurrency=spec.concurrency,
            condition=spec.condition,
            ops=0,
            ops_per_sec=0.0,
            mib_per_sec=0.0,
            p50_ms=0.0,
            p90_ms=0.0,
            p99_ms=0.0,
            p999_ms=0.0,
            measured=False,
            note=f"warm-up failed ({last_error})",
            peak_rss_mb=rwb.get_process_peak_rss_mb(server_pid) if server_pid else 0.0,
        )

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(spec.concurrency)]
    with rwb.CpuSampler(server_pid) as cpu_sampler, rwb.PeakRssSampler(server_pid) as rss_sampler:
        t_start = time.perf_counter()
        for t in threads:
            t.start()
        time.sleep(duration_sec)
        stop_event.set()
        stuck = rwb.join_all(threads, f"{spec.key}_{phase_tag}")
        t_total = time.perf_counter() - t_start

    ops = len(latencies)
    ops_per_sec = ops / t_total if t_total > 0 else 0.0
    mib_per_sec = (total_bytes / float(MIB)) / t_total if t_total > 0 else 0.0
    p50, p90, p99, p999 = rwb.calculate_percentiles(latencies)
    peak_rss = max(rwb.get_process_peak_rss_mb(server_pid) if server_pid else 0.0, rss_sampler.peak_rss_mb or 0.0)
    note = rwb.format_phase_failure_note(error_statuses, exc_counts, last_error, stuck)
    measured = (not stuck) and errors == 0 and ops > 0
    if ops == 0 and not note:
        note = "0 successful operations"

    return SmallPutMetrics(
        workload_key=spec.key,
        label=spec.label,
        size_bytes=spec.size_bytes,
        concurrency=spec.concurrency,
        condition=spec.condition,
        ops=ops,
        ops_per_sec=ops_per_sec,
        mib_per_sec=mib_per_sec,
        p50_ms=p50,
        p90_ms=p90,
        p99_ms=p99,
        p999_ms=p999,
        measured=measured,
        note=note,
        p50_min_ms=p50 if measured else None,
        p50_max_ms=p50 if measured else None,
        p99_min_ms=p99 if measured else None,
        p99_max_ms=p99 if measured else None,
        server_cpu_s=cpu_sampler.server_cpu_s,
        peak_rss_mb=peak_rss,
    )


def aggregate_rounds(rounds: Sequence[Dict[str, SmallPutMetrics]]) -> Dict[str, SmallPutMetrics]:
    """Aggregates per-round workload dicts into medians with min..max P50/P99 bounds."""
    if not rounds:
        return {}
    if len(rounds) == 1:
        return dict(rounds[0])

    keys: List[str] = []
    seen = set()
    for r in rounds:
        for k in r:
            if k not in seen:
                seen.add(k)
                keys.append(k)

    out: Dict[str, SmallPutMetrics] = {}
    for k in keys:
        samples = [r[k] for r in rounds if k in r]
        measured_samples = [s for s in samples if s.measured]
        pool = measured_samples if measured_samples else samples
        first = pool[0]
        p50_vals = [s.p50_ms for s in measured_samples]
        p99_vals = [s.p99_ms for s in measured_samples]
        notes = [s.note for s in samples if s.note]
        out[k] = SmallPutMetrics(
            workload_key=k,
            label=first.label,
            size_bytes=first.size_bytes,
            concurrency=first.concurrency,
            condition=first.condition,
            ops=int(round(statistics.median(s.ops for s in pool))),
            ops_per_sec=statistics.median(s.ops_per_sec for s in pool),
            mib_per_sec=statistics.median(s.mib_per_sec for s in pool),
            p50_ms=statistics.median(s.p50_ms for s in pool),
            p90_ms=statistics.median(s.p90_ms for s in pool),
            p99_ms=statistics.median(s.p99_ms for s in pool),
            p999_ms=statistics.median(s.p999_ms for s in pool),
            measured=bool(measured_samples),
            note="; ".join(dict.fromkeys(notes)),
            p50_min_ms=min(p50_vals) if p50_vals else None,
            p50_max_ms=max(p50_vals) if p50_vals else None,
            p99_min_ms=min(p99_vals) if p99_vals else None,
            p99_max_ms=max(p99_vals) if p99_vals else None,
            server_cpu_s=statistics.median(s.server_cpu_s for s in pool),
            peak_rss_mb=statistics.median(s.peak_rss_mb for s in pool),
        )
    return out


def _start_logged(argv: List[str], log_path: str, env: Optional[dict] = None) -> subprocess.Popen:
    kwargs: Dict[str, object] = {}
    if sys.platform != "win32":
        kwargs["start_new_session"] = True
    log = open(log_path, "ab")
    try:
        return subprocess.Popen(argv, env=env, stdout=log, stderr=subprocess.STDOUT, **kwargs)
    finally:
        log.close()


def _stop_proc(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _setup_mc_bucket(mc_bin: str, alias: str, port: int, access_key: str, secret_key: str, mc_home: str) -> None:
    os.makedirs(mc_home, exist_ok=True)
    env = dict(os.environ, HOME=mc_home)
    cmds = (
        ["alias", "set", alias, f"http://127.0.0.1:{port}", access_key, secret_key],
        ["mb", f"{alias}/{BUCKET}"],
        ["anonymous", "set", "public", f"{alias}/{BUCKET}"],
    )
    for args in cmds:
        last_err = ""
        for _ in range(25):
            res = subprocess.run([mc_bin, *args], env=env, capture_output=True, text=True)
            if res.returncode == 0:
                break
            last_err = (res.stderr or res.stdout or f"exit {res.returncode}").strip()
            time.sleep(0.4)
        else:
            raise RuntimeError(f"mc {args[0]} failed: {last_err[:160]}")


def _start_awanfs(server_bin: str, durability: str, data_root: str, srv_mask: Optional[int]) -> Tuple[subprocess.Popen, int, str, str]:
    data_dir = tempfile.mkdtemp(prefix=f"spl_awanfs_{durability}_", dir=data_root)
    port = rwb.get_free_port()
    log_path = os.path.join(data_dir, "server.log")
    argv = build_awanfs_server_argv(server_bin, port, data_dir, durability=durability)
    proc = _start_logged(argv, log_path)
    if srv_mask is not None and rwb.can_pin():
        rwb.set_affinity_mask(proc.pid, srv_mask)
    if not rwb.wait_for_port("127.0.0.1", port, timeout_sec=30.0):
        _stop_proc(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
        raise RuntimeError(f"awanfs ({durability}) did not start on port {port}")
    conn = _connect_http("127.0.0.1", port, timeout=5.0)
    try:
        conn.request("PUT", f"/{BUCKET}")
        resp = conn.getresponse()
        resp.read()
        if resp.status not in (200, 409):
            raise RuntimeError(f"bucket PUT returned HTTP {resp.status}")
    finally:
        conn.close()
    return proc, port, data_dir, rwb.binary_version(server_bin)


def _start_minio(minio_bin: str, mc_bin: str, data_root: str, srv_mask: Optional[int]) -> Tuple[subprocess.Popen, int, str, str]:
    data_dir = tempfile.mkdtemp(prefix="spl_minio_", dir=data_root)
    port, console = rwb.get_free_port(), rwb.get_free_port()
    log_path = os.path.join(data_dir, "server.log")
    mc_home = os.path.join(data_dir, "mc_home")
    env = dict(
        os.environ,
        MINIO_ROOT_USER="minioadmin",
        MINIO_ROOT_PASSWORD="minioadmin",
        MINIO_UPDATE="off",
        MINIO_BROWSER="off",
    )
    argv = [minio_bin, "server", data_dir, "--address", f"127.0.0.1:{port}", "--console-address", f"127.0.0.1:{console}"]
    proc = _start_logged(argv, log_path, env=env)
    if srv_mask is not None and rwb.can_pin():
        rwb.set_affinity_mask(proc.pid, srv_mask)
    if not rwb.wait_for_port("127.0.0.1", port, timeout_sec=30.0):
        _stop_proc(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
        raise RuntimeError(f"minio did not start on port {port}")
    _setup_mc_bucket(mc_bin, f"spl_minio_{port}", port, "minioadmin", "minioadmin", mc_home)
    return proc, port, data_dir, rwb.binary_version(minio_bin)


def _start_rustfs(rustfs_bin: str, mc_bin: str, data_root: str, srv_mask: Optional[int]) -> Tuple[subprocess.Popen, int, str, str]:
    data_dir = tempfile.mkdtemp(prefix="spl_rustfs_", dir=data_root)
    port = rwb.get_free_port()
    log_path = os.path.join(data_dir, "server.log")
    mc_home = os.path.join(data_dir, "mc_home")
    env = dict(os.environ, RUSTFS_ACCESS_KEY="rustfsadmin", RUSTFS_SECRET_KEY="rustfsadmin")
    env.pop("RUSTFS_DURABILITY_MODE", None)
    argv = [rustfs_bin, "server", "--address", f"127.0.0.1:{port}", "--access-key", "rustfsadmin", "--secret-key", "rustfsadmin", data_dir]
    proc = _start_logged(argv, log_path, env=env)
    if srv_mask is not None and rwb.can_pin():
        rwb.set_affinity_mask(proc.pid, srv_mask)
    if not rwb.wait_for_port("127.0.0.1", port, timeout_sec=30.0):
        _stop_proc(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
        raise RuntimeError(f"rustfs did not start on port {port}")
    _setup_mc_bucket(mc_bin, f"spl_rustfs_{port}", port, "rustfsadmin", "rustfsadmin", mc_home)
    return proc, port, data_dir, rwb.binary_version(rustfs_bin)


def format_markdown_report(
    cmd: str,
    hw_info: Dict[str, str],
    workloads: Sequence[WorkloadSpec],
    target_results: Sequence[Tuple[str, Dict[str, SmallPutMetrics]]],
    runs: int,
    duration_sec: float,
    durability_notes: Sequence[str],
    pin_labels: Optional[Tuple[str, str]] = None,
    mc_version: Optional[str] = None,
) -> str:
    lines = [
        f"Command: `{cmd}`",
        "",
        f"# Small-PUT P50/P99 Latency ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"- **Machine:** {hw_info.get('node', platform.node())} ({hw_info.get('os', platform.platform())}, "
        f"{hw_info.get('cores', os.cpu_count() or 'unknown')} CPUs, {hw_info.get('ram_gb', 'unknown')} GB RAM, "
        f"CPU: {hw_info.get('cpu', 'unknown')})",
        f"- **Schedule:** {runs} round(s), {duration_sec:.1f}s per phase, alternating server order across rounds (medians reported)",
        f"- **Data directory:** `{hw_info.get('data_dir', tempfile.gettempdir())}`",
    ]
    if mc_version:
        lines.append(f"- **mc:** {mc_version}")
    if pin_labels:
        lines.append(f"- **Core pinning:** server `{pin_labels[0]}`, client `{pin_labels[1]}`")
    for note in durability_notes:
        lines.append(f"- **Durability:** {note}")
    lines.extend([
        "",
        "| Workload | Size | Concurrency | Condition | Target | ops/s | MiB/s | P50 (ms) | P50 (us) | P99 (ms) | P99 (us) |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ])
    for spec in workloads:
        sz_lbl = format_size_label(spec.size_bytes)
        cond_lbl = CONDITION_LABELS.get(spec.condition, spec.condition)
        for target_label, by_wl in target_results:
            m = by_wl.get(spec.key)
            if m is None:
                lines.append(
                    f"| {spec.label} | {sz_lbl} | {spec.concurrency} | {cond_lbl} | {target_label} | not measured | - | - | - | - | - |"
                )
            elif not m.measured:
                reason = m.note or "did not finish"
                lines.append(
                    f"| {spec.label} | {sz_lbl} | {spec.concurrency} | {cond_lbl} | {target_label} | not measured ({reason}) | - | - | - | - | - |"
                )
            else:
                p50_us = m.p50_ms * 1000.0
                p99_us = m.p99_ms * 1000.0
                lines.append(
                    f"| {spec.label} | {sz_lbl} | {spec.concurrency} | {cond_lbl} | {target_label} "
                    f"| {m.ops_per_sec:.1f} | {m.mib_per_sec:.2f} "
                    f"| {m.p50_ms:.3f} | {p50_us:.1f} | {m.p99_ms:.3f} | {p99_us:.1f} |"
                )
    if runs > 1:
        spread_rows: List[str] = []
        for spec in workloads:
            for target_label, by_wl in target_results:
                m = by_wl.get(spec.key)
                if m and m.measured and m.p50_min_ms is not None and m.p99_min_ms is not None:
                    spread_rows.append(
                        f"| `{spec.key}` | {target_label} | {m.p50_ms:.3f} ({m.p50_min_ms:.3f}..{m.p50_max_ms:.3f}) "
                        f"| {m.p99_ms:.3f} ({m.p99_min_ms:.3f}..{m.p99_max_ms:.3f}) |"
                    )
        if spread_rows:
            lines.extend([
                "",
                "## Round Spread (`median (min..max)` ms)",
                "",
                "| Workload Key | Target | P50 (ms) | P99 (ms) |",
                "|---|---|---|---|",
                *spread_rows,
            ])
    lines.append("")
    return "\n".join(lines)


def _find_default_server_bin() -> Optional[str]:
    exe = ".exe" if sys.platform == "win32" else ""
    cand = os.path.join(REPO_ROOT, "server", "target", "release", f"awanfs{exe}")
    if os.path.isfile(cand):
        return cand
    return shutil.which(f"awanfs{exe}")


def _awanfs_label(durability: str) -> str:
    if durability == "group":
        return "AwanFS (--durability=group, default)"
    return f"AwanFS (--durability={durability})"


def _durability_notes(durabilities: Sequence[str], has_minio: bool, has_rustfs: bool) -> List[str]:
    notes: List[str] = []
    for d in durabilities:
        if d == "group":
            notes.append("AwanFS (`--durability=group`, default): shared group commit sync before 200 OK (#29).")
        elif d == "strict":
            notes.append("AwanFS (`--durability=strict`): per-write `fsync`/`FlushFileBuffers` before 200 OK.")
        else:
            notes.append("AwanFS (`--durability=fast`): in-memory acknowledge; background flush every ~1s.")
    if has_minio:
        notes.append(f"MinIO: {rwb.minio_durability_note()}")
    if has_rustfs:
        notes.append("RustFS (release defaults): durable on Linux (3.09 sync calls/PUT, measurements/2026-11-21-durability-probe.md; Windows sync count is not probed).")
    return notes


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Small-PUT P50/P99 latency benchmark (#362)")
    ap.add_argument("--server-bin", help="Path to awanfs binary (auto-detected if omitted)")
    ap.add_argument("--build", action="store_true", help="Build awanfs (--release) before running")
    ap.add_argument("--minio-bin", help="Path to minio binary")
    ap.add_argument("--mc-bin", help="Path to mc binary")
    ap.add_argument("--rustfs-bin", help="Path to rustfs binary")
    ap.add_argument("--awanfs-only", action="store_true", help="Skip MinIO and RustFS even if present")
    ap.add_argument("--durabilities", default="group", help="Comma-separated AwanFS durability modes (default: group)")
    ap.add_argument("--sizes", default="4KiB,16KiB,64KiB", help="Comma-separated unconditional PUT sizes")
    ap.add_argument("--concurrencies", default="1,16", help="Comma-separated worker concurrencies")
    ap.add_argument("--cond-size", default="4KiB", help="Payload size for conditional PUTs (default: 4KiB)")
    ap.add_argument("--no-conditional", dest="include_conditional", action="store_false", help="Skip If-None-Match / If-Match PUT phases")
    ap.set_defaults(include_conditional=True)
    ap.add_argument("--runs", type=int, default=3, help="Number of alternating rounds (default: 3)")
    ap.add_argument("--duration", default="2s", help="Duration per workload phase (default: 2s)")
    ap.add_argument("--data-dir", default=None, help="Parent directory for temporary server data")
    ap.add_argument("--output", default="", help="Write markdown report to this path")
    ap.add_argument("--json-output", default="", help="Write JSON summary to this path")
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = build_parser()
    a = ap.parse_args(argv)
    if a.runs < 1:
        ap.error("--runs must be >= 1")

    raw_args = list(argv) if argv is not None else sys.argv[1:]
    cmd = "python benchmarks/small_put_latency.py" + (" " + " ".join(raw_args) if raw_args else "")

    durabilities = parse_durabilities(a.durabilities)
    sizes = [parse_size_bytes(s) for s in a.sizes.split(",") if s.strip()]
    if not sizes:
        ap.error("--sizes must not be empty")
    concurrencies = parse_concurrencies(a.concurrencies)
    cond_size = parse_size_bytes(a.cond_size)
    duration_sec = parse_duration_seconds(a.duration)
    workloads = build_workloads(sizes, concurrencies, cond_size=cond_size, include_conditional=a.include_conditional)

    if a.build:
        subprocess.run(
            ["cargo", "build", "--locked", "--release", "--bin", "awanfs",
             "--manifest-path", os.path.join(REPO_ROOT, "server", "Cargo.toml")],
            check=True,
        )
    server_bin = a.server_bin or _find_default_server_bin()
    if not server_bin or not os.path.isfile(server_bin):
        print("error: awanfs binary not found (pass --server-bin or --build)", file=sys.stderr)
        return 2

    minio_bin, mc_bin = (a.minio_bin, a.mc_bin) if (a.minio_bin and a.mc_bin) else rwb.find_minio_binaries()
    rustfs_bin = a.rustfs_bin or rwb.find_rustfs_binary()
    data_root = a.data_dir or tempfile.gettempdir()
    os.makedirs(data_root, exist_ok=True)

    srv_mask: Optional[int] = None
    pin_labels: Optional[Tuple[str, str]] = None
    if rwb.can_pin():
        srv_mask, cli_mask = rwb.affinity_masks()
        rwb.set_affinity_mask(None, cli_mask)
        pin_labels = rwb.affinity_labels()

    targets: List[Tuple[str, Callable[[], Tuple[subprocess.Popen, int, str, str]]]] = []
    for dur in durabilities:
        targets.append((_awanfs_label(dur), lambda d=dur: _start_awanfs(server_bin, d, data_root, srv_mask)))

    has_minio = False
    if not a.awanfs_only and minio_bin and os.path.isfile(minio_bin) and mc_bin and os.path.isfile(mc_bin):
        has_minio = True
        targets.append(("MinIO", lambda: _start_minio(minio_bin, mc_bin, data_root, srv_mask)))

    has_rustfs = False
    if not a.awanfs_only and rustfs_bin and os.path.isfile(rustfs_bin) and mc_bin and os.path.isfile(mc_bin):
        has_rustfs = True
        targets.append(("RustFS", lambda: _start_rustfs(rustfs_bin, mc_bin, data_root, srv_mask)))

    mc_version: Optional[str] = None
    if (has_minio or has_rustfs) and mc_bin:
        mc_version = rwb.binary_version(mc_bin)
        print(f"mc version: {mc_version}", flush=True)

    rounds_by_target: Dict[str, List[Dict[str, SmallPutMetrics]]] = {label: [] for label, _ in targets}
    for r_idx in range(a.runs):
        shift = r_idx % len(targets)
        order = targets[shift:] + targets[:shift]
        for target_label, starter in order:
            print(f"[round {r_idx + 1}/{a.runs}] {target_label} ...", flush=True)
            proc, port, data_dir, _ver = starter()
            phase_map: Dict[str, SmallPutMetrics] = {}
            try:
                for idx, spec in enumerate(workloads):
                    tag = f"r{r_idx}_{idx}"
                    m = run_small_put_phase("127.0.0.1", port, BUCKET, spec, duration_sec, server_pid=proc.pid, phase_tag=tag)
                    phase_map[spec.key] = m
                    if m.measured:
                        print(
                            f"  {spec.key}: {m.ops_per_sec:.1f} ops/s, P50={m.p50_ms:.3f} ms ({m.p50_ms * 1000.0:.1f} us), "
                            f"P99={m.p99_ms:.3f} ms ({m.p99_ms * 1000.0:.1f} us)",
                            flush=True,
                        )
                    else:
                        print(f"  {spec.key}: not measured ({m.note})", flush=True)
            finally:
                _stop_proc(proc)
                shutil.rmtree(data_dir, ignore_errors=True)
            rounds_by_target[target_label].append(phase_map)

    target_results: List[Tuple[str, Dict[str, SmallPutMetrics]]] = [
        (label, aggregate_rounds(rounds_by_target[label])) for label, _ in targets
    ]
    hw_info = {
        "node": platform.node(),
        "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
        "cores": str(os.cpu_count() or "unknown"),
        "ram_gb": rwb.host_ram_gb(),
        "cpu": rwb.cpu_model(),
        "data_dir": data_root,
    }
    report = format_markdown_report(
        cmd=cmd,
        hw_info=hw_info,
        workloads=workloads,
        target_results=target_results,
        runs=a.runs,
        duration_sec=duration_sec,
        durability_notes=_durability_notes(durabilities, has_minio, has_rustfs),
        pin_labels=pin_labels,
        mc_version=mc_version,
    )
    print(report)
    if a.output:
        out_dir = os.path.dirname(os.path.abspath(a.output))
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(a.output, "w", encoding="utf-8") as fh:
            fh.write(report)
    if a.json_output:
        j_dir = os.path.dirname(os.path.abspath(a.json_output))
        if j_dir:
            os.makedirs(j_dir, exist_ok=True)
        payload = {
            "command": cmd,
            "hw_info": hw_info,
            "runs": a.runs,
            "duration_sec": duration_sec,
            **({"mc_version": mc_version} if mc_version else {}),
            "results": {
                label: {
                    k: {
                        "ops": m.ops,
                        "ops_per_sec": m.ops_per_sec,
                        "mib_per_sec": m.mib_per_sec,
                        "p50_ms": m.p50_ms,
                        "p90_ms": m.p90_ms,
                        "p99_ms": m.p99_ms,
                        "p999_ms": m.p999_ms,
                        "measured": m.measured,
                        "note": m.note,
                    }
                    for k, m in by_wl.items()
                }
                for label, by_wl in target_results
            },
        }
        with open(a.json_output, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
