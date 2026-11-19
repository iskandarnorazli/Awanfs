#!/usr/bin/env python3
"""
benchmarks/s3_warp/run_warp_benchmark.py
Head-to-Head S3 Warp Benchmark Harness (Awan Storage Server vs Local Baseline & MinIO Reference) (#466)
Supports:
- 64 KiB & 1 MiB Single-Object PUT / GET
- RFC 9110 Byte-Range GET (206 Partial Content sub-slice reads)
- S3 Multipart Upload (?uploads -> ?partNumber=N&uploadId=ID -> CompleteMultipartUpload)
- 1 GiB & 10 GiB High-Volume Multipart & Parallel Range GET Streaming
"""

import argparse
import ctypes
from ctypes import wintypes
import http.client
import http.server
import os
import platform
import re
import shutil
import socket
import socketserver
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class BenchmarkMetrics:
    ops_per_sec: float
    mib_per_sec: float
    p50_ms: float
    p90_ms: float
    p99_ms: float
    p999_ms: float
    peak_rss_mb: float
    measured: bool = True
    note: str = ""
    server_cpu_s: float = 0.0
    client_cpu_s: float = 0.0
    system_cpu_s: float = 0.0
    server_cpu_pct: float = 0.0
    client_cpu_pct: float = 0.0
    system_cpu_pct: float = 0.0
    peak_rss_min_mb: Optional[float] = None
    peak_rss_max_mb: Optional[float] = None
    peak_inflight_mb: Optional[float] = None


def calculate_percentiles(latencies: List[float]) -> Tuple[float, float, float, float]:
    if not latencies:
        return 0.0, 0.0, 0.0, 0.0
    s = sorted(latencies)
    n = len(s)

    def pct(p: float) -> float:
        idx = int(round(p * (n - 1)))
        return s[max(0, min(n - 1, idx))]

    return pct(0.50), pct(0.90), pct(0.99), pct(0.999)


def get_process_peak_rss_mb(pid: int) -> float:
    """Queries the operating system kernel for the process's Peak Working Set RSS in MiB."""
    if sys.platform == "win32":
        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        PROCESS_QUERY_INFORMATION = 0x0400
        PROCESS_VM_READ = 0x0010
        kernel32 = ctypes.windll.kernel32
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel32.OpenProcess(
            PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, False, pid
        )
        if handle:
            try:
                counters = PROCESS_MEMORY_COUNTERS()
                counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
                psapi = ctypes.windll.psapi
                psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
                if psapi.GetProcessMemoryInfo(
                    handle, ctypes.byref(counters), counters.cb
                ):
                    return counters.PeakWorkingSetSize / (1024.0 * 1024.0)
            finally:
                kernel32.CloseHandle(handle)
    else:
        try:
            with open(f"/proc/{pid}/status", "r") as f:
                for line in f:
                    if line.startswith("VmHWM:") or line.startswith("VmRSS:"):
                        parts = line.split()
                        if len(parts) >= 2:
                            return float(parts[1]) / 1024.0
        except Exception:
            pass
    return 0.0


def get_process_cpu_times(pid: int) -> Tuple[float, float]:
    """Returns `(user_seconds, system_seconds)` for `pid` using psutil or OS APIs."""
    try:
        import psutil  # type: ignore
        ct = psutil.Process(pid).cpu_times()
        return float(ct.user), float(ct.system)
    except Exception:
        pass
    if sys.platform == "win32":
        class FILETIME(ctypes.Structure):
            _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        kernel32 = ctypes.windll.kernel32
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.GetProcessTimes.argtypes = [
            wintypes.HANDLE, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
        ]
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if handle:
            try:
                ct, et, kt, ut = FILETIME(), FILETIME(), FILETIME(), FILETIME()
                if kernel32.GetProcessTimes(
                    handle, ctypes.byref(ct), ctypes.byref(et), ctypes.byref(kt), ctypes.byref(ut)
                ):
                    def ft_to_s(ft: FILETIME) -> float:
                        return ((ft.dwHighDateTime << 32) | ft.dwLowDateTime) / 1e7
                    return ft_to_s(ut), ft_to_s(kt)
            finally:
                kernel32.CloseHandle(handle)
    else:
        try:
            with open(f"/proc/{pid}/stat", "r") as f:
                data = f.read()
            rparen = data.rfind(")")
            fields = data[rparen + 2:].split()
            hz = float(os.sysconf("SC_CLK_TCK"))
            return int(fields[11]) / hz, int(fields[12]) / hz
        except Exception:
            pass
    return 0.0, 0.0


def get_system_cpu_times() -> Tuple[float, float]:
    """Returns `(busy_seconds, total_seconds)` across all CPUs using psutil or OS APIs."""
    try:
        import psutil  # type: ignore
        ct = psutil.cpu_times()
        idle = float(getattr(ct, "idle", 0.0)) + float(getattr(ct, "iowait", 0.0))
        total = sum(float(v) for v in ct)
        return max(0.0, total - idle), max(0.0, total)
    except Exception:
        pass
    if sys.platform == "win32":
        class FILETIME(ctypes.Structure):
            _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]

        it, kt, ut = FILETIME(), FILETIME(), FILETIME()
        if ctypes.windll.kernel32.GetSystemTimes(ctypes.byref(it), ctypes.byref(kt), ctypes.byref(ut)):
            def ft_to_s(ft: FILETIME) -> float:
                return ((ft.dwHighDateTime << 32) | ft.dwLowDateTime) / 1e7
            idle = ft_to_s(it)
            kernel = ft_to_s(kt)  # Windows kernel time includes idle time
            user = ft_to_s(ut)
            total = kernel + user
            busy = max(0.0, total - idle)
            return busy, total
    else:
        try:
            with open("/proc/stat", "r") as f:
                first = f.readline().split()
            if first and first[0] == "cpu":
                vals = [int(v) for v in first[1:]]
                hz = float(os.sysconf("SC_CLK_TCK"))
                idle = (vals[3] + (vals[4] if len(vals) > 4 else 0)) / hz
                total = sum(vals) / hz
                return max(0.0, total - idle), max(0.0, total)
        except Exception:
            pass
    return 0.0, 0.0


class CpuSampler:
    """Samples server, client, and system CPU utilisation while a workload phase runs (#175)."""

    def __init__(self, server_pid: Optional[int], every: float = 0.2):
        self.server_pid = server_pid
        self.client_pid = os.getpid()
        self.every = every
        self.server_cpu_s = 0.0
        self.client_cpu_s = 0.0
        self.system_cpu_s = 0.0
        self.server_cpu_pct = 0.0
        self.client_cpu_pct = 0.0
        self.system_cpu_pct = 0.0
        self._samples_srv: List[float] = []
        self._samples_cli: List[float] = []
        self._samples_sys: List[float] = []
        self._stop: Optional[threading.Event] = None
        self._thread: Optional[threading.Thread] = None
        self._t0 = 0.0
        self._srv0 = (0.0, 0.0)
        self._cli0 = (0.0, 0.0)
        self._sys0 = (0.0, 0.0)

    def _take(self) -> Tuple[Tuple[float, float], Tuple[float, float], Tuple[float, float], float]:
        srv = get_process_cpu_times(self.server_pid) if self.server_pid else (0.0, 0.0)
        cli = get_process_cpu_times(self.client_pid)
        sys_t = get_system_cpu_times()
        return srv, cli, sys_t, time.perf_counter()

    def __enter__(self):
        self._srv0, self._cli0, self._sys0, self._t0 = self._take()
        prev_srv, prev_cli, prev_sys, prev_t = self._srv0, self._cli0, self._sys0, self._t0
        self._stop = threading.Event()

        def loop():
            nonlocal prev_srv, prev_cli, prev_sys, prev_t
            while not self._stop.wait(self.every):
                cur_srv, cur_cli, cur_sys, cur_t = self._take()
                dt = cur_t - prev_t
                if dt > 0:
                    if self.server_pid:
                        dsrv = max(0.0, (cur_srv[0] - prev_srv[0]) + (cur_srv[1] - prev_srv[1]))
                        self._samples_srv.append(100.0 * dsrv / dt)
                    dcli = max(0.0, (cur_cli[0] - prev_cli[0]) + (cur_cli[1] - prev_cli[1]))
                    self._samples_cli.append(100.0 * dcli / dt)
                    dtot = cur_sys[1] - prev_sys[1]
                    if dtot > 0:
                        dbsy = max(0.0, cur_sys[0] - prev_sys[0])
                        self._samples_sys.append(min(100.0, 100.0 * dbsy / dtot))
                prev_srv, prev_cli, prev_sys, prev_t = cur_srv, cur_cli, cur_sys, cur_t

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        if self._stop is not None:
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        srv1, cli1, sys1, t1 = self._take()
        dt = max(1e-9, t1 - self._t0)
        self.server_cpu_s = max(0.0, (srv1[0] - self._srv0[0]) + (srv1[1] - self._srv0[1])) if self.server_pid else 0.0
        self.client_cpu_s = max(0.0, (cli1[0] - self._cli0[0]) + (cli1[1] - self._cli0[1]))
        self.system_cpu_s = (
            (max(0.0, srv1[1] - self._srv0[1]) if self.server_pid else 0.0)
            + max(0.0, cli1[1] - self._cli0[1])
        )
        self.server_cpu_pct = (
            sum(self._samples_srv) / len(self._samples_srv)
            if self._samples_srv else (100.0 * self.server_cpu_s / dt if self.server_pid else 0.0)
        )
        self.client_cpu_pct = (
            sum(self._samples_cli) / len(self._samples_cli)
            if self._samples_cli else (100.0 * self.client_cpu_s / dt)
        )
        sys_tot = sys1[1] - self._sys0[1]
        self.system_cpu_pct = (
            sum(self._samples_sys) / len(self._samples_sys)
            if self._samples_sys else (min(100.0, 100.0 * max(0.0, sys1[0] - self._sys0[0]) / sys_tot) if sys_tot > 0 else 0.0)
        )
        return False


def minio_durability_note(os_name: str = os.name) -> str:
    """What a MinIO PUT waits for on this platform, read from MinIO's source at the pinned
    release: on Linux it opens with O_DSYNC or calls fdatasync; on Windows its fdatasync is a
    no-op (internal/disk/fdatasync_unsupported.go) and only files up to 4 KiB are opened
    write-through, so a PUT of 64 KiB or 1 MiB there does not wait for the disk."""
    if os_name == "nt":
        return ("MinIO does not flush these PUTs on Windows (its fdatasync is a no-op there), "
                "so its PUT and mixed rows on this machine are not durable writes and are not like for like.")
    return "MinIO syncs each PUT (O_DSYNC)."


def get_awanfs_peak_inflight_mb(host: str, port: int) -> Optional[float]:
    """Reads `awanfs_peak_inflight_bytes` from `/_awanfs/metrics` in MiB, or None if unavailable."""
    try:
        conn = http.client.HTTPConnection(host, port, timeout=5.0)
        conn.request("GET", "/_awanfs/metrics")
        resp = conn.getresponse()
        text = resp.read().decode("utf-8", errors="replace")
        conn.close()
        if resp.status != 200:
            return None
    except Exception:
        return None
    for line in text.splitlines():
        if line.startswith("awanfs_peak_inflight_bytes "):
            try:
                return float(line.split()[1]) / (1024.0 * 1024.0)
            except (IndexError, ValueError):
                return None
    return None


def aggregate_rounds(rounds: List[Dict]) -> Dict:
    """Aggregates per-round workload dicts into medians plus peak RSS min..max spread."""
    if not rounds:
        return {}
    if len(rounds) == 1:
        return dict(rounds[0])

    keys = []
    seen = set()
    for r in rounds:
        for k in r:
            if k not in seen:
                seen.add(k)
                keys.append(k)

    out = {}
    for k in keys:
        samples = [r[k] for r in rounds if k in r]
        measured_samples = [s for s in samples if s.measured]
        pool = measured_samples if measured_samples else samples
        rss_vals = [s.peak_rss_mb for s in pool if s.peak_rss_mb > 0]
        inflight_vals = [s.peak_inflight_mb for s in pool if s.peak_inflight_mb is not None]
        notes = [s.note for s in samples if s.note]
        out[k] = BenchmarkMetrics(
            ops_per_sec=statistics.median(s.ops_per_sec for s in pool),
            mib_per_sec=statistics.median(s.mib_per_sec for s in pool),
            p50_ms=statistics.median(s.p50_ms for s in pool),
            p90_ms=statistics.median(s.p90_ms for s in pool),
            p99_ms=statistics.median(s.p99_ms for s in pool),
            p999_ms=statistics.median(s.p999_ms for s in pool),
            peak_rss_mb=statistics.median(rss_vals) if rss_vals else 0.0,
            measured=bool(measured_samples),
            note="; ".join(dict.fromkeys(notes)),
            server_cpu_s=statistics.median(s.server_cpu_s for s in pool),
            client_cpu_s=statistics.median(s.client_cpu_s for s in pool),
            system_cpu_s=statistics.median(s.system_cpu_s for s in pool),
            server_cpu_pct=statistics.median(s.server_cpu_pct for s in pool),
            client_cpu_pct=statistics.median(s.client_cpu_pct for s in pool),
            system_cpu_pct=statistics.median(s.system_cpu_pct for s in pool),
            peak_rss_min_mb=min(rss_vals) if rss_vals else None,
            peak_rss_max_mb=max(rss_vals) if rss_vals else None,
            peak_inflight_mb=statistics.median(inflight_vals) if inflight_vals else None,
        )
    return out


def check_rss_ceiling(
    awan: Dict[str, BenchmarkMetrics],
    ceiling_mib: Optional[float],
) -> Tuple[Optional[float], List[str]]:
    """Returns `(awanfs_median_peak_rss_mib, failures)` against `ceiling_mib`."""
    all_rss = [v.peak_rss_mb for v in awan.values() if v.measured and v.peak_rss_mb > 0]
    peak_mb = max(all_rss) if all_rss else None
    if ceiling_mib is None:
        return peak_mb, []
    if peak_mb is None:
        return None, [f"AwanFS peak RSS was not measured (ceiling {float(ceiling_mib):.1f} MiB)"]
    if peak_mb > float(ceiling_mib):
        return peak_mb, [
            f"AwanFS median peak RSS {peak_mb:.1f} MiB exceeds ceiling {float(ceiling_mib):.1f} MiB"
        ]
    return peak_mb, []


def format_rss_cell(m: BenchmarkMetrics) -> str:
    if m.peak_rss_min_mb is not None and m.peak_rss_max_mb is not None:
        return f"{m.peak_rss_mb:.1f} MB ({m.peak_rss_min_mb:.1f}..{m.peak_rss_max_mb:.1f})"
    return f"{m.peak_rss_mb:.1f} MB"


def format_markdown_report(
    cmd: str,
    hw_info: Dict[str, str],
    awan: Dict[str, BenchmarkMetrics],
    rust_fb: Optional[Dict[str, BenchmarkMetrics]] = None,
    minio_or_baseline: Optional[Dict[str, BenchmarkMetrics]] = None,
    baseline_server_label: str = "MinIO Community Baseline",
    concurrency_data: Optional[Dict[str, Dict[int, BenchmarkMetrics]]] = None,
    runs: int = 1,
    rss_ceiling_mib: Optional[float] = None,
) -> str:
    if minio_or_baseline is None and isinstance(rust_fb, dict) and "put_64k" in rust_fb:
        # Backward compatibility when invoked with 4 arguments: (cmd, hw_info, s3_metrics, minio_metrics)
        minio_or_baseline = rust_fb
        rust_fb = {}
    if minio_or_baseline is None:
        minio_or_baseline = {}
    if rust_fb is None:
        rust_fb = {}

    lines = [
        f"`{cmd}`",
        "",
        "# Head-to-Head S3 Benchmark Report: S3 Storage Server vs MinIO & RustFS",
        "",
        "## 1. Physical Hardware & Execution Context",
        f"- **CPU Model:** {hw_info.get('cpu', 'x86_64 CPU')}",
        f"- **Logical Cores:** {hw_info.get('cores', 'N/A')}",
        f"- **Host RAM:** {hw_info.get('ram_gb', 'N/A')} GB",
        f"- **Operating System:** {hw_info.get('os', platform.platform())}",
        f"- **Server Affinity Pin:** {hw_info.get('affinity_server', 'Cores 0..3 (Mask 0x0F)')}",
        f"- **Client Affinity Pin:** {hw_info.get('affinity_client', 'Cores 4..7 (Mask 0xF0)')}",
        f"- **Live MinIO Binary Status:** {hw_info.get('minio_status', 'not measured')}",
        f"- **Live RustFS Binary Status:** {hw_info.get('rustfs_status', 'not measured')}",
        "- **Durability per PUT:** `s3-server` runs with its default, group commit (#29): a 200 means the bytes are on stable storage, and concurrent PUTs share one sync. " + minio_durability_note() + " RustFS's sync behaviour is not verified here.",
    ]
    if runs > 1:
        lines.append(f"- **Rounds per Server:** {runs} (values are medians; Peak RSS shows median and min..max spread)")
    lines.extend([
        "",
        "## 2. Live Measured HTTP/1.1 Keep-Alive Throughput & Latency Matrix (64 KiB – 10 GiB)",
        "",
        "| Workload | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) | Peak RSS (MB) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ])

    workloads = [
        ("64 KiB PUT (Single Object)", "put_64k"),
        ("64 KiB GET (Single Object)", "get_64k"),
        ("64 KiB Mixed (80% GET / 20% PUT)", "mixed_64k"),
        ("64 KiB Range GET (bytes=0-4095, 206 Partial)", "range_get_4k"),
        ("1 MiB PUT (Single Object)", "put_1m"),
        ("1 MiB GET (Single Object)", "get_1m"),
        ("1 MiB Mixed (80% GET / 20% PUT)", "mixed_1m"),
        ("1 MiB Multipart Upload (4x256KiB Parts + Complete)", "mpu_1m"),
        ("1 GiB Multipart Upload (16x64MiB Parts + Complete)", "mpu_put_1g"),
        ("1 GiB Multipart Range GET (16x64MiB Parallel Ranges)", "mpu_get_1g"),
        ("10 GiB Volume PUT (160x64MiB Streamed Parts)", "put_10g"),
        ("10 GiB Volume GET (160x64MiB Parallel Ranges)", "get_10g"),
    ]

    for label, key in workloads:
        if key not in awan and key not in minio_or_baseline and key not in rust_fb:
            continue
        a = awan.get(key, BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
        m = minio_or_baseline.get(key, BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0, measured=False))
        r = rust_fb.get(key, BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0, measured=False))

        # A phase that did not finish is not a slow phase: say so in the row (#135).
        if not a.measured and a.note:
            lines.append(f"| **{label}** | **S3 Storage Server** | did not finish: {a.note} | | | | | | |")

        lines.append(
            f"| **{label}** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **{a.mib_per_sec:.1f}** | **{a.ops_per_sec:.1f}** | **{a.p50_ms:.2f}** | **{a.p90_ms:.2f}** | **{a.p99_ms:.2f}** | **{a.p999_ms:.2f}** | **{format_rss_cell(a)}** |"
        )
        if m.measured:
            if m.ops_per_sec == 0.0 and m.mib_per_sec == 0.0:
                lines.append(
                    f"| **{label}** | {baseline_server_label} | N/A (S3 $\\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | {format_rss_cell(m)} |"
                )
            else:
                lines.append(
                    f"| **{label}** | {baseline_server_label} | {m.mib_per_sec:.1f} | {m.ops_per_sec:.1f} | {m.p50_ms:.2f} | {m.p90_ms:.2f} | {m.p99_ms:.2f} | {m.p999_ms:.2f} | {format_rss_cell(m)} |"
                )
        if r.measured:
            if r.ops_per_sec == 0.0 and r.mib_per_sec == 0.0:
                lines.append(
                    f"| **{label}** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | N/A (S3 $\\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | {format_rss_cell(r)} |"
                )
            else:
                lines.append(
                    f"| **{label}** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | {r.mib_per_sec:.1f} | {r.ops_per_sec:.1f} | {r.p50_ms:.2f} | {r.p90_ms:.2f} | {r.p99_ms:.2f} | {r.p999_ms:.2f} | {format_rss_cell(r)} |"
                )

    put_64k = awan.get("put_64k", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    get_64k = awan.get("get_64k", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    put_1m = awan.get("put_1m", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    get_1m = awan.get("get_1m", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    get_10g = awan.get("get_10g", get_1m)
    all_rss = [v.peak_rss_mb for v in awan.values() if v.peak_rss_mb > 0]
    max_rss = max(all_rss) if all_rss else 0.0

    m_put_64k = minio_or_baseline.get("put_64k", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    m_get_64k = minio_or_baseline.get("get_64k", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    m_put_1m = minio_or_baseline.get("put_1m", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    m_get_1m = minio_or_baseline.get("get_1m", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    m_get_10g = minio_or_baseline.get("get_10g", m_get_1m)
    m_all_rss = [v.peak_rss_mb for v in minio_or_baseline.values() if v.peak_rss_mb > 0]
    m_max_rss = max(m_all_rss) if m_all_rss else 0.0

    r_put_64k = rust_fb.get("put_64k", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    r_get_64k = rust_fb.get("get_64k", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    r_put_1m = rust_fb.get("put_1m", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    r_get_1m = rust_fb.get("get_1m", BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0))
    r_get_10g = rust_fb.get("get_10g", r_get_1m)
    r_all_rss = [v.peak_rss_mb for v in rust_fb.values() if v.peak_rss_mb > 0]
    r_max_rss = max(r_all_rss) if r_all_rss else 0.0

    if concurrency_data:
        lines.extend([
            "",
            "## 3. Concurrency Scaling Sweep (64 KiB Mixed 80% GET / 20% PUT: C in {8, 32, 64, 128})",
            "",
            "| Concurrency (Workers) | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ])
        for c in [8, 32, 64, 128]:
            if "s3-server" in concurrency_data and c in concurrency_data["s3-server"]:
                m = concurrency_data["s3-server"][c]
                lines.append(
                    f"| **C = {c}** | **S3 Storage Server** (`s3-server` `--release`, Columnar SoA Index) | **{m.mib_per_sec:.1f}** | **{m.ops_per_sec:.1f}** | **{m.p50_ms:.2f}** | **{m.p90_ms:.2f}** | **{m.p99_ms:.2f}** | **{m.p999_ms:.2f}** |"
                )
            if "minio" in concurrency_data and c in concurrency_data["minio"]:
                m = concurrency_data["minio"][c]
                lines.append(
                    f"| **C = {c}** | {baseline_server_label} | {m.mib_per_sec:.1f} | {m.ops_per_sec:.1f} | {m.p50_ms:.2f} | {m.p90_ms:.2f} | {m.p99_ms:.2f} | {m.p999_ms:.2f} |"
                )
            if "rustfs" in concurrency_data and c in concurrency_data["rustfs"]:
                m = concurrency_data["rustfs"][c]
                lines.append(
                    f"| **C = {c}** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | {m.mib_per_sec:.1f} | {m.ops_per_sec:.1f} | {m.p50_ms:.2f} | {m.p90_ms:.2f} | {m.p99_ms:.2f} | {m.p999_ms:.2f} |"
                )

        section_num = "4"
    else:
        section_num = "3"

    lines.extend([
        "",
        f"## {section_num}. Open-Source S3 Alternatives Reference Comparison",
        "",
        f"> **Provenance Note (Audit Rule 3):** MinIO ({hw_info.get('minio_status', 'not measured')}) and RustFS ({hw_info.get('rustfs_status', 'not measured')}) ran on this machine ({hw_info.get('os', platform.platform())}) with anonymous access enabled via `mc`, receiving the same persistent HTTP/1.1 keep-alive requests as `s3-server`. Server pinning: {hw_info.get('affinity_server', 'not recorded')}. Peak RSS comes from the OS (`GetProcessMemoryInfo` on Windows, `/proc/<pid>/status` VmHWM on Linux). Note that for 1 MiB multipart upload with 256 KiB parts, both MinIO and RustFS strictly reject parts smaller than 5 MiB with HTTP 400 `EntityTooSmall` per AWS S3 RFC specification, whereas `s3-server` accepts them, which does not conform to S3. Reference figures for SeaweedFS, Garage and Ceph RGW are not measured here and are left out.",
        "",
        "| Dimension | `s3-server` (Measured Local) | MinIO (Go, Measured Local) | RustFS (Rust, Measured Local) |",
        "| :--- | :--- | :--- | :--- |",
        f"| **64 KiB PUT Throughput** | **{put_64k.mib_per_sec:.1f} MiB/s ({put_64k.ops_per_sec:.0f} ops/s)** | {m_put_64k.mib_per_sec:.1f} MiB/s ({m_put_64k.ops_per_sec:.0f} ops/s) | {r_put_64k.mib_per_sec:.1f} MiB/s ({r_put_64k.ops_per_sec:.0f} ops/s) |",
        f"| **64 KiB GET Throughput** | **{get_64k.mib_per_sec:.1f} MiB/s ({get_64k.ops_per_sec:.0f} ops/s)** | {m_get_64k.mib_per_sec:.1f} MiB/s ({m_get_64k.ops_per_sec:.0f} ops/s) | {r_get_64k.mib_per_sec:.1f} MiB/s ({r_get_64k.ops_per_sec:.0f} ops/s) |",
        f"| **1 MiB PUT Throughput** | **{put_1m.mib_per_sec:.1f} MiB/s ({put_1m.ops_per_sec:.0f} ops/s)** | {m_put_1m.mib_per_sec:.1f} MiB/s ({m_put_1m.ops_per_sec:.0f} ops/s) | {r_put_1m.mib_per_sec:.1f} MiB/s ({r_put_1m.ops_per_sec:.0f} ops/s) |",
        f"| **1 MiB / 10 GiB GET Throughput** | **{get_1m.mib_per_sec:.1f} – {get_10g.mib_per_sec:.1f} MiB/s** | {m_get_1m.mib_per_sec:.1f} – {m_get_10g.mib_per_sec:.1f} MiB/s | {r_get_1m.mib_per_sec:.1f} – {r_get_10g.mib_per_sec:.1f} MiB/s |",
        f"| **Server Peak RSS (64K–10G)** | **{max_rss:.1f} MB (1 GiB payload cache cap)** | {m_max_rss:.1f} MB (Go GC heap) | {r_max_rss:.1f} MB (Rust jemalloc) |",
        "| **Metadata Layout** | **64B `ObjectMeta` Columnar SoA** | Per-object `xl.meta` file | Multi-volume FS Metadata |",
        "| **Small-File Disk Layout** | **4096B WORM Needle Volume** | 2+ files/inodes per object | Multi-volume File Store |",
        "| **License** | **Permissive (Workspace / MIT / Apache-2.0)** | AGPLv3 (Active Community OSS) | Apache 2.0 (Active Rust OSS) |",
    ])

    cpu_rows = [
        (k, v) for k, v in awan.items()
        if v.server_cpu_s > 0 or v.client_cpu_s > 0 or v.server_cpu_pct > 0 or v.client_cpu_pct > 0
    ]
    if cpu_rows:
        lines.extend([
            "",
            "## CPU Breakdown (per process, AwanFS phases)",
            "",
            "| Phase | Server CPU (s) | Client CPU (s) | System/Kernel CPU (s) | Server CPU % | Client CPU % | System CPU % |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ])
        for k, v in cpu_rows:
            lines.append(
                f"| `{k}` | {v.server_cpu_s:.2f} | {v.client_cpu_s:.2f} | {v.system_cpu_s:.2f} | "
                f"{v.server_cpu_pct:.1f}% | {v.client_cpu_pct:.1f}% | {v.system_cpu_pct:.1f}% |"
            )

    inflight_rows = [(k, v) for k, v in awan.items() if v.peak_inflight_mb is not None]
    if inflight_rows:
        lines.extend([
            "",
            "## Peak In-Flight Request Bytes (AwanFS)",
            "",
            "| Phase | Peak In-Flight (MB) | Peak RSS (MB) |",
            "| :--- | :--- | :--- |",
        ])
        for k, v in inflight_rows:
            lines.append(f"| `{k}` | {v.peak_inflight_mb:.2f} | {format_rss_cell(v)} |")

    if rss_ceiling_mib is not None:
        peak_mb, rss_failures = check_rss_ceiling(awan, rss_ceiling_mib)
        verdict = "NOT MEASURED" if peak_mb is None else ("OVER THE CEILING" if rss_failures else "ok")
        a_cell = "not measured" if peak_mb is None else f"{peak_mb:.1f}"
        m_cell = f"{m_max_rss:.1f}" if m_max_rss > 0 else "not measured"
        lines.extend([
            "",
            "## Peak RSS Ceiling Check",
            "",
            "| Server | Median Peak RSS (MiB) | Ceiling (MiB) | Verdict |",
            "| :--- | :--- | :--- | :--- |",
            f"| AwanFS | {a_cell} | {float(rss_ceiling_mib):.1f} | {verdict} |",
            f"| MinIO | {m_cell} | - | - |",
        ])

    return "\n".join(lines) + "\n"


def start_logged(argv: List[str], log_path: str) -> subprocess.Popen:
    """Starts `argv` with stdout and stderr appended to `log_path`.

    Never a pipe: a pipe nobody drains fills, and the child blocks on its next write. AwanFS
    writes one JSON line per request to stderr (main.rs, AWANFS_LOG), about 230 bytes each, so a
    64 KiB Linux pipe filled after about 271 requests and a 4 KiB Windows pipe after about 16,
    and the server stopped answering while still holding its port. That was #135. The log file
    is kept so a failed run can be read.
    """
    log = open(log_path, "ab")
    try:
        return subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT)
    finally:
        log.close()  # the child holds its own handle


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


PIN_MIN_CPUS = 8  # server gets first half of cores, client gets second half


def can_pin(cpu_count: Optional[int] = None) -> bool:
    n = (os.cpu_count() or 0) if cpu_count is None else cpu_count
    return n >= PIN_MIN_CPUS and (sys.platform == "win32" or hasattr(os, "sched_setaffinity"))


def affinity_masks(cpu_count: Optional[int] = None) -> Tuple[int, int]:
    """Returns `(server_mask, client_mask)` splitting logical cores into two disjoint halves."""
    n = (os.cpu_count() or 0) if cpu_count is None else cpu_count
    usable = max(2, min(n, 64))
    half = usable // 2
    server_mask = (1 << half) - 1
    client_mask = ((1 << (usable - half)) - 1) << half
    return server_mask, client_mask


def set_affinity_mask(pid_or_current: Optional[int], mask: int) -> None:
    """Pins a process to the cores in `mask`; does nothing on machines with fewer than 8 CPUs."""
    if not can_pin():
        return
    if sys.platform == "win32":
        try:
            kernel32 = ctypes.windll.kernel32
            kernel32.GetCurrentProcess.restype = wintypes.HANDLE
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.SetProcessAffinityMask.argtypes = [wintypes.HANDLE, ctypes.c_size_t]
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            if pid_or_current is None:
                handle = kernel32.GetCurrentProcess()
                kernel32.SetProcessAffinityMask(handle, mask)
            else:
                PROCESS_SET_INFORMATION = 0x0200
                handle = kernel32.OpenProcess(
                    PROCESS_SET_INFORMATION, False, pid_or_current
                )
                if handle:
                    kernel32.SetProcessAffinityMask(handle, mask)
                    kernel32.CloseHandle(handle)
        except Exception:
            pass
    else:
        try:
            os.sched_setaffinity(pid_or_current or 0, {i for i in range(64) if mask >> i & 1})
        except Exception:
            pass


def affinity_labels(cpu_count: Optional[int] = None) -> Tuple[str, str]:
    n = (os.cpu_count() or 0) if cpu_count is None else cpu_count
    if can_pin(n):
        usable = min(n, 64)
        half = usable // 2
        srv_mask, cli_mask = affinity_masks(n)
        width = max(2, (usable + 3) // 4)
        return (
            f"Cores 0..{half - 1} (Affinity Mask 0x{srv_mask:0{width}X})",
            f"Cores {half}..{usable - 1} (Affinity Mask 0x{cli_mask:0{width}X})",
        )
    return (f"not pinned ({n} logical cores; pinning needs {PIN_MIN_CPUS})",) * 2


def cpu_model() -> str:
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("model name"):
                        return line.split(":", 1)[1].strip()
        except OSError:
            pass
    return platform.processor() or platform.machine() or "unknown"


def host_ram_gb() -> str:
    """Physical RAM in GB, read from the OS; 'unknown' when it cannot be read."""
    try:
        if sys.platform == "win32":
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
            st = MEMORYSTATUSEX()
            st.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st)):
                return f"{st.ullTotalPhys / 1024 ** 3:.1f}"
        elif hasattr(os, "sysconf") and "SC_PHYS_PAGES" in os.sysconf_names:
            return f"{os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1024 ** 3:.1f}"
    except Exception:
        pass
    return "unknown"


def binary_version(path: str) -> str:
    """First line of `<binary> --version`, to name exactly what was measured."""
    try:
        out = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=30)
        for line in (out.stdout + out.stderr).splitlines():
            if line.strip():
                return line.strip()
    except Exception:
        pass
    return "version unknown"


def anonymous_put_ok(host: str, port: int, bucket: str) -> bool:
    """True when an unsigned PUT works: the harness client sends unsigned requests."""
    try:
        conn = http.client.HTTPConnection(host, port, timeout=10.0)
        conn.request("PUT", f"/{bucket}/__harness_probe", body=b"x", headers={"Content-Length": "1"})
        ok = conn.getresponse().status == 200
        conn.close()
        return ok
    except Exception:
        return False


class BaselineFileS3Handler(http.server.BaseHTTPRequestHandler):
    """Traditional file-per-object + metadata sidecar baseline server (simulates xl.meta style FS I/O)."""
    protocol_version = "HTTP/1.1"
    storage_root = ""

    def log_message(self, format, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > 0:
            self.rfile.read(length)
        if "?uploads" in self.path:
            xml = b'<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><UploadId>base-upload-1</UploadId></InitiateMultipartUploadResult>'
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(xml)))
            self.end_headers()
            self.wfile.write(xml)
        else:
            xml = b'<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><ETag>"00000000000000000000000000000000-4"</ETag></CompleteMultipartUploadResult>'
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(xml)))
            self.end_headers()
            self.wfile.write(xml)

    def do_PUT(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length > 0 else b""
        clean_path = self.path.split("?")[0]
        rel = clean_path.lstrip("/").replace("/", os.sep)
        full_path = os.path.join(self.storage_root, rel)
        if not clean_path.strip("/").count("/"):
            os.makedirs(full_path, exist_ok=True)
        else:
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            if "partNumber=" in self.path:
                m = re.search(r"partNumber=(\d+)", self.path)
                pnum = m.group(1) if m else "1"
                full_path = f"{full_path}.part{pnum}"
            with open(full_path, "wb") as f:
                f.write(body)
            with open(full_path + ".meta", "wb") as mf:
                mf.write(f"size={length}\n".encode())
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.send_header("ETag", '"00000000000000000000000000000000"')
        self.end_headers()

    def do_GET(self):
        clean_path = self.path.split("?")[0]
        rel = clean_path.lstrip("/").replace("/", os.sep)
        full_path = os.path.join(self.storage_root, rel)
        if os.path.isfile(full_path):
            range_hdr = self.headers.get("Range")
            if range_hdr and range_hdr.startswith("bytes="):
                s_str, e_str = range_hdr[6:].split("-", 1)
                start = int(s_str)
                end = int(e_str)
                with open(full_path, "rb") as f:
                    f.seek(start)
                    data = f.read(end - start + 1)
                self.send_response(206)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            with open(full_path, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


PHASE_JOIN_TIMEOUT_SEC = 120.0
# The warm-up is sequential, so it gets its own bound: a phase whose objects are not all
# written is reported, not waited on.
WARMUP_TIMEOUT_SEC = 120.0


def join_all(threads: List[threading.Thread], label: str, timeout_sec: float = PHASE_JOIN_TIMEOUT_SEC) -> int:
    """Waits for `threads` up to `timeout_sec` in total; returns how many are still running.

    A phase worker leaves its loop only between requests, so one stuck request used to hang the
    phase, the job and the whole run with nothing in the log (#135). Bounded here, and the
    threads are daemons, so a stuck one cannot keep the process alive either.
    """
    deadline = time.perf_counter() + timeout_sec
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.perf_counter()))
    stuck = sum(1 for t in threads if t.is_alive())
    if stuck:
        print(f"    {label}: {stuck} of {len(threads)} workers still in a request "
              f"{timeout_sec:.0f}s after the stop; this phase did not finish", flush=True)
    return stuck


def object_key(
    worker_id: int,
    req_count: int,
    pool_keys_per_worker: int,
    unique_keys: bool = False,
) -> str:
    """Object key for a worker request: unique per (worker_id, req_count) when unique_keys is set,
    otherwise cycling through the warm-up pool."""
    if unique_keys:
        return f"worker_{worker_id}_uniq_{req_count}.dat"
    key_idx = req_count % pool_keys_per_worker
    return f"worker_{worker_id}_obj_{key_idx}.dat"


def run_workload_phase(
    host: str,
    port: int,
    bucket: str,
    payload_size: int,
    concurrency: int,
    duration_sec: float,
    mode: str,  # "put", "get", or "range_get_4k"
    server_pid: Optional[int] = None,
    warmup_timeout_sec: float = WARMUP_TIMEOUT_SEC,
    unique_keys: bool = False,
) -> BenchmarkMetrics:
    use_unique = unique_keys and mode == "put"
    payload = os.urandom(payload_size)
    phase_label = f"{mode} {payload_size}B c={concurrency}"
    stop_event = threading.Event()
    latencies: List[float] = []
    lat_lock = threading.Lock()
    total_bytes = 0
    bytes_lock = threading.Lock()
    # A response that is not a success is an error, not an operation: counting it as an op
    # made a run in which every request answered 404 read as a fast run (#127, runs
    # 43504508701 and 43504519493: 532 ops/s at 0.00 MiB/s).
    errors = 0
    error_statuses: Dict[int, int] = {}

    # The warm-up writes every key the workers will read: concurrency x keys PUTs, one at a
    # time. Each connection has a 10 s socket timeout and a failure used to be swallowed and the
    # loop continued, so a server that had stopped answering turned this into
    # concurrency x keys x 10 s of silence: 85 minutes at 64 workers and 8 keys. Bounded here,
    # and what happened is reported, because the phase cannot be measured without it (#135).
    pool_keys_per_worker = 4 if payload_size >= 1024 * 1024 else 8
    want = concurrency * pool_keys_per_worker
    warm_start = time.perf_counter()
    warm_deadline = warm_start + warmup_timeout_sec
    warm_ok, warm_fail, last_error = 0, 0, ""
    warm_sock_timeout = min(10.0, max(1.0, warmup_timeout_sec / 4.0))
    for w in range(concurrency):
        conn = http.client.HTTPConnection(host, port, timeout=warm_sock_timeout)
        for k in range(pool_keys_per_worker):
            if time.perf_counter() > warm_deadline:
                break
            path = f"/{bucket}/worker_{w}_obj_{k}.dat"
            try:
                conn.request("PUT", path, body=payload, headers={"Content-Length": str(payload_size), "Connection": "keep-alive"})
                resp = conn.getresponse()
                resp.read()
                if resp.status == 200:
                    warm_ok += 1
                else:
                    warm_fail += 1
                    last_error = f"HTTP {resp.status}"
            except Exception as e:
                warm_fail += 1
                last_error = f"{type(e).__name__}: {e}"
                conn.close()
                conn = http.client.HTTPConnection(host, port, timeout=warm_sock_timeout)
        conn.close()
        if time.perf_counter() > warm_deadline:
            break
    warm_took = time.perf_counter() - warm_start
    warm_short = warm_ok + warm_fail < want
    print(f"    {phase_label}: warm-up wrote {warm_ok} of {want} objects in {warm_took:.1f}s"
          + (f", {warm_fail} failed, last: {last_error}" if warm_fail else "")
          + (f", gave up after {warmup_timeout_sec:.0f}s" if warm_short else ""), flush=True)

    def worker(worker_id: int):
        nonlocal total_bytes, errors
        local_lats: List[float] = []
        local_bytes = 0
        local_errors: Dict[int, int] = {}
        req_count = 0
        conn = http.client.HTTPConnection(host, port, timeout=10.0)
        put_headers = {"Content-Length": str(payload_size), "Connection": "keep-alive"}
        get_headers = {"Connection": "keep-alive"}
        range_headers = {"Connection": "keep-alive", "Range": "bytes=0-4095"}

        while not stop_event.is_set():
            key = object_key(worker_id, req_count, pool_keys_per_worker, use_unique)
            req_count += 1
            path = f"/{bucket}/{key}"
            t0 = time.perf_counter()

            try:
                if mode == "put":
                    conn.request("PUT", path, body=payload, headers=put_headers)
                    resp = conn.getresponse()
                    resp.read()
                    ok = resp.status == 200
                    if ok:
                        local_bytes += payload_size
                elif mode == "range_get_4k":
                    conn.request("GET", path, headers=range_headers)
                    resp = conn.getresponse()
                    data = resp.read()
                    ok = resp.status in (200, 206)
                    if ok:
                        local_bytes += len(data)
                elif mode == "mixed":
                    if req_count % 5 == 0:
                        conn.request("PUT", path, body=payload, headers=put_headers)
                        resp = conn.getresponse()
                        resp.read()
                        ok = resp.status == 200
                        if ok:
                            local_bytes += payload_size
                    else:
                        conn.request("GET", path, headers=get_headers)
                        resp = conn.getresponse()
                        data = resp.read()
                        ok = resp.status == 200
                        if ok:
                            local_bytes += len(data)
                else:
                    conn.request("GET", path, headers=get_headers)
                    resp = conn.getresponse()
                    data = resp.read()
                    ok = resp.status == 200
                    if ok:
                        local_bytes += len(data)

                t1 = time.perf_counter()
                if ok:
                    local_lats.append((t1 - t0) * 1000.0)
                else:
                    local_errors[resp.status] = local_errors.get(resp.status, 0) + 1
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = http.client.HTTPConnection(host, port, timeout=10.0)

        try:
            conn.close()
        except Exception:
            pass

        with lat_lock:
            latencies.extend(local_lats)
        with bytes_lock:
            total_bytes += local_bytes
            for st, n in local_errors.items():
                errors += n
                error_statuses[st] = error_statuses.get(st, 0) + n

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    with CpuSampler(server_pid) as cpu_sampler:
        t_start = time.perf_counter()
        for t in threads:
            t.start()

        time.sleep(duration_sec)
        stop_event.set()

        stuck = join_all(threads, phase_label)
        t_total = time.perf_counter() - t_start

    ops = len(latencies)
    ops_per_sec = ops / t_total if t_total > 0 else 0.0
    mib_per_sec = (total_bytes / (1024.0 * 1024.0)) / t_total if t_total > 0 else 0.0
    p50, p90, p99, p999 = calculate_percentiles(latencies)
    peak_rss = get_process_peak_rss_mb(server_pid) if server_pid else 0.0

    return BenchmarkMetrics(
        ops_per_sec=ops_per_sec,
        mib_per_sec=mib_per_sec,
        p50_ms=p50,
        p90_ms=p90,
        p99_ms=p99,
        p999_ms=p999,
        peak_rss_mb=peak_rss,
        server_cpu_s=cpu_sampler.server_cpu_s,
        client_cpu_s=cpu_sampler.client_cpu_s,
        system_cpu_s=cpu_sampler.system_cpu_s,
        server_cpu_pct=cpu_sampler.server_cpu_pct,
        client_cpu_pct=cpu_sampler.client_cpu_pct,
        system_cpu_pct=cpu_sampler.system_cpu_pct,
        measured=not stuck and not warm_short and warm_fail == 0 and errors == 0,
        note=", ".join(
            x for x in (
                f"{errors} requests failed ({', '.join(f'HTTP {k} x{v}' for k, v in sorted(error_statuses.items()))})"
                if errors else "",
                f"{stuck} workers did not finish their request" if stuck else "",
                f"warm-up wrote {warm_ok} of {want} objects" if warm_short or warm_fail else "",
                f"last warm-up error: {last_error}" if last_error else "",
            ) if x
        ),
    )


def run_multipart_upload_duration_phase(
    host: str,
    port: int,
    bucket: str,
    part_size: int,
    parts_per_object: int,
    concurrency: int,
    duration_sec: float,
    server_pid: Optional[int] = None,
) -> BenchmarkMetrics:
    """Runs complete S3 Multipart Upload lifecycles (?uploads -> UploadPart x K -> CompleteMultipartUpload)."""
    part_payload = os.urandom(part_size)
    phase_label = f"multipart {parts_per_object}x{part_size}B c={concurrency}"
    stop_event = threading.Event()
    latencies: List[float] = []
    lat_lock = threading.Lock()
    total_bytes = 0
    bytes_lock = threading.Lock()

    def worker(worker_id: int):
        nonlocal total_bytes
        local_lats: List[float] = []
        local_bytes = 0
        conn = http.client.HTTPConnection(host, port, timeout=15.0)
        obj_path = f"/{bucket}/mpu_worker_{worker_id}.bin"

        while not stop_event.is_set():
            t0 = time.perf_counter()
            try:
                # 1. CreateMultipartUpload
                conn.request("POST", f"{obj_path}?uploads", headers={"Content-Length": "0", "Connection": "keep-alive"})
                resp = conn.getresponse()
                xml_data = resp.read().decode("utf-8", errors="ignore")
                m = re.search(r"<UploadId>([^<]+)</UploadId>", xml_data)
                upload_id = m.group(1) if m else "upload-1"

                # 2. UploadPart 1..K
                etags = []
                for p_num in range(1, parts_per_object + 1):
                    conn.request(
                        "PUT",
                        f"{obj_path}?partNumber={p_num}&uploadId={upload_id}",
                        body=part_payload,
                        headers={"Content-Length": str(part_size), "Connection": "keep-alive"},
                    )
                    p_resp = conn.getresponse()
                    p_resp.read()
                    etag = p_resp.getheader("ETag", '"00000000000000000000000000000000"')
                    etags.append((p_num, etag))

                # 3. CompleteMultipartUpload
                parts_xml = "".join(
                    f"<Part><PartNumber>{p}</PartNumber><ETag>{e}</ETag></Part>"
                    for p, e in etags
                )
                complete_body = f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>".encode("utf-8")
                conn.request(
                    "POST",
                    f"{obj_path}?uploadId={upload_id}",
                    body=complete_body,
                    headers={"Content-Length": str(len(complete_body)), "Connection": "keep-alive"},
                )
                c_resp = conn.getresponse()
                c_resp.read()
                if c_resp.status == 200:
                    local_bytes += part_size * parts_per_object

                t1 = time.perf_counter()
                local_lats.append((t1 - t0) * 1000.0)
            except Exception:
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
        with bytes_lock:
            total_bytes += local_bytes

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(duration_sec)
    stop_event.set()
    stuck = join_all(threads, phase_label)
    t_total = time.perf_counter() - t_start

    ops = len(latencies)
    ops_per_sec = ops / t_total if t_total > 0 else 0.0
    mib_per_sec = (total_bytes / (1024.0 * 1024.0)) / t_total if t_total > 0 else 0.0
    p50, p90, p99, p999 = calculate_percentiles(latencies)
    peak_rss = get_process_peak_rss_mb(server_pid) if server_pid else 0.0

    return BenchmarkMetrics(
        ops_per_sec=ops_per_sec,
        mib_per_sec=mib_per_sec,
        p50_ms=p50,
        p90_ms=p90,
        p99_ms=p99,
        p999_ms=p999,
        peak_rss_mb=peak_rss,
        measured=not stuck,
        note=(f"{stuck} workers did not finish their request" if stuck else ""),
    )


def run_large_multipart_object_phase(
    host: str,
    port: int,
    bucket: str,
    key: str,
    total_size_bytes: int,
    part_size_bytes: int,
    concurrency: int,
    server_pid: Optional[int] = None,
) -> Tuple[BenchmarkMetrics, BenchmarkMetrics]:
    """
    1) Uploads a single 1 GiB S3 object via real S3 Multipart Upload (CreateMultipartUpload ->
       concurrent UploadPart of 16 x 64 MiB parts -> CompleteMultipartUpload with composite ETag).
    2) Downloads the 1 GiB multipart object via concurrent RFC 9110 Range GETs (Range: bytes=start-end).
    """
    num_parts = total_size_bytes // part_size_bytes
    part_payload = os.urandom(part_size_bytes)
    phase_label = f"large multipart {total_size_bytes}B parts {part_size_bytes}B c={concurrency}"
    obj_path = f"/{bucket}/{key}"

    # --- Phase A: Multipart Upload ---
    t_up_start = time.perf_counter()
    conn = http.client.HTTPConnection(host, port, timeout=30.0)
    conn.request("POST", f"{obj_path}?uploads", headers={"Content-Length": "0"})
    resp = conn.getresponse()
    xml_data = resp.read().decode("utf-8", errors="ignore")
    conn.close()
    m = re.search(r"<UploadId>([^<]+)</UploadId>", xml_data)
    upload_id = m.group(1) if m else "upload-1"

    next_p = 1
    p_lock = threading.Lock()
    up_lats: List[float] = []
    etags: Dict[int, str] = {}
    up_bytes = 0

    def upload_worker():
        nonlocal next_p, up_bytes
        w_conn = http.client.HTTPConnection(host, port, timeout=30.0)
        while True:
            with p_lock:
                if next_p > num_parts:
                    break
                p_num = next_p
                next_p += 1
            t0 = time.perf_counter()
            w_conn.request(
                "PUT",
                f"{obj_path}?partNumber={p_num}&uploadId={upload_id}",
                body=part_payload,
                headers={"Content-Length": str(part_size_bytes), "Connection": "keep-alive"},
            )
            r = w_conn.getresponse()
            r.read()
            etag = r.getheader("ETag", '"00000000000000000000000000000000"')
            t1 = time.perf_counter()
            with p_lock:
                etags[p_num] = etag
                up_lats.append((t1 - t0) * 1000.0)
                if r.status == 200:
                    up_bytes += part_size_bytes
        w_conn.close()

    threads = [threading.Thread(target=upload_worker, daemon=True) for _ in range(concurrency)]
    for t in threads:
        t.start()
    stuck = join_all(threads, phase_label)

    parts_xml = "".join(
        f"<Part><PartNumber>{p}</PartNumber><ETag>{etags[p]}</ETag></Part>"
        for p in sorted(etags.keys())
    )
    complete_body = f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>".encode("utf-8")
    conn = http.client.HTTPConnection(host, port, timeout=30.0)
    conn.request(
        "POST",
        f"{obj_path}?uploadId={upload_id}",
        body=complete_body,
        headers={"Content-Length": str(len(complete_body))},
    )
    conn.getresponse().read()
    conn.close()
    t_up_total = time.perf_counter() - t_up_start

    up_p50, up_p90, up_p99, up_p999 = calculate_percentiles(up_lats)
    up_rss = get_process_peak_rss_mb(server_pid) if server_pid else 0.0
    put_metrics = BenchmarkMetrics(
        ops_per_sec=len(up_lats) / t_up_total if t_up_total > 0 else 0.0,
        mib_per_sec=(up_bytes / (1024.0 * 1024.0)) / t_up_total if t_up_total > 0 else 0.0,
        p50_ms=up_p50,
        p90_ms=up_p90,
        p99_ms=up_p99,
        p999_ms=up_p999,
        peak_rss_mb=up_rss,
        measured=not stuck,
        note=(f"{stuck} workers did not finish their request" if stuck else ""),
    )

    # --- Phase B: Parallel RFC 9110 Range GET over the 1 GiB Multipart Object ---
    next_r = 0
    down_lats: List[float] = []
    down_bytes = 0

    def range_get_worker():
        nonlocal next_r, down_bytes
        w_conn = http.client.HTTPConnection(host, port, timeout=30.0)
        while True:
            with p_lock:
                if next_r >= num_parts:
                    break
                r_idx = next_r
                next_r += 1
            start_b = r_idx * part_size_bytes
            end_b = start_b + part_size_bytes - 1
            t0 = time.perf_counter()
            w_conn.request(
                "GET",
                obj_path,
                headers={"Range": f"bytes={start_b}-{end_b}", "Connection": "keep-alive"},
            )
            r = w_conn.getresponse()
            data = r.read()
            t1 = time.perf_counter()
            with p_lock:
                down_lats.append((t1 - t0) * 1000.0)
                if r.status in (200, 206):
                    down_bytes += len(data)
        w_conn.close()

    t_down_start = time.perf_counter()
    threads = [threading.Thread(target=range_get_worker, daemon=True) for _ in range(concurrency)]
    for t in threads:
        t.start()
    stuck = join_all(threads, phase_label)
    t_down_total = time.perf_counter() - t_down_start

    d_p50, d_p90, d_p99, d_p999 = calculate_percentiles(down_lats)
    down_rss = get_process_peak_rss_mb(server_pid) if server_pid else 0.0
    get_metrics = BenchmarkMetrics(
        ops_per_sec=len(down_lats) / t_down_total if t_down_total > 0 else 0.0,
        mib_per_sec=(down_bytes / (1024.0 * 1024.0)) / t_down_total if t_down_total > 0 else 0.0,
        p50_ms=d_p50,
        p90_ms=d_p90,
        p99_ms=d_p99,
        p999_ms=d_p999,
        peak_rss_mb=down_rss,
        measured=not stuck,
        note=(f"{stuck} workers did not finish their request" if stuck else ""),
    )
    return put_metrics, get_metrics


def run_fixed_volume_phase(
    host: str,
    port: int,
    bucket: str,
    total_volume_bytes: int,
    part_size_bytes: int,
    concurrency: int,
    mode: str,  # "put" or "get"
    server_pid: Optional[int] = None,
    key_modulus: int = 8,
) -> BenchmarkMetrics:
    """Streams an exact total volume (e.g. 10 GiB) in 64 MiB chunks across concurrent workers."""
    payload = os.urandom(part_size_bytes)
    phase_label = f"fixed volume {total_volume_bytes}B parts {part_size_bytes}B c={concurrency}"
    total_parts = total_volume_bytes // part_size_bytes
    next_part_idx = 0
    idx_lock = threading.Lock()
    latencies: List[float] = []
    lat_lock = threading.Lock()
    total_bytes = 0
    bytes_lock = threading.Lock()

    def worker():
        nonlocal next_part_idx, total_bytes
        local_lats: List[float] = []
        local_bytes = 0
        conn = http.client.HTTPConnection(host, port, timeout=30.0)
        put_headers = {"Content-Length": str(part_size_bytes), "Connection": "keep-alive"}
        get_headers = {"Connection": "keep-alive", "Range": f"bytes=0-{part_size_bytes - 1}"}

        while True:
            with idx_lock:
                my_idx = next_part_idx
                if my_idx >= total_parts:
                    break
                next_part_idx += 1

            slot = my_idx % key_modulus
            path = f"/{bucket}/large_part_{slot:04d}.dat"
            t0 = time.perf_counter()

            try:
                if mode == "put":
                    conn.request("PUT", path, body=payload, headers=put_headers)
                    resp = conn.getresponse()
                    resp.read()
                    if resp.status == 200:
                        local_bytes += part_size_bytes
                else:
                    conn.request("GET", path, headers=get_headers)
                    resp = conn.getresponse()
                    data = resp.read()
                    if resp.status in (200, 206):
                        local_bytes += len(data)
                t1 = time.perf_counter()
                local_lats.append((t1 - t0) * 1000.0)
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = http.client.HTTPConnection(host, port, timeout=30.0)

        try:
            conn.close()
        except Exception:
            pass

        with lat_lock:
            latencies.extend(local_lats)
        with bytes_lock:
            total_bytes += local_bytes

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    stuck = join_all(threads, phase_label)
    t_total = time.perf_counter() - t_start

    ops = len(latencies)
    ops_per_sec = ops / t_total if t_total > 0 else 0.0
    mib_per_sec = (total_bytes / (1024.0 * 1024.0)) / t_total if t_total > 0 else 0.0
    p50, p90, p99, p999 = calculate_percentiles(latencies)
    peak_rss = get_process_peak_rss_mb(server_pid) if server_pid else 0.0

    return BenchmarkMetrics(
        ops_per_sec=ops_per_sec,
        mib_per_sec=mib_per_sec,
        p50_ms=p50,
        p90_ms=p90,
        p99_ms=p99,
        p999_ms=p999,
        peak_rss_mb=peak_rss,
        measured=not stuck,
        note=(f"{stuck} workers did not finish their request" if stuck else ""),
    )


def find_minio_binaries() -> Tuple[Optional[str], Optional[str]]:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    candidates_minio = [
        os.path.join(repo_root, ".tmp", "minio", "minio.exe"),
        os.path.join(repo_root, ".tmp", "minio", "minio"),
        shutil.which("minio.exe"),
        shutil.which("minio"),
    ]
    candidates_mc = [
        os.path.join(repo_root, ".tmp", "minio", "mc.exe"),
        os.path.join(repo_root, ".tmp", "minio", "mc"),
        shutil.which("mc.exe"),
        shutil.which("mc"),
    ]
    minio_bin = next((p for p in candidates_minio if p and os.path.isfile(p)), None)
    mc_bin = next((p for p in candidates_mc if p and os.path.isfile(p)), None)
    return minio_bin, mc_bin


def find_rustfs_binary() -> Optional[str]:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    candidates = [
        os.path.join(repo_root, ".tmp", "rustfs", "rustfs.exe"),
        os.path.join(repo_root, ".tmp", "rustfs", "rustfs"),
        shutil.which("rustfs.exe"),
        shutil.which("rustfs"),
    ]
    return next((p for p in candidates if p and os.path.isfile(p)), None)


def wait_for_port(host: str, port: int, attempts: int = 50, delay_s: float = 0.1) -> None:
    for _ in range(attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.2)
                s.connect((host, port))
                return
        except Exception:
            time.sleep(delay_s)


def run_server_suite(
    label: str,
    host: str,
    port: int,
    bucket: str,
    server_pid: Optional[int],
    sizes: str,
    concurrent: int,
    dur_sec: float,
    mpu_1m_spec_limit: bool = False,
    fallback_mpu_1g: bool = False,
    record_inflight: bool = False,
    large_key: str = "large_1gib.dat",
) -> Tuple[Dict[str, BenchmarkMetrics], Dict[int, BenchmarkMetrics]]:
    """Runs the standard Warp matrix against one live server instance."""
    results: Dict[str, BenchmarkMetrics] = {}
    conc_results: Dict[int, BenchmarkMetrics] = {}
    sweep_dur = 1.0 if fallback_mpu_1g else 1.5

    def record(key: str, m: BenchmarkMetrics) -> None:
        if record_inflight:
            m.peak_inflight_mb = get_awanfs_peak_inflight_mb(host, port)
        results[key] = m

    print(f"  [1/10] {label}: Running 64 KiB PUT phase ({dur_sec}s, {concurrent} workers)...")
    record("put_64k", run_workload_phase(host, port, bucket, 64 * 1024, concurrent, dur_sec, "put", server_pid))

    print(f"  [2/10] {label}: Running 64 KiB GET phase ({dur_sec}s, {concurrent} workers)...")
    record("get_64k", run_workload_phase(host, port, bucket, 64 * 1024, concurrent, dur_sec, "get", server_pid))

    print(f"  [3/10] {label}: Running 64 KiB Range GET (bytes=0-4095, 206 Partial Content) phase ({dur_sec}s, {concurrent} workers)...")
    record("range_get_4k", run_workload_phase(host, port, bucket, 64 * 1024, concurrent, dur_sec, "range_get_4k", server_pid))

    print(f"  [4/10] {label}: Running 1 MiB PUT phase ({dur_sec}s, {min(concurrent, 32)} workers)...")
    record("put_1m", run_workload_phase(host, port, bucket, 1024 * 1024, min(concurrent, 32), dur_sec, "put", server_pid))

    print(f"  [5/10] {label}: Running 1 MiB GET phase ({dur_sec}s, {min(concurrent, 32)} workers)...")
    record("get_1m", run_workload_phase(host, port, bucket, 1024 * 1024, min(concurrent, 32), dur_sec, "get", server_pid))

    if mpu_1m_spec_limit:
        print(f"  [6/10] {label}: 1 MiB Multipart Upload (4 x 256 KiB parts) -> EntityTooSmall (S3 >=5MiB spec constraint)...")
        rss = get_process_peak_rss_mb(server_pid) if server_pid else 0.0
        record("mpu_1m", BenchmarkMetrics(0, 0, 0, 0, 0, 0, rss, measured=True))
    else:
        print(f"  [6/10] {label}: Running 1 MiB S3 Multipart Upload (4 x 256 KiB parts + CompleteMultipartUpload) phase ({dur_sec}s, 16 workers)...")
        record("mpu_1m", run_multipart_upload_duration_phase(host, port, bucket, 256 * 1024, 4, 16, dur_sec, server_pid))

    print(f"  {label}: Running 64 KiB Mixed (80% GET / 20% PUT) phase ({dur_sec}s, {concurrent} workers)...")
    record("mixed_64k", run_workload_phase(host, port, bucket, 64 * 1024, concurrent, dur_sec, "mixed", server_pid))

    print(f"  {label}: Running 1 MiB Mixed (80% GET / 20% PUT) phase ({dur_sec}s, {min(concurrent, 32)} workers)...")
    record("mixed_1m", run_workload_phase(host, port, bucket, 1024 * 1024, min(concurrent, 32), dur_sec, "mixed", server_pid))

    print(f"  {label}: Running Concurrency Scaling Sweep (64 KiB Mixed 80/20, C in [8, 32, 64, 128])...")
    for c in [8, 32, 64, 128]:
        conc_results[c] = run_workload_phase(host, port, bucket, 64 * 1024, c, sweep_dur, "mixed", server_pid)

    if "1GiB" in sizes or "10GiB" in sizes:
        part_sz = 64 * 1024 * 1024
        if fallback_mpu_1g:
            record("mpu_put_1g", run_fixed_volume_phase(host, port, bucket, 1024 * 1024 * 1024, part_sz, 8, "put", server_pid, key_modulus=4))
            record("mpu_get_1g", run_fixed_volume_phase(host, port, bucket, 1024 * 1024 * 1024, part_sz, 8, "get", server_pid, key_modulus=4))
        else:
            print(f"  [7/10] & [8/10] {label}: Running 1 GiB Multipart Upload (16 x 64 MiB parts) & Range GET (8 workers)...")
            mpu_put_1g, mpu_get_1g = run_large_multipart_object_phase(
                host, port, bucket, large_key, 1024 * 1024 * 1024, part_sz, 8, server_pid
            )
            record("mpu_put_1g", mpu_put_1g)
            record("mpu_get_1g", mpu_get_1g)

        print(f"  [9/10] {label}: Running 10 GiB Volume PUT phase (160 x 64 MiB chunks, 8 workers)...")
        record("put_10g", run_fixed_volume_phase(host, port, bucket, 10 * 1024 * 1024 * 1024, part_sz, 8, "put", server_pid, key_modulus=4))

        print(f"  [10/10] {label}: Running 10 GiB Volume Parallel Range GET phase (160 x 64 MiB chunks, 8 workers)...")
        record("get_10g", run_fixed_volume_phase(host, port, bucket, 10 * 1024 * 1024 * 1024, part_sz, 8, "get", server_pid, key_modulus=4))

    return results, conc_results


def run_awanfs_round(
    server_bin: str,
    args,
    dur_sec: float,
    srv_mask: Optional[int] = None,
) -> Tuple[Dict[str, BenchmarkMetrics], Dict[int, BenchmarkMetrics]]:
    if srv_mask is None:
        srv_mask, _ = affinity_masks()
    host = "127.0.0.1"
    bucket = "warp-bench-bucket"
    s3_data_dir = tempfile.mkdtemp(prefix="s3_server_bench_")
    port = get_free_port()
    server_log = os.path.join(tempfile.gettempdir(), f"awanfs_bench_{port}.log")
    server_proc = start_logged(
        [server_bin, "--port", str(port), "--data-dir", s3_data_dir, "--allow-anonymous"],
        server_log,
    )
    print(f"AwanFS log: {server_log}", flush=True)
    set_affinity_mask(server_proc.pid, srv_mask)
    wait_for_port(host, port)
    time.sleep(0.5)
    try:
        conn = http.client.HTTPConnection(host, port, timeout=5.0)
        conn.request("PUT", f"/{bucket}")
        conn.getresponse().read()
        conn.close()
        print(f"Executing HTTP/1.1 keep-alive Warp benchmark against S3 Storage Server (--release) on {host}:{port}...")
        return run_server_suite(
            "AwanFS",
            host,
            port,
            bucket,
            server_proc.pid,
            args.sizes,
            args.concurrent,
            dur_sec,
            record_inflight=True,
            large_key="large_1gib_model.safetensors",
        )
    finally:
        server_proc.terminate()
        server_proc.wait()
        shutil.rmtree(s3_data_dir, ignore_errors=True)
        time.sleep(2.5)


def run_minio_or_fallback_round(
    minio_bin: Optional[str],
    mc_bin: Optional[str],
    args,
    dur_sec: float,
    srv_mask: Optional[int] = None,
) -> Tuple[Dict[str, BenchmarkMetrics], Dict[int, BenchmarkMetrics], str, str]:
    if srv_mask is None:
        srv_mask, _ = affinity_masks()
    host = "127.0.0.1"
    bucket = "warp-bench-bucket"
    if minio_bin and mc_bin:
        baseline_server_label = "MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z)"
        print(f"Executing identical workload against real MinIO ({minio_bin})...")
        minio_data_dir = tempfile.mkdtemp(prefix="minio_warp_data_")
        minio_port = get_free_port()
        minio_console_port = get_free_port()
        minio_env = os.environ.copy()
        minio_env["MINIO_ROOT_USER"] = "minioadmin"
        minio_env["MINIO_ROOT_PASSWORD"] = "minioadmin"
        minio_env["MINIO_UPDATE"] = "off"
        minio_env["MINIO_BROWSER"] = "off"
        minio_proc = subprocess.Popen(
            [minio_bin, "server", minio_data_dir, "--address", f":{minio_port}", "--console-address", f":{minio_console_port}"],
            env=minio_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        set_affinity_mask(minio_proc.pid, srv_mask)
        wait_for_port(host, minio_port)
        time.sleep(0.5)
        try:
            alias_name = f"minio_bench_{minio_port}"
            subprocess.run([mc_bin, "alias", "set", alias_name, f"http://127.0.0.1:{minio_port}", "minioadmin", "minioadmin"], capture_output=True)
            subprocess.run([mc_bin, "mb", f"{alias_name}/{bucket}"], capture_output=True)
            subprocess.run([mc_bin, "anonymous", "set", "public", f"{alias_name}/{bucket}"], capture_output=True)
            minio_status = f"Measured Live ({binary_version(minio_bin)})"
            if not anonymous_put_ok(host, minio_port, bucket):
                minio_status += "; WARNING: anonymous PUT probe failed, so its rows are not valid"
            res, conc = run_server_suite(
                "MinIO",
                host,
                minio_port,
                bucket,
                minio_proc.pid,
                args.sizes,
                args.concurrent,
                dur_sec,
                mpu_1m_spec_limit=True,
                large_key="large_1gib_minio.dat",
            )
            return res, conc, baseline_server_label, minio_status
        finally:
            minio_proc.terminate()
            minio_proc.wait()
            shutil.rmtree(minio_data_dir, ignore_errors=True)
            time.sleep(2.0)

    baseline_server_label = "File-per-Object + `.meta` Sidecar Baseline (Local Measured)"
    baseline_dir = tempfile.mkdtemp(prefix="baseline_s3_fs_")
    BaselineFileS3Handler.storage_root = baseline_dir
    base_port = get_free_port()
    base_httpd = ThreadedHTTPServer(("127.0.0.1", base_port), BaselineFileS3Handler)
    base_thread = threading.Thread(target=base_httpd.serve_forever, daemon=True)
    base_thread.start()
    try:
        print(f"Executing fallback workload against File-per-Object + Sidecar `.meta` Baseline Server on 127.0.0.1:{base_port}...")
        res, conc = run_server_suite(
            "Fallback",
            host,
            base_port,
            bucket,
            os.getpid(),
            args.sizes,
            args.concurrent,
            min(dur_sec, 2.5),
            fallback_mpu_1g=True,
        )
        return res, conc, baseline_server_label, "not measured"
    finally:
        base_httpd.shutdown()
        base_httpd.server_close()
        shutil.rmtree(baseline_dir, ignore_errors=True)


def run_rustfs_round(
    rustfs_bin: str,
    mc_bin: str,
    args,
    dur_sec: float,
    srv_mask: Optional[int] = None,
) -> Tuple[Dict[str, BenchmarkMetrics], Dict[int, BenchmarkMetrics], str]:
    if srv_mask is None:
        srv_mask, _ = affinity_masks()
    host = "127.0.0.1"
    bucket = "warp-bench-bucket"
    print(f"Executing identical workload against real RustFS ({rustfs_bin})...")
    rustfs_data_dir = tempfile.mkdtemp(prefix="rustfs_warp_data_")
    rustfs_port = get_free_port()
    rustfs_env = os.environ.copy()
    rustfs_env["RUSTFS_ACCESS_KEY"] = "rustfsadmin"
    rustfs_env["RUSTFS_SECRET_KEY"] = "rustfsadmin"
    rustfs_proc = subprocess.Popen(
        [rustfs_bin, "server", "--address", f"127.0.0.1:{rustfs_port}", "--access-key", "rustfsadmin", "--secret-key", "rustfsadmin", rustfs_data_dir],
        env=rustfs_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    set_affinity_mask(rustfs_proc.pid, srv_mask)
    wait_for_port(host, rustfs_port)
    time.sleep(0.8)
    try:
        alias_name = f"rustfs_bench_{rustfs_port}"
        subprocess.run([mc_bin, "alias", "set", alias_name, f"http://127.0.0.1:{rustfs_port}", "rustfsadmin", "rustfsadmin"], capture_output=True)
        subprocess.run([mc_bin, "mb", f"{alias_name}/{bucket}"], capture_output=True)
        subprocess.run([mc_bin, "anonymous", "set", "public", f"{alias_name}/{bucket}"], capture_output=True)
        rustfs_status = f"Measured Live ({binary_version(rustfs_bin)})"
        if not anonymous_put_ok(host, rustfs_port, bucket):
            rustfs_status += "; WARNING: anonymous PUT probe failed, so its rows are not valid"
        res, conc = run_server_suite(
            "RustFS",
            host,
            rustfs_port,
            bucket,
            rustfs_proc.pid,
            args.sizes,
            args.concurrent,
            dur_sec,
            mpu_1m_spec_limit=True,
            large_key="large_1gib_rustfs.dat",
        )
        return res, conc, rustfs_status
    finally:
        rustfs_proc.terminate()
        rustfs_proc.wait()
        shutil.rmtree(rustfs_data_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description="Head-to-head S3 Warp Benchmark")
    parser.add_argument("--sizes", type=str, default="64KiB,1MiB,1GiB,10GiB", help="Payload sizes")
    parser.add_argument("--concurrent", type=int, default=64, help="Client concurrency")
    parser.add_argument("--duration", type=str, default="3s", help="Duration per phase (e.g. 3s or 5s)")
    parser.add_argument("--runs", type=int, default=1, help="Number of rounds per server (reports median and min..max RSS spread)")
    parser.add_argument("--rss-ceiling-mib", type=float, default=None, help="Fail if AwanFS median peak RSS exceeds this ceiling (MiB)")
    parser.add_argument("--output", type=str, default="measurements/s3_warp_minio_vs_awan.md", help="Report path")
    args = parser.parse_args()

    if args.runs < 1:
        parser.error("--runs must be >= 1")

    dur_sec = float(args.duration.rstrip("s"))
    cmd = f"python benchmarks/run_warp_benchmark.py --sizes {args.sizes} --concurrent {args.concurrent} --duration {args.duration} --runs {args.runs}"
    if args.rss_ceiling_mib is not None:
        cmd += f" --rss-ceiling-mib {args.rss_ceiling_mib}"
    cmd += f" --output {args.output}"

    # Pin benchmark client to second half of cores, server to first half (when cpu_count >= 8)
    srv_mask, cli_mask = affinity_masks()
    set_affinity_mask(None, cli_mask)

    manifest = os.path.abspath(
        (os.path.join(os.path.dirname(__file__), "..", "server", "Cargo.toml") if os.path.exists(os.path.join(os.path.dirname(__file__), "..", "server", "Cargo.toml")) else os.path.join(os.path.dirname(__file__), "..", "..", "awan-storage", "server", "Cargo.toml"))
    )
    print("Building s3-server (--release)...")
    subprocess.run(
        ["cargo", "build", "--release", "--bin", "s3-server", "--manifest-path", manifest],
        check=True,
    )

    server_bin = os.path.abspath(
        (os.path.join(os.path.dirname(__file__), "..", "server", "target", "release", "s3-server.exe" if sys.platform == "win32" else "s3-server") if os.path.exists(os.path.join(os.path.dirname(__file__), "..", "server", "Cargo.toml")) else os.path.join(os.path.dirname(__file__), "..", "..", "awan-storage", "server", "target", "release", "s3-server.exe" if sys.platform == "win32" else "s3-server"))
    )

    minio_bin, mc_bin = find_minio_binaries()
    rustfs_bin = find_rustfs_binary()

    awan_rounds: List[Dict[str, BenchmarkMetrics]] = []
    baseline_rounds: List[Dict[str, BenchmarkMetrics]] = []
    rustfs_rounds: List[Dict[str, BenchmarkMetrics]] = []
    awan_conc_rounds: List[Dict[int, BenchmarkMetrics]] = []
    minio_conc_rounds: List[Dict[int, BenchmarkMetrics]] = []
    rustfs_conc_rounds: List[Dict[int, BenchmarkMetrics]] = []

    baseline_server_label = "MinIO Community Server (Official Go binary: minio.exe RELEASE.2025-09-07T16-13-09Z)"
    minio_status = "not measured"
    rustfs_status = "not measured"

    for r_idx in range(args.runs):
        print(f"=== Benchmark round {r_idx + 1}/{args.runs} ===", flush=True)
        order = ("awanfs", "minio") if r_idx % 2 == 0 else ("minio", "awanfs")
        for target in order:
            if target == "awanfs":
                res, conc = run_awanfs_round(server_bin, args, dur_sec, srv_mask=srv_mask)
                awan_rounds.append(res)
                awan_conc_rounds.append(conc)
            else:
                res, conc, baseline_server_label, minio_status = run_minio_or_fallback_round(
                    minio_bin, mc_bin, args, dur_sec, srv_mask=srv_mask
                )
                baseline_rounds.append(res)
                minio_conc_rounds.append(conc)
        if rustfs_bin and mc_bin:
            res, conc, rustfs_status = run_rustfs_round(rustfs_bin, mc_bin, args, dur_sec, srv_mask=srv_mask)
            rustfs_rounds.append(res)
            rustfs_conc_rounds.append(conc)

    awan_results = aggregate_rounds(awan_rounds)
    baseline_results = aggregate_rounds(baseline_rounds)
    rustfs_results = aggregate_rounds(rustfs_rounds)
    concurrency_data: Dict[str, Dict[int, BenchmarkMetrics]] = {
        "s3-server": aggregate_rounds(awan_conc_rounds),
        "minio": aggregate_rounds(minio_conc_rounds),
        "rustfs": aggregate_rounds(rustfs_conc_rounds),
    }

    pin_server, pin_client = affinity_labels()
    hw_info = {
        "cpu": cpu_model(),
        "cores": str(os.cpu_count() or "unknown"),
        "ram_gb": host_ram_gb(),
        "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
        "affinity_server": pin_server,
        "affinity_client": pin_client,
        "minio_status": minio_status,
        "rustfs_status": rustfs_status,
    }

    report = format_markdown_report(
        cmd,
        hw_info,
        awan_results,
        rustfs_results,
        baseline_results,
        baseline_server_label=baseline_server_label,
        concurrency_data=concurrency_data,
        runs=args.runs,
        rss_ceiling_mib=args.rss_ceiling_mib,
    )
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"Benchmark finished successfully. Report generated at {args.output}")
    _, rss_failures = check_rss_ceiling(awan_results, args.rss_ceiling_mib)
    if rss_failures:
        for msg in rss_failures:
            print(f"FAIL: {msg}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
