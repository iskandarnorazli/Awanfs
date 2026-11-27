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
import types
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
    peak_rss_file_mb: Optional[float] = None
    peak_rss_anon_mb: Optional[float] = None
    peak_win_private_mb: Optional[float] = None
    peak_win_wset_mb: Optional[float] = None


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


def parse_proc_status(text: str) -> Dict[str, float]:
    """Parses Linux /proc/<pid>/status memory fields (VmHWM, VmRSS, RssAnon, RssFile, RssShmem) into MiB."""
    out: Dict[str, float] = {}
    for line in text.splitlines():
        name, _, rest = line.partition(":")
        if name in ("VmHWM", "VmRSS", "RssAnon", "RssFile", "RssShmem"):
            fields = rest.split()
            if len(fields) == 2 and fields[1] == "kB" and fields[0].isdigit():
                out[name] = int(fields[0]) / 1024.0
    return out


def parse_windows_memory_info(info) -> Dict[str, float]:
    """Extracts Windows `private`, `wset`, and `peak_wset` in MiB from a psutil memory_full_info() object."""
    out: Dict[str, float] = {}
    for attr in ("private", "wset", "peak_wset"):
        val = getattr(info, attr, None)
        if isinstance(val, (int, float)) and not isinstance(val, bool) and val >= 0:
            out[attr] = float(val) / (1024.0 * 1024.0)
    return out


def _win32_process_memory_counters_ex(pid: int):
    """Queries Win32 PROCESS_MEMORY_COUNTERS_EX for PrivateUsage, WorkingSetSize, and PeakWorkingSetSize."""
    class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
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
            ("PrivateUsage", ctypes.c_size_t),
        ]

    PROCESS_QUERY_INFORMATION = 0x0400
    PROCESS_VM_READ = 0x0010
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    STILL_ACTIVE = 259
    kernel32 = ctypes.windll.kernel32
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    handle = kernel32.OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, False, pid)
    if not handle:
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return None
    try:
        exit_code = wintypes.DWORD()
        if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)) and exit_code.value != STILL_ACTIVE:
            return None
        counters = PROCESS_MEMORY_COUNTERS_EX()
        counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS_EX)
        psapi = ctypes.windll.psapi
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
        if psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
            return types.SimpleNamespace(
                private=counters.PrivateUsage,
                wset=counters.WorkingSetSize,
                peak_wset=counters.PeakWorkingSetSize,
            )
    finally:
        kernel32.CloseHandle(handle)
    return None


def sample_windows_memory(pid: int) -> Dict[str, float]:
    """Reads Windows `private` and `wset` (in MiB) via psutil memory_full_info(), falling back to Win32 API."""
    try:
        import psutil  # type: ignore
    except ImportError:
        psutil = None
    if psutil is not None:
        try:
            info = psutil.Process(pid).memory_full_info()
            got = parse_windows_memory_info(info)
            if "private" in got and "wset" in got:
                return got
        except (AttributeError, NotImplementedError):
            pass
        except Exception:
            return {}
    if sys.platform == "win32":
        info = _win32_process_memory_counters_ex(pid)
        if info is not None:
            return parse_windows_memory_info(info)
    return {}


class PeakRssSampler:
    """Samples peak memory split while a benchmark phase runs:
    - Linux: RssFile and RssAnon (plus VmHWM/VmRSS) from /proc/<pid>/status.
    - Windows: private and wset from psutil memory_full_info() (or Win32 PROCESS_MEMORY_COUNTERS_EX).
    Fields stay None when unavailable (missing/exited process or unsupported platform)."""

    def __init__(self, pid: Optional[int], every: float = 0.05, status_path: Optional[str] = None):
        self.pid = pid
        self.every = every
        self._default_path = f"/proc/{pid}/status" if pid is not None else None
        self._explicit_path = status_path is not None
        self.path = status_path if status_path is not None else self._default_path
        self.peak_rss_mb: Optional[float] = None
        self.rss_file_mb: Optional[float] = None
        self.rss_anon_mb: Optional[float] = None
        self.win_private_mb: Optional[float] = None
        self.win_wset_mb: Optional[float] = None
        self._stop: Optional[threading.Event] = None
        self._thread: Optional[threading.Thread] = None

    def sample(self) -> None:
        if self.pid is None and not self._explicit_path:
            return
        use_proc = self._explicit_path or (self.path != self._default_path) or sys.platform != "win32"
        if use_proc:
            if not self.path:
                return
            try:
                with open(self.path, encoding="ascii") as fh:
                    got = parse_proc_status(fh.read())
            except OSError:
                return
            if "RssFile" in got:
                self.rss_file_mb = max(self.rss_file_mb or 0.0, got["RssFile"])
            if "RssAnon" in got:
                self.rss_anon_mb = max(self.rss_anon_mb or 0.0, got["RssAnon"])
            for k in ("VmHWM", "VmRSS"):
                if k in got:
                    self.peak_rss_mb = max(self.peak_rss_mb or 0.0, got[k])
            return
        if self.pid is not None and sys.platform == "win32":
            got = sample_windows_memory(self.pid)
            if "private" in got:
                self.win_private_mb = max(self.win_private_mb or 0.0, got["private"])
            if "wset" in got:
                self.win_wset_mb = max(self.win_wset_mb or 0.0, got["wset"])
            for k in ("peak_wset", "wset"):
                if k in got:
                    self.peak_rss_mb = max(self.peak_rss_mb or 0.0, got[k])

    def __enter__(self):
        self.sample()
        if self.pid is None and not self._explicit_path:
            return self
        self._stop = threading.Event()

        def loop():
            while not self._stop.wait(self.every):
                self.sample()

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        if self._stop is not None:
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self.sample()
        return False


def instant_memory_split(pid: Optional[int]) -> PeakRssSampler:
    """Takes a single instantaneous memory-split sample for `pid`."""
    sampler = PeakRssSampler(pid)
    sampler.sample()
    return sampler


def _rss_split_fields(server_pid: Optional[int], sampler: PeakRssSampler) -> dict:
    """Returns BenchmarkMetrics keyword fields for total peak RSS and platform memory split."""
    peak_rss = max(get_process_peak_rss_mb(server_pid) if server_pid else 0.0, sampler.peak_rss_mb or 0.0)
    return {
        "peak_rss_mb": peak_rss,
        "peak_rss_file_mb": sampler.rss_file_mb,
        "peak_rss_anon_mb": sampler.rss_anon_mb,
        "peak_win_private_mb": sampler.win_private_mb,
        "peak_win_wset_mb": sampler.win_wset_mb,
    }


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

    def med(vals: List[Optional[float]]) -> Optional[float]:
        vals = [v for v in vals if v is not None]
        return statistics.median(vals) if vals else None

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
            # #274's memory split, as medians like the other columns.
            **{f: med([getattr(x, f) for x in pool]) for f in ("peak_rss_file_mb", "peak_rss_anon_mb", "peak_win_private_mb", "peak_win_wset_mb")},
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


def _detect_memory_split_kind(hw_info: Dict[str, str], *metric_dicts: Dict[str, BenchmarkMetrics]) -> str:
    for d in metric_dicts:
        for m in d.values():
            if m.peak_rss_file_mb is not None or m.peak_rss_anon_mb is not None:
                return "linux"
            if m.peak_win_private_mb is not None or m.peak_win_wset_mb is not None:
                return "windows"
    os_str = str(hw_info.get("os", "")).lower()
    if "windows" in os_str:
        return "windows"
    if "linux" in os_str:
        return "linux"
    return "windows" if sys.platform == "win32" else "linux"


def _format_split_cells(m: BenchmarkMetrics, split_kind: str, bold: bool = False) -> Tuple[str, str]:
    if split_kind == "windows":
        vals = (m.peak_win_private_mb, m.peak_win_wset_mb)
    else:
        vals = (m.peak_rss_file_mb, m.peak_rss_anon_mb)

    def fmt(v: Optional[float]) -> str:
        if v is None:
            return "not measured"
        return f"**{v:.1f} MB**" if bold else f"{v:.1f} MB"

    return fmt(vals[0]), fmt(vals[1])


def _format_summary_split(metrics_dict: Dict[str, BenchmarkMetrics], split_kind: str) -> str:
    def fmt(v: Optional[float]) -> str:
        return "not measured" if v is None else f"{v:.1f} MB"

    if split_kind == "windows":
        priv = max((v.peak_win_private_mb for v in metrics_dict.values() if v.peak_win_private_mb is not None), default=None)
        wset = max((v.peak_win_wset_mb for v in metrics_dict.values() if v.peak_win_wset_mb is not None), default=None)
        return f"private {fmt(priv)}, wset {fmt(wset)}"
    file_p = max((v.peak_rss_file_mb for v in metrics_dict.values() if v.peak_rss_file_mb is not None), default=None)
    anon_p = max((v.peak_rss_anon_mb for v in metrics_dict.values() if v.peak_rss_anon_mb is not None), default=None)
    return f"RssFile {fmt(file_p)}, RssAnon {fmt(anon_p)}"


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

    split_kind = _detect_memory_split_kind(hw_info, awan, minio_or_baseline, rust_fb)
    if split_kind == "windows":
        split_headers = ("Private Bytes (private, MB)", "Working Set (wset, MB)")
    else:
        split_headers = ("File-Backed (RssFile, MB)", "Anonymous (RssAnon, MB)")

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
        "- **Durability per PUT:** `awanfs` runs with its default, group commit (#29): a 200 means the bytes are on stable storage, and concurrent PUTs share one sync. " + minio_durability_note() + " RustFS (1.0.5, release defaults) is durable on Linux (3.09 sync calls/PUT, measurements/2026-11-21-durability-probe.md; Windows sync count is not probed).",
    ]
    if runs > 1:
        lines.append(f"- **Rounds per Server:** {runs} (values are medians; Peak RSS shows median and min..max spread)")
    lines.extend([
        "",
        "## 2. Live Measured HTTP/1.1 Keep-Alive Throughput & Latency Matrix (64 KiB – 10 GiB)",
        "",
        f"| Workload | Target Engine | Throughput (MiB/s) | Operations/sec | P50 (ms) | P90 (ms) | P99 (ms) | P99.9 (ms) | Peak RSS (MB) | {split_headers[0]} | {split_headers[1]} |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
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
            lines.append(f"| **{label}** | **S3 Storage Server** | did not finish: {a.note} | | | | | | | | |")

        a_s1, a_s2 = _format_split_cells(a, split_kind, bold=True)
        lines.append(
            f"| **{label}** | **S3 Storage Server** (`awanfs` `--release`, Columnar SoA Index) | **{a.mib_per_sec:.1f}** | **{a.ops_per_sec:.1f}** | **{a.p50_ms:.2f}** | **{a.p90_ms:.2f}** | **{a.p99_ms:.2f}** | **{a.p999_ms:.2f}** | **{format_rss_cell(a)}** | {a_s1} | {a_s2} |"
        )
        if m.measured:
            m_s1, m_s2 = _format_split_cells(m, split_kind)
            if m.ops_per_sec == 0.0 and m.mib_per_sec == 0.0:
                lines.append(
                    f"| **{label}** | {baseline_server_label} | N/A (S3 $\\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | {format_rss_cell(m)} | {m_s1} | {m_s2} |"
                )
            else:
                lines.append(
                    f"| **{label}** | {baseline_server_label} | {m.mib_per_sec:.1f} | {m.ops_per_sec:.1f} | {m.p50_ms:.2f} | {m.p90_ms:.2f} | {m.p99_ms:.2f} | {m.p999_ms:.2f} | {format_rss_cell(m)} | {m_s1} | {m_s2} |"
                )
        if r.measured:
            r_s1, r_s2 = _format_split_cells(r, split_kind)
            if r.ops_per_sec == 0.0 and r.mib_per_sec == 0.0:
                lines.append(
                    f"| **{label}** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | N/A (S3 $\\ge$5MiB spec limit) | N/A | N/A | N/A | N/A | N/A | {format_rss_cell(r)} | {r_s1} | {r_s2} |"
                )
            else:
                lines.append(
                    f"| **{label}** | RustFS Community Server (Official Rust binary: rustfs.exe v1.0.5) | {r.mib_per_sec:.1f} | {r.ops_per_sec:.1f} | {r.p50_ms:.2f} | {r.p90_ms:.2f} | {r.p99_ms:.2f} | {r.p999_ms:.2f} | {format_rss_cell(r)} | {r_s1} | {r_s2} |"
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
            if "awanfs" in concurrency_data and c in concurrency_data["awanfs"]:
                m = concurrency_data["awanfs"][c]
                lines.append(
                    f"| **C = {c}** | **S3 Storage Server** (`awanfs` `--release`, Columnar SoA Index) | **{m.mib_per_sec:.1f}** | **{m.ops_per_sec:.1f}** | **{m.p50_ms:.2f}** | **{m.p90_ms:.2f}** | **{m.p99_ms:.2f}** | **{m.p999_ms:.2f}** |"
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

    a_split = _format_summary_split(awan, split_kind)
    m_split = _format_summary_split(minio_or_baseline, split_kind)
    r_split = _format_summary_split(rust_fb, split_kind)

    lines.extend([
        "",
        f"## {section_num}. Open-Source S3 Alternatives Reference Comparison",
        "",
        f"> **Provenance Note (Audit Rule 3):** MinIO ({hw_info.get('minio_status', 'not measured')}) and RustFS ({hw_info.get('rustfs_status', 'not measured')}) ran on this machine ({hw_info.get('os', platform.platform())}) with anonymous access enabled via `mc`, receiving the same persistent HTTP/1.1 keep-alive requests as `awanfs`. Server pinning: {hw_info.get('affinity_server', 'not recorded')}. Peak RSS comes from the OS (`GetProcessMemoryInfo` / `PROCESS_MEMORY_COUNTERS_EX` `private` & `wset` on Windows, `/proc/<pid>/status` `VmHWM`, `RssFile`, `RssAnon` on Linux). Note that for 1 MiB multipart upload with 256 KiB parts, both MinIO and RustFS strictly reject parts smaller than 5 MiB with HTTP 400 `EntityTooSmall` per AWS S3 RFC specification, whereas `awanfs` accepts them, which does not conform to S3. Reference figures for SeaweedFS, Garage and Ceph RGW are not measured here and are left out.",
        "",
        "| Dimension | `awanfs` (Measured Local) | MinIO (Go, Measured Local) | RustFS (Rust, Measured Local) |",
        "| :--- | :--- | :--- | :--- |",
        f"| **64 KiB PUT Throughput** | **{put_64k.mib_per_sec:.1f} MiB/s ({put_64k.ops_per_sec:.0f} ops/s)** | {m_put_64k.mib_per_sec:.1f} MiB/s ({m_put_64k.ops_per_sec:.0f} ops/s) | {r_put_64k.mib_per_sec:.1f} MiB/s ({r_put_64k.ops_per_sec:.0f} ops/s) |",
        f"| **64 KiB GET Throughput** | **{get_64k.mib_per_sec:.1f} MiB/s ({get_64k.ops_per_sec:.0f} ops/s)** | {m_get_64k.mib_per_sec:.1f} MiB/s ({m_get_64k.ops_per_sec:.0f} ops/s) | {r_get_64k.mib_per_sec:.1f} MiB/s ({r_get_64k.ops_per_sec:.0f} ops/s) |",
        f"| **1 MiB PUT Throughput** | **{put_1m.mib_per_sec:.1f} MiB/s ({put_1m.ops_per_sec:.0f} ops/s)** | {m_put_1m.mib_per_sec:.1f} MiB/s ({m_put_1m.ops_per_sec:.0f} ops/s) | {r_put_1m.mib_per_sec:.1f} MiB/s ({r_put_1m.ops_per_sec:.0f} ops/s) |",
        f"| **1 MiB / 10 GiB GET Throughput** | **{get_1m.mib_per_sec:.1f} – {get_10g.mib_per_sec:.1f} MiB/s** | {m_get_1m.mib_per_sec:.1f} – {m_get_10g.mib_per_sec:.1f} MiB/s | {r_get_1m.mib_per_sec:.1f} – {r_get_10g.mib_per_sec:.1f} MiB/s |",
        f"| **Server Peak RSS (64K–10G)** | **{max_rss:.1f} MB ({a_split}; 1 GiB payload cache cap)** | {m_max_rss:.1f} MB ({m_split}; Go GC heap) | {r_max_rss:.1f} MB ({r_split}; Rust jemalloc) |",
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


def build_awanfs_server_argv(
    server_bin: str,
    port: int,
    data_dir: str,
    allow_small_parts: bool = False,
) -> List[str]:
    """Builds CLI arguments for starting `awanfs`; keeps defaults intact unless requested."""
    argv = [server_bin, "--port", str(port), "--data-dir", data_dir, "--allow-anonymous"]
    if allow_small_parts:
        argv.append("--allow-small-parts")
    return argv


def wait_for_port(host: str, port: int, timeout_sec: float = 10.0) -> bool:
    deadline = time.perf_counter() + timeout_sec
    while time.perf_counter() < deadline:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.2)
                s.connect((host, port))
                return True
        except Exception:
            time.sleep(0.05)
    return False


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
# Large 64 MiB parts can pause beyond 30s during Windows FlushFileBuffers + same-key relocation,
# and 10 GiB across 160 parts can take longer than 120s total while making steady progress (#261).
LARGE_PART_REQUEST_TIMEOUT_SEC = 120.0
FIXED_VOLUME_MAX_WAIT_SEC = 1800.0
# The warm-up is sequential, so it gets its own bound: a phase whose objects are not all
# written is reported, not waited on.
WARMUP_TIMEOUT_SEC = 120.0


def join_all(threads: List[threading.Thread], label: str, timeout_sec: Optional[float] = None) -> int:
    """Waits for `threads` up to `timeout_sec` in total; returns how many are still running.

    A phase worker leaves its loop only between requests, so one stuck request used to hang the
    phase, the job and the whole run with nothing in the log (#135). Bounded here, and the
    threads are daemons, so a stuck one cannot keep the process alive either.
    """
    bound = PHASE_JOIN_TIMEOUT_SEC if timeout_sec is None else timeout_sec
    deadline = time.perf_counter() + bound
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.perf_counter()))
    stuck = sum(1 for t in threads if t.is_alive())
    if stuck:
        print(f"    {label}: {stuck} of {len(threads)} workers still in a request "
              f"{bound:.0f}s after the stop; this phase did not finish", flush=True)
    return stuck


def wait_for_workers_with_progress(
    threads: List[threading.Thread],
    completed_fn,
    stop_event: threading.Event,
    stall_timeout_sec: Optional[float] = None,
    max_wait_sec: float = FIXED_VOLUME_MAX_WAIT_SEC,
) -> bool:
    """Waits for fixed-volume workers as long as requests keep completing.

    Returns True when stopped due to no completions for `stall_timeout_sec` or hitting `max_wait_sec`.
    """
    stall_bound = PHASE_JOIN_TIMEOUT_SEC if stall_timeout_sec is None else stall_timeout_sec
    t_start = time.perf_counter()
    last_progress = t_start
    last_completed = completed_fn()
    while any(t.is_alive() for t in threads):
        now = time.perf_counter()
        cur = completed_fn()
        if cur > last_completed:
            last_completed = cur
            last_progress = now
        if now - last_progress >= stall_bound or now - t_start >= max_wait_sec:
            stop_event.set()
            return True
        time.sleep(min(0.1, max(0.01, stall_bound / 10.0)))
    return False


def format_phase_failure_note(
    error_statuses: Dict[int, int],
    exc_counts: Dict[str, int],
    last_error: str,
    stuck: int,
    ok_parts: Optional[int] = None,
    total_parts: Optional[int] = None,
    stalled: bool = False,
) -> str:
    errors = sum(error_statuses.values()) + sum(exc_counts.values())
    detail_parts = [f"HTTP {k} x{v}" for k, v in sorted(error_statuses.items())]
    detail_parts.extend(f"{k} x{v}" for k, v in sorted(exc_counts.items()))
    return ", ".join(
        x for x in (
            f"{errors} requests failed ({', '.join(detail_parts)})" if errors else "",
            f"last error: {last_error}" if last_error else "",
            f"{ok_parts} of {total_parts} parts succeeded" if (total_parts is not None and ok_parts is not None and ok_parts != total_parts) else "",
            f"{stuck} workers did not finish their request" if stuck else "",
            "stalled with no progress" if (stalled and not stuck) else "",
        ) if x
    )


def _new_worker_state(worker_id: int) -> Dict[str, object]:
    return {
        "id": worker_id,
        "completed": 0, "ok": 0, "errors": 0,
        "last_part": None, "last_status": None, "last_dur_s": None,
        "in_flight": None, "in_flight_started": None,
    }


def _record_worker_outcome(
    ws: Dict[str, object],
    item_id: int,
    status_text: str,
    dur_s: float,
    ok: bool,
) -> None:
    ws["completed"] = int(ws["completed"]) + 1
    ws["last_part"] = item_id
    ws["last_status"] = status_text
    ws["last_dur_s"] = dur_s
    ws["in_flight"] = None
    if ok:
        ws["ok"] = int(ws["ok"]) + 1
    else:
        ws["errors"] = int(ws["errors"]) + 1


def _reconnect_http(conn: http.client.HTTPConnection, host: str, port: int, timeout: float) -> http.client.HTTPConnection:
    try:
        conn.close()
    except Exception:
        pass
    return http.client.HTTPConnection(host, port, timeout=timeout)


def print_worker_diagnostics(label: str, worker_states: List[Dict[str, object]]) -> None:
    now = time.perf_counter()
    print(f"    {label}: worker summary:", flush=True)
    for ws in worker_states:
        in_flight = ws["in_flight"]
        started = ws["in_flight_started"]
        in_fl_str = (
            f"part {in_flight} ({now - started:.1f}s)"
            if in_flight is not None and started is not None
            else "idle"
        )
        last_dur = f"{ws['last_dur_s']:.2f}s" if ws["last_dur_s"] is not None else "-"
        print(
            f"      worker {ws['id']}: completed={ws['completed']} ok={ws['ok']} "
            f"err={ws['errors']} last=(part={ws['last_part']}, {ws['last_status']}, {last_dur}) "
            f"in_flight={in_fl_str}",
            flush=True,
        )


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
    with CpuSampler(server_pid) as cpu_sampler, PeakRssSampler(server_pid) as rss_sampler:
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

    return BenchmarkMetrics(
        ops_per_sec=ops_per_sec,
        mib_per_sec=mib_per_sec,
        p50_ms=p50,
        p90_ms=p90,
        p99_ms=p99,
        p999_ms=p999,
        **_rss_split_fields(server_pid, rss_sampler),
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
    errors = 0
    error_statuses: Dict[int, int] = {}

    def worker(worker_id: int):
        nonlocal total_bytes, errors
        local_lats: List[float] = []
        local_bytes = 0
        local_errors: Dict[int, int] = {}
        conn = http.client.HTTPConnection(host, port, timeout=15.0)
        obj_path = f"/{bucket}/mpu_worker_{worker_id}.bin"

        while not stop_event.is_set():
            t0 = time.perf_counter()
            try:
                # 1. CreateMultipartUpload
                conn.request("POST", f"{obj_path}?uploads", headers={"Content-Length": "0", "Connection": "keep-alive"})
                resp = conn.getresponse()
                xml_data = resp.read().decode("utf-8", errors="ignore")
                if resp.status != 200:
                    local_errors[resp.status] = local_errors.get(resp.status, 0) + 1
                    continue
                m = re.search(r"<UploadId>([^<]+)</UploadId>", xml_data)
                upload_id = m.group(1) if m else "upload-1"

                # 2. UploadPart 1..K
                etags = []
                part_failed = False
                for p_num in range(1, parts_per_object + 1):
                    conn.request(
                        "PUT",
                        f"{obj_path}?partNumber={p_num}&uploadId={upload_id}",
                        body=part_payload,
                        headers={"Content-Length": str(part_size), "Connection": "keep-alive"},
                    )
                    p_resp = conn.getresponse()
                    p_resp.read()
                    if p_resp.status != 200:
                        local_errors[p_resp.status] = local_errors.get(p_resp.status, 0) + 1
                        part_failed = True
                        break
                    etag = p_resp.getheader("ETag", '"00000000000000000000000000000000"')
                    etags.append((p_num, etag))
                if part_failed:
                    continue

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
                t1 = time.perf_counter()
                if c_resp.status == 200:
                    local_bytes += part_size * parts_per_object
                    local_lats.append((t1 - t0) * 1000.0)
                else:
                    local_errors[c_resp.status] = local_errors.get(c_resp.status, 0) + 1
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
            for st, n in local_errors.items():
                errors += n
                error_statuses[st] = error_statuses.get(st, 0) + n

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    with PeakRssSampler(server_pid) as rss_sampler:
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

    return BenchmarkMetrics(
        ops_per_sec=ops_per_sec,
        mib_per_sec=mib_per_sec,
        p50_ms=p50,
        p90_ms=p90,
        p99_ms=p99,
        p999_ms=p999,
        **_rss_split_fields(server_pid, rss_sampler),
        measured=not stuck and errors == 0 and ops > 0,
        note=", ".join(
            x for x in (
                f"{errors} requests failed ({', '.join(f'HTTP {k} x{v}' for k, v in sorted(error_statuses.items()))})"
                if errors else "",
                f"{stuck} workers did not finish their request" if stuck else "",
            ) if x
        ),
    )


def run_awanfs_mpu_1m_phase(
    server_bin: str,
    bucket: str,
    dur_sec: float,
    srv_mask: int,
) -> BenchmarkMetrics:
    """Runs `mpu_1m` (4 x 256 KiB parts) on an isolated AwanFS server started with `--allow-small-parts`."""
    mpu_data_dir = tempfile.mkdtemp(prefix="s3_server_bench_mpu_")
    mpu_port = get_free_port()
    mpu_log = os.path.join(tempfile.gettempdir(), f"awanfs_bench_mpu_{mpu_port}.log")
    mpu_proc = start_logged(
        build_awanfs_server_argv(server_bin, mpu_port, mpu_data_dir, allow_small_parts=True),
        mpu_log,
    )
    print(f"AwanFS mpu_1m log (--allow-small-parts): {mpu_log}", flush=True)
    try:
        set_affinity_mask(mpu_proc.pid, srv_mask)
        if not wait_for_port("127.0.0.1", mpu_port):
            return BenchmarkMetrics(0, 0, 0, 0, 0, 0, 0, measured=False, note="awanfs --allow-small-parts did not become ready")
        conn = http.client.HTTPConnection("127.0.0.1", mpu_port, timeout=5.0)
        try:
            conn.request("PUT", f"/{bucket}")
            conn.getresponse().read()
        finally:
            conn.close()
        return run_multipart_upload_duration_phase("127.0.0.1", mpu_port, bucket, 256 * 1024, 4, 16, dur_sec, mpu_proc.pid)
    finally:
        mpu_proc.terminate()
        mpu_proc.wait()
        shutil.rmtree(mpu_data_dir, ignore_errors=True)


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
    req_timeout = LARGE_PART_REQUEST_TIMEOUT_SEC

    # --- Phase A: Multipart Upload ---
    t_up_start = time.perf_counter()
    up_err_statuses: Dict[int, int] = {}
    up_exc_counts: Dict[str, int] = {}
    up_last_err = ""
    upload_id = "upload-1"
    try:
        conn = http.client.HTTPConnection(host, port, timeout=req_timeout)
        conn.request("POST", f"{obj_path}?uploads", headers={"Content-Length": "0"})
        resp = conn.getresponse()
        xml_data = resp.read().decode("utf-8", errors="ignore")
        conn.close()
        if resp.status != 200:
            up_err_statuses[resp.status] = up_err_statuses.get(resp.status, 0) + 1
            up_last_err = f"CreateMultipartUpload HTTP {resp.status}"
        m = re.search(r"<UploadId>([^<]+)</UploadId>", xml_data)
        upload_id = m.group(1) if m else "upload-1"
    except Exception as e:
        up_exc_counts[type(e).__name__] = up_exc_counts.get(type(e).__name__, 0) + 1
        up_last_err = f"CreateMultipartUpload {type(e).__name__}: {e}"

    next_p = 1
    p_lock = threading.Lock()
    up_lats: List[float] = []
    etags: Dict[int, str] = {}
    up_bytes = 0
    up_completed = 0
    up_ok = 0
    up_stop = threading.Event()
    up_worker_states: List[Dict[str, object]] = []

    def upload_worker():
        nonlocal next_p, up_bytes, up_completed, up_ok, up_last_err
        with p_lock:
            ws = _new_worker_state(len(up_worker_states))
            up_worker_states.append(ws)
        w_conn = http.client.HTTPConnection(host, port, timeout=req_timeout)
        while not up_stop.is_set():
            with p_lock:
                if next_p > num_parts:
                    break
                p_num = next_p
                next_p += 1
                ws["in_flight"] = p_num
                ws["in_flight_started"] = time.perf_counter()
            t0 = time.perf_counter()
            try:
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
                ok = r.status == 200
                with p_lock:
                    _record_worker_outcome(ws, p_num, f"HTTP {r.status}", t1 - t0, ok)
                    up_completed += 1
                    if ok:
                        etags[p_num] = etag
                        up_lats.append((t1 - t0) * 1000.0)
                        up_bytes += part_size_bytes
                        up_ok += 1
                    else:
                        up_err_statuses[r.status] = up_err_statuses.get(r.status, 0) + 1
                        up_last_err = f"part {p_num} HTTP {r.status}"
            except Exception as e:
                t1 = time.perf_counter()
                with p_lock:
                    _record_worker_outcome(ws, p_num, f"{type(e).__name__}: {e}", t1 - t0, False)
                    up_completed += 1
                    up_exc_counts[type(e).__name__] = up_exc_counts.get(type(e).__name__, 0) + 1
                    up_last_err = f"part {p_num} {type(e).__name__}: {e}"
                w_conn = _reconnect_http(w_conn, host, port, req_timeout)
        try:
            w_conn.close()
        except Exception:
            pass

    threads = [threading.Thread(target=upload_worker, daemon=True) for _ in range(concurrency)]
    with PeakRssSampler(server_pid) as up_rss_sampler:
        for t in threads:
            t.start()
        up_stalled = wait_for_workers_with_progress(threads, lambda: up_completed, up_stop)
        stuck = join_all(threads, phase_label)

    if len(etags) == num_parts:
        parts_xml = "".join(
            f"<Part><PartNumber>{p}</PartNumber><ETag>{etags[p]}</ETag></Part>"
            for p in sorted(etags.keys())
        )
        complete_body = f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>".encode("utf-8")
        try:
            conn = http.client.HTTPConnection(host, port, timeout=req_timeout)
            conn.request(
                "POST",
                f"{obj_path}?uploadId={upload_id}",
                body=complete_body,
                headers={"Content-Length": str(len(complete_body))},
            )
            c_resp = conn.getresponse()
            c_resp.read()
            conn.close()
            if c_resp.status != 200:
                up_err_statuses[c_resp.status] = up_err_statuses.get(c_resp.status, 0) + 1
                up_last_err = f"CompleteMultipartUpload HTTP {c_resp.status}"
        except Exception as e:
            up_exc_counts[type(e).__name__] = up_exc_counts.get(type(e).__name__, 0) + 1
            up_last_err = f"CompleteMultipartUpload {type(e).__name__}: {e}"
    t_up_total = time.perf_counter() - t_up_start

    up_errors = sum(up_err_statuses.values()) + sum(up_exc_counts.values())
    if up_errors or stuck or up_stalled or up_ok != num_parts:
        print_worker_diagnostics(f"{phase_label} put", up_worker_states)
    up_p50, up_p90, up_p99, up_p999 = calculate_percentiles(up_lats)
    put_metrics = BenchmarkMetrics(
        ops_per_sec=len(up_lats) / t_up_total if t_up_total > 0 else 0.0,
        mib_per_sec=(up_bytes / (1024.0 * 1024.0)) / t_up_total if t_up_total > 0 else 0.0,
        p50_ms=up_p50,
        p90_ms=up_p90,
        p99_ms=up_p99,
        p999_ms=up_p999,
        **_rss_split_fields(server_pid, up_rss_sampler),
        measured=not stuck and not up_stalled and up_errors == 0 and up_ok == num_parts,
        note=format_phase_failure_note(up_err_statuses, up_exc_counts, up_last_err, stuck, up_ok, num_parts, up_stalled),
    )

    # --- Phase B: Parallel RFC 9110 Range GET over the 1 GiB Multipart Object ---
    next_r = 0
    down_lats: List[float] = []
    down_bytes = 0
    down_completed = 0
    down_ok = 0
    down_err_statuses: Dict[int, int] = {}
    down_exc_counts: Dict[str, int] = {}
    down_last_err = ""
    down_stop = threading.Event()
    down_worker_states: List[Dict[str, object]] = []

    def range_get_worker():
        nonlocal next_r, down_bytes, down_completed, down_ok, down_last_err
        with p_lock:
            ws = _new_worker_state(len(down_worker_states))
            down_worker_states.append(ws)
        w_conn = http.client.HTTPConnection(host, port, timeout=req_timeout)
        while not down_stop.is_set():
            with p_lock:
                if next_r >= num_parts:
                    break
                r_idx = next_r
                next_r += 1
                ws["in_flight"] = r_idx
                ws["in_flight_started"] = time.perf_counter()
            start_b = r_idx * part_size_bytes
            end_b = start_b + part_size_bytes - 1
            t0 = time.perf_counter()
            try:
                w_conn.request(
                    "GET",
                    obj_path,
                    headers={"Range": f"bytes={start_b}-{end_b}", "Connection": "keep-alive"},
                )
                r = w_conn.getresponse()
                data = r.read()
                t1 = time.perf_counter()
                ok = r.status in (200, 206)
                with p_lock:
                    _record_worker_outcome(ws, r_idx, f"HTTP {r.status}", t1 - t0, ok)
                    down_completed += 1
                    if ok:
                        down_lats.append((t1 - t0) * 1000.0)
                        down_bytes += len(data)
                        down_ok += 1
                    else:
                        down_err_statuses[r.status] = down_err_statuses.get(r.status, 0) + 1
                        down_last_err = f"range {r_idx} HTTP {r.status}"
            except Exception as e:
                t1 = time.perf_counter()
                with p_lock:
                    _record_worker_outcome(ws, r_idx, f"{type(e).__name__}: {e}", t1 - t0, False)
                    down_completed += 1
                    down_exc_counts[type(e).__name__] = down_exc_counts.get(type(e).__name__, 0) + 1
                    down_last_err = f"range {r_idx} {type(e).__name__}: {e}"
                w_conn = _reconnect_http(w_conn, host, port, req_timeout)
        try:
            w_conn.close()
        except Exception:
            pass

    with PeakRssSampler(server_pid) as down_rss_sampler:
        t_down_start = time.perf_counter()
        threads = [threading.Thread(target=range_get_worker, daemon=True) for _ in range(concurrency)]
        for t in threads:
            t.start()
        down_stalled = wait_for_workers_with_progress(threads, lambda: down_completed, down_stop)
        stuck = join_all(threads, phase_label)
        t_down_total = time.perf_counter() - t_down_start

    down_errors = sum(down_err_statuses.values()) + sum(down_exc_counts.values())
    if down_errors or stuck or down_stalled or down_ok != num_parts:
        print_worker_diagnostics(f"{phase_label} get", down_worker_states)
    d_p50, d_p90, d_p99, d_p999 = calculate_percentiles(down_lats)
    get_metrics = BenchmarkMetrics(
        ops_per_sec=len(down_lats) / t_down_total if t_down_total > 0 else 0.0,
        mib_per_sec=(down_bytes / (1024.0 * 1024.0)) / t_down_total if t_down_total > 0 else 0.0,
        p50_ms=d_p50,
        p90_ms=d_p90,
        p99_ms=d_p99,
        p999_ms=d_p999,
        **_rss_split_fields(server_pid, down_rss_sampler),
        measured=not stuck and not down_stalled and down_errors == 0 and down_ok == num_parts,
        note=format_phase_failure_note(down_err_statuses, down_exc_counts, down_last_err, stuck, down_ok, num_parts, down_stalled),
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
    req_timeout = LARGE_PART_REQUEST_TIMEOUT_SEC
    next_part_idx = 0
    idx_lock = threading.Lock()
    latencies: List[float] = []
    lat_lock = threading.Lock()
    total_bytes = 0
    completed_parts = 0
    ok_parts = 0
    error_statuses: Dict[int, int] = {}
    exc_counts: Dict[str, int] = {}
    last_error = ""
    bytes_lock = threading.Lock()
    stop_event = threading.Event()
    worker_states: List[Dict[str, object]] = []

    def worker():
        nonlocal next_part_idx, total_bytes, completed_parts, ok_parts, last_error
        with idx_lock:
            ws = _new_worker_state(len(worker_states))
            worker_states.append(ws)
        local_lats: List[float] = []
        local_bytes = 0
        local_ok = 0
        local_err_statuses: Dict[int, int] = {}
        local_exc_counts: Dict[str, int] = {}
        local_last_err = ""
        conn = http.client.HTTPConnection(host, port, timeout=req_timeout)
        put_headers = {"Content-Length": str(part_size_bytes), "Connection": "keep-alive"}
        get_headers = {"Connection": "keep-alive", "Range": f"bytes=0-{part_size_bytes - 1}"}

        while not stop_event.is_set():
            with idx_lock:
                my_idx = next_part_idx
                if my_idx >= total_parts:
                    break
                next_part_idx += 1
                ws["in_flight"] = my_idx
                ws["in_flight_started"] = time.perf_counter()

            slot = my_idx % key_modulus
            path = f"/{bucket}/large_part_{slot:04d}.dat"
            t0 = time.perf_counter()

            try:
                if mode == "put":
                    conn.request("PUT", path, body=payload, headers=put_headers)
                    resp = conn.getresponse()
                    resp.read()
                    ok = resp.status == 200
                    if ok:
                        local_bytes += part_size_bytes
                else:
                    conn.request("GET", path, headers=get_headers)
                    resp = conn.getresponse()
                    data = resp.read()
                    ok = resp.status in (200, 206)
                    if ok:
                        local_bytes += len(data)
                t1 = time.perf_counter()
                _record_worker_outcome(ws, my_idx, f"HTTP {resp.status}", t1 - t0, ok)
                if ok:
                    local_lats.append((t1 - t0) * 1000.0)
                    local_ok += 1
                else:
                    local_err_statuses[resp.status] = local_err_statuses.get(resp.status, 0) + 1
                    local_last_err = f"part {my_idx} HTTP {resp.status}"
            except Exception as e:
                t1 = time.perf_counter()
                _record_worker_outcome(ws, my_idx, f"{type(e).__name__}: {e}", t1 - t0, False)
                ename = type(e).__name__
                local_exc_counts[ename] = local_exc_counts.get(ename, 0) + 1
                local_last_err = f"part {my_idx} {ename}: {e}"
                conn = _reconnect_http(conn, host, port, req_timeout)
            with bytes_lock:
                completed_parts += 1

        try:
            conn.close()
        except Exception:
            pass

        with lat_lock:
            latencies.extend(local_lats)
        with bytes_lock:
            total_bytes += local_bytes
            ok_parts += local_ok
            for st, n in local_err_statuses.items():
                error_statuses[st] = error_statuses.get(st, 0) + n
            for en, n in local_exc_counts.items():
                exc_counts[en] = exc_counts.get(en, 0) + n
            if local_last_err:
                last_error = local_last_err

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    with PeakRssSampler(server_pid) as rss_sampler:
        t_start = time.perf_counter()
        for t in threads:
            t.start()
        stalled = wait_for_workers_with_progress(threads, lambda: completed_parts, stop_event)
        stuck = join_all(threads, phase_label)
        t_total = time.perf_counter() - t_start

    errors = sum(error_statuses.values()) + sum(exc_counts.values())
    if errors or stuck or stalled or ok_parts != total_parts:
        print_worker_diagnostics(f"{phase_label} {mode}", worker_states)

    ops = len(latencies)
    ops_per_sec = ops / t_total if t_total > 0 else 0.0
    mib_per_sec = (total_bytes / (1024.0 * 1024.0)) / t_total if t_total > 0 else 0.0
    p50, p90, p99, p999 = calculate_percentiles(latencies)

    return BenchmarkMetrics(
        ops_per_sec=ops_per_sec,
        mib_per_sec=mib_per_sec,
        p50_ms=p50,
        p90_ms=p90,
        p99_ms=p99,
        p999_ms=p999,
        **_rss_split_fields(server_pid, rss_sampler),
        measured=not stuck and not stalled and errors == 0 and ok_parts == total_parts,
        note=format_phase_failure_note(error_statuses, exc_counts, last_error, stuck, ok_parts, total_parts, stalled),
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
    skip_mpu_1m: bool = False,
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

    if skip_mpu_1m:
        pass  # measured on its own server by the caller (#111: AwanFS runs it with --allow-small-parts)
    elif mpu_1m_spec_limit:
        print(f"  [6/10] {label}: 1 MiB Multipart Upload (4 x 256 KiB parts) -> EntityTooSmall (S3 >=5MiB spec constraint)...")
        record("mpu_1m", BenchmarkMetrics(0, 0, 0, 0, 0, 0, **_rss_split_fields(server_pid, instant_memory_split(server_pid)), measured=True))
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

    # 1 GiB and 10 GiB are separate requests (#57): bench-full asks for 1GiB without 10GiB.
    size_set = {x.strip() for x in sizes.split(",") if x.strip()}
    part_sz = 64 * 1024 * 1024
    if "1GiB" in size_set:
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

    if "10GiB" in size_set:
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
        build_awanfs_server_argv(server_bin, port, s3_data_dir),
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
        results, conc = run_server_suite(
            "AwanFS",
            host,
            port,
            bucket,
            server_proc.pid,
            args.sizes,
            args.concurrent,
            dur_sec,
            skip_mpu_1m=True,
            record_inflight=True,
            large_key="large_1gib_model.safetensors",
        )
    finally:
        server_proc.terminate()
        server_proc.wait()
        shutil.rmtree(s3_data_dir, ignore_errors=True)
        time.sleep(2.5)
    # The server above keeps the 5 MiB part minimum; mpu_1m (4 x 256 KiB parts) runs on its own
    # server started with --allow-small-parts (#111, owner decision 2026-11-19, option A).
    print(f"  [6/10] AwanFS: Running 1 MiB S3 Multipart Upload (4 x 256 KiB parts + CompleteMultipartUpload, --allow-small-parts) phase ({dur_sec}s, 16 workers)...")
    results["mpu_1m"] = run_awanfs_mpu_1m_phase(server_bin, bucket, dur_sec, srv_mask)
    time.sleep(1.5)
    return results, conc


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
    print("Building awanfs (--release)...")
    subprocess.run(
        ["cargo", "build", "--release", "--bin", "awanfs", "--manifest-path", manifest],
        check=True,
    )

    server_bin = os.path.abspath(
        (os.path.join(os.path.dirname(__file__), "..", "server", "target", "release", "awanfs.exe" if sys.platform == "win32" else "awanfs") if os.path.exists(os.path.join(os.path.dirname(__file__), "..", "server", "Cargo.toml")) else os.path.join(os.path.dirname(__file__), "..", "..", "awan-storage", "server", "target", "release", "awanfs.exe" if sys.platform == "win32" else "awanfs"))
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
        "awanfs": aggregate_rounds(awan_conc_rounds),
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
