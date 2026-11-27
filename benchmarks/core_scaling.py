"""CPU core scaling benchmark: PUT and GET throughput against cores, AwanFS vs MinIO vs RustFS vs Garage vs SeaweedFS.

    python benchmarks/core_scaling.py [--cores 1,2,4] [--runs 2] [--duration 3] [--output core_scaling.md]

Measures how 4 KiB and 64 KiB PUT and GET ops/s, P50/P99 latency, server CPU %, and ops/s per
busy core scale with N pinned server cores (1, 2, 4, ...). Writes a markdown report starting with
its command and SeaStore-style 4-panel PNG and SVG charts next to the report.
"""
import argparse
import ctypes
import hashlib
import hmac
import http.client
import http.server
import json
import math
import os
import platform
import random
import re
import secrets
import select
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
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
SERVERS: Tuple[str, ...] = ("AwanFS", "MinIO", "RustFS", "Garage", "SeaweedFS")
PATTERNS: Tuple[str, ...] = ("seq-write", "rand-write", "seq-read", "rand-read")

DEFAULT_SIZES = "4KiB,1MiB"
DEFAULT_CONCURRENCIES = "1,4,16,64,256"
DEFAULT_SEED = 42
# AwanFS runs at its own default memory budget, like every other server (#423, owner 2026-11-26).
# A budget given with --memory-budget is passed through, and then the prefill must exceed its cache.
DEFAULT_MEMORY_BUDGET = ""
DEFAULT_PREFILL_SIZE = "48MiB"
DURABILITIES: Tuple[str, ...] = ("group", "fast", "strict")
DEFAULT_PROFILE_THREAD_CORES: Tuple[int, ...] = (4, 8)

MINIO_RELEASE = "RELEASE.2025-09-07T16-13-09Z"
MC_RELEASE = "RELEASE.2025-08-13T08-35-41Z"
MINIO_ASSETS: Dict[Tuple[str, str], Dict[str, Tuple[str, str, str]]] = {
    ("win32", "x86_64"): {
        "minio": (
            "minio.exe",
            f"https://github.com/minio/minio/releases/download/{MINIO_RELEASE}/minio.windows-amd64.{MINIO_RELEASE}.exe",
            "af709e6ba68488404e85acdd22a3030d0f5e56a108d4b27d744f18ceb50861b4",
        ),
        "mc": (
            "mc.exe",
            f"https://github.com/minio/mc/releases/download/{MC_RELEASE}/mc.windows-amd64.{MC_RELEASE}.exe",
            "c8db13ebeda31497f354c0e950809db0ae9b2a2a69b8afee68c128c37300c157",
        ),
    },
    ("linux", "x86_64"): {
        "minio": (
            "minio",
            f"https://github.com/minio/minio/releases/download/{MINIO_RELEASE}/minio.linux-amd64.{MINIO_RELEASE}",
            "7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f",
        ),
        "mc": (
            "mc",
            f"https://github.com/minio/mc/releases/download/{MC_RELEASE}/mc.linux-amd64.{MC_RELEASE}",
            "01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891",
        ),
    },
}
RAND_READ_LARGE_RANGE_BYTES = 4 * KIB

# (key, display label, mode, payload_size)
WORKLOADS: List[Tuple[str, str, str, int]] = [
    ("put_4k", "PUT 4 KiB", "put", 4 * KIB),
    ("put_64k", "PUT 64 KiB", "put", 64 * KIB),
    ("get_4k", "GET 4 KiB", "get", 4 * KIB),
    ("get_64k", "GET 64 KiB", "get", 64 * KIB),
    # Up to 1 MiB (owner, 2026-11-26). In the report tables; the charts keep the first four.
    ("put_1m", "PUT 1 MiB", "put", 1024 * KIB),
    ("get_1m", "GET 1 MiB", "get", 1024 * KIB),
]

CHART_WORKLOADS: List[Tuple[str, str, str, int]] = WORKLOADS[:4]

# AwanFS `--durability fast` runs every PUT workload (owner, 2026-11-26).
PUT_WORKLOADS: List[Tuple[str, str, str, int]] = [w for w in WORKLOADS if w[2] == "put"]

SERVER_STYLES: Dict[str, Dict[str, object]] = {
    "AwanFS": {"linestyle": "-", "marker": "o", "color": "#1f77b4", "hatch": ""},
    "MinIO": {"linestyle": "--", "marker": "s", "color": "#d62728", "hatch": "//"},
    "RustFS": {"linestyle": "-.", "marker": "^", "color": "#2ca02c", "hatch": ".."},
    "Garage": {"linestyle": ":", "marker": "D", "color": "#9467bd", "hatch": "xx"},
    "SeaweedFS": {"linestyle": (0, (3, 1, 1, 1)), "marker": "v", "color": "#ff7f0e", "hatch": "\\\\"},
}

CORE_STYLES: Tuple[Dict[str, str], ...] = (
    {"linestyle": "-", "marker": "o", "color": "#1f77b4"},
    {"linestyle": "--", "marker": "s", "color": "#ff7f0e"},
    {"linestyle": "-.", "marker": "^", "color": "#2ca02c"},
    {"linestyle": ":", "marker": "D", "color": "#d62728"},
    {"linestyle": "-", "marker": "v", "color": "#9467bd"},
    {"linestyle": "--", "marker": "P", "color": "#8c564b"},
)


@dataclass(frozen=True)
class ThreadCpuSample:
    tid: int
    name: str
    user_s: float
    system_s: float

    @property
    def total_s(self) -> float:
        return self.user_s + self.system_s


@dataclass(frozen=True)
class PhaseWork:
    """What AwanFS's own counters (`/_awanfs/metrics`) say one phase did (#431)."""
    syncs_per_sec: float
    puts_per_sync: Optional[float]
    mib_per_sync: Optional[float]
    disk_mib_per_sec: float
    peak_inflight_mib: float


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
    thread_cpu: Optional[Tuple[ThreadCpuSample, ...]] = None
    wall_sec: Optional[float] = None
    client_cpu_pct: Optional[float] = None
    client_cores_given: Optional[int] = None
    client_procs: Optional[int] = None
    concurrency: Optional[int] = None
    work: Optional[PhaseWork] = None

    @property
    def measured(self) -> bool:
        return self.ops_per_sec is not None


@dataclass(frozen=True)
class WorkerResult:
    worker_index: int
    concurrency: int
    ops: int
    total_bytes: int
    latencies_ms: Tuple[float, ...]
    errors: int = 0
    error_notes: Tuple[Tuple[str, int], ...] = ()
    stuck: int = 0
    client_user_s: float = 0.0
    client_sys_s: float = 0.0
    wall_sec: float = 0.0


@dataclass(frozen=True)
class CeilingProbeRow:
    workload: str
    size_bytes: int
    concurrency: int
    client_procs: int
    client_cores: int
    result: PointResult


@dataclass(frozen=True)
class RequestSpec:
    method: str
    key: str
    range_start: Optional[int] = None
    range_end: Optional[int] = None


@dataclass(frozen=True)
class S3Auth:
    """Credentials for S3 servers that require AWS SigV4 (e.g. Garage)."""
    access_key: str
    secret_key: str
    region: str = "garage"
    service: str = "s3"


class SigV4Signer:
    """Thread-safe AWS SigV4 header signer using UNSIGNED-PAYLOAD and cached daily keys."""

    def __init__(self, auth: S3Auth):
        self.auth = auth
        self._lock = threading.Lock()
        self._cached_sec: int = -1
        self._cached_amz_date: str = ""
        self._cached_date_stamp: str = ""
        self._key_date: str = ""
        self._signing_key: bytes = b""

    @staticmethod
    def _hmac(key: bytes, msg: bytes) -> bytes:
        return hmac.new(key, msg, hashlib.sha256).digest()

    def _date_and_key(self, utc_ts: Optional[float] = None) -> Tuple[str, str, bytes]:
        sec = int(time.time() if utc_ts is None else utc_ts)
        with self._lock:
            if sec != self._cached_sec:
                tm = time.gmtime(sec)
                self._cached_date_stamp = time.strftime("%Y%m%d", tm)
                self._cached_amz_date = time.strftime("%Y%m%dT%H%M%SZ", tm)
                self._cached_sec = sec
            date_stamp = self._cached_date_stamp
            amz_date = self._cached_amz_date
            if date_stamp != self._key_date:
                k_date = self._hmac(("AWS4" + self.auth.secret_key).encode("utf-8"), date_stamp.encode("ascii"))
                k_region = self._hmac(k_date, self.auth.region.encode("utf-8"))
                k_service = self._hmac(k_region, self.auth.service.encode("utf-8"))
                self._signing_key = self._hmac(k_service, b"aws4_request")
                self._key_date = date_stamp
            return amz_date, date_stamp, self._signing_key

    def sign_headers(
        self,
        method: str,
        path: str,
        host_header: str,
        extra_headers: Optional[Dict[str, str]] = None,
        utc_ts: Optional[float] = None,
    ) -> Dict[str, str]:
        amz_date, date_stamp, signing_key = self._date_and_key(utc_ts)
        canonical_uri = urllib.parse.quote(path, safe="/-_.~")
        canonical_headers = (
            f"host:{host_header}\n"
            f"x-amz-content-sha256:UNSIGNED-PAYLOAD\n"
            f"x-amz-date:{amz_date}\n"
        )
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_request = (
            f"{method}\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD"
        )
        req_hash = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        scope = f"{date_stamp}/{self.auth.region}/{self.auth.service}/aws4_request"
        string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{req_hash}"
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        headers: Dict[str, str] = dict(extra_headers) if extra_headers else {}
        headers["Host"] = host_header
        headers["x-amz-date"] = amz_date
        headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
        headers["Authorization"] = (
            f"AWS4-HMAC-SHA256 Credential={self.auth.access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        return headers


def server_durability_rows(os_name: Optional[str] = None) -> List[Tuple[str, str, str, str]]:
    """Return `(server, documented_default, put_waits_for_sync, notes)` for all five servers."""
    plat = os.name if os_name is None else os_name
    minio_row = (
        (
            "MinIO",
            "fdatasync_unsupported.go (no-op on Windows)",
            "No (for > 4 KiB)",
            "Only files <= 4 KiB open write-through on Windows; 64 KiB and 1 MiB PUTs do not wait for disk sync.",
        )
        if plat == "nt"
        else (
            "MinIO",
            "O_DSYNC / fdatasync",
            "Yes",
            "Syncs each PUT (O_DSYNC / fdatasync) before returning 200 OK.",
        )
    )
    return [
        (
            "AwanFS",
            "--durability=group (group commit, #29)",
            "Yes",
            "Concurrent PUTs share one fsync before 200 OK.",
        ),
        minio_row,
        (
            "RustFS",
            "release defaults (durable)",
            "Yes",
            "Measured on Linux (measurements/2026-11-21-durability-probe.md, #334): 3.09 sync calls/PUT (618 syncs / 200 PUTs).",
        ),
        (
            "Garage",
            "metadata_fsync = false, data_fsync = false (LMDB)",
            "No",
            "Default single-node config skips fsync on metadata and data blocks (cluster mode relies on replication); client uses AWS SigV4 (UNSIGNED-PAYLOAD).",
        ),
        (
            "SeaweedFS",
            "weed mini (filer.conf fsync unset / false)",
            "No",
            "Default storage rule skips fsync on volume .dat/.idx and filer metadata unless fsync = true is configured.",
        ),
    ]


def _durability_section_lines(os_name: Optional[str] = None) -> List[str]:
    lines = [
        "## Durability Settings",
        "",
        "| Server | Documented Single-Node Default | PUT waits for sync? | Notes |",
        "|---|---|---|---|",
    ]
    for srv, default_cfg, waits_sync, note in server_durability_rows(os_name):
        lines.append(f"| {srv} | {default_cfg} | {waits_sync} | {note} |")
    lines.append("")
    return lines


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


def parse_servers(spec: str) -> List[str]:
    """`--servers`: a subset of SERVERS by name, any case; empty means all of them (#431)."""
    if not spec or not spec.strip():
        return list(SERVERS)
    by_lower = {s.lower(): s for s in SERVERS}
    vals: List[str] = []
    for part in spec.split(","):
        name = part.strip()
        if not name:
            continue
        srv = by_lower.get(name.lower())
        if srv is None:
            raise ValueError(f"unknown server {name!r}; expected one of {', '.join(SERVERS)}")
        if srv not in vals:
            vals.append(srv)
    if not vals:
        raise ValueError("servers list cannot be empty")
    return vals


WORK_COUNTERS: Tuple[str, ...] = (
    "awanfs_disk_syncs_total",
    "awanfs_disk_write_bytes_total",
    "awanfs_disk_writes_total",
    "awanfs_peak_inflight_bytes",
)


def parse_awanfs_metrics(text: str) -> Dict[str, float]:
    """The WORK_COUNTERS values in Prometheus text from `/_awanfs/metrics`."""
    out: Dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2 or parts[0] not in WORK_COUNTERS:
            continue
        try:
            out[parts[0]] = float(parts[1])
        except ValueError:
            continue
    return out


def scrape_awanfs_metrics(host: str, port: int, timeout_sec: float = 5.0) -> Optional[Dict[str, float]]:
    """GET `/_awanfs/metrics` (no authentication); None when the server does not answer."""
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout_sec)
        try:
            conn.request("GET", "/_awanfs/metrics")
            resp = conn.getresponse()
            body = resp.read().decode("utf-8", errors="replace")
            if resp.status != 200:
                return None
            return parse_awanfs_metrics(body)
        finally:
            conn.close()
    except (OSError, http.client.HTTPException):
        return None


def phase_work(
    before: Dict[str, float],
    after: Dict[str, float],
    elapsed_sec: float,
    ops_per_sec: Optional[float],
) -> PhaseWork:
    """Rates from the counters' change over one phase. `elapsed_sec` is the phase's wall time."""
    elapsed = max(elapsed_sec, 1e-9)
    syncs = max(0.0, after.get("awanfs_disk_syncs_total", 0.0) - before.get("awanfs_disk_syncs_total", 0.0))
    wbytes = max(0.0, after.get("awanfs_disk_write_bytes_total", 0.0) - before.get("awanfs_disk_write_bytes_total", 0.0))
    syncs_per_sec = syncs / elapsed
    puts_per_sync = (ops_per_sec / syncs_per_sec) if (syncs > 0 and ops_per_sec is not None) else None
    mib_per_sync = (wbytes / syncs / MIB) if syncs > 0 else None
    return PhaseWork(
        syncs_per_sec=syncs_per_sec,
        puts_per_sync=puts_per_sync,
        mib_per_sync=mib_per_sync,
        disk_mib_per_sec=wbytes / elapsed / MIB,
        peak_inflight_mib=after.get("awanfs_peak_inflight_bytes", 0.0) / MIB,
    )


def format_work_cells(w: PhaseWork) -> List[str]:
    """syncs/s, PUTs per sync, MiB per sync, disk MiB/s, peak in-flight MiB."""
    return [
        f"{w.syncs_per_sec:.1f}",
        "no syncs" if w.puts_per_sync is None else f"{w.puts_per_sync:.1f}",
        "no syncs" if w.mib_per_sync is None else f"{w.mib_per_sync:.2f}",
        f"{w.disk_mib_per_sec:.1f}",
        f"{w.peak_inflight_mib:.1f}",
    ]


def _median_work(points: Sequence["PointResult"]) -> Optional[PhaseWork]:
    works = [p.work for p in points if p.work is not None]
    if not works:
        return None

    def med(vals: List[Optional[float]]) -> Optional[float]:
        got = [v for v in vals if v is not None]
        return statistics.median(got) if got else None

    return PhaseWork(
        syncs_per_sec=statistics.median(w.syncs_per_sec for w in works),
        puts_per_sync=med([w.puts_per_sync for w in works]),
        mib_per_sync=med([w.mib_per_sync for w in works]),
        disk_mib_per_sec=statistics.median(w.disk_mib_per_sec for w in works),
        peak_inflight_mib=statistics.median(w.peak_inflight_mib for w in works),
    )


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


def prefill_cache_check(memory_budget: str, prefill_size: str) -> Optional[int]:
    """The stated cache of a budget given by name, checked against the prefill; `None` for the
    server's default budget, whose cache is not known here (#423)."""
    if not memory_budget:
        return None
    cache_bytes = stated_cache_bytes(parse_size(memory_budget))
    validate_prefill_exceeds_cache(parse_size(prefill_size), cache_bytes)
    return cache_bytes


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


def prefill_prefix_for(size: int) -> str:
    """Object key prefix for prefilled read sets below 1 MiB, distinct per size."""
    if size % KIB == 0:
        return f"prefill_{size // KIB}k"
    return f"prefill_{size}b"


def prefill_count_for(prefill_bytes: int, size: int) -> int:
    """Number of `size`-byte objects needed to cover `prefill_bytes`."""
    return max(1, prefill_bytes // max(1, size))


def row_size_label(pattern: str, size: int) -> str:
    """Display size label; marks `rand-read` at >= 1 MiB with the range length it reads."""
    if pattern == "rand-read" and size >= MIB:
        return f"{format_size(size)} ({format_size(RAND_READ_LARGE_RANGE_BYTES)} range)"
    return format_size(size)


def describe_prefill_sets(prefill_bytes: int, patterns: Sequence[str], sizes: Sequence[int]) -> str:
    """Human-readable summary of read sets written during prefill."""
    if not any(p in ("seq-read", "rand-read") for p in patterns):
        return "none (write-only patterns)"
    parts = [
        f"{prefill_count_for(prefill_bytes, sz):,} x {format_size(sz)} objects"
        for sz in sorted({s for s in sizes if s < MIB})
    ]
    if any(s >= MIB for s in sizes):
        parts.append(f"1 x {format_size(prefill_bytes)} large object for range GETs")
    return " + ".join(parts) if parts else "none"


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
            return RequestSpec("GET", f"{prefill_prefix_for(size)}/{idx:06d}")
        chunks = max(1, large_obj_size // size)
        start = (step_idx % chunks) * size
        end = min(large_obj_size - 1, start + size - 1)
        return RequestSpec("GET", "prefill_large/seq_obj.bin", start, end)
    if pattern == "rand-read":
        if size < MIB:
            idx = rng.randrange(max(1, prefill_count))
            return RequestSpec("GET", f"{prefill_prefix_for(size)}/{idx:06d}")
        range_len = RAND_READ_LARGE_RANGE_BYTES
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
        step_offset: int = 0,
        step_stride: int = 1,
    ):
        self.pattern = pattern
        self.size = size
        self.rng = random.Random(seed)
        self.prefill_count = prefill_count
        self.large_obj_size = large_obj_size
        self.rand_write_key_space = rand_write_key_space
        self._idx = int(step_offset)
        self._stride = max(1, int(step_stride))
        self._lock = threading.Lock()

    def next_request(self) -> RequestSpec:
        with self._lock:
            idx = self._idx
            self._idx += self._stride
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


def split_concurrency(concurrency: int, client_procs: int) -> List[int]:
    """Splits `concurrency` evenly across `min(concurrency, client_procs)` active processes."""
    if concurrency < 1:
        raise ValueError(f"concurrency must be >= 1, got {concurrency}")
    if client_procs < 1:
        raise ValueError(f"client_procs must be >= 1, got {client_procs}")
    active = min(concurrency, client_procs)
    base, rem = divmod(concurrency, active)
    return [base + (1 if i < rem else 0) for i in range(active)]


def client_proc_cores(proc_idx: int, active_procs: int, cli_cores: Sequence[int]) -> List[int]:
    """Assigns core affinity slice within `cli_cores` for client process `proc_idx`."""
    cores = [int(c) for c in cli_cores]
    if not cores or active_procs <= 1:
        return cores
    if len(cores) >= active_procs:
        base, rem = divmod(len(cores), active_procs)
        start = proc_idx * base + min(proc_idx, rem)
        end = start + base + (1 if proc_idx < rem else 0)
        return cores[start:end]
    return cores


def pinning_command_cores(
    argv: Sequence[str],
    cores: Sequence[int],
    platform_name: str = sys.platform,
) -> List[str]:
    """Wraps `argv` with `taskset -c` for `cores` on Linux; leaves argv untouched on Windows."""
    core_list = [int(c) for c in cores]
    if platform_name.startswith("linux") and core_list:
        return ["taskset", "-c", ",".join(str(c) for c in core_list), *argv]
    return list(argv)


def should_auto_profile_put(total_cores: int, cores: Sequence[int]) -> bool:
    """True on 16+ core split-client machines (e.g. self-hosted) when 4 or 8 cores are tested."""
    if total_cores < 16 or not cores:
        return False
    if not any(c in DEFAULT_PROFILE_THREAD_CORES for c in cores):
        return False
    return all(c <= total_cores and not client_cores_for(c, total_cores)[1] for c in cores)


def _platform_asset_key(platform_name: str = sys.platform, machine: Optional[str] = None) -> Tuple[str, str]:
    plat = "win32" if platform_name == "win32" else "linux" if platform_name.startswith("linux") else platform_name
    arch_raw = (machine if machine is not None else platform.machine()).lower()
    arch = "x86_64" if arch_raw in ("amd64", "x86_64", "x64") else arch_raw
    return plat, arch


def fetch_verified_binary(url: str, expected_sha256: str, dest_path: str, timeout: float = 120.0) -> str:
    """Downloads `url` to `dest_path` atomically, verifying SHA-256 before committing."""
    os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
    last_err: Optional[Exception] = None
    for attempt in range(2):
        fd, tmp_path = tempfile.mkstemp(prefix=".dl_", dir=os.path.dirname(os.path.abspath(dest_path)))
        os.close(fd)
        try:
            digest = hashlib.sha256()
            with urllib.request.urlopen(url, timeout=timeout) as resp, open(tmp_path, "wb") as out:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    digest.update(chunk)
                    out.write(chunk)
            got = digest.hexdigest().lower()
            if got != expected_sha256.lower():
                raise RuntimeError(f"SHA-256 mismatch for {url}: expected {expected_sha256}, got {got}")
            if sys.platform != "win32":
                os.chmod(tmp_path, 0o755)
            os.replace(tmp_path, dest_path)
            return dest_path
        except RuntimeError:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise
        except Exception as e:
            last_err = e
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            if attempt == 0:
                time.sleep(1.0)
    raise RuntimeError(f"download failed for {url}: {last_err}")


def ensure_minio_binaries(
    minio_bin: Optional[str],
    mc_bin: Optional[str],
    auto_fetch: bool = True,
    dest_dir: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """Returns `(minio_bin, mc_bin)`, fetching pinned GitHub release binaries when missing."""
    if minio_bin and os.path.isfile(minio_bin) and mc_bin and os.path.isfile(mc_bin):
        return minio_bin, mc_bin
    if not auto_fetch:
        return minio_bin, mc_bin

    assets = MINIO_ASSETS.get(_platform_asset_key())
    if not assets:
        return minio_bin, mc_bin

    target_dir = dest_dir or os.path.join(REPO_ROOT, ".tmp", "minio")
    for kind, current in (("minio", minio_bin), ("mc", mc_bin)):
        if current and os.path.isfile(current):
            continue
        fname, url, sha = assets[kind]
        candidate = os.path.join(target_dir, fname)
        if os.path.isfile(candidate):
            if kind == "minio":
                minio_bin = candidate
            else:
                mc_bin = candidate
            continue
        try:
            print(f"Fetching {kind} ({fname}) from pinned GitHub release...", flush=True)
            fetched = fetch_verified_binary(url, sha, candidate)
            if kind == "minio":
                minio_bin = fetched
            else:
                mc_bin = fetched
        except Exception as e:
            print(f"warning: could not fetch {kind} binary: {e}", file=sys.stderr)
    return minio_bin, mc_bin


def get_thread_name(
    pid: int,
    tid: int,
    platform_name: str = sys.platform,
    psutil_thread: Optional[object] = None,
    proc_root: str = "/proc",
) -> str:
    """Returns OS-reported thread name for `(pid, tid)`, or empty string if unavailable."""
    if psutil_thread is not None:
        ps_name = getattr(psutil_thread, "name", None)
        if isinstance(ps_name, str) and ps_name.strip():
            return ps_name.strip()

    if platform_name.startswith("linux"):
        comm_path = os.path.join(proc_root, str(pid), "task", str(tid), "comm")
        try:
            with open(comm_path, "r", encoding="utf-8", errors="replace") as fh:
                return fh.read().strip()
        except OSError:
            return ""

    if platform_name == "win32":
        from ctypes import wintypes

        THREAD_QUERY_LIMITED_INFORMATION = 0x0800
        try:
            k32 = ctypes.WinDLL("kernel32", use_last_error=True)
            k32.OpenThread.restype = wintypes.HANDLE
            k32.OpenThread.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            k32.GetThreadDescription.restype = ctypes.c_long
            k32.GetThreadDescription.argtypes = [wintypes.HANDLE, ctypes.POINTER(ctypes.c_void_p)]
            k32.LocalFree.restype = ctypes.c_void_p
            k32.LocalFree.argtypes = [ctypes.c_void_p]
            k32.CloseHandle.restype = wintypes.BOOL
            k32.CloseHandle.argtypes = [wintypes.HANDLE]
        except Exception:
            return ""

        handle = k32.OpenThread(THREAD_QUERY_LIMITED_INFORMATION, False, int(tid))
        if not handle:
            return ""
        desc_ptr = ctypes.c_void_p()
        try:
            hr = k32.GetThreadDescription(handle, ctypes.byref(desc_ptr))
            if hr >= 0 and desc_ptr.value:
                return ctypes.wstring_at(desc_ptr.value).strip()
            return ""
        except Exception:
            return ""
        finally:
            if desc_ptr.value:
                k32.LocalFree(desc_ptr)
            k32.CloseHandle(handle)

    return ""


def snapshot_process_threads(
    pid: int,
    known_names: Optional[Dict[int, str]] = None,
) -> Dict[int, Tuple[float, float, str]]:
    """Returns `{tid: (user_s, system_s, name)}` for all live threads of `pid`."""
    try:
        import psutil  # type: ignore
        threads = psutil.Process(pid).threads()
    except Exception:
        return {}

    snap: Dict[int, Tuple[float, float, str]] = {}
    for thr in threads:
        tid = int(thr.id)
        name = known_names.get(tid, "") if known_names is not None else ""
        if not name:
            name = get_thread_name(pid, tid, psutil_thread=thr)
            if known_names is not None and name:
                known_names[tid] = name
        snap[tid] = (float(thr.user_time), float(thr.system_time), name)
    return snap


def _collect_thread_samples(
    before: Dict[int, Tuple[float, float, str]],
    last_seen: Dict[int, Tuple[float, float]],
    after: Dict[int, Tuple[float, float, str]],
    names: Dict[int, str],
) -> Tuple[ThreadCpuSample, ...]:
    """Combines start, polled, and end thread snapshots into sorted `ThreadCpuSample` entries."""
    tids = set(before) | set(last_seen) | set(after)
    out: List[ThreadCpuSample] = []
    for tid in tids:
        u0, s0, name0 = before.get(tid, (0.0, 0.0, ""))
        if tid in after:
            u1, s1, name1 = after[tid]
        elif tid in last_seen:
            u1, s1 = last_seen[tid]
            name1 = ""
        else:
            u1, s1, name1 = u0, s0, name0
        name = names.get(tid) or name1 or name0
        out.append(
            ThreadCpuSample(
                tid=tid,
                name=name,
                user_s=max(0.0, u1 - u0),
                system_s=max(0.0, s1 - s0),
            )
        )
    out.sort(key=lambda s: (-s.total_s, s.tid))
    return tuple(out)


class ThreadCpuSampler:
    """Samples per-thread CPU times and OS thread names for `pid` over a timed phase."""

    def __init__(self, pid: int, every: float = 0.25):
        self.pid = pid
        self.every = every
        self._names: Dict[int, str] = {}
        self._before: Optional[Dict[int, Tuple[float, float, str]]] = None
        self._last_seen: Dict[int, Tuple[float, float]] = {}
        self._after: Optional[Dict[int, Tuple[float, float, str]]] = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def record_snapshot(self, snap: Dict[int, Tuple[float, float, str]]) -> None:
        with self._lock:
            if self._before is None:
                self._before = snap
            for tid, (u, s, name) in snap.items():
                self._last_seen[tid] = (u, s)
                if name and not self._names.get(tid):
                    self._names[tid] = name
            self._after = snap

    def poll_once(self) -> None:
        self.record_snapshot(snapshot_process_threads(self.pid, self._names))

    def __enter__(self):
        self.poll_once()

        def poll_loop():
            while not self._stop.wait(self.every):
                self.poll_once()

        self._thread = threading.Thread(target=poll_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self.poll_once()
        return False

    def samples(self) -> Optional[Tuple[ThreadCpuSample, ...]]:
        with self._lock:
            if self._before is None or self._after is None:
                return None
            return _collect_thread_samples(self._before, self._last_seen, self._after, self._names)


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


def sum_processes_cpu_times(pids: Sequence[int]) -> Tuple[float, float]:
    """Sums `(user_s, system_s)` across distinct process IDs."""
    total_user = 0.0
    total_sys = 0.0
    for pid in dict.fromkeys(int(p) for p in pids if p):
        u, s = process_cpu_times_total(pid)
        total_user += u
        total_sys += s
    return total_user, total_sys


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
        cli_cores_vals = [p.client_cores_given for p in run_points if p.client_cores_given is not None]
        cli_procs_vals = [p.client_procs for p in run_points if p.client_procs is not None]
        conc_vals = [p.concurrency for p in run_points if p.concurrency is not None]
        return PointResult(
            reason=reason,
            shares_cores=shares,
            client_cores_given=cli_cores_vals[0] if cli_cores_vals else None,
            client_procs=cli_procs_vals[0] if cli_procs_vals else None,
            concurrency=conc_vals[0] if conc_vals else None,
        )

    ops_med = statistics.median(p.ops_per_sec for p in measured if p.ops_per_sec is not None)
    mib_vals = [p.mib_per_sec for p in measured if p.mib_per_sec is not None]
    mib_med = statistics.median(mib_vals) if mib_vals else None
    p50_med = statistics.median(p.p50_ms for p in measured if p.p50_ms is not None)
    p99_med = statistics.median(p.p99_ms for p in measured if p.p99_ms is not None)
    cpu_vals = [p.cpu_pct for p in measured if p.cpu_pct is not None]
    cpu_med = statistics.median(cpu_vals) if cpu_vals else None
    eff_vals = [p.ops_per_busy_core for p in measured if p.ops_per_busy_core is not None]
    eff_med = statistics.median(eff_vals) if eff_vals else ops_per_busy_core(ops_med, cpu_med)
    cli_cpu_vals = [p.client_cpu_pct for p in measured if p.client_cpu_pct is not None]
    cli_cpu_med = statistics.median(cli_cpu_vals) if cli_cpu_vals else None
    cli_cores_vals = [p.client_cores_given for p in measured if p.client_cores_given is not None]
    cli_procs_vals = [p.client_procs for p in measured if p.client_procs is not None]
    conc_vals = [p.concurrency for p in measured if p.concurrency is not None]

    thread_candidates = [p for p in measured if p.thread_cpu]
    rep = min(
        thread_candidates,
        key=lambda p: abs((p.ops_per_sec or 0.0) - ops_med),
    ) if thread_candidates else None
    wall_vals = [p.wall_sec for p in measured if p.wall_sec is not None]
    wall_rep = rep.wall_sec if rep is not None else (statistics.median(wall_vals) if wall_vals else None)

    return PointResult(
        ops_per_sec=ops_med,
        mib_per_sec=mib_med,
        p50_ms=p50_med,
        p99_ms=p99_med,
        cpu_pct=cpu_med,
        ops_per_busy_core=eff_med,
        shares_cores=any(p.shares_cores for p in measured),
        thread_cpu=rep.thread_cpu if rep is not None else None,
        wall_sec=wall_rep,
        client_cpu_pct=cli_cpu_med,
        client_cores_given=cli_cores_vals[0] if cli_cores_vals else None,
        client_procs=cli_procs_vals[0] if cli_procs_vals else None,
        concurrency=conc_vals[0] if conc_vals else None,
        work=_median_work(measured),
    )


def aggregate_worker_results(
    worker_results: Sequence[WorkerResult],
    wall_sec: float,
    server_cpu_before: Optional[Tuple[float, float]] = None,
    server_cpu_after: Optional[Tuple[float, float]] = None,
    client_cpu_before: Optional[Tuple[float, float]] = None,
    client_cpu_after: Optional[Tuple[float, float]] = None,
    shares_cores: bool = False,
    client_cores_given: Optional[int] = None,
    client_procs: Optional[int] = None,
    concurrency: Optional[int] = None,
    thread_cpu: Optional[Tuple[ThreadCpuSample, ...]] = None,
) -> PointResult:
    """Combines per-process `WorkerResult` records into one `PointResult`."""
    total_ops = sum(w.ops for w in worker_results)
    total_bytes = sum(w.total_bytes for w in worker_results)
    total_errors = sum(w.errors for w in worker_results)
    total_stuck = sum(w.stuck for w in worker_results)
    merged_errors: Dict[str, int] = {}
    for w in worker_results:
        for msg, cnt in w.error_notes:
            merged_errors[msg] = merged_errors.get(msg, 0) + cnt

    procs_count = client_procs if client_procs is not None else len(worker_results)
    total_conc = concurrency if concurrency is not None else sum(w.concurrency for w in worker_results)

    if total_stuck > 0 or total_errors > 0 or total_ops == 0 or wall_sec <= 0:
        reasons = []
        if total_errors > 0:
            summary = ", ".join(f"{k} x{v}" for k, v in sorted(merged_errors.items()))
            reasons.append(f"{total_errors} failed ({summary})")
        if total_stuck > 0:
            reasons.append(f"{total_stuck} stuck workers")
        if total_ops == 0 and not reasons:
            reasons.append("no requests completed")
        return PointResult(
            reason="; ".join(reasons),
            shares_cores=shares_cores,
            client_cores_given=client_cores_given,
            client_procs=procs_count,
            concurrency=total_conc,
        )

    all_lats: List[float] = []
    for w in worker_results:
        all_lats.extend(w.latencies_ms)
    if not all_lats:
        return PointResult(
            reason="no latency samples recorded",
            shares_cores=shares_cores,
            client_cores_given=client_cores_given,
            client_procs=procs_count,
            concurrency=total_conc,
        )

    ops_per_sec = total_ops / wall_sec
    mib_per_sec = (total_bytes / float(MIB)) / wall_sec
    p50, _p90, p99, _p999 = rwb.calculate_percentiles(all_lats)
    srv_pct = (
        cpu_percent(server_cpu_before, server_cpu_after, wall_sec)
        if server_cpu_before is not None and server_cpu_after is not None
        else None
    )
    eff = ops_per_busy_core(ops_per_sec, srv_pct)

    worker_cli_s = sum(max(0.0, w.client_user_s + w.client_sys_s) for w in worker_results)
    parent_cli_pct = (
        cpu_percent(client_cpu_before, client_cpu_after, wall_sec)
        if client_cpu_before is not None and client_cpu_after is not None
        else None
    )
    worker_cli_pct = (worker_cli_s / wall_sec) * 100.0
    cli_pct = parent_cli_pct if (parent_cli_pct is not None and parent_cli_pct > 0.0) else worker_cli_pct

    return PointResult(
        ops_per_sec=ops_per_sec,
        mib_per_sec=mib_per_sec,
        p50_ms=p50,
        p99_ms=p99,
        cpu_pct=srv_pct,
        ops_per_busy_core=eff,
        shares_cores=shares_cores,
        thread_cpu=thread_cpu,
        wall_sec=wall_sec,
        client_cpu_pct=cli_pct,
        client_cores_given=client_cores_given,
        client_procs=procs_count,
        concurrency=total_conc,
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


class _InheritedAffinity:
    """Narrows this process to `cores` for a spawn on Windows, so the child starts pinned and
    anything it sizes from its affinity at startup sees only those cores (#474); restores the
    old affinity afterwards. Without psutil, or elsewhere, it does nothing."""

    def __init__(self, cores: List[int], enabled: bool):
        self.cores = cores
        self.enabled = enabled
        self.saved: Optional[List[int]] = None
        self.me = None

    def __enter__(self):
        if not self.enabled:
            return self
        try:
            import psutil  # type: ignore
            self.me = psutil.Process()
            self.saved = list(self.me.cpu_affinity())
            self.me.cpu_affinity(self.cores)
        except Exception:
            self.saved = None
        return self

    def __exit__(self, *exc):
        if self.saved is not None and self.me is not None:
            try:
                self.me.cpu_affinity(self.saved)
            except Exception:
                pass
        return False


def _spawn_pinned(argv: List[str], n_cores: int, log_path: str, env: Optional[dict] = None) -> subprocess.Popen:
    cmd = pinning_command(argv, n_cores)
    use_taskset = sys.platform.startswith("linux") and cmd[0] == "taskset" and shutil.which("taskset") is not None
    launch_argv = cmd if use_taskset else argv
    cores = list(range(n_cores))
    log = open(log_path, "ab")
    try:
        # Windows has no taskset: the child inherits this process's affinity at creation.
        with _InheritedAffinity(cores, enabled=sys.platform == "win32"):
            proc = subprocess.Popen(launch_argv, env=env, stdout=log, stderr=subprocess.STDOUT)
    finally:
        log.close()
    pin_process(proc.pid, cores)
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
    durability: str = "group",
) -> Tuple[subprocess.Popen, int, str, str]:
    if durability not in DURABILITIES:
        raise ValueError(f"invalid durability {durability!r}; expected one of {', '.join(DURABILITIES)}")
    if not server_bin or not os.path.isfile(server_bin):
        raise RuntimeError("s3-server binary not found")
    data = tempfile.mkdtemp(prefix="scaling_awanfs_", dir=data_root)
    port = rwb.get_free_port()
    log_path = os.path.join(data_root, f"scaling_awanfs_{port}.log")
    argv = [
        server_bin,
        "--port", str(port),
        "--data-dir", data,
        "--allow-anonymous",
        "--durability", durability,
    ]
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
    env.pop("RUSTFS_DURABILITY_MODE", None)
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


def _free_ports(count: int) -> List[int]:
    """Allocate `count` distinct ephemeral localhost ports at once."""
    socks: List[socket.socket] = []
    ports: List[int] = []
    try:
        for _ in range(count):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("127.0.0.1", 0))
            socks.append(s)
            ports.append(s.getsockname()[1])
        return ports
    finally:
        for s in socks:
            s.close()


def find_garage_binary() -> Optional[str]:
    candidates = [
        os.path.join(REPO_ROOT, ".tmp", "garage", "garage.exe"),
        os.path.join(REPO_ROOT, ".tmp", "garage", "garage"),
        shutil.which("garage.exe"),
        shutil.which("garage"),
    ]
    return next((p for p in candidates if p and os.path.isfile(p)), None)


def find_seaweedfs_binary() -> Optional[str]:
    candidates = [
        os.path.join(REPO_ROOT, ".tmp", "seaweedfs", "weed.exe"),
        os.path.join(REPO_ROOT, ".tmp", "seaweedfs", "weed"),
        shutil.which("weed.exe"),
        shutil.which("weed"),
    ]
    return next((p for p in candidates if p and os.path.isfile(p)), None)


def garage_config_toml(
    meta_dir: str,
    data_dir: str,
    s3_port: int,
    rpc_port: int,
    rpc_secret: str,
    region: str = "garage",
) -> str:
    """Minimal single-node Garage config leaving fsync/db defaults untouched."""
    meta_posix = meta_dir.replace("\\", "/")
    data_posix = data_dir.replace("\\", "/")
    return (
        f'metadata_dir = "{meta_posix}"\n'
        f'data_dir = "{data_posix}"\n'
        f"replication_factor = 1\n"
        f'rpc_bind_addr = "127.0.0.1:{rpc_port}"\n'
        f'rpc_public_addr = "127.0.0.1:{rpc_port}"\n'
        f'rpc_secret = "{rpc_secret}"\n\n'
        f"[s3_api]\n"
        f's3_region = "{region}"\n'
        f'api_bind_addr = "127.0.0.1:{s3_port}"\n'
    )


def _signed_put_ok(host: str, port: int, bucket: str, auth: S3Auth) -> bool:
    signer = SigV4Signer(auth)
    path = f"/{bucket}/__harness_probe"
    headers = signer.sign_headers("PUT", path, f"{host}:{port}", {"Content-Length": "1", "Connection": "close"})
    try:
        conn = http.client.HTTPConnection(host, port, timeout=5.0)
        conn.request("PUT", path, body=b"x", headers=headers)
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return resp.status == 200
    except Exception:
        return False


def _start_garage(
    garage_bin: Optional[str],
    n_cores: int,
    data_root: str,
) -> Tuple[subprocess.Popen, int, str, str, S3Auth]:
    if not garage_bin or not os.path.isfile(garage_bin):
        if sys.platform == "win32":
            raise RuntimeError("garage binary not found (upstream releases Linux binaries only)")
        raise RuntimeError("garage binary not found")
    data = tempfile.mkdtemp(prefix="scaling_garage_", dir=data_root)
    meta_dir = os.path.join(data, "meta")
    blocks_dir = os.path.join(data, "data")
    os.makedirs(meta_dir, exist_ok=True)
    os.makedirs(blocks_dir, exist_ok=True)
    s3_port, rpc_port = _free_ports(2)
    log_path = os.path.join(data_root, f"scaling_garage_{s3_port}.log")
    cfg_path = os.path.join(data, "garage.toml")
    rpc_secret = secrets.token_hex(32)
    access_key = "GK" + secrets.token_hex(16)
    secret_key = secrets.token_hex(32)
    with open(cfg_path, "w", encoding="utf-8") as fh:
        fh.write(garage_config_toml(meta_dir, blocks_dir, s3_port, rpc_port, rpc_secret))
    env = dict(
        os.environ,
        GARAGE_DEFAULT_ACCESS_KEY=access_key,
        GARAGE_DEFAULT_SECRET_KEY=secret_key,
        GARAGE_DEFAULT_BUCKET=BUCKET,
    )
    proc = _spawn_pinned(
        [garage_bin, "-c", cfg_path, "server", "--single-node", "--default-bucket"],
        n_cores,
        log_path,
        env=env,
    )
    err = _wait_port(proc, s3_port, timeout=25.0)
    if err:
        _stop_proc(proc)
        tail = _tail_log(log_path)
        shutil.rmtree(data, ignore_errors=True)
        raise RuntimeError(f"garage {err}" + (f": {tail}" if tail else ""))
    auth = S3Auth(access_key=access_key, secret_key=secret_key, region="garage")
    deadline = time.time() + 15.0
    while time.time() < deadline:
        if proc.poll() is not None:
            tail = _tail_log(log_path)
            shutil.rmtree(data, ignore_errors=True)
            raise RuntimeError(f"garage exited with code {proc.returncode}" + (f": {tail}" if tail else ""))
        if _signed_put_ok("127.0.0.1", s3_port, BUCKET, auth):
            return proc, s3_port, data, log_path, auth
        time.sleep(0.2)
    _stop_proc(proc)
    tail = _tail_log(log_path)
    shutil.rmtree(data, ignore_errors=True)
    raise RuntimeError("garage setup failed: signed PUT probe failed" + (f": {tail}" if tail else ""))


def seaweedfs_mini_argv(
    weed_bin: str,
    data_dir: str,
    ports: Sequence[int],
    bucket: str = BUCKET,
) -> List[str]:
    if len(ports) != 10:
        raise ValueError(f"expected 10 ports for weed mini, got {len(ports)}")
    s3_p, s3_g, m_p, m_g, v_p, v_g, f_p, f_g, a_p, a_g = ports
    return [
        weed_bin,
        "mini",
        "-dir", data_dir,
        "-ip", "127.0.0.1",
        "-ip.bind", "127.0.0.1",
        "-master.port", str(m_p), "-master.port.grpc", str(m_g),
        "-volume.port", str(v_p), "-volume.port.grpc", str(v_g),
        "-filer.port", str(f_p), "-filer.port.grpc", str(f_g),
        "-s3.port", str(s3_p), "-s3.port.grpc", str(s3_g),
        "-s3.port.iceberg", "0", "-s3.port.lance", "0",
        "-admin.port", str(a_p), "-admin.port.grpc", str(a_g), "-admin.port.legacy", "0",
        "-webdav=false",
        "-admin.ui=false",
        "-master.telemetry=false",
        "-bucket", bucket,
    ]


def _seaweedfs_env(base_env: Dict[str, str]) -> Dict[str, str]:
    """Strip AWS credential env vars so `weed mini` runs with anonymous S3 access enabled."""
    env = dict(base_env)
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "S3_BUCKET", "S3_TABLE_BUCKET", "S3_VECTOR_BUCKET"):
        env.pop(k, None)
    return env


def _start_seaweedfs(weed_bin: Optional[str], n_cores: int, data_root: str) -> Tuple[subprocess.Popen, int, str, str]:
    if not weed_bin or not os.path.isfile(weed_bin):
        raise RuntimeError("weed binary not found")
    data = tempfile.mkdtemp(prefix="scaling_seaweedfs_", dir=data_root)
    ports = _free_ports(10)
    s3_port = ports[0]
    log_path = os.path.join(data_root, f"scaling_seaweedfs_{s3_port}.log")
    argv = seaweedfs_mini_argv(weed_bin, data, ports, BUCKET)
    proc = _spawn_pinned(argv, n_cores, log_path, env=_seaweedfs_env(os.environ))
    err = _wait_port(proc, s3_port, timeout=25.0)
    if err:
        _stop_proc(proc)
        tail = _tail_log(log_path)
        shutil.rmtree(data, ignore_errors=True)
        raise RuntimeError(f"seaweedfs {err}" + (f": {tail}" if tail else ""))
    deadline = time.time() + 15.0
    while time.time() < deadline:
        if proc.poll() is not None:
            tail = _tail_log(log_path)
            shutil.rmtree(data, ignore_errors=True)
            raise RuntimeError(f"seaweedfs exited with code {proc.returncode}" + (f": {tail}" if tail else ""))
        if rwb.anonymous_put_ok("127.0.0.1", s3_port, BUCKET):
            return proc, s3_port, data, log_path
        time.sleep(0.2)
    _stop_proc(proc)
    tail = _tail_log(log_path)
    shutil.rmtree(data, ignore_errors=True)
    raise RuntimeError("seaweedfs setup failed: anonymous PUT probe failed" + (f": {tail}" if tail else ""))


def _unpack_starter_result(res) -> Tuple[subprocess.Popen, int, str, str, Optional[S3Auth]]:
    if len(res) == 5:
        return res
    proc, port, data_dir, log_path = res
    return proc, port, data_dir, log_path, None


def _make_headers(
    signer: Optional[SigV4Signer],
    method: str,
    path: str,
    host_header: str,
    base_headers: Dict[str, str],
) -> Dict[str, str]:
    if signer is None:
        return base_headers
    return signer.sign_headers(method, path, host_header, base_headers)


def _warmup_pool_objects(
    host: str,
    port: int,
    bucket: str,
    size: int,
    concurrency: int,
    auth: Optional[S3Auth] = None,
) -> Optional[str]:
    """Uploads `concurrency * pool_keys` warm-up objects before timed GET phases."""
    signer = SigV4Signer(auth) if auth is not None else None
    host_hdr = f"{host}:{port}"
    pool_keys_per_worker = 4 if size >= MIB else 8
    payload = deterministic_bytes(size, DEFAULT_SEED)
    conn = http.client.HTTPConnection(host, port, timeout=15.0)
    base_headers = {"Content-Length": str(size), "Connection": "keep-alive"}
    try:
        for w in range(concurrency):
            for k in range(pool_keys_per_worker):
                path = f"/{bucket}/worker_{w}_obj_{k}.dat"
                hdrs = _make_headers(signer, "PUT", path, host_hdr, base_headers)
                conn.request("PUT", path, body=payload, headers=hdrs)
                resp = conn.getresponse()
                resp.read()
                if resp.status != 200:
                    return f"warm-up PUT HTTP {resp.status}"
        return None
    except Exception as e:
        return f"warm-up failed: {e}"
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _run_http_threads(
    host: str,
    port: int,
    concurrency: int,
    duration_sec: float,
    size: int,
    request_fn: Callable[[int, int], RequestSpec],
    payload: bytes = b"",
    bucket: Optional[str] = BUCKET,
    auth: Optional[S3Auth] = None,
    validate_get_size: bool = True,
    worker_index: int = 0,
    ready_cb: Optional[Callable[[], None]] = None,
    label: str = "phase",
) -> WorkerResult:
    """Runs `concurrency` keep-alive HTTP worker threads for `duration_sec`."""
    signer = SigV4Signer(auth) if auth is not None else None
    host_hdr = f"{host}:{port}"
    stop_event = threading.Event()
    latencies: List[float] = []
    lat_lock = threading.Lock()
    total_bytes = 0
    errors = 0
    error_notes: Dict[str, int] = {}
    metrics_lock = threading.Lock()
    ready_lock = threading.Lock()
    threads_ready = 0
    all_ready = threading.Event()
    start_gate = threading.Event()

    def thread_worker(local_idx: int) -> None:
        nonlocal total_bytes, errors, threads_ready
        local_lats: List[float] = []
        local_bytes = 0
        local_errs: Dict[str, int] = {}
        put_headers = {"Content-Length": str(size), "Connection": "keep-alive"}
        get_headers = {"Connection": "keep-alive"}
        req_count = 0

        with ready_lock:
            threads_ready += 1
            if threads_ready >= concurrency:
                all_ready.set()
        start_gate.wait()
        if stop_event.is_set():
            return

        conn = http.client.HTTPConnection(host, port, timeout=15.0)
        while not stop_event.is_set():
            spec = request_fn(local_idx, req_count)
            req_count += 1
            if bucket:
                path = f"/{bucket}/{spec.key.lstrip('/')}"
            else:
                path = spec.key if spec.key.startswith("/") else f"/{spec.key}"
            t0 = time.perf_counter()
            try:
                if spec.method == "PUT":
                    req_hdrs = _make_headers(signer, "PUT", path, host_hdr, put_headers)
                    conn.request("PUT", path, body=payload, headers=req_hdrs)
                    resp = conn.getresponse()
                    resp.read()
                    ok = resp.status == 200
                    if ok:
                        local_bytes += size
                    else:
                        msg = f"HTTP {resp.status}"
                        local_errs[msg] = local_errs.get(msg, 0) + 1
                elif spec.range_start is not None and spec.range_end is not None:
                    req_hdrs = _make_headers(
                        signer,
                        "GET",
                        path,
                        host_hdr,
                        {"Connection": "keep-alive", "Range": f"bytes={spec.range_start}-{spec.range_end}"},
                    )
                    conn.request("GET", path, headers=req_hdrs)
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
                    req_hdrs = _make_headers(signer, "GET", path, host_hdr, get_headers)
                    conn.request("GET", path, headers=req_hdrs)
                    resp = conn.getresponse()
                    data = resp.read()
                    if resp.status == 200 and (not validate_get_size or len(data) == size):
                        ok = True
                        local_bytes += len(data)
                    elif resp.status == 200:
                        ok = False
                        msg = f"GET size mismatch ({len(data)}/{size})"
                        local_errs[msg] = local_errs.get(msg, 0) + 1
                    else:
                        ok = False
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

    threads = [threading.Thread(target=thread_worker, args=(i,), daemon=True) for i in range(concurrency)]
    if concurrency <= 0:
        all_ready.set()
    for t in threads:
        t.start()
    all_ready.wait(timeout=30.0)
    try:
        if ready_cb is not None:
            ready_cb()
        cli_0 = process_cpu_times_total(os.getpid())
        t_start = time.perf_counter()
        start_gate.set()
        time.sleep(duration_sec)
        stop_event.set()
        stuck = rwb.join_all(threads, label, timeout_sec=max(20.0, duration_sec * 10.0))
        t_total = time.perf_counter() - t_start
        cli_1 = process_cpu_times_total(os.getpid())
    finally:
        stop_event.set()
        start_gate.set()

    return WorkerResult(
        worker_index=worker_index,
        concurrency=concurrency,
        ops=len(latencies),
        total_bytes=total_bytes,
        latencies_ms=tuple(latencies),
        errors=errors,
        error_notes=tuple(sorted(error_notes.items())),
        stuck=stuck,
        client_user_s=max(0.0, cli_1[0] - cli_0[0]),
        client_sys_s=max(0.0, cli_1[1] - cli_0[1]),
        wall_sec=t_total,
    )


def _recv_line(conn: socket.socket, timeout: float = 30.0) -> str:
    conn.settimeout(timeout)
    chunks: List[bytes] = []
    while True:
        data = conn.recv(1024)
        if not data:
            break
        chunks.append(data)
        if b"\n" in data:
            break
    return b"".join(chunks).decode("utf-8", errors="replace").strip()


def _build_worker_request_fn(spec: dict) -> Tuple[Callable[[int, int], RequestSpec], bytes]:
    kind = spec["kind"]
    size = int(spec["size"])
    if kind == "pattern":
        source = PatternRequestSource(
            pattern=spec["pattern"],
            size=size,
            seed=int(spec["seed"]),
            prefill_count=int(spec["prefill_count"]),
            large_obj_size=int(spec["large_obj_size"]),
            step_offset=int(spec.get("step_offset", 0)),
            step_stride=int(spec.get("step_stride", 1)),
        )
        payload = deterministic_bytes(size, int(spec["payload_seed"])) if spec["pattern"] in ("seq-write", "rand-write") else b""
        return (lambda _wid, _rc: source.next_request()), payload

    mode = spec["mode"]
    method = "PUT" if mode == "put" else "GET"
    payload = deterministic_bytes(size, int(spec.get("payload_seed", DEFAULT_SEED))) if mode == "put" else b""
    req_path = spec.get("request_path")
    if req_path:
        return (lambda _wid, _rc, m=method, p=req_path: RequestSpec(m, p)), payload

    wid_offset = int(spec.get("worker_id_offset", 0))
    pool_keys = 4 if size >= MIB else 8
    return (
        lambda local_w, rc, m=method, off=wid_offset, pk=pool_keys: RequestSpec(m, f"worker_{off + local_w}_obj_{rc % pk}.dat")
    ), payload


def _client_worker_main(spec_path: str) -> int:
    with open(spec_path, "r", encoding="utf-8") as fh:
        spec = json.load(fh)

    cores = spec.get("cores") or []
    if cores:
        pin_process(os.getpid(), cores)

    auth_dict = spec.get("auth")
    auth = S3Auth(**auth_dict) if auth_dict else None
    request_fn, payload = _build_worker_request_fn(spec)

    ctrl = socket.create_connection(("127.0.0.1", int(spec["ctrl_port"])), timeout=30.0)
    try:
        def wait_for_go() -> None:
            ctrl.sendall(f"READY {spec['worker_index']}\n".encode("utf-8"))
            cmd = _recv_line(ctrl, timeout=60.0)
            if cmd != "GO":
                raise RuntimeError(f"worker {spec['worker_index']} expected GO, got {cmd!r}")

        res = _run_http_threads(
            host=spec["host"],
            port=int(spec["port"]),
            concurrency=int(spec["concurrency"]),
            duration_sec=float(spec["duration_sec"]),
            size=int(spec["size"]),
            request_fn=request_fn,
            payload=payload,
            bucket=spec.get("bucket"),
            auth=auth,
            validate_get_size=bool(spec.get("validate_get_size", True)),
            worker_index=int(spec["worker_index"]),
            ready_cb=wait_for_go,
            label=spec.get("label", "worker"),
        )
        out_payload = {
            "worker_index": res.worker_index,
            "concurrency": res.concurrency,
            "ops": res.ops,
            "total_bytes": res.total_bytes,
            "latencies_ms": [round(x, 4) for x in res.latencies_ms],
            "errors": res.errors,
            "error_notes": [list(pair) for pair in res.error_notes],
            "stuck": res.stuck,
            "client_user_s": res.client_user_s,
            "client_sys_s": res.client_sys_s,
            "wall_sec": res.wall_sec,
        }
        with open(spec["result_path"], "w", encoding="utf-8") as fh:
            json.dump(out_payload, fh)
        ctrl.sendall(b"DONE\n")
        _recv_line(ctrl, timeout=30.0)
    finally:
        try:
            ctrl.close()
        except Exception:
            pass
    return 0


def _run_multiprocess_phase(
    shares: Sequence[int],
    cli_cores: Sequence[int],
    worker_spec_builder: Callable[[int, int, int], dict],
    duration_sec: float,
    server_pid: Optional[int],
    shares_cores: bool,
    profile_threads: bool = False,
    client_procs_requested: Optional[int] = None,
    concurrency: Optional[int] = None,
    data_root: Optional[str] = None,
    done_timeout_sec: Optional[float] = None,
) -> PointResult:
    """Coordinates `len(shares)` worker subprocesses via TCP barrier and aggregates results."""
    n_workers = len(shares)
    n_cli_cores = max(1, len(cli_cores)) if cli_cores else None
    req_procs = client_procs_requested if client_procs_requested is not None else n_workers
    total_conc = concurrency if concurrency is not None else sum(shares)
    work_dir = tempfile.mkdtemp(prefix="scaling_cli_", dir=data_root)
    ctrl_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    procs: List[subprocess.Popen] = []
    conns: List[socket.socket] = []
    conn_to_idx: Dict[socket.socket, int] = {}
    thread_sampler = ThreadCpuSampler(server_pid) if (profile_threads and server_pid) else None

    try:
        ctrl_sock.bind(("127.0.0.1", 0))
        ctrl_sock.listen(max(8, n_workers))
        ctrl_sock.settimeout(0.2)
        ctrl_port = ctrl_sock.getsockname()[1]

        offsets: List[int] = []
        running_off = 0
        for c in shares:
            offsets.append(running_off)
            running_off += c

        for i, conc_i in enumerate(shares):
            p_cores = client_proc_cores(i, n_workers, cli_cores)
            spec = worker_spec_builder(i, conc_i, offsets[i])
            spec["worker_index"] = i
            spec["concurrency"] = conc_i
            spec["cores"] = p_cores
            spec["ctrl_port"] = ctrl_port
            spec["result_path"] = os.path.join(work_dir, f"result_{i}.json")
            spec_path = os.path.join(work_dir, f"spec_{i}.json")
            with open(spec_path, "w", encoding="utf-8") as fh:
                json.dump(spec, fh)

            base_argv = [sys.executable, os.path.abspath(__file__), "--client-worker", spec_path]
            cmd = pinning_command_cores(base_argv, p_cores)
            use_taskset = sys.platform.startswith("linux") and cmd[0] == "taskset" and shutil.which("taskset") is not None
            launch_argv = cmd if use_taskset else base_argv
            log_path = os.path.join(work_dir, f"worker_{i}.log")
            log_fh = open(log_path, "ab")
            try:
                proc = subprocess.Popen(launch_argv, stdout=log_fh, stderr=subprocess.STDOUT)
            finally:
                log_fh.close()
            if p_cores:
                pin_process(proc.pid, p_cores)
            procs.append(proc)

        ready_deadline = time.perf_counter() + max(30.0, duration_sec + 20.0)
        while len(conns) < n_workers:
            if time.perf_counter() > ready_deadline:
                return PointResult(
                    reason=f"client workers did not become ready ({len(conns)}/{n_workers})",
                    shares_cores=shares_cores,
                    client_cores_given=n_cli_cores,
                    client_procs=req_procs,
                    concurrency=total_conc,
                )
            for idx, p in enumerate(procs):
                code = p.poll()
                if code is not None and code != 0:
                    tail = _tail_log(os.path.join(work_dir, f"worker_{idx}.log"))
                    return PointResult(
                        reason=f"client worker {idx} exited with code {code}" + (f": {tail}" if tail else ""),
                        shares_cores=shares_cores,
                        client_cores_given=n_cli_cores,
                        client_procs=req_procs,
                        concurrency=total_conc,
                    )
            try:
                conn, _ = ctrl_sock.accept()
            except socket.timeout:
                continue
            try:
                line = _recv_line(conn, timeout=15.0)
            except OSError as e:
                conn.close()
                return PointResult(
                    reason=f"worker handshake failed: {e}",
                    shares_cores=shares_cores,
                    client_cores_given=n_cli_cores,
                    client_procs=req_procs,
                    concurrency=total_conc,
                )
            if not line.startswith("READY "):
                conn.close()
                return PointResult(
                    reason=f"unexpected worker handshake: {line!r}",
                    shares_cores=shares_cores,
                    client_cores_given=n_cli_cores,
                    client_procs=req_procs,
                    concurrency=total_conc,
                )
            try:
                conn_to_idx[conn] = int(line.split()[1])
            except (IndexError, ValueError):
                conn_to_idx[conn] = len(conns)
            conns.append(conn)

        if thread_sampler is not None:
            thread_sampler.__enter__()
        srv_cpu_0 = process_cpu_times_total(server_pid) if server_pid else None
        cli_pids = [p.pid for p in procs]
        cli_cpu_0 = sum_processes_cpu_times(cli_pids)
        t_start = time.perf_counter()
        for conn in conns:
            conn.sendall(b"GO\n")

        wait_sec = done_timeout_sec if done_timeout_sec is not None else max(40.0, duration_sec * 10.0 + 30.0)
        done_deadline = time.perf_counter() + wait_sec
        pending = list(conns)
        bufs: Dict[socket.socket, bytes] = {c: b"" for c in conns}
        while pending:
            rem = done_deadline - time.perf_counter()
            if rem <= 0:
                pending_idxs = [conn_to_idx.get(c, -1) for c in pending]
                tails = []
                for idx in pending_idxs:
                    if idx >= 0:
                        t = _tail_log(os.path.join(work_dir, f"worker_{idx}.log"))
                        if t:
                            tails.append(f"w{idx}: {t}")
                detail = f" ({'; '.join(tails)})" if tails else ""
                return PointResult(
                    reason=f"client workers timed out waiting for DONE ({len(pending)}/{n_workers} pending){detail}",
                    shares_cores=shares_cores,
                    client_cores_given=n_cli_cores,
                    client_procs=req_procs,
                    concurrency=total_conc,
                )
            readable, _, _ = select.select(pending, [], [], min(0.25, rem))
            for conn in readable:
                try:
                    chunk = conn.recv(1024)
                except OSError:
                    chunk = b""
                if not chunk:
                    pending.remove(conn)
                    idx = conn_to_idx.get(conn)
                    if idx is not None:
                        code = procs[idx].poll()
                        tail = _tail_log(os.path.join(work_dir, f"worker_{idx}.log"))
                        return PointResult(
                            reason=f"client worker {idx} closed control channel before DONE (code={code})"
                            + (f": {tail}" if tail else ""),
                            shares_cores=shares_cores,
                            client_cores_given=n_cli_cores,
                            client_procs=req_procs,
                            concurrency=total_conc,
                        )
                    continue
                bufs[conn] += chunk
                if b"\n" in bufs[conn]:
                    msg = bufs[conn].split(b"\n", 1)[0].decode("utf-8", errors="replace").strip()
                    pending.remove(conn)
                    if msg != "DONE":
                        idx = conn_to_idx.get(conn, -1)
                        return PointResult(
                            reason=f"client worker {idx} sent unexpected message {msg!r}",
                            shares_cores=shares_cores,
                            client_cores_given=n_cli_cores,
                            client_procs=req_procs,
                            concurrency=total_conc,
                        )
            if pending:
                for idx, p in enumerate(procs):
                    code = p.poll()
                    if code not in (None, 0):
                        tail = _tail_log(os.path.join(work_dir, f"worker_{idx}.log"))
                        return PointResult(
                            reason=f"client worker {idx} exited early ({code})" + (f": {tail}" if tail else ""),
                            shares_cores=shares_cores,
                            client_cores_given=n_cli_cores,
                            client_procs=req_procs,
                            concurrency=total_conc,
                        )

        t_total = time.perf_counter() - t_start
        srv_cpu_1 = process_cpu_times_total(server_pid) if server_pid else None
        cli_cpu_1 = sum_processes_cpu_times(cli_pids)
        if thread_sampler is not None:
            thread_sampler.__exit__(None, None, None)

        for conn in conns:
            try:
                conn.sendall(b"EXIT\n")
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass
        conns.clear()

        for p in procs:
            try:
                p.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                _stop_proc(p)

        worker_results: List[WorkerResult] = []
        for i in range(n_workers):
            res_path = os.path.join(work_dir, f"result_{i}.json")
            if not os.path.isfile(res_path):
                tail = _tail_log(os.path.join(work_dir, f"worker_{i}.log"))
                return PointResult(
                    reason=f"client worker {i} wrote no result" + (f": {tail}" if tail else ""),
                    shares_cores=shares_cores,
                    client_cores_given=n_cli_cores,
                    client_procs=req_procs,
                    concurrency=total_conc,
                )
            with open(res_path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            worker_results.append(
                WorkerResult(
                    worker_index=int(raw["worker_index"]),
                    concurrency=int(raw["concurrency"]),
                    ops=int(raw["ops"]),
                    total_bytes=int(raw["total_bytes"]),
                    latencies_ms=tuple(float(x) for x in raw["latencies_ms"]),
                    errors=int(raw.get("errors", 0)),
                    error_notes=tuple((str(k), int(v)) for k, v in raw.get("error_notes", ())),
                    stuck=int(raw.get("stuck", 0)),
                    client_user_s=float(raw.get("client_user_s", 0.0)),
                    client_sys_s=float(raw.get("client_sys_s", 0.0)),
                    wall_sec=float(raw.get("wall_sec", t_total)),
                )
            )

        return aggregate_worker_results(
            worker_results,
            wall_sec=t_total,
            server_cpu_before=srv_cpu_0,
            server_cpu_after=srv_cpu_1,
            client_cpu_before=cli_cpu_0,
            client_cpu_after=cli_cpu_1,
            shares_cores=shares_cores,
            client_cores_given=n_cli_cores,
            client_procs=req_procs,
            concurrency=total_conc,
            thread_cpu=thread_sampler.samples() if thread_sampler is not None else None,
        )
    finally:
        for conn in conns:
            try:
                conn.close()
            except OSError:
                pass
        try:
            ctrl_sock.close()
        except OSError:
            pass
        for p in procs:
            _stop_proc(p)
        if thread_sampler is not None and thread_sampler._thread is not None and thread_sampler._thread.is_alive():
            thread_sampler.__exit__(None, None, None)
        shutil.rmtree(work_dir, ignore_errors=True)


def _run_phase_with_cpu(
    host: str,
    port: int,
    bucket: Optional[str],
    size: int,
    concurrency: int,
    duration_sec: float,
    mode: str,
    server_pid: Optional[int],
    shares_cores: bool,
    profile_threads: bool = False,
    auth: Optional[S3Auth] = None,
    client_procs: int = 1,
    cli_cores: Optional[Sequence[int]] = None,
    request_path: Optional[str] = None,
    force_subprocess: bool = False,
    data_root: Optional[str] = None,
) -> PointResult:
    """Runs one workload phase across `client_procs` and measures server + client CPU."""
    shares = split_concurrency(concurrency, client_procs)
    n_cli_cores = len(cli_cores) if cli_cores else None
    if len(shares) > 1 or force_subprocess:
        if mode == "get" and not request_path and bucket:
            warm_err = _warmup_pool_objects(host, port, bucket, size, concurrency, auth=auth)
            if warm_err:
                return PointResult(
                    reason=warm_err,
                    shares_cores=shares_cores,
                    client_cores_given=n_cli_cores,
                    client_procs=client_procs,
                    concurrency=concurrency,
                )

        auth_dict = (
            {"access_key": auth.access_key, "secret_key": auth.secret_key, "region": auth.region, "service": auth.service}
            if auth is not None else None
        )
        return _run_multiprocess_phase(
            shares=shares,
            cli_cores=cli_cores or [],
            worker_spec_builder=lambda i, conc_i, offset: {
                "kind": "workload",
                "host": host,
                "port": port,
                "bucket": bucket,
                "size": size,
                "duration_sec": duration_sec,
                "mode": mode,
                "worker_id_offset": offset,
                "payload_seed": DEFAULT_SEED,
                "request_path": request_path,
                "validate_get_size": True,
                "auth": auth_dict,
                "label": f"{mode} {size}B c={concurrency} p={i}",
            },
            duration_sec=duration_sec,
            server_pid=server_pid,
            shares_cores=shares_cores,
            profile_threads=profile_threads,
            client_procs_requested=client_procs,
            concurrency=concurrency,
            data_root=data_root,
        )

    if auth is not None or request_path is not None:
        if mode == "get" and not request_path and bucket:
            warm_err = _warmup_pool_objects(host, port, bucket, size, concurrency, auth=auth)
            if warm_err:
                return PointResult(
                    reason=warm_err,
                    shares_cores=shares_cores,
                    client_cores_given=n_cli_cores,
                    client_procs=client_procs,
                    concurrency=concurrency,
                )
        method = "PUT" if mode == "put" else "GET"
        payload = deterministic_bytes(size, DEFAULT_SEED) if mode == "put" else b""
        pool_keys = 4 if size >= MIB else 8
        req_fn = (
            (lambda _w, _rc, m=method, p=request_path: RequestSpec(m, p))
            if request_path
            else (lambda wid, rc, m=method, pk=pool_keys: RequestSpec(m, f"worker_{wid}_obj_{rc % pk}.dat"))
        )
        thread_sampler = ThreadCpuSampler(server_pid) if (profile_threads and server_pid) else None
        if thread_sampler is not None:
            thread_sampler.__enter__()
        srv_0 = process_cpu_times_total(server_pid) if server_pid else None
        res = _run_http_threads(
            host=host,
            port=port,
            concurrency=concurrency,
            duration_sec=duration_sec,
            size=size,
            request_fn=req_fn,
            payload=payload,
            bucket=bucket,
            auth=auth,
            validate_get_size=True,
            worker_index=0,
            label=f"{mode} {size}B c={concurrency}",
        )
        srv_1 = process_cpu_times_total(server_pid) if server_pid else None
        if thread_sampler is not None:
            thread_sampler.__exit__(None, None, None)
        return aggregate_worker_results(
            [res],
            wall_sec=res.wall_sec,
            server_cpu_before=srv_0,
            server_cpu_after=srv_1,
            shares_cores=shares_cores,
            client_cores_given=n_cli_cores,
            client_procs=1,
            concurrency=concurrency,
            thread_cpu=thread_sampler.samples() if thread_sampler is not None else None,
        )

    samples: List[Tuple[float, Tuple[float, float]]] = []
    thread_sampler = ThreadCpuSampler(server_pid) if (profile_threads and server_pid) else None
    orig_cpu_fn = rwb.get_process_cpu_times

    def hooked_cpu(pid: int) -> Tuple[float, float]:
        if pid == server_pid:
            ct = process_cpu_times_total(pid)
            samples.append((time.perf_counter(), ct))
            if thread_sampler is not None:
                thread_sampler.poll_once()
            return ct
        return orig_cpu_fn(pid)

    rwb.get_process_cpu_times = hooked_cpu
    try:
        metrics = rwb.run_workload_phase(host, port, bucket, size, concurrency, duration_sec, mode, server_pid)
    finally:
        rwb.get_process_cpu_times = orig_cpu_fn

    if not metrics.measured:
        return PointResult(
            reason=metrics.note or "phase did not complete",
            shares_cores=shares_cores,
            client_cores_given=n_cli_cores,
            client_procs=1,
            concurrency=concurrency,
        )

    if len(samples) >= 2:
        t0, cpu0 = samples[0]
        t1, cpu1 = samples[-1]
        wall_sec = t1 - t0
        pct = cpu_percent(cpu0, cpu1, wall_sec)
    else:
        wall_sec = duration_sec if duration_sec > 0 else None
        pct = (metrics.server_cpu_s / duration_sec) * 100.0 if duration_sec > 0 else None

    cli_pct = ((metrics.client_cpu_s / wall_sec) * 100.0) if (wall_sec and wall_sec > 0 and metrics.client_cpu_s >= 0) else metrics.client_cpu_pct
    thread_cpu = thread_sampler.samples() if thread_sampler is not None else None
    eff = ops_per_busy_core(metrics.ops_per_sec, pct)
    return PointResult(
        ops_per_sec=metrics.ops_per_sec,
        p50_ms=metrics.p50_ms,
        p99_ms=metrics.p99_ms,
        cpu_pct=pct,
        ops_per_busy_core=eff,
        shares_cores=shares_cores,
        thread_cpu=thread_cpu,
        wall_sec=wall_sec,
        client_cpu_pct=cli_pct,
        client_cores_given=n_cli_cores,
        client_procs=1,
        concurrency=concurrency,
    )


def _log_thread_samples(
    server_name: str,
    n_cores: int,
    concurrency: int,
    samples: Sequence[ThreadCpuSample],
    wall_sec: Optional[float],
) -> None:
    total = sum(s.total_s for s in samples)
    wall_s = f"{wall_sec:.2f}s" if wall_sec is not None else "?"
    print(f"      {server_name} N={n_cores} c={concurrency} threads (wall={wall_s}, sum={total:.3f}s):", flush=True)
    for s in samples[:12]:
        label = s.name or "(unnamed)"
        print(f"        tid={s.tid} ({label}): {s.total_s:.3f}s (user={s.user_s:.3f}s, sys={s.system_s:.3f}s)", flush=True)


def measure_server_for_cores(
    server_name: str,
    starter: Callable[[], Tuple],
    n_cores: int,
    cli_cores: Sequence[int],
    shares_cores: bool,
    duration_sec: float,
    workloads: Sequence[Tuple[str, str, str, int]] = WORKLOADS,
    profile_threads: bool = False,
    profile_workload: str = "put_4k",
    client_procs: int = 1,
    data_root: Optional[str] = None,
) -> Dict[str, PointResult]:
    """Starts one server, pins the load generator to `cli_cores`, runs all workloads, cleans up."""
    concurrency = 16 * n_cores
    n_cli_cores = len(cli_cores) if cli_cores else None
    try:
        proc, port, data_dir, log_path, auth = _unpack_starter_result(starter())
    except Exception as e:
        reason = str(e).strip() or f"{server_name} failed to start"
        return {
            wkey: PointResult(
                reason=reason,
                shares_cores=shares_cores,
                client_cores_given=n_cli_cores,
                client_procs=client_procs,
                concurrency=concurrency,
            )
            for wkey, *_ in workloads
        }

    out: Dict[str, PointResult] = {}
    try:
        pin_process(os.getpid(), cli_cores)
        for wkey, label, mode, size in workloads:
            print(f"    {server_name} N={n_cores} (c={concurrency}, procs={client_procs}): {label}...", flush=True)
            sample_here = profile_threads and wkey == profile_workload
            pt = _run_phase_with_cpu(
                "127.0.0.1", port, BUCKET, size, concurrency, duration_sec, mode, proc.pid, shares_cores,
                profile_threads=sample_here, auth=auth, client_procs=client_procs, cli_cores=cli_cores, data_root=data_root,
            )
            out[wkey] = pt
            if sample_here and pt.thread_cpu:
                _log_thread_samples(server_name, n_cores, concurrency, pt.thread_cpu, pt.wall_sec)
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
    for wkey, wlabel, mode, size in CHART_WORKLOADS:
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
    fig5, axes5 = plt.subplots(1, len(CHART_WORKLOADS), figsize=(16, 4.2))
    for ax, (wkey, wlabel, _mode, _size) in zip(axes5, CHART_WORKLOADS):
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


def _render_thread_breakdown_lines(
    entries: Sequence[Tuple[str, int, int, PointResult]],
) -> List[str]:
    rows: List[str] = []
    for durability, n_cores, concurrency, pt in entries:
        if not pt or not pt.measured or not pt.thread_cpu:
            continue
        total_cpu_s = sum(s.total_s for s in pt.thread_cpu)
        wall_s = f"{pt.wall_sec:.2f}" if pt.wall_sec is not None else "-"
        for s in pt.thread_cpu:
            label = s.name or "(unnamed)"
            share = f"{(s.total_s / total_cpu_s) * 100.0:.1f}%" if total_cpu_s > 0 else "-"
            rows.append(
                f"| {durability} | {n_cores} | {s.tid} | {label} | {s.user_s:.3f} | {s.system_s:.3f} | {s.total_s:.3f} | {share} | {concurrency} | {wall_s} |"
            )
    if not rows:
        return []
    return [
        "",
        "## Per-Thread CPU Breakdown (AwanFS PUT 4 KiB)",
        "",
        "| Durability | Cores | Thread ID | Thread Name | User (s) | System (s) | Total CPU (s) | Share | Concurrency | Wall (s) |",
        "|---|---|---|---|---|---|---|---|---|---|",
        *rows,
    ]


def _durability_label(durability: str) -> str:
    if durability == "fast":
        return "AwanFS `--durability fast` (no sync on PUT; background flusher every 1s)"
    if durability == "strict":
        return "AwanFS `--durability strict` (sync per PUT)"
    return "AwanFS group commit (#29)"


def client_cap_info(
    pt: Optional[PointResult],
    n_cores: int,
    total_cores: int,
    default_client_procs: int = 1,
    concurrency: Optional[int] = None,
) -> Tuple[int, Optional[float], str]:
    """Evaluates `pt.client_cpu_pct` against `client_cores_given` and active process ceiling."""
    if pt is not None and pt.client_cores_given is not None:
        cli_cores_given = max(1, int(pt.client_cores_given))
    else:
        default_cli, _ = client_cores_for(n_cores, total_cores)
        cli_cores_given = max(1, len(default_cli))

    if pt is None or not pt.measured or pt.client_cpu_pct is None:
        return cli_cores_given, None, "-"

    util_pct = (pt.client_cpu_pct / (cli_cores_given * 100.0)) * 100.0
    req_procs = max(1, int(pt.client_procs or default_client_procs))
    conc_val = concurrency if concurrency is not None else pt.concurrency
    procs_used = max(1, min(req_procs, conc_val)) if conc_val is not None else req_procs
    effective_cap_pct = min(cli_cores_given, procs_used) * 100.0

    if pt.client_cpu_pct >= 0.80 * effective_cap_pct:
        if pt.shares_cores and pt.cpu_pct is not None and pt.cpu_pct > pt.client_cpu_pct:
            return cli_cores_given, util_pct, "no (server-heavy on shared cores)"
        if not pt.shares_cores and pt.cpu_pct is not None and pt.cpu_pct >= 0.85 * n_cores * 100.0:
            return cli_cores_given, util_pct, "no (server also >=85%)"
        if cli_cores_given <= procs_used:
            return cli_cores_given, util_pct, "yes (saturated client cores)"
        return cli_cores_given, util_pct, "yes (saturated client procs)"

    if (
        pt.shares_cores
        and pt.cpu_pct is not None
        and (pt.client_cpu_pct + pt.cpu_pct) >= 0.85 * cli_cores_given * 100.0
        and pt.client_cpu_pct > pt.cpu_pct * 1.5
    ):
        return cli_cores_given, util_pct, "yes (shared cores, client-heavy)"

    return cli_cores_given, util_pct, "no"


def _client_cap_summary_line(entries: Sequence[Tuple[str, str]]) -> str:
    measured_caps = [(label, cap) for label, cap in entries if cap != "-"]
    if not measured_caps:
        return "Client cap check: client CPU not measured."
    capped = [f"{label} ({cap})" for label, cap in measured_caps if cap.startswith("yes")]
    if not capped:
        return f"Client cap check: none of {len(measured_caps)} measured phases were client-capped."
    shown = ", ".join(capped[:8])
    extra = f" (+{len(capped) - 8} more)" if len(capped) > 8 else ""
    return f"Client cap check: {len(capped)} of {len(measured_caps)} measured phases were client-capped: {shown}{extra}."


def render_report(
    cmd: str,
    cores: Sequence[int],
    results: Dict[str, Dict[str, Dict[int, PointResult]]],
    total_cores: int,
    runs: int,
    duration_sec: float,
    max_put_ops: Optional[Dict[int, Optional[float]]] = None,
    chart_paths: Optional[Sequence[str]] = None,
    fast_results: Optional[Dict] = None,
    durability: str = "group",
    client_procs: int = 1,
) -> str:
    # `fast_results` is {workload key: {cores: point}}; a bare {cores: point} is PUT 4 KiB.
    if fast_results is not None and all(isinstance(k, int) for k in fast_results):
        fast_results = {"put_4k": fast_results}
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
        f"# Core Scaling Benchmark: AwanFS vs MinIO vs RustFS vs Garage vs SeaweedFS ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"- **Machine:** {platform.node()} ({platform.platform()}, {total_cores} logical cores, {rwb.host_ram_gb()} GB RAM, CPU: {rwb.cpu_model()})",
        f"- **Methodology:** `cores = {', '.join(str(n) for n in cores)}`, `concurrency = 16 × N`, `client_procs = {client_procs}`, {runs} alternating runs per point (median reported), {duration_sec:.1f}s per phase.",
        f"- **Core pinning:** {sharing_note}",
        f"- **Durability:** {_durability_label(durability)}. {rwb.minio_durability_note()} RustFS release defaults (durable, 3.09 syncs/PUT; measurements/2026-11-21-durability-probe.md, #334). Garage `metadata_fsync=false, data_fsync=false`. SeaweedFS `fsync` unset (`false`) by default. {probe_note}",
        "",
    ]
    lines.extend(_durability_section_lines())
    lines.extend([
        "## Throughput and Latency (`ops/s (P50, P99 ms)`)",
        "",
    ])

    core_headers = [f"{n} core" if n == 1 else f"{n} cores" for n in cores]
    lines.append("| Workload | Size | Server | " + " | ".join(core_headers) + f" | Scaling ({max_n}/1) |")
    lines.append("|---|---|---|" + "|".join(["---"] * (len(core_headers) + 1)) + "|")

    def _throughput_row(wlabel: str, size_label: str, srv_label: str, pts: Dict[int, PointResult]) -> str:
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
        return f"| {wlabel} | {size_label} | {srv_label} | " + " | ".join(cells) + f" | {sc_str} |"

    for wkey, wlabel, _mode, size in WORKLOADS:
        size_label = f"{size // KIB} KiB"
        for srv in SERVERS:
            pts = results.get(wkey, {}).get(srv, {})
            lines.append(_throughput_row(wlabel, size_label, srv, pts))
            if srv == "AwanFS" and fast_results is not None and wkey in fast_results:
                lines.append(_throughput_row(wlabel, size_label, "AwanFS (--durability fast)", fast_results[wkey]))

    lines.extend([
        "",
        "## Server CPU Usage and Efficiency (`CPU %` where 100% = 1 core busy; `ops/s per busy core`)",
        "",
        "| Workload | Size | Server | " + " | ".join(core_headers) + " |",
        "|---|---|---|" + "|".join(["---"] * len(core_headers)) + "|",
    ])

    def _cpu_row(wlabel: str, size_label: str, srv_label: str, pts: Dict[int, PointResult]) -> str:
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
        return f"| {wlabel} | {size_label} | {srv_label} | " + " | ".join(cells) + " |"

    for wkey, wlabel, _mode, size in WORKLOADS:
        size_label = f"{size // KIB} KiB"
        for srv in SERVERS:
            pts = results.get(wkey, {}).get(srv, {})
            lines.append(_cpu_row(wlabel, size_label, srv, pts))
            if srv == "AwanFS" and fast_results is not None and wkey in fast_results:
                lines.append(_cpu_row(wlabel, size_label, "AwanFS (--durability fast)", fast_results[wkey]))

    cap_entries: List[Tuple[str, str]] = []
    cli_rows: List[str] = []

    def _client_cpu_row(wlabel: str, size_label: str, srv_label: str, pts: Dict[int, PointResult]) -> str:
        cells = []
        for n in cores:
            pt = pts.get(n)
            if pt and pt.measured:
                cli_cores_g, util_pct, cap_lbl = client_cap_info(
                    pt, n_cores=n, total_cores=total_cores, default_client_procs=client_procs, concurrency=16 * n
                )
                core_lbl = "core" if cli_cores_g == 1 else "cores"
                if pt.client_cpu_pct is not None and util_pct is not None:
                    shared_tag = "shared, " if pt.shares_cores else ""
                    cells.append(f"{pt.client_cpu_pct:.1f}% / {cli_cores_g} {core_lbl} ({util_pct:.0f}%, {shared_tag}{cap_lbl})")
                    cap_entries.append((f"{srv_label} {wlabel} {n}c", cap_lbl))
                else:
                    cells.append(f"- / {cli_cores_g} {core_lbl}")
            else:
                reason = pt.reason if pt and pt.reason else "not measured"
                cells.append(f"not measured ({reason})")
        return f"| {wlabel} | {size_label} | {srv_label} | " + " | ".join(cells) + " |"

    for wkey, wlabel, _mode, size in WORKLOADS:
        size_label = f"{size // KIB} KiB"
        for srv in SERVERS:
            pts = results.get(wkey, {}).get(srv, {})
            cli_rows.append(_client_cpu_row(wlabel, size_label, srv, pts))
            if srv == "AwanFS" and fast_results is not None and wkey in fast_results:
                cli_rows.append(_client_cpu_row(wlabel, size_label, "AwanFS (--durability fast)", fast_results[wkey]))

    lines.extend([
        "",
        "## Client CPU Usage (`CPU %` where 100% = 1 core busy, against client cores given)",
        "",
        _client_cap_summary_line(cap_entries),
        "",
        "| Workload | Size | Server | " + " | ".join(core_headers) + " |",
        "|---|---|---|" + "|".join(["---"] * len(core_headers)) + "|",
        *cli_rows,
    ])

    thread_entries: List[Tuple[str, int, int, PointResult]] = []
    awan_put4k = results.get("put_4k", {}).get("AwanFS", {})
    for n in cores:
        pt = awan_put4k.get(n)
        if pt and pt.measured and pt.thread_cpu:
            thread_entries.append((durability, n, 16 * n, pt))
    if fast_results is not None:
        for n in cores:
            pt = fast_results.get("put_4k", {}).get(n)
            if pt and pt.measured and pt.thread_cpu:
                thread_entries.append(("fast", n, 16 * n, pt))
    lines.extend(_render_thread_breakdown_lines(thread_entries))

    if chart_paths:
        lines.extend(["", "## Charts", ""])
        for p in chart_paths:
            lines.append(f"- `{os.path.basename(p)}`")

    lines.append("")
    return "\n".join(lines)


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _prefill_small_set(
    host: str,
    port: int,
    bucket: str,
    size: int,
    prefill_count: int,
    seed: int,
    deadline: float,
    signer: Optional["SigV4Signer"] = None,
    host_hdr: Optional[str] = None,
) -> Optional[str]:
    """Uploads `prefill_count` deterministic `size`-byte objects for one read size."""
    prefix = prefill_prefix_for(size)
    size_label = format_size(size)
    payload = deterministic_bytes(size, seed)
    workers = min(16, prefill_count)
    next_idx = 0
    idx_lock = threading.Lock()
    errors: List[str] = []

    def prefill_worker() -> None:
        nonlocal next_idx
        conn = http.client.HTTPConnection(host, port, timeout=15.0)
        headers = {"Content-Length": str(len(payload)), "Connection": "keep-alive"}
        try:
            while time.perf_counter() < deadline:
                with idx_lock:
                    if errors or next_idx >= prefill_count:
                        return
                    i = next_idx
                    next_idx += 1
                try:
                    path = f"/{bucket}/{prefix}/{i:06d}"
                    req_hdrs = _make_headers(signer, "PUT", path, host_hdr or f"{host}:{port}", headers)
                    conn.request("PUT", path, body=payload, headers=req_hdrs)
                    resp = conn.getresponse()
                    resp.read()
                    if resp.status != 200:
                        with idx_lock:
                            errors.append(f"prefill {size_label} HTTP {resp.status}")
                        return
                except Exception as e:
                    with idx_lock:
                        errors.append(f"prefill {size_label} {type(e).__name__}: {e}")
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
        return f"prefill {size_label} timed out ({next_idx}/{prefill_count})"
    return None


def _prefill_server(
    host: str,
    port: int,
    bucket: str,
    seed: int,
    prefill_bytes: int,
    patterns: Sequence[str],
    sizes: Sequence[int],
    timeout_sec: Optional[float] = None,
    auth: Optional[S3Auth] = None,
) -> Optional[str]:
    """Writes identical deterministic prefill data before timing read/overwrite phases."""
    need_reads = any(p in ("seq-read", "rand-read") for p in patterns)
    small_sizes = sorted({s for s in sizes if s < MIB}) if need_reads else []
    need_large = need_reads and any(s >= MIB for s in sizes)
    need_rand_write = "rand-write" in patterns
    if timeout_sec is None:
        total_small_objects = sum(prefill_count_for(prefill_bytes, sz) for sz in small_sizes)
        timeout_sec = max(
            120.0,
            total_small_objects * 0.015 + (120.0 if need_large else 0.0) + (60.0 if need_rand_write else 0.0),
        )
    deadline = time.perf_counter() + timeout_sec
    signer = SigV4Signer(auth) if auth is not None else None
    host_hdr = f"{host}:{port}"

    for sz in small_sizes:
        err = _prefill_small_set(
            host=host,
            port=port,
            bucket=bucket,
            size=sz,
            prefill_count=prefill_count_for(prefill_bytes, sz),
            seed=seed,
            deadline=deadline,
            signer=signer,
            host_hdr=host_hdr,
        )
        if err:
            return err

    if need_large:
        large_payload = deterministic_bytes(prefill_bytes, seed + 1)
        conn = http.client.HTTPConnection(host, port, timeout=max(30.0, timeout_sec))
        try:
            path = f"/{bucket}/prefill_large/seq_obj.bin"
            req_hdrs = _make_headers(
                signer, "PUT", path, host_hdr, {"Content-Length": str(len(large_payload)), "Connection": "close"}
            )
            conn.request("PUT", path, body=large_payload, headers=req_hdrs)
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
                    path = f"/{bucket}/rand/{i:06d}"
                    req_hdrs = _make_headers(signer, "PUT", path, host_hdr, headers)
                    conn.request("PUT", path, body=payload, headers=req_hdrs)
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
    profile_threads: bool = False,
    auth: Optional[S3Auth] = None,
    client_procs: int = 1,
    cli_cores: Optional[Sequence[int]] = None,
    data_root: Optional[str] = None,
) -> PointResult:
    """Runs one `(pattern, size, concurrency)` phase across `client_procs` and measures server + client CPU."""
    prefill_count = prefill_count_for(prefill_bytes, size)
    shares = split_concurrency(concurrency, client_procs)
    n_cli_cores = len(cli_cores) if cli_cores else None
    if len(shares) > 1:
        auth_dict = (
            {"access_key": auth.access_key, "secret_key": auth.secret_key, "region": auth.region, "service": auth.service}
            if auth is not None else None
        )
        active = len(shares)
        return _run_multiprocess_phase(
            shares=shares,
            cli_cores=cli_cores or [],
            worker_spec_builder=lambda i, _conc_i, _off: {
                "kind": "pattern",
                "host": host,
                "port": port,
                "bucket": bucket,
                "pattern": pattern,
                "size": size,
                "duration_sec": duration_sec,
                "seed": seed + i,
                "payload_seed": seed + 3,
                "prefill_count": prefill_count,
                "large_obj_size": prefill_bytes,
                "step_offset": i,
                "step_stride": active,
                "validate_get_size": True,
                "auth": auth_dict,
                "label": f"{pattern} {format_size(size)} c={concurrency} p={i}",
            },
            duration_sec=duration_sec,
            server_pid=server_pid,
            shares_cores=shares_cores,
            profile_threads=profile_threads,
            client_procs_requested=client_procs,
            concurrency=concurrency,
            data_root=data_root,
        )

    source = PatternRequestSource(
        pattern=pattern,
        size=size,
        seed=seed,
        prefill_count=prefill_count,
        large_obj_size=prefill_bytes,
    )
    put_payload = deterministic_bytes(size, seed + 3) if pattern in ("seq-write", "rand-write") else b""
    thread_sampler = ThreadCpuSampler(server_pid) if profile_threads else None
    if thread_sampler is not None:
        thread_sampler.__enter__()
    cpu_0 = process_cpu_times_total(server_pid)
    res = _run_http_threads(
        host=host,
        port=port,
        concurrency=concurrency,
        duration_sec=duration_sec,
        size=size,
        request_fn=lambda _wid, _rc: source.next_request(),
        payload=put_payload,
        bucket=bucket,
        auth=auth,
        validate_get_size=True,
        worker_index=0,
        label=f"{pattern} {format_size(size)} c={concurrency}",
    )
    cpu_1 = process_cpu_times_total(server_pid)
    if thread_sampler is not None:
        thread_sampler.__exit__(None, None, None)

    return aggregate_worker_results(
        [res],
        wall_sec=res.wall_sec,
        server_cpu_before=cpu_0,
        server_cpu_after=cpu_1,
        shares_cores=shares_cores,
        client_cores_given=n_cli_cores,
        client_procs=1,
        concurrency=concurrency,
        thread_cpu=thread_sampler.samples() if thread_sampler is not None else None,
    )


def measure_server_matrix_for_cores(
    server_name: str,
    starter: Callable[[], Tuple],
    n_cores: int,
    cli_cores: Sequence[int],
    shares_cores: bool,
    patterns: Sequence[str],
    sizes: Sequence[int],
    concurrencies: Sequence[int],
    duration_sec: float,
    seed: int,
    prefill_bytes: int,
    profile_threads_points: Optional[Sequence[Tuple[str, int, int]]] = None,
    client_procs: int = 1,
    data_root: Optional[str] = None,
    scrape_work: bool = False,
) -> Dict[Tuple[str, int, int], PointResult]:
    """Starts one server, prefills reads once, runs the `(pattern, size, concurrency)` sweep, cleans up.

    With `scrape_work` (AwanFS only), reads `/_awanfs/metrics` before and after each phase (#431).
    """
    all_keys = [(pat, sz, c) for pat in patterns for sz in sizes for c in concurrencies]
    n_cli_cores = len(cli_cores) if cli_cores else None
    try:
        proc, port, data_dir, log_path, auth = _unpack_starter_result(starter())
    except Exception as e:
        reason = str(e).strip() or f"{server_name} failed to start"
        return {
            (pat, sz, c): PointResult(
                reason=reason,
                shares_cores=shares_cores,
                client_cores_given=n_cli_cores,
                client_procs=client_procs,
                concurrency=c,
            )
            for pat, sz, c in all_keys
        }

    profile_set = set(profile_threads_points or ())
    out: Dict[Tuple[str, int, int], PointResult] = {}
    try:
        pin_process(os.getpid(), cli_cores)
        print(f"    {server_name} N={n_cores}: prefilling ({describe_prefill_sets(prefill_bytes, patterns, sizes)})...", flush=True)
        prefill_err = _prefill_server("127.0.0.1", port, BUCKET, seed, prefill_bytes, patterns, sizes, auth=auth)
        if prefill_err:
            print(f"    {server_name} N={n_cores}: prefill failed: {prefill_err}", flush=True)
            for pat, sz, c in all_keys:
                out[(pat, sz, c)] = PointResult(
                    reason=prefill_err,
                    shares_cores=shares_cores,
                    client_cores_given=n_cli_cores,
                    client_procs=client_procs,
                    concurrency=c,
                )
            return out

        for pat in patterns:
            for sz in sizes:
                for c in concurrencies:
                    print(f"    {server_name} N={n_cores} {pat} {format_size(sz)} c={c} (procs={client_procs})...", flush=True)
                    sample_here = (pat, sz, c) in profile_set
                    before = scrape_awanfs_metrics("127.0.0.1", port) if scrape_work else None
                    t0 = time.monotonic()
                    pt = _run_pattern_phase_with_cpu(
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
                        profile_threads=sample_here,
                        auth=auth,
                        client_procs=client_procs,
                        cli_cores=cli_cores,
                        data_root=data_root,
                    )
                    if scrape_work:
                        elapsed = time.monotonic() - t0
                        after = scrape_awanfs_metrics("127.0.0.1", port)
                        if before is not None and after is not None and pt.measured:
                            pt.work = phase_work(before, after, elapsed, pt.ops_per_sec)
                    out[(pat, sz, c)] = pt
                    if sample_here and pt.thread_cpu:
                        _log_thread_samples(server_name, n_cores, c, pt.thread_cpu, pt.wall_sec)
    finally:
        _stop_proc(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
        try:
            os.remove(log_path)
        except OSError:
            pass
    return out


def fast_concurrencies_for(n_cores: int, concurrencies: Sequence[int], every: bool = False) -> List[int]:
    """Concurrencies the fast pass runs at N cores: all of them with `--fast-every-concurrency`."""
    if every:
        return list(concurrencies)
    return sorted({_pick_target(concurrencies, 64), _pick_target(concurrencies, 16 * n_cores)})


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
            fig.suptitle(f"{pat} ({row_size_label(pat, sz)}) — Core Scaling (concurrency = {fixed_conc})", y=1.06)
            fig.tight_layout(rect=[0, 0, 1, 0.92])
            _save_both(fig, f"cores_{_slug(pat)}_{_slug(sz_lbl)}")

    # 2. Per pattern and size: ops/s and P99 against concurrency (log2 x), one line per core count, one panel per server, shared y scale
    for pat in patterns:
        for sz in sizes:
            sz_lbl = format_size(sz)
            fig, axes = plt.subplots(2, len(SERVERS), figsize=(max(15.0, 4.0 * len(SERVERS)), 7.6))
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
            fig.suptitle(f"{pat} ({row_size_label(pat, sz)}) — Concurrency Sweep across Core Counts", y=1.03)
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
    bar_w = min(0.24, 0.8 / max(1, len(SERVERS)))
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
    fast_results: Optional[Dict[int, Dict[int, PointResult]]] = None,
    durability: str = "group",
    client_procs: int = 1,
    fast_by_size: Optional[Dict[int, Dict[int, Dict[int, PointResult]]]] = None,
    servers: Sequence[str] = SERVERS,
) -> str:
    # `fast_by_size` is {size: {cores: {concurrency: point}}}; `fast_results` alone is 4 KiB.
    fast_sizes: Dict[int, Dict[int, Dict[int, PointResult]]] = dict(fast_by_size or {})
    if fast_results is not None:
        fast_sizes.setdefault(4 * KIB, fast_results)
    fast_results = fast_sizes.get(4 * KIB)
    if memory_budget:
        budget_bytes = parse_size(memory_budget)
        budget_note = f"AwanFS `--memory-budget {format_size(budget_bytes)}` (stated cache half = {format_size(stated_cache_bytes(budget_bytes))})"
    else:
        budget_note = "AwanFS at its default `--memory-budget` (a quarter of RAM, held between 128 MiB and 2 GiB; half of it is the cache)"
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
        f"# Pattern & Concurrency Scaling Benchmark: {' vs '.join(servers)} ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"- **Machine:** {platform.node()} ({platform.platform()}, {total_cores} logical cores, {rwb.host_ram_gb()} GB RAM, CPU: {rwb.cpu_model()})",
        f"- **Methodology:** `patterns = {', '.join(patterns)}`, `sizes = {', '.join(format_size(s) for s in sizes)}`, `cores = {', '.join(str(n) for n in cores)}`, `concurrencies = {', '.join(str(c) for c in concurrencies)}`, `client_procs = {client_procs}`, {runs} alternating runs per point (median reported), {duration_sec:.1f}s per phase, seed = `{seed}`.",
        f"- **Cache & Prefill:** {budget_note}. Read prefill size = {format_size(prefill_bytes)} ({describe_prefill_sets(prefill_bytes, patterns, sizes)}), identical deterministic bytes for every server, written before timing.",
        f"- **Core pinning:** {sharing_note}",
        f"- **Durability:** {_durability_label(durability)}. {rwb.minio_durability_note()} RustFS release defaults (durable, 3.09 syncs/PUT; measurements/2026-11-21-durability-probe.md, #334). Garage `metadata_fsync=false, data_fsync=false`. SeaweedFS `fsync` unset (`false`) by default.",
        "",
    ]
    lines.extend(_durability_section_lines())
    lines.extend([
        "## All Measured Points",
        "",
        "| Server | Pattern | Size | Cores | Concurrency | ops/s | MiB/s | P50 (ms) | P99 (ms) | CPU % |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ])

    def _matrix_row(srv_label: str, pat: str, sz_lbl: str, n: int, c: int, pt: Optional[PointResult]) -> str:
        if pt and pt.measured:
            mib_s = f"{pt.mib_per_sec:.2f}" if pt.mib_per_sec is not None else "-"
            cpu_s = f"{pt.cpu_pct:.1f}%" if pt.cpu_pct is not None else "-"
            return f"| {srv_label} | {pat} | {sz_lbl} | {n} | {c} | {pt.ops_per_sec:,.0f} | {mib_s} | {pt.p50_ms:.2f} | {pt.p99_ms:.2f} | {cpu_s} |"
        reason = pt.reason if pt and pt.reason else "unknown"
        return f"| {srv_label} | {pat} | {sz_lbl} | {n} | {c} | not measured ({reason}) | - | - | - | - |"

    put_pat = "seq-write" if "seq-write" in patterns else ("rand-write" if "rand-write" in patterns else patterns[0])
    cli_rows: List[str] = []
    cap_entries: List[Tuple[str, str]] = []

    def _matrix_client_row(srv_label: str, pat: str, sz_lbl: str, n: int, c: int, pt: Optional[PointResult]) -> str:
        cli_cores_g, util_pct, cap_lbl = client_cap_info(
            pt, n_cores=n, total_cores=total_cores, default_client_procs=client_procs, concurrency=c
        )
        procs_shown = min(pt.client_procs or client_procs, c) if pt else min(client_procs, c)
        cores_cell = f"{cli_cores_g} (shared)" if (pt and pt.shares_cores) else str(cli_cores_g)
        if pt and pt.measured:
            if pt.client_cpu_pct is not None and util_pct is not None:
                cap_entries.append((f"{srv_label} {pat} {sz_lbl} {n}c c={c}", cap_lbl))
                return f"| {srv_label} | {pat} | {sz_lbl} | {n} | {c} | {procs_shown} | {cores_cell} | {pt.client_cpu_pct:.1f}% | {util_pct:.1f}% | {cap_lbl} |"
            return f"| {srv_label} | {pat} | {sz_lbl} | {n} | {c} | {procs_shown} | {cores_cell} | - | - | - |"
        reason = pt.reason if pt and pt.reason else "unknown"
        return f"| {srv_label} | {pat} | {sz_lbl} | {n} | {c} | {procs_shown} | {cores_cell} | not measured ({reason}) | - | - |"

    for srv in servers:
        for pat in patterns:
            for sz in sizes:
                sz_lbl = row_size_label(pat, sz)
                for n in cores:
                    for c in concurrencies:
                        pt = results.get(pat, {}).get(sz, {}).get(srv, {}).get(n, {}).get(c)
                        lines.append(_matrix_row(srv, pat, sz_lbl, n, c, pt))
                        cli_rows.append(_matrix_client_row(srv, pat, sz_lbl, n, c, pt))
        if srv == "AwanFS":
            for sz in [z for z in sizes if z in fast_sizes]:
                sz_lbl = row_size_label(put_pat, sz)
                for n in cores:
                    for c in sorted(fast_sizes[sz].get(n, {})):
                        pt = fast_sizes[sz][n][c]
                        lines.append(_matrix_row("AwanFS (--durability fast)", put_pat, sz_lbl, n, c, pt))
                        cli_rows.append(_matrix_client_row("AwanFS (--durability fast)", put_pat, sz_lbl, n, c, pt))

    lines.extend([
        "",
        "## Client CPU Usage (`CPU %` where 100% = 1 core busy, against client cores given)",
        "",
        _client_cap_summary_line(cap_entries),
        "",
        "| Server | Pattern | Size | Cores | Concurrency | Client Procs | Client Cores | Client CPU % | Client Core Util | Client-Capped? |",
        "|---|---|---|---|---|---|---|---|---|---|",
        *cli_rows,
    ])

    thread_entries: List[Tuple[str, int, int, PointResult]] = []
    if (4 * KIB) in sizes:
        for n in cores:
            c_map = results.get(put_pat, {}).get(4 * KIB, {}).get("AwanFS", {}).get(n, {})
            for c in concurrencies:
                pt = c_map.get(c)
                if pt and pt.measured and pt.thread_cpu:
                    thread_entries.append((durability, n, c, pt))
    if fast_results is not None:
        for n in cores:
            for c in sorted(fast_results.get(n, {})):
                pt = fast_results[n][c]
                if pt and pt.measured and pt.thread_cpu:
                    thread_entries.append(("fast", n, c, pt))
    lines.extend(_render_thread_breakdown_lines(thread_entries))
    lines.extend(_render_work_lines(results, patterns, sizes, cores, concurrencies, durability, fast_sizes, put_pat))

    if chart_paths:
        lines.extend(["", "## Charts", ""])
        for p in chart_paths:
            lines.append(f"- `{os.path.basename(p)}`")

    lines.append("")
    return "\n".join(lines)


def _render_work_lines(
    results: Dict[str, Dict[int, Dict[str, Dict[int, Dict[int, PointResult]]]]],
    patterns: Sequence[str],
    sizes: Sequence[int],
    cores: Sequence[int],
    concurrencies: Sequence[int],
    durability: str,
    fast_sizes: Dict[int, Dict[int, Dict[int, PointResult]]],
    put_pat: str,
) -> List[str]:
    """One row per AwanFS phase with counters; nothing when no phase was scraped (#431)."""
    rows: List[str] = []

    def row(mode: str, pat: str, sz: int, n: int, c: int, pt: Optional[PointResult]) -> None:
        if pt is None or not pt.measured or pt.work is None:
            return
        cells = format_work_cells(pt.work)
        rows.append(f"| {mode} | {pat} | {row_size_label(pat, sz)} | {n} | {c} | {pt.ops_per_sec:,.0f} | " + " | ".join(cells) + " |")

    for pat in patterns:
        for sz in sizes:
            for n in cores:
                for c in concurrencies:
                    row(durability, pat, sz, n, c, results.get(pat, {}).get(sz, {}).get("AwanFS", {}).get(n, {}).get(c))
    for sz in [z for z in sizes if z in fast_sizes]:
        for n in cores:
            for c in sorted(fast_sizes[sz].get(n, {})):
                row("fast", put_pat, sz, n, c, fast_sizes[sz][n][c])
    if not rows:
        return []
    return [
        "",
        "## AwanFS work per phase (`/_awanfs/metrics`, before and after each phase)",
        "",
        "Syncs and disk bytes are the server's own counters; rates use the phase's wall time. PUTs per sync is ops/s over syncs/s.",
        "",
        "| Durability | Pattern | Size | Cores | Concurrency | ops/s | syncs/s | PUTs per sync | MiB per sync | disk MiB/s | peak in-flight MiB |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
        *rows,
    ]


_TRIVIAL_HTTP_RS = r"""use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

fn find_header_end(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i + 1 < buf.len() {
        if i + 3 < buf.len() && &buf[i..i + 4] == b"\r\n\r\n" {
            return Some((i, i + 4));
        }
        if &buf[i..i + 2] == b"\n\n" {
            return Some((i, i + 2));
        }
        i += 1;
    }
    None
}

fn trim_ascii(mut s: &[u8]) -> &[u8] {
    while let Some(&b) = s.first() {
        if b == b' ' || b == b'\t' || b == b'\r' {
            s = &s[1..];
        } else {
            break;
        }
    }
    while let Some(&b) = s.last() {
        if b == b' ' || b == b'\t' || b == b'\r' {
            s = &s[..s.len() - 1];
        } else {
            break;
        }
    }
    s
}

fn parse_usize_ascii(s: &[u8]) -> usize {
    let mut v = 0usize;
    for &b in trim_ascii(s) {
        if b.is_ascii_digit() {
            v = v.saturating_mul(10).saturating_add((b - b'0') as usize);
        } else {
            break;
        }
    }
    v
}

fn contains_ascii_ci(hay: &[u8], needle: &[u8]) -> bool {
    if needle.len() > hay.len() {
        return false;
    }
    hay.windows(needle.len()).any(|w| w.eq_ignore_ascii_case(needle))
}

fn handle(mut stream: TcpStream, get_resp: Arc<Vec<u8>>) {
    let _ = stream.set_nodelay(true);
    let mut buf = vec![0u8; 65536];
    let mut start: usize = 0;
    let mut end: usize = 0;

    loop {
        while start < end && (buf[start] == b'\r' || buf[start] == b'\n') {
            start += 1;
        }
        let (hdr_len, hdr_consume) = loop {
            if let Some(pos) = find_header_end(&buf[start..end]) {
                break pos;
            }
            if start > 0 {
                buf.copy_within(start..end, 0);
                end -= start;
                start = 0;
            }
            if end == buf.len() {
                return;
            }
            match stream.read(&mut buf[end..]) {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    end += n;
                    while start < end && (buf[start] == b'\r' || buf[start] == b'\n') {
                        start += 1;
                    }
                }
            }
        };

        let hdr_bytes = &buf[start..start + hdr_len];
        let mut lines = hdr_bytes.split(|&b| b == b'\n');
        let first = lines.next().unwrap_or(b"");
        let is_get = first.starts_with(b"GET ");
        let mut content_len: usize = 0;
        let mut close = false;
        for line in lines {
            if let Some(colon) = line.iter().position(|&b| b == b':') {
                let name = trim_ascii(&line[..colon]);
                let val = &line[colon + 1..];
                if name.eq_ignore_ascii_case(b"content-length") {
                    content_len = parse_usize_ascii(val);
                } else if name.eq_ignore_ascii_case(b"connection") && contains_ascii_ci(val, b"close") {
                    close = true;
                }
            }
        }

        start += hdr_consume;
        let avail = end - start;
        if avail >= content_len {
            start += content_len;
        } else {
            let mut rem = content_len - avail;
            start = 0;
            end = 0;
            while rem > 0 {
                let to_read = rem.min(buf.len());
                match stream.read(&mut buf[..to_read]) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => rem -= n,
                }
            }
        }
        if start == end {
            start = 0;
            end = 0;
        }

        let reply: &[u8] = if is_get {
            &get_resp
        } else {
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
        };
        if stream.write_all(reply).is_err() {
            return;
        }
        if close {
            return;
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let port_file = &args[1];
    let body_size: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4096);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();
    let tmp_file = format!("{}.tmp", port_file);
    std::fs::write(&tmp_file, port.to_string()).expect("write port");
    std::fs::rename(&tmp_file, port_file).expect("rename port");

    let hdr = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body_size);
    let mut get_resp = Vec::with_capacity(hdr.len() + body_size);
    get_resp.extend_from_slice(hdr.as_bytes());
    get_resp.resize(hdr.len() + body_size, b'a');
    let get_resp = Arc::new(get_resp);

    for stream in listener.incoming() {
        if let Ok(s) = stream {
            let r = Arc::clone(&get_resp);
            thread::spawn(move || handle(s, r));
        }
    }
}
"""


def _find_rustc() -> Optional[str]:
    found = shutil.which("rustc")
    if found:
        return found
    exe = ".exe" if sys.platform == "win32" else ""
    candidate = os.path.join(os.path.expanduser("~"), ".cargo", "bin", f"rustc{exe}")
    return candidate if os.path.isfile(candidate) else None


def _trivial_http_server_main(port_file: str, body_size: int) -> int:
    payload = b"a" * body_size

    class TrivialHandler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):
            pass

        def do_PUT(self):
            length = int(self.headers.get("Content-Length", "0"))
            if length > 0:
                self.rfile.read(length)
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(body_size))
            self.end_headers()
            if body_size > 0:
                self.wfile.write(payload)

    httpd = rwb.ThreadedHTTPServer(("127.0.0.1", 0), TrivialHandler)
    port = httpd.server_address[1]
    tmp_file = f"{port_file}.tmp"
    with open(tmp_file, "w", encoding="utf-8") as fh:
        fh.write(str(port))
    os.replace(tmp_file, port_file)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
    return 0


def start_trivial_http_server(
    data_root: str,
    get_size: int = 4 * KIB,
) -> Tuple[subprocess.Popen, int, str]:
    """Starts a local non-S3 HTTP/1.1 keep-alive server for client ceiling probes."""
    os.makedirs(data_root, exist_ok=True)
    exe = ".exe" if sys.platform == "win32" else ""
    rustc = _find_rustc()
    work = tempfile.mkdtemp(prefix="trivial_http_", dir=data_root)
    port_file = os.path.join(work, "port.txt")
    log_path = os.path.join(work, "server.log")

    launch_argv: List[str]
    if rustc:
        cached_bin = os.path.join(data_root, f"trivial_http_srv_v2{exe}")
        if not os.path.isfile(cached_bin):
            src_path = os.path.join(work, "trivial_http.rs")
            tmp_bin = os.path.join(work, f"trivial_http_srv{exe}")
            with open(src_path, "w", encoding="utf-8") as fh:
                fh.write(_TRIVIAL_HTTP_RS)
            subprocess.run([rustc, "-O", src_path, "-o", tmp_bin], check=True)
            os.replace(tmp_bin, cached_bin)
        launch_argv = [cached_bin, port_file, str(get_size)]
    else:
        launch_argv = [sys.executable, os.path.abspath(__file__), "--trivial-http-server", port_file, str(get_size)]

    log_fh = open(log_path, "ab")
    try:
        proc = subprocess.Popen(launch_argv, stdout=log_fh, stderr=subprocess.STDOUT)
    finally:
        log_fh.close()

    deadline = time.time() + 20.0
    port: Optional[int] = None
    while time.time() < deadline:
        code = proc.poll()
        if code is not None:
            tail = _tail_log(log_path)
            raise RuntimeError(f"trivial http server exited with code {code}" + (f": {tail}" if tail else ""))
        if os.path.isfile(port_file):
            try:
                with open(port_file, "r", encoding="utf-8") as fh:
                    raw = fh.read().strip()
                if raw:
                    port = int(raw)
                    break
            except (OSError, ValueError):
                pass
        time.sleep(0.05)
    if port is None:
        _stop_proc(proc)
        raise RuntimeError("trivial http server did not publish its listening port")
    err = _wait_port(proc, port, timeout=10.0)
    if err:
        _stop_proc(proc)
        tail = _tail_log(log_path)
        raise RuntimeError(f"trivial http server {err}" + (f": {tail}" if tail else ""))
    return proc, port, log_path


def run_ceiling_probe(
    data_root: str,
    procs_list: Sequence[int] = (1, 4),
    concurrency: int = 32,
    duration_sec: float = 2.0,
    runs: int = 2,
    size: int = 4 * KIB,
) -> List[CeilingProbeRow]:
    """Measures `procs_list` client processes against a trivial local HTTP server."""
    total_cores = os.cpu_count() or 1
    cli_cores = list(range(total_cores))
    workloads = [
        ("put_4k", f"PUT {format_size(size)}", "put", size),
        ("get_4k", f"GET {format_size(size)}", "get", size),
    ]
    proc, port, log_path = start_trivial_http_server(data_root=data_root, get_size=size)
    raw: Dict[Tuple[str, int], List[PointResult]] = {
        (wkey, k): [] for wkey, *_ in workloads for k in procs_list
    }
    try:
        for wkey, wlabel, mode, sz in workloads:
            for run_idx in range(runs):
                order = list(procs_list) if run_idx % 2 == 0 else list(reversed(procs_list))
                for k in order:
                    print(
                        f"  [ceiling run {run_idx + 1}/{runs}] {wlabel} client_procs={k} c={concurrency}...",
                        flush=True,
                    )
                    pt = _run_phase_with_cpu(
                        "127.0.0.1",
                        port,
                        None,
                        sz,
                        concurrency,
                        duration_sec,
                        mode,
                        proc.pid,
                        shares_cores=True,
                        client_procs=k,
                        cli_cores=cli_cores,
                        request_path="/obj",
                        force_subprocess=True,
                        data_root=data_root,
                    )
                    raw[(wkey, k)].append(pt)
    finally:
        _stop_proc(proc)
        work_dir = os.path.dirname(log_path)
        shutil.rmtree(work_dir, ignore_errors=True)

    rows: List[CeilingProbeRow] = []
    for wkey, wlabel, _mode, sz in workloads:
        for k in procs_list:
            rows.append(
                CeilingProbeRow(
                    workload=wlabel,
                    size_bytes=sz,
                    concurrency=concurrency,
                    client_procs=k,
                    client_cores=len(cli_cores),
                    result=aggregate_runs(raw[(wkey, k)]),
                )
            )
    return rows


def render_ceiling_report(
    cmd: str,
    rows: Sequence[CeilingProbeRow],
    procs_list: Sequence[int],
    concurrency: int,
    duration_sec: float,
    runs: int,
    total_cores: int,
    size: int = 4 * KIB,
) -> str:
    server_kind = "Rust std::net HTTP/1.1 keep-alive (benchmarks/, no S3)" if _find_rustc() else "Python HTTP/1.1 keep-alive (benchmarks/, no S3)"
    base_k = 1 if 1 in procs_list else min(procs_list)
    by_wl_k: Dict[str, Dict[int, PointResult]] = {}
    workload_order: List[str] = []
    for row in rows:
        if row.workload not in by_wl_k:
            by_wl_k[row.workload] = {}
            workload_order.append(row.workload)
        by_wl_k[row.workload][row.client_procs] = row.result

    lines = [
        f"Command: `{cmd}`",
        "",
        f"# Client Ceiling Probe: Trivial Local HTTP Server ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"- **Machine:** {platform.node()} ({platform.platform()}, {total_cores} logical cores, {rwb.host_ram_gb()} GB RAM, CPU: {rwb.cpu_model()})",
        f"- **Server:** {server_kind}",
        f"- **Methodology:** `client_procs = {', '.join(str(k) for k in procs_list)}`, `concurrency = {concurrency}`, size = {format_size(size)}, {runs} alternating runs per point (median reported), {duration_sec:.1f}s per phase, client cores given = {total_cores}.",
        "",
        "## Client Process Scaling (`100%` CPU = 1 core busy)",
        "",
        "| Workload | Concurrency | Client Procs | Client Cores | ops/s | MiB/s | P50 (ms) | P99 (ms) | Client CPU % | Server CPU % | Speedup vs 1 proc |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]

    verdicts: List[str] = []
    for row in rows:
        pt = row.result
        base_pt = by_wl_k[row.workload].get(base_k)
        if pt.measured:
            mib_s = f"{pt.mib_per_sec:.2f}" if pt.mib_per_sec is not None else "-"
            cli_s = f"{pt.client_cpu_pct:.1f}% ({(pt.client_cpu_pct / (row.client_cores * 100.0)) * 100.0:.0f}%)" if pt.client_cpu_pct is not None else "-"
            srv_s = f"{pt.cpu_pct:.1f}%" if pt.cpu_pct is not None else "-"
            if row.client_procs == base_k:
                sp_s = "1.00x"
            elif base_pt and base_pt.measured and base_pt.ops_per_sec and base_pt.ops_per_sec > 0:
                sp_s = f"{pt.ops_per_sec / base_pt.ops_per_sec:.2f}x"
            else:
                sp_s = "-"
            lines.append(
                f"| {row.workload} | {row.concurrency} | {row.client_procs} | {row.client_cores} | "
                f"{pt.ops_per_sec:,.0f} | {mib_s} | {pt.p50_ms:.2f} | {pt.p99_ms:.2f} | {cli_s} | {srv_s} | {sp_s} |"
            )
        else:
            lines.append(
                f"| {row.workload} | {row.concurrency} | {row.client_procs} | {row.client_cores} | "
                f"not measured ({pt.reason or 'unknown'}) | - | - | - | - | - | - |"
            )

    max_k = max(procs_list)
    for wl in workload_order:
        b = by_wl_k[wl].get(base_k)
        t = by_wl_k[wl].get(max_k)
        if b and t and b.measured and t.measured and b.ops_per_sec and b.ops_per_sec > 0:
            ratio = t.ops_per_sec / b.ops_per_sec
            verdicts.append(f"{wl}: {ratio:.2f}x ({b.ops_per_sec:,.0f} -> {t.ops_per_sec:,.0f} ops/s)")
    if verdicts:
        lines.extend(["", f"- **Scaling ({max_k}/{base_k} client procs):** " + "; ".join(verdicts) + "."])

    lines.append("")
    return "\n".join(lines)


def _default_awanfs_bin() -> Optional[str]:
    exe = ".exe" if sys.platform == "win32" else ""
    p = os.path.join(REPO_ROOT, "server", "target", "release", f"s3-server{exe}")
    return p if os.path.isfile(p) else None


def _resolve_profile_config(
    a: argparse.Namespace,
    cores: Sequence[int],
    total_cores: int,
) -> Tuple[str, set, bool]:
    durability = getattr(a, "durability", "group")
    if durability not in DURABILITIES:
        raise ValueError(f"invalid durability {durability!r}; expected one of {', '.join(DURABILITIES)}")
    auto = should_auto_profile_put(total_cores, cores)
    prof_spec = getattr(a, "profile_threads_cores", "") or ""
    if prof_spec.strip():
        profile_cores = set(parse_cores(prof_spec, total_cores))
    else:
        profile_cores = set(DEFAULT_PROFILE_THREAD_CORES) if auto else set()
    cmp_flag = getattr(a, "compare_fast", None)
    # Benchmarks run every server at its defaults (owner, 2026-11-20): the fast pass runs only
    # when asked for by name. bench-scaling.yml asks for it (owner, 2026-11-26).
    compare_fast = bool(cmp_flag) and (durability != "fast")
    return durability, profile_cores, compare_fast


def _starter_for(
    srv: str,
    n: int,
    data_root: str,
    awanfs_bin: Optional[str],
    minio_bin: Optional[str],
    mc_bin: Optional[str],
    rustfs_bin: Optional[str],
    garage_bin: Optional[str],
    seaweedfs_bin: Optional[str],
    memory_budget: Optional[str] = None,
    durability: str = "group",
) -> Callable[[], Tuple]:
    if srv == "AwanFS":
        return lambda: _start_awanfs(awanfs_bin, n, data_root, memory_budget=memory_budget, durability=durability)
    if srv == "MinIO":
        return lambda: _start_minio(minio_bin, mc_bin, n, data_root)
    if srv == "RustFS":
        return lambda: _start_rustfs(rustfs_bin, mc_bin, n, data_root)
    if srv == "Garage":
        return lambda: _start_garage(garage_bin, n, data_root)
    if srv == "SeaweedFS":
        return lambda: _start_seaweedfs(seaweedfs_bin, n, data_root)
    raise ValueError(f"unknown server {srv!r}")


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
    garage_bin: Optional[str],
    seaweedfs_bin: Optional[str],
) -> int:
    patterns = parse_patterns(a.pattern)
    sizes = parse_sizes(a.sizes)
    concurrencies = parse_concurrencies(a.concurrencies)
    prefill_bytes = parse_size(a.prefill_size)
    prefill_cache_check(a.memory_budget, a.prefill_size)
    durability, profile_cores, compare_fast = _resolve_profile_config(a, cores, total_cores)
    put_pat = "seq-write" if "seq-write" in patterns else ("rand-write" if "rand-write" in patterns else None)
    servers = parse_servers(getattr(a, "servers", "") or "")
    fast_every = bool(getattr(a, "fast_every_concurrency", False))
    if "AwanFS" not in servers:
        compare_fast = False

    orig_affinity = get_process_affinity(os.getpid())
    raw_runs: Dict[str, Dict[int, Dict[str, Dict[int, Dict[int, List[PointResult]]]]]] = {
        pat: {
            sz: {
                srv: {n: {c: [] for c in concurrencies} for n in cores}
                for srv in servers
            }
            for sz in sizes
        }
        for pat in patterns
    }

    # {size: {cores: {concurrency: runs}}}: the fast pass runs every PUT size (owner, 2026-11-26).
    fast_raw: Optional[Dict[int, Dict[int, Dict[int, List[PointResult]]]]] = None
    fast_concs_by_core: Dict[int, List[int]] = {}
    if compare_fast and put_pat is not None:
        fast_raw = {sz: {} for sz in sizes}
        for n in cores:
            targets = fast_concurrencies_for(n, concurrencies, every=fast_every)
            fast_concs_by_core[n] = targets
            for sz in sizes:
                fast_raw[sz][n] = {c: [] for c in targets}

    try:
        for n in cores:
            if n > total_cores:
                reason = f"requested {n} cores > {total_cores} available"
                for pat in patterns:
                    for sz in sizes:
                        for srv in servers:
                            for c in concurrencies:
                                raw_runs[pat][sz][srv][n][c].append(PointResult(reason=reason))
                if fast_raw is not None:
                    for sz in fast_raw:
                        for c in fast_concs_by_core[n]:
                            fast_raw[sz][n][c].append(PointResult(reason=reason))
                continue

            cli_cores, shares = client_cores_for(n, total_cores)
            prof_points: Optional[List[Tuple[str, int, int]]] = None
            if n in profile_cores and put_pat is not None and (4 * KIB) in sizes:
                prof_points = [
                    (put_pat, 4 * KIB, c)
                    for c in sorted({_pick_target(concurrencies, 64), _pick_target(concurrencies, 16 * n)})
                ]

            for run_idx in range(a.runs):
                order = server_order_for_run(run_idx, servers)
                for srv in order:
                    print(
                        f"[N={n}, run {run_idx + 1}/{a.runs}] Starting {srv} "
                        f"(client cores={cli_cores}, shares={shares})...",
                        flush=True,
                    )
                    starter = _starter_for(
                        srv, n, data_root, awanfs_bin, minio_bin, mc_bin, rustfs_bin,
                        garage_bin, seaweedfs_bin, memory_budget=a.memory_budget, durability=durability,
                    )
                    srv_prof = prof_points if srv == "AwanFS" else None

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
                        profile_threads_points=srv_prof,
                        client_procs=a.client_procs,
                        data_root=data_root,
                        scrape_work=(srv == "AwanFS"),
                    )
                    for (pat, sz, c), pt in phase_map.items():
                        raw_runs[pat][sz][srv][n][c].append(pt)

            if fast_raw is not None and put_pat is not None:
                targets = fast_concs_by_core[n]
                fast_prof = [(put_pat, 4 * KIB, c) for c in targets] if (n in profile_cores and (4 * KIB) in sizes) else None
                for run_idx in range(a.runs):
                    print(
                        f"[N={n}, run {run_idx + 1}/{a.runs}] Starting AwanFS (--durability fast) "
                        f"(client cores={cli_cores}, shares={shares})...",
                        flush=True,
                    )
                    fast_map = measure_server_matrix_for_cores(
                        server_name="AwanFS (--durability fast)",
                        starter=lambda n=n: _start_awanfs(
                            awanfs_bin, n, data_root, memory_budget=a.memory_budget, durability="fast"
                        ),
                        n_cores=n,
                        cli_cores=cli_cores,
                        shares_cores=shares,
                        patterns=[put_pat],
                        sizes=list(sizes),
                        concurrencies=targets,
                        duration_sec=a.duration,
                        seed=a.seed,
                        prefill_bytes=prefill_bytes,
                        profile_threads_points=fast_prof,
                        client_procs=a.client_procs,
                        data_root=data_root,
                        scrape_work=True,
                    )
                    for (_pat, sz, c), pt in fast_map.items():
                        fast_raw[sz][n][c].append(pt)
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
                for srv in servers
            }
            for sz in sizes
        }
        for pat in patterns
    }
    fast_by_size: Optional[Dict[int, Dict[int, Dict[int, PointResult]]]] = None
    if fast_raw is not None:
        fast_by_size = {
            sz: {n: {c: aggregate_runs(fast_raw[sz][n][c]) for c in fast_concs_by_core[n]} for n in cores}
            for sz in fast_raw
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
        durability=durability,
        client_procs=a.client_procs,
        fast_by_size=fast_by_size,
        servers=servers,
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
    ap.add_argument("--client-procs", type=int, default=4, help="Client worker processes used to drive load (default: 4)")
    ap.add_argument("--ceiling-probe", action="store_true", help="Run local trivial HTTP server ceiling probe (1 vs 4 client procs)")
    ap.add_argument("--ceiling-procs", default="1,4", help="Comma-separated client process counts for --ceiling-probe (default: 1,4)")
    ap.add_argument("--ceiling-concurrency", type=int, default=32, help="Total concurrency for --ceiling-probe (default: 32)")
    ap.add_argument("--client-worker", default="", help=argparse.SUPPRESS)
    ap.add_argument("--trivial-http-server", nargs=2, metavar=("PORT_FILE", "BODY_SIZE"), help=argparse.SUPPRESS)
    ap.add_argument("--matrix", action="store_true", help="Run pattern x size x concurrency x cores matrix (#251)")
    ap.add_argument("--pattern", default="", help="Comma-separated patterns: seq-write,rand-write,seq-read,rand-read")
    ap.add_argument("--sizes", default="", help="Comma-separated object sizes (e.g. 4KiB,1MiB or 4096,1048576)")
    ap.add_argument("--concurrencies", default="", help="Comma-separated concurrencies (e.g. 1,4,16,64,256)")
    ap.add_argument("--servers", default="", help=f"Matrix only: comma-separated subset of {', '.join(SERVERS)} (default: all)")
    ap.add_argument("--fast-every-concurrency", action="store_true",
                    help="Matrix only: run AwanFS --durability fast at every listed concurrency, not two per core count")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED, help=f"RNG seed for random patterns and prefill (default: {DEFAULT_SEED})")
    ap.add_argument("--memory-budget", default=DEFAULT_MEMORY_BUDGET, help="AwanFS --memory-budget (default: none passed, the server's own)")
    ap.add_argument("--prefill-size", default=DEFAULT_PREFILL_SIZE, help=f"Total prefill size per read set, > cache (default: {DEFAULT_PREFILL_SIZE})")
    ap.add_argument("--durability", default="group", choices=DURABILITIES, help="AwanFS --durability mode (default: group)")
    ap.add_argument("--compare-fast", dest="compare_fast", action="store_true", default=None,
                    help="Also measure AwanFS --durability fast on PUT 4 KiB")
    ap.add_argument("--no-compare-fast", dest="compare_fast", action="store_false",
                    help="Do not measure AwanFS --durability fast (the default)")
    ap.add_argument("--profile-threads-cores", default="",
                    help="Comma-separated core counts where AwanFS per-thread CPU is sampled during PUT 4 KiB (default: 4,8 on 16+ core split-client runs)")
    ap.add_argument("--no-fetch-minio", action="store_true", help="Do not auto-fetch missing MinIO/mc binaries from GitHub Releases")
    ap.add_argument("--server-bin", default="", help="Path to AwanFS release s3-server binary")
    ap.add_argument("--minio-bin", default="", help="Path to MinIO binary")
    ap.add_argument("--mc-bin", default="", help="Path to MinIO mc binary")
    ap.add_argument("--rustfs-bin", default="", help="Path to RustFS binary")
    ap.add_argument("--garage-bin", default="", help="Path to Garage binary")
    ap.add_argument("--seaweedfs-bin", default="", help="Path to SeaweedFS weed binary")
    ap.add_argument("--data-dir", default="", help="Directory for temporary server data and sync_probe (defaults to temp dir)")
    ap.add_argument("--skip-sync-probe", action="store_true", help="Skip disk sync_probe MAX baseline")
    ap.add_argument("--output", "--report", dest="output", default="core_scaling.md",
                    help="Markdown report path (PNG and SVG charts are written alongside it)")
    a = ap.parse_args(argv)

    if a.client_worker:
        return _client_worker_main(a.client_worker)
    if a.trivial_http_server:
        return _trivial_http_server_main(a.trivial_http_server[0], int(a.trivial_http_server[1]))
    if a.client_procs < 1:
        ap.error("--client-procs must be >= 1")
    if a.ceiling_concurrency < 1:
        ap.error("--ceiling-concurrency must be >= 1")

    raw_args = list(argv) if argv is not None else sys.argv[1:]
    cmd = "python benchmarks/core_scaling.py" + (" " + " ".join(raw_args) if raw_args else "")

    total_cores = os.cpu_count() or 1
    data_root = os.path.abspath(a.data_dir) if a.data_dir else tempfile.gettempdir()
    os.makedirs(data_root, exist_ok=True)

    if a.ceiling_probe:
        procs_list = parse_cores(a.ceiling_procs, total_cores=1024)
        rows = run_ceiling_probe(
            data_root=data_root,
            procs_list=procs_list,
            concurrency=a.ceiling_concurrency,
            duration_sec=a.duration,
            runs=a.runs,
            size=4 * KIB,
        )
        report = render_ceiling_report(
            cmd=cmd,
            rows=rows,
            procs_list=procs_list,
            concurrency=a.ceiling_concurrency,
            duration_sec=a.duration,
            runs=a.runs,
            total_cores=total_cores,
            size=4 * KIB,
        )
        out_abs = os.path.abspath(a.output)
        os.makedirs(os.path.dirname(out_abs), exist_ok=True)
        with open(out_abs, "w", encoding="utf-8") as fh:
            fh.write(report)
        print(report)
        print(f"Report written to {out_abs}")
        return 0

    cores = parse_cores(a.cores, total_cores)
    awanfs_bin = a.server_bin or _default_awanfs_bin()
    auto_minio, auto_mc = rwb.find_minio_binaries()
    minio_bin = a.minio_bin or auto_minio
    mc_bin = a.mc_bin or auto_mc
    if not a.no_fetch_minio and (not minio_bin or not mc_bin):
        minio_bin, mc_bin = ensure_minio_binaries(minio_bin, mc_bin, auto_fetch=True)
    rustfs_bin = a.rustfs_bin or rwb.find_rustfs_binary()
    garage_bin = a.garage_bin or find_garage_binary()
    seaweedfs_bin = a.seaweedfs_bin or find_seaweedfs_binary()

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
            garage_bin=garage_bin,
            seaweedfs_bin=seaweedfs_bin,
        )

    durability, profile_cores, compare_fast = _resolve_profile_config(a, cores, total_cores)
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
    raw_fast_runs: Optional[Dict[str, Dict[int, List[PointResult]]]] = (
        {wk: {n: [] for n in cores} for wk, *_ in PUT_WORKLOADS} if compare_fast else None
    )

    try:
        for n in cores:
            if n > total_cores:
                reason = f"requested {n} cores > {total_cores} available"
                for wkey, *_ in WORKLOADS:
                    for srv in SERVERS:
                        raw_runs[wkey][srv][n].append(PointResult(reason=reason))
                if raw_fast_runs is not None:
                    for wk in raw_fast_runs:
                        raw_fast_runs[wk][n].append(PointResult(reason=reason))
                continue

            cli_cores, shares = client_cores_for(n, total_cores)
            for run_idx in range(a.runs):
                order = server_order_for_run(run_idx, SERVERS)
                for srv in order:
                    print(f"[N={n}, run {run_idx + 1}/{a.runs}] Starting {srv} (client cores={cli_cores}, shares={shares}, client_procs={a.client_procs})...", flush=True)
                    starter = _starter_for(
                        srv, n, data_root, awanfs_bin, minio_bin, mc_bin, rustfs_bin,
                        garage_bin, seaweedfs_bin, durability=durability,
                    )
                    sample_srv_threads = srv == "AwanFS" and n in profile_cores

                    phase_map = measure_server_for_cores(
                        srv, starter, n, cli_cores, shares, a.duration,
                        profile_threads=sample_srv_threads,
                        client_procs=a.client_procs,
                        data_root=data_root,
                    )
                    for wkey, pt in phase_map.items():
                        raw_runs[wkey][srv][n].append(pt)

            if raw_fast_runs is not None:
                for run_idx in range(a.runs):
                    print(f"[N={n}, run {run_idx + 1}/{a.runs}] Starting AwanFS (--durability fast) (client cores={cli_cores}, shares={shares}, client_procs={a.client_procs})...", flush=True)
                    fast_map = measure_server_for_cores(
                        "AwanFS (--durability fast)",
                        lambda n=n: _start_awanfs(awanfs_bin, n, data_root, durability="fast"),
                        n,
                        cli_cores,
                        shares,
                        a.duration,
                        workloads=PUT_WORKLOADS,
                        profile_threads=(n in profile_cores),
                        client_procs=a.client_procs,
                        data_root=data_root,
                    )
                    for wk in raw_fast_runs:
                        raw_fast_runs[wk][n].append(fast_map[wk])
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
    fast_results: Optional[Dict[str, Dict[int, PointResult]]] = (
        {wk: {n: aggregate_runs(runs) for n, runs in by_n.items()} for wk, by_n in raw_fast_runs.items()}
        if raw_fast_runs is not None else None
    )

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
        fast_results=fast_results,
        durability=durability,
        client_procs=a.client_procs,
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
