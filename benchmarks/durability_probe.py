"""Linux strace durability probe and 64 KiB PUT throughput comparison (#334).

    python3 benchmarks/durability_probe.py --server-bin server/target/release/awanfs

Starts each default server (AwanFS --durability group, MinIO, RustFS at release defaults) on
Linux and runs three isolated passes:
1. 200 PUTs of 64 KiB (16 concurrent) with `strace -f -c -e trace=fsync,fdatasync,sync_file_range,syncfs,msync`
   attached after startup and bucket creation.
2. 200 PUTs of 64 KiB (16 concurrent) with `strace -f -e trace=openat` attached, counting
   successful opens carrying `O_DSYNC` or `O_SYNC`.
3. A separate un-straced pass (64 KiB, 64 workers, 10 s) using `run_warp_benchmark.run_workload_phase`.

Note: the Python client caps near 5,000 requests a second (#262).
"""
import argparse
from dataclasses import dataclass
import http.client
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import core_scaling as cs  # noqa: E402
import run_warp_benchmark as rwb  # noqa: E402

SYNC_SYSCALLS: Tuple[str, ...] = ("fsync", "fdatasync", "sync_file_range", "syncfs", "msync")
SYNC_TRACE_EXPR = ",".join(SYNC_SYSCALLS)
BUCKET = "durability-probe"
PUT_SIZE_BYTES = 64 * 1024
TRACE_PUT_COUNT = 200
TRACE_CONCURRENCY = 16
THROUGHPUT_WORKERS = 64
THROUGHPUT_SECONDS = 10.0

_PID_PREFIX_RE = re.compile(r"^\s*\[pid\s+(\d+)\]\s*(.*)$")
_QUOTED_STR_RE = re.compile(r'"(?:\\.|[^"\\])*"')
_OPENAT_RET_RE = re.compile(r"\)\s*=\s*(\d+)\b")
_SYNC_FLAG_RE = re.compile(r"\b(?:O_DSYNC|O_SYNC)\b")


class StraceAttachError(RuntimeError):
    """Raised when strace is unavailable or fails to attach to a running server."""


@dataclass(frozen=True)
class ServerSpec:
    name: str
    kind: str
    documented_syncs: bool


@dataclass(frozen=True)
class ProbeRow:
    server: str
    puts: int
    sync_calls: int
    sync_opens: int
    syncs_per_put: float
    opens_per_put: float
    label: str
    put_ops_per_sec: Optional[float]
    version: str = ""
    note: str = ""


def default_servers() -> Tuple[ServerSpec, ...]:
    """Return the three default server configurations narrowed by the 2026-11-20 owner decision."""
    return (
        ServerSpec(name="AwanFS (--durability group)", kind="awanfs", documented_syncs=True),
        ServerSpec(name="MinIO", kind="minio", documented_syncs=True),
        ServerSpec(name="RustFS (release defaults)", kind="rustfs", documented_syncs=False),
    )


def parse_strace_summary(text: str) -> int:
    """Parse `strace -c` summary text and sum calls across `SYNC_SYSCALLS`."""
    if not text.strip():
        return 0
    lines = text.splitlines()
    if not any("syscall" in ln and "calls" in ln for ln in lines):
        raise ValueError(f"Unrecognized strace -c summary output: {lines[0][:120]!r}")

    calls_idx = 3
    for ln in lines:
        hdr_parts = ln.split()
        if "calls" in hdr_parts and hdr_parts[-1] == "syscall":
            # `% time` splits into two tokens (`%` and `time`), shifting header indices by +1.
            calls_idx = hdr_parts.index("calls") - (1 if hdr_parts[:2] == ["%", "time"] else 0)
            break

    total = 0
    for raw in lines:
        parts = raw.split()
        if len(parts) < 5:
            continue
        syscall = parts[-1]
        if syscall not in SYNC_SYSCALLS:
            continue
        if calls_idx >= len(parts) - 1 or not parts[calls_idx].isdigit():
            raise ValueError(f"Malformed strace -c row: {raw!r}")
        total += int(parts[calls_idx])
    return total


def parse_openat_sync_opens(text: str) -> int:
    """Count successful `openat` calls carrying `O_DSYNC` or `O_SYNC` in `strace -f` output."""
    pending: Dict[Optional[str], str] = {}
    count = 0
    for raw in text.splitlines():
        line = raw.rstrip("\r\n")
        m_pid = _PID_PREFIX_RE.match(line)
        if m_pid:
            pid: Optional[str] = m_pid.group(1)
            body = m_pid.group(2)
        else:
            pid = None
            body = line.lstrip()

        if body.endswith("<unfinished ...>"):
            pending[pid] = body[: -len("<unfinished ...>")]
            continue
        if body.startswith("<... openat resumed>"):
            body = pending.pop(pid, "") + body[len("<... openat resumed>"):]
        if not body.startswith("openat("):
            continue

        ret_m = _OPENAT_RET_RE.search(body)
        if not ret_m:
            continue
        args = body[len("openat("):ret_m.start()]
        args_without_paths = _QUOTED_STR_RE.sub('""', args)
        if _SYNC_FLAG_RE.search(args_without_paths):
            count += 1
    return count


def classify_durability(puts: int, sync_calls: int, sync_opens: int) -> Tuple[float, float, str]:
    """Return `(syncs_per_put, opens_per_put, label)`.

    At least one sync call or synchronous open per PUT is labelled `durable`; otherwise `not durable`.
    """
    if puts <= 0:
        raise ValueError(f"puts must be positive, got {puts}")
    syncs_per_put = sync_calls / puts
    opens_per_put = sync_opens / puts
    label = "durable" if (syncs_per_put + opens_per_put) >= 1.0 else "not durable"
    return syncs_per_put, opens_per_put, label


def build_probe_row(
    server: str,
    puts: int,
    sync_calls: int,
    sync_opens: int,
    put_ops_per_sec: Optional[float],
    version: str = "",
    note: str = "",
) -> ProbeRow:
    syncs_per_put, opens_per_put, label = classify_durability(puts, sync_calls, sync_opens)
    return ProbeRow(
        server=server,
        puts=puts,
        sync_calls=sync_calls,
        sync_opens=sync_opens,
        syncs_per_put=syncs_per_put,
        opens_per_put=opens_per_put,
        label=label,
        put_ops_per_sec=put_ops_per_sec,
        version=version,
        note=note,
    )


def _start_logged(argv: List[str], log_path: str, env: Optional[dict] = None) -> subprocess.Popen:
    kwargs: Dict[str, object] = {}
    if sys.platform != "win32":
        kwargs["start_new_session"] = True
    log = open(log_path, "ab")
    try:
        return subprocess.Popen(argv, env=env, stdout=log, stderr=subprocess.STDOUT, **kwargs)
    finally:
        log.close()


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


def _start_server(
    spec: ServerSpec,
    data_root: str,
    server_bin: str,
    minio_bin: Optional[str],
    mc_bin: Optional[str],
    rustfs_bin: Optional[str],
) -> Tuple[subprocess.Popen, int, str, str]:
    """Start `spec`, create `BUCKET`, verify anonymous PUT works, return `(proc, port, data_dir, version)`."""
    data_dir = tempfile.mkdtemp(prefix=f"dprobe_{spec.kind}_", dir=data_root)
    port = rwb.get_free_port()
    log_path = os.path.join(data_dir, "server.log")
    mc_home = os.path.join(data_dir, "mc_home")

    if spec.kind == "awanfs":
        if not os.path.isfile(server_bin):
            shutil.rmtree(data_dir, ignore_errors=True)
            raise RuntimeError(f"awanfs binary not found: {server_bin}")
        argv = [server_bin, "--port", str(port), "--data-dir", data_dir, "--allow-anonymous", "--durability", "group"]
        proc = _start_logged(argv, log_path)
        ver = rwb.binary_version(server_bin)
    elif spec.kind == "minio":
        if not minio_bin or not os.path.isfile(minio_bin) or not mc_bin or not os.path.isfile(mc_bin):
            shutil.rmtree(data_dir, ignore_errors=True)
            raise RuntimeError("minio or mc binary not found")
        console_port = rwb.get_free_port()
        env = dict(
            os.environ,
            MINIO_ROOT_USER="minioadmin",
            MINIO_ROOT_PASSWORD="minioadmin",
            MINIO_UPDATE="off",
            MINIO_BROWSER="off",
        )
        argv = [minio_bin, "server", data_dir, "--address", f"127.0.0.1:{port}", "--console-address", f"127.0.0.1:{console_port}"]
        proc = _start_logged(argv, log_path, env=env)
        ver = rwb.binary_version(minio_bin)
    elif spec.kind == "rustfs":
        if not rustfs_bin or not os.path.isfile(rustfs_bin) or not mc_bin or not os.path.isfile(mc_bin):
            shutil.rmtree(data_dir, ignore_errors=True)
            raise RuntimeError("rustfs or mc binary not found")
        env = dict(os.environ, RUSTFS_ACCESS_KEY="rustfsadmin", RUSTFS_SECRET_KEY="rustfsadmin")
        env.pop("RUSTFS_DURABILITY_MODE", None)
        argv = [rustfs_bin, "server", "--address", f"127.0.0.1:{port}", "--access-key", "rustfsadmin", "--secret-key", "rustfsadmin", data_dir]
        proc = _start_logged(argv, log_path, env=env)
        ver = rwb.binary_version(rustfs_bin)
    else:
        shutil.rmtree(data_dir, ignore_errors=True)
        raise ValueError(f"Unknown server kind: {spec.kind!r}")

    err = cs._wait_port(proc, port, timeout=30.0)
    if err:
        cs._stop_proc(proc)
        tail = cs._tail_log(log_path, max_chars=400)
        shutil.rmtree(data_dir, ignore_errors=True)
        raise RuntimeError(f"{spec.name} {err}" + (f": {tail}" if tail else ""))

    try:
        if spec.kind == "awanfs":
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10.0)
            try:
                conn.request("PUT", f"/{BUCKET}")
                resp = conn.getresponse()
                resp.read()
                if resp.status not in (200, 409):
                    raise RuntimeError(f"bucket PUT returned HTTP {resp.status}")
            finally:
                conn.close()
        elif spec.kind == "minio":
            _setup_mc_bucket(mc_bin, f"dp_minio_{port}", port, "minioadmin", "minioadmin", mc_home)
        else:
            _setup_mc_bucket(mc_bin, f"dp_rustfs_{port}", port, "rustfsadmin", "rustfsadmin", mc_home)

        if not rwb.anonymous_put_ok("127.0.0.1", port, BUCKET):
            raise RuntimeError("anonymous PUT probe failed")
    except Exception:
        cs._stop_proc(proc)
        shutil.rmtree(data_dir, ignore_errors=True)
        raise

    return proc, port, data_dir, ver


def _send_concurrent_puts(
    host: str,
    port: int,
    bucket: str,
    count: int,
    concurrency: int,
    size: int,
    prefix: str,
) -> None:
    payload = os.urandom(size)
    lock = threading.Lock()
    next_idx = 0
    errors: List[str] = []

    def worker() -> None:
        nonlocal next_idx
        conn = http.client.HTTPConnection(host, port, timeout=30.0)
        headers = {"Content-Length": str(size), "Connection": "keep-alive"}
        try:
            while True:
                with lock:
                    if errors or next_idx >= count:
                        return
                    idx = next_idx
                    next_idx += 1
                path = f"/{bucket}/{prefix}_{idx}.dat"
                try:
                    conn.request("PUT", path, body=payload, headers=headers)
                    resp = conn.getresponse()
                    resp.read()
                    if resp.status != 200:
                        with lock:
                            errors.append(f"PUT {path}: HTTP {resp.status}")
                        return
                except Exception as exc:  # noqa: BLE001
                    with lock:
                        errors.append(f"PUT {path}: {type(exc).__name__}: {exc}")
                    return
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120.0)
    alive = sum(1 for t in threads if t.is_alive())
    if alive:
        raise RuntimeError(f"{alive} PUT worker(s) did not finish within 120s")
    if errors:
        raise RuntimeError(errors[0])


def _read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return fh.read()
    except OSError:
        return ""


def _wait_strace_attached(strace_proc: subprocess.Popen, strace_log: str, server_proc: subprocess.Popen, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        log_text = _read_text(strace_log)
        if "Operation not permitted" in log_text or "Cannot attach" in log_text:
            raise StraceAttachError(log_text.strip())
        if "attached" in log_text:
            time.sleep(0.05)
            return
        if strace_proc.poll() is not None:
            raise StraceAttachError(f"strace exited ({strace_proc.returncode}) before attaching:\n{log_text.strip()}")
        if server_proc.poll() is not None:
            raise RuntimeError(f"server exited ({server_proc.returncode}) while waiting for strace")
        time.sleep(0.02)
    log_text = _read_text(strace_log)
    strace_proc.terminate()
    try:
        strace_proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        strace_proc.kill()
        strace_proc.wait(timeout=5.0)
    raise StraceAttachError(f"strace did not report attach within {timeout:.0f}s:\n{log_text.strip()}")


def _run_strace_pass(
    spec: ServerSpec,
    mode: str,
    data_root: str,
    server_bin: str,
    minio_bin: Optional[str],
    mc_bin: Optional[str],
    rustfs_bin: Optional[str],
    puts: int,
    concurrency: int,
    size: int,
) -> Tuple[int, str]:
    strace_bin = shutil.which("strace")
    if not strace_bin:
        raise StraceAttachError("strace binary not found in PATH")

    proc, port, data_dir, ver = _start_server(spec, data_root, server_bin, minio_bin, mc_bin, rustfs_bin)
    trace_out = os.path.join(data_dir, f"strace_{mode}.txt")
    strace_log = os.path.join(data_dir, f"strace_{mode}.log")
    try:
        if mode == "summary":
            cmd = [strace_bin, "-f", "-c", "-e", f"trace={SYNC_TRACE_EXPR}", "-o", trace_out, "-p", str(proc.pid)]
        elif mode == "openat":
            cmd = [strace_bin, "-f", "-e", "trace=openat", "-o", trace_out, "-p", str(proc.pid)]
        else:
            raise ValueError(f"Unknown strace mode: {mode!r}")

        strace_proc = _start_logged(cmd, strace_log)
        try:
            _wait_strace_attached(strace_proc, strace_log, proc)
            _send_concurrent_puts("127.0.0.1", port, BUCKET, puts, concurrency, size, f"dp_{mode}")
        finally:
            if strace_proc.poll() is None:
                strace_proc.send_signal(signal.SIGINT)
                try:
                    strace_proc.wait(timeout=30.0)
                except subprocess.TimeoutExpired:
                    strace_proc.terminate()
                    strace_proc.wait(timeout=5.0)

        log_text = _read_text(strace_log)
        if "Operation not permitted" in log_text or "Cannot attach" in log_text:
            raise StraceAttachError(log_text.strip())
        if strace_proc.returncode not in (0, -signal.SIGINT, 128 + signal.SIGINT):
            raise StraceAttachError(f"strace {mode} failed (exit {strace_proc.returncode}):\n{log_text.strip()}")

        out_text = _read_text(trace_out)
        count = parse_strace_summary(out_text) if mode == "summary" else parse_openat_sync_opens(out_text)
        return count, ver
    finally:
        cs._stop_proc(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


def _run_throughput_pass(
    spec: ServerSpec,
    data_root: str,
    server_bin: str,
    minio_bin: Optional[str],
    mc_bin: Optional[str],
    rustfs_bin: Optional[str],
    workers: int,
    duration_sec: float,
    size: int,
) -> Tuple[float, str]:
    proc, port, data_dir, ver = _start_server(spec, data_root, server_bin, minio_bin, mc_bin, rustfs_bin)
    try:
        m = rwb.run_workload_phase("127.0.0.1", port, BUCKET, size, workers, duration_sec, "put", server_pid=proc.pid)
        if not m.measured:
            raise RuntimeError(f"throughput pass failed for {spec.name}: {m.note}")
        return m.ops_per_sec, ver
    finally:
        cs._stop_proc(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


def probe_server(
    spec: ServerSpec,
    data_root: str,
    server_bin: str,
    minio_bin: Optional[str],
    mc_bin: Optional[str],
    rustfs_bin: Optional[str],
    puts: int = TRACE_PUT_COUNT,
    trace_concurrency: int = TRACE_CONCURRENCY,
    size: int = PUT_SIZE_BYTES,
    throughput_workers: int = THROUGHPUT_WORKERS,
    throughput_seconds: float = THROUGHPUT_SECONDS,
) -> ProbeRow:
    print(f"Probing {spec.name}: pass 1 (strace -c sync syscalls, {puts} PUTs @ c={trace_concurrency})...", flush=True)
    sync_calls, ver1 = _run_strace_pass(
        spec, "summary", data_root, server_bin, minio_bin, mc_bin, rustfs_bin, puts, trace_concurrency, size
    )
    print(f"  -> sync syscalls: {sync_calls} ({sync_calls / puts:.2f}/PUT)", flush=True)

    print(f"Probing {spec.name}: pass 2 (strace openat O_DSYNC/O_SYNC, {puts} PUTs @ c={trace_concurrency})...", flush=True)
    sync_opens, _ = _run_strace_pass(
        spec, "openat", data_root, server_bin, minio_bin, mc_bin, rustfs_bin, puts, trace_concurrency, size
    )
    print(f"  -> sync opens: {sync_opens} ({sync_opens / puts:.2f}/PUT)", flush=True)

    print(f"Probing {spec.name}: pass 3 (64 KiB PUT throughput, {throughput_workers} workers, {throughput_seconds:.0f}s, no strace)...", flush=True)
    ops_per_sec, _ = _run_throughput_pass(
        spec, data_root, server_bin, minio_bin, mc_bin, rustfs_bin, throughput_workers, throughput_seconds, size
    )
    print(f"  -> {ops_per_sec:,.1f} ops/s", flush=True)

    return build_probe_row(spec.name, puts, sync_calls, sync_opens, ops_per_sec, version=ver1)


def render_report(
    rows: Sequence[ProbeRow],
    cmd: str,
    run_id: Optional[str] = None,
    machine_info: Optional[str] = None,
    puts: int = TRACE_PUT_COUNT,
    trace_concurrency: int = TRACE_CONCURRENCY,
    throughput_workers: int = THROUGHPUT_WORKERS,
    throughput_seconds: float = THROUGHPUT_SECONDS,
) -> str:
    machine = machine_info or (
        f"{platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs, "
        f"{rwb.host_ram_gb()} GB RAM, CPU: {rwb.cpu_model()})"
    )
    lines = [
        f"Command: `{cmd}`",
        "",
    ]
    if run_id:
        lines.extend([f"Run ID: `{run_id}`", ""])
    lines.extend([
        f"# Server Durability Probe ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"- **Machine:** {machine}",
        f"- **Sync trace passes:** {puts} PUTs of 64 KiB, {trace_concurrency} concurrent, under `strace -f -c -e trace={SYNC_TRACE_EXPR}` and `strace -f -e trace=openat` (`O_DSYNC` or `O_SYNC`).",
        f"- **Throughput pass:** separate un-straced pass, 64 KiB PUTs, {throughput_workers} workers, {throughput_seconds:.0f} s, same Python `http.client` client for every row.",
        "- **Rule:** at least one sync call or synchronous open per PUT (`>= 1.00`) is `durable`, otherwise `not durable`.",
        "- **Client ceiling:** the Python client caps near 5,000 requests a second (#262).",
        "",
        "| Server | Sync calls / PUT | Sync opens / PUT | Durability | 64 KiB PUT ops/s |",
        "|---|---|---|---|---|",
    ])
    for r in rows:
        ops_str = f"{r.put_ops_per_sec:,.1f}" if r.put_ops_per_sec is not None else "not measured"
        lines.append(
            f"| {r.server} | {r.syncs_per_put:.2f} | {r.opens_per_put:.2f} | {r.label} | {ops_str} |"
        )
    lines.extend([
        "",
        "## Raw Trace Counts",
        "",
        "| Server | Binary | PUTs traced | Sync syscalls | Sync opens (`O_DSYNC`/`O_SYNC`) |",
        "|---|---|---|---|---|",
    ])
    for r in rows:
        lines.append(
            f"| {r.server} | {r.version or 'unknown'} | {r.puts} | {r.sync_calls} | {r.sync_opens} |"
        )
    lines.append("")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    if sys.platform != "linux":
        print("durability_probe.py requires Linux (strace)", file=sys.stderr)
        return 2

    ap = argparse.ArgumentParser(description="Count sync calls per PUT and compare 64 KiB PUT rates across default S3 servers.")
    ap.add_argument("--server-bin", default=os.path.join(REPO_ROOT, "server", "target", "release", "awanfs"))
    ap.add_argument("--minio-bin", default="")
    ap.add_argument("--mc-bin", default="")
    ap.add_argument("--rustfs-bin", default="")
    ap.add_argument("--data-dir", default="")
    ap.add_argument("--puts", type=int, default=TRACE_PUT_COUNT)
    ap.add_argument("--trace-concurrency", type=int, default=TRACE_CONCURRENCY)
    ap.add_argument("--workers", type=int, default=THROUGHPUT_WORKERS)
    ap.add_argument("--duration", type=float, default=THROUGHPUT_SECONDS)
    ap.add_argument("--output", default="")
    ap.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID", ""))
    args = ap.parse_args(argv)

    data_root = args.data_dir or tempfile.gettempdir()
    os.makedirs(data_root, exist_ok=True)

    auto_minio, auto_mc = rwb.find_minio_binaries()
    minio_bin = args.minio_bin or auto_minio
    mc_bin = args.mc_bin or auto_mc
    if not minio_bin or not mc_bin:
        minio_bin, mc_bin = cs.ensure_minio_binaries(minio_bin, mc_bin, auto_fetch=True)
    rustfs_bin = args.rustfs_bin or rwb.find_rustfs_binary()

    rows: List[ProbeRow] = []
    try:
        for spec in default_servers():
            rows.append(
                probe_server(
                    spec,
                    data_root=data_root,
                    server_bin=args.server_bin,
                    minio_bin=minio_bin,
                    mc_bin=mc_bin,
                    rustfs_bin=rustfs_bin,
                    puts=args.puts,
                    trace_concurrency=args.trace_concurrency,
                    size=PUT_SIZE_BYTES,
                    throughput_workers=args.workers,
                    throughput_seconds=args.duration,
                )
            )
    except StraceAttachError as exc:
        print(f"STRACE ATTACH FAILED: {exc}", file=sys.stderr)
        return 3

    raw_args = list(argv) if argv is not None else sys.argv[1:]
    cmd = "python3 benchmarks/durability_probe.py" + (" " + " ".join(raw_args) if raw_args else "")
    report = render_report(
        rows,
        cmd=cmd,
        run_id=args.run_id or None,
        puts=args.puts,
        trace_concurrency=args.trace_concurrency,
        throughput_workers=args.workers,
        throughput_seconds=args.duration,
    )
    print("\n" + report, flush=True)
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
