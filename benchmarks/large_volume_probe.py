"""Does a volume that grows cost anything once it is large? AwanFS only.

    python benchmarks/large_volume_probe.py --server-bin server/target/release/s3-server --fill-gib 10

A new volume grows as it is written and its read mapping is redone when a read reaches past
it (decision 52); before, the first volume was a 32 GiB sparse file mapped once. The remap
was measured on small volumes only. This fills a volume with `--fill-gib` of 4 MiB objects,
then measures 64 KiB PUT, GET and mixed load at 64 clients, and the latency of reading an
object straight after writing it (the read that redoes the mapping), for the server started
both ways: growing (the default) and sparse (`AWAN_SPARSE_VOLUME=1`).
"""
import argparse
import http.client
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from budget_rss import create_bucket  # noqa: E402
from large_object_check import NotMeasured, conn_to, get, in_parallel, percentile, put  # noqa: E402
from ratio_check import wait_port  # noqa: E402
from run_warp_benchmark import get_free_port, get_process_peak_rss_mb, run_workload_phase  # noqa: E402

MIB = 1 << 20
FILL_OBJECT = 4 * MIB
SMALL = 64 * 1024
CONFIGS = [("growing (default)", {}), ("sparse (AWAN_SPARSE_VOLUME=1)", {"AWAN_SPARSE_VOLUME": "1"})]


def volume_bytes(data_dir: str) -> int:
    """Bytes the volume files really occupy (a sparse file's length says nothing)."""
    total = 0
    for name in os.listdir(data_dir):
        if name.startswith("volume_") and name.endswith(".dat"):
            st = os.stat(os.path.join(data_dir, name))
            total += getattr(st, "st_blocks", 0) * 512 or st.st_size
    return total


def timed_get(c: http.client.HTTPConnection, path: str, want_len: int) -> float:
    """Milliseconds from sending a GET on an open connection to its last byte."""
    t0 = time.perf_counter()
    c.request("GET", path)
    r = c.getresponse()
    got = len(r.read())
    took = (time.perf_counter() - t0) * 1000.0
    if r.status != 200 or got != want_len:
        raise NotMeasured(f"GET {path}: HTTP {r.status}, {got} bytes")
    return took


def read_after_write_split(port: int, small: bytes, old_keys, rounds: int) -> dict:
    """#204: where a GET straight after its PUT spends its time. Each row changes one thing:
    connect: opening the connection alone, before the GET.
    open_conn: the GET on a connection opened beforehand, straight after its PUT.
    pause: the same, 100 ms after the PUT (a flush still running would be done).
    keep_alive: PUT and GET on one connection.
    old: a GET on a fresh open connection of an object written before the fill."""
    rows = {k: [] for k in ("connect", "open_conn", "pause", "keep_alive", "old")}
    for i in range(rounds):
        key = f"/probe/split-{i}"
        put(port, key, small)
        c = conn_to(port)
        try:
            t0 = time.perf_counter()
            c.connect()
            rows["connect"].append((time.perf_counter() - t0) * 1000.0)
            rows["open_conn"].append(timed_get(c, key, SMALL))
        finally:
            c.close()
        key = f"/probe/pause-{i}"
        put(port, key, small)
        time.sleep(0.1)
        c = conn_to(port)
        try:
            c.connect()
            rows["pause"].append(timed_get(c, key, SMALL))
        finally:
            c.close()
        key = f"/probe/ka-{i}"
        c = conn_to(port)
        try:
            c.request("PUT", key, body=small, headers={"Content-Length": str(len(small))})
            r = c.getresponse()
            r.read()
            if r.status != 200:
                raise NotMeasured(f"PUT {key}: HTTP {r.status}")
            rows["keep_alive"].append(timed_get(c, key, SMALL))
        finally:
            c.close()
        c = conn_to(port)
        try:
            c.connect()
            rows["old"].append(timed_get(c, old_keys[i % len(old_keys)], SMALL))
        finally:
            c.close()
    return {k: (percentile(v, 0.50), percentile(v, 0.99)) for k, v in rows.items()}


def _read_log_tail(log_path: str, limit: int = 2000) -> str:
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            return fh.read()[-limit:]
    except OSError:
        return ""


def stop_server(proc: subprocess.Popen, data: str) -> None:
    proc.kill()
    proc.wait()
    shutil.rmtree(data, ignore_errors=True)


def start_awanfs(server_bin: str, env_extra: dict = None):
    data = tempfile.mkdtemp(prefix="volume_probe_")
    port = get_free_port()
    log_path = os.path.join(data, "server.log")
    with open(log_path, "ab") as log:
        proc = subprocess.Popen(
            [server_bin, "--port", str(port), "--data-dir", data, "--allow-anonymous", "--auto-compaction", "off"],
            stdout=log, stderr=subprocess.STDOUT, env=dict(os.environ, **(env_extra or {})),
        )
    if not wait_port(port, timeout=40.0):
        proc.kill()
        proc.wait()
        tail = _read_log_tail(log_path)
        shutil.rmtree(data, ignore_errors=True)
        raise SystemExit(f"the server did not answer on port {port} with {env_extra}\n{tail}")
    try:
        create_bucket(port, "probe")
    except BaseException:
        stop_server(proc, data)
        raise
    return proc, port, data


def start_minio(minio_bin: str, mc_bin: str):
    if not mc_bin:
        raise SystemExit("--mc-bin is required alongside --minio-bin (mc not found)")
    data = tempfile.mkdtemp(prefix="minio_probe_")
    port, console = get_free_port(), get_free_port()
    env = dict(os.environ, MINIO_ROOT_USER="minioadmin", MINIO_ROOT_PASSWORD="minioadmin",
               MINIO_UPDATE="off", MINIO_BROWSER="off")
    log_path = os.path.join(data, "minio.log")
    with open(log_path, "ab") as log:
        proc = subprocess.Popen(
            [minio_bin, "server", data, "--address", f"127.0.0.1:{port}", "--console-address", f"127.0.0.1:{console}"],
            env=env, stdout=log, stderr=subprocess.STDOUT,
        )
    if not wait_port(port, timeout=40.0):
        proc.kill()
        proc.wait()
        tail = _read_log_tail(log_path)
        shutil.rmtree(data, ignore_errors=True)
        raise SystemExit(f"minio did not answer on port {port}\n{tail}")
    alias = f"probe{port}"
    for args in (["alias", "set", alias, f"http://127.0.0.1:{port}", "minioadmin", "minioadmin"],
                 ["mb", f"{alias}/probe"],
                 ["anonymous", "set", "public", f"{alias}/probe"]):
        last = None
        for _ in range(20):
            res = subprocess.run([mc_bin, *args], capture_output=True, text=True)
            if res.returncode == 0:
                break
            last = res.stderr or res.stdout
            time.sleep(0.5)
        else:
            proc.kill()
            proc.wait()
            tail = _read_log_tail(log_path)
            shutil.rmtree(data, ignore_errors=True)
            raise SystemExit(f"mc {args[0]} failed: {last}\n{tail}")
    return proc, port, data


def measure_connect(port: int, rounds: int, bucket: str = "probe") -> dict:
    """Measure fresh loopback connection setup (`c.connect()`) followed by a 64 KiB GET."""
    small = os.urandom(SMALL)
    key = f"/{bucket}/connect-probe"
    put(port, key, small)
    connect_ms = []
    open_get_ms = []
    for _ in range(rounds):
        c = conn_to(port)
        try:
            t0 = time.perf_counter()
            c.connect()
            connect_ms.append((time.perf_counter() - t0) * 1000.0)
            open_get_ms.append(timed_get(c, key, SMALL))
        finally:
            c.close()
    return {
        "connect": (percentile(connect_ms, 0.50), percentile(connect_ms, 0.99)),
        "open_conn": (percentile(open_get_ms, 0.50), percentile(open_get_ms, 0.99)),
    }


def resolve_mc_bin(minio_bin: str, mc_bin: str) -> str:
    if mc_bin:
        return mc_bin
    if not minio_bin:
        return ""
    exe = ".exe" if minio_bin.lower().endswith(".exe") else ""
    sibling = os.path.join(os.path.dirname(os.path.abspath(minio_bin)), "mc" + exe)
    return sibling if os.path.isfile(sibling) else ""


def run_connect_comparison(server_bin: str, minio_bin: str, mc_bin: str, rounds: int) -> list:
    starters = [("AwanFS", lambda: start_awanfs(server_bin))]
    if minio_bin:
        mc = resolve_mc_bin(minio_bin, mc_bin)
        starters.append(("MinIO", lambda m=mc: start_minio(minio_bin, m)))
    results = []
    for round_no, order in enumerate((starters, list(reversed(starters))), 1):
        for name, start in order:
            print(f"connect round {round_no}: {name} ...", flush=True)
            proc, port, data = start()
            try:
                results.append((name, round_no, measure_connect(port, rounds)))
            finally:
                stop_server(proc, data)
    return results


def summarize_connect(results) -> dict:
    by_server = {}
    for server, _round_no, m in results:
        by_server.setdefault(server, []).append(m)
    summary = {}
    for server in ("AwanFS", "MinIO"):
        entries = by_server.get(server)
        if not entries:
            summary[server] = None
            continue
        summary[server] = {
            "connect_p50_ms": statistics.median(e["connect"][0] for e in entries),
            "connect_p99_ms": statistics.median(e["connect"][1] for e in entries),
            "open_conn_p50_ms": statistics.median(e["open_conn"][0] for e in entries),
            "open_conn_p99_ms": statistics.median(e["open_conn"][1] for e in entries),
        }
    a = summary.get("AwanFS")
    b = summary.get("MinIO")
    summary["ratio"] = (a["connect_p50_ms"] / b["connect_p50_ms"]) if (a and b and b["connect_p50_ms"] > 0) else None
    return summary


def render_connect(results, rounds: int, cmd: str) -> str:
    lines = [
        f"Command: `{cmd}`",
        "",
        f"# Loopback connect cost: AwanFS against MinIO ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs). Data under {tempfile.gettempdir()}. "
        f"{rounds} fresh connections per round, alternating order.",
        "",
        "| | " + " | ".join(f"{srv}, round {rnd}" for srv, rnd, _ in results) + " |",
        "|---|" + "---|" * len(results),
        "| opening the connection (connect), P50, P99 ms | "
        + " | ".join(f"{m['connect'][0]:.2f}, {m['connect'][1]:.2f}" for _, _, m in results) + " |",
        "| GET on the open connection (64 KiB), P50, P99 ms | "
        + " | ".join(f"{m['open_conn'][0]:.2f}, {m['open_conn'][1]:.2f}" for _, _, m in results) + " |",
        "",
        "| server | connect P50 ms (median) | connect P99 ms (median) | open-conn GET P50 ms (median) |",
        "|---|---|---|---|",
    ]
    summary = summarize_connect(results)
    for server in ("AwanFS", "MinIO"):
        s = summary.get(server)
        if s is None:
            lines.append(f"| {server} | not measured | not measured | not measured |")
        else:
            lines.append(
                f"| {server} | {s['connect_p50_ms']:.2f} | {s['connect_p99_ms']:.2f} | {s['open_conn_p50_ms']:.2f} |"
            )
    ratio = summary.get("ratio")
    lines.append("")
    lines.append(
        f"AwanFS / MinIO connect P50 ratio: {ratio:.2f}x"
        if ratio is not None else "AwanFS / MinIO connect P50 ratio: not measured"
    )
    lines.append("")
    return "\n".join(lines)


def parse_connect_summary(text: str) -> dict:
    """Parse the summary table and ratio line from render_connect output."""
    out = {"AwanFS": None, "MinIO": None, "ratio": None}
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("| AwanFS |") or s.startswith("| MinIO |"):
            parts = [p.strip() for p in s.strip("|").split("|")]
            if len(parts) >= 4 and parts[1] != "not measured":
                out[parts[0]] = {
                    "connect_p50_ms": float(parts[1]),
                    "connect_p99_ms": float(parts[2]),
                    "open_conn_p50_ms": float(parts[3]),
                }
        elif s.startswith("AwanFS / MinIO connect P50 ratio:"):
            val = s.split(":", 1)[1].strip()
            if val != "not measured" and val.endswith("x"):
                out["ratio"] = float(val[:-1])
    return out


def measure(server_bin: str, env_extra: dict, fill_gib: float, seconds: float, rounds: int):
    proc, port, data = start_awanfs(server_bin, env_extra)
    try:
        # Small objects at the start of the volume, for the "old object" row (#204).
        small = os.urandom(SMALL)
        old_keys = [f"/probe/old-{i}" for i in range(64)]
        for k in old_keys:
            put(port, k, small)
        body = memoryview(os.urandom(FILL_OBJECT))
        n = int(fill_gib * 1024 * MIB // FILL_OBJECT)
        t0 = time.perf_counter()
        in_parallel(lambda i: put(port, f"/probe/fill-{i}", body), list(range(n)), 8)
        fill_seconds = time.perf_counter() - t0
        out = {"fill_mibs": n * FILL_OBJECT / MIB / fill_seconds, "volume_gib": volume_bytes(data) / (1024 * MIB)}
        for mode in ("put", "get", "mixed"):
            m = run_workload_phase("127.0.0.1", port, "probe", SMALL, 64, seconds, mode, server_pid=proc.pid)
            if m.ops_per_sec * SMALL < SMALL:
                raise NotMeasured(f"{mode} moved nothing with {env_extra}")
            out[mode] = (m.ops_per_sec, m.p50_ms, m.p99_ms)
        lat = []
        for i in range(rounds):
            key = f"/probe/raw-{i}"
            put(port, key, small)
            _, total = get(port, key, SMALL)
            lat.append(total * 1000.0)
        out["read_after_write"] = (percentile(lat, 0.50), percentile(lat, 0.99))
        out["split"] = read_after_write_split(port, small, old_keys, rounds)
        # A fill object from the start of the volume is still what was written.
        import zlib
        get(port, "/probe/fill-0", FILL_OBJECT, want_crc=zlib.crc32(body))
        out["rss"] = get_process_peak_rss_mb(proc.pid)
        return out
    finally:
        stop_server(proc, data)


def render(results, fill_gib, cmd):
    lines = [
        f"Command: `{cmd}`",
        "",
        f"# A large volume: growing against sparse ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs). Data under {tempfile.gettempdir()}. "
        f"Each server first stores {fill_gib:g} GiB of 4 MiB objects; the rows after that are 64 KiB objects at 64 clients.",
        "",
        "| | " + " | ".join(label for label, _ in results) + " |",
        "|---|" + "---|" * len(results),
    ]

    def row(name, f):
        lines.append(f"| {name} | " + " | ".join(f(r) for _, r in results) + " |")

    row("volume on disk after the fill, GiB", lambda r: f"{r['volume_gib']:.1f}")
    row("fill, MiB/s (8 clients)", lambda r: f"{r['fill_mibs']:.0f}")
    for mode, name in (("put", "PUT"), ("get", "GET"), ("mixed", "mixed (80% GET)")):
        row(f"{name}: ops/s (P50, P99 ms)", lambda r, m=mode: f"{r[m][0]:.0f} ({r[m][1]:.2f}, {r[m][2]:.2f})")
    row("GET straight after its PUT, 1 client: P50, P99 ms", lambda r: f"{r['read_after_write'][0]:.2f}, {r['read_after_write'][1]:.2f}")
    for k, name in (
        ("connect", "  of which: opening the connection, P50, P99 ms"),
        ("open_conn", "  GET straight after its PUT, connection already open: P50, P99 ms"),
        ("pause", "  the same, 100 ms after the PUT: P50, P99 ms"),
        ("keep_alive", "  PUT and GET on one connection, the GET: P50, P99 ms"),
        ("old", "  GET of an object written before the fill, connection open: P50, P99 ms"),
    ):
        row(name, lambda r, k=k: f"{r['split'][k][0]:.2f}, {r['split'][k][1]:.2f}")
    row("peak RSS, MiB", lambda r: f"{r['rss']:.1f}")
    lines.append("")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--minio-bin", default="")
    ap.add_argument("--mc-bin", default="")
    ap.add_argument("--connect-only", action="store_true")
    ap.add_argument("--fill-gib", type=float, default=10.0)
    ap.add_argument("--seconds", type=float, default=5.0)
    ap.add_argument("--rounds", type=int, default=300)
    ap.add_argument("--report", default="")
    a = ap.parse_args(argv)
    cmd = "python benchmarks/large_volume_probe.py " + " ".join(argv if argv is not None else sys.argv[1:])
    if a.connect_only:
        connect_results = run_connect_comparison(a.server_bin, a.minio_bin, a.mc_bin, a.rounds)
        text = render_connect(connect_results, a.rounds, cmd)
        print(text)
        if a.report:
            with open(a.report, "w", encoding="utf-8") as fh:
                fh.write(text)
        return 0
    free_gib = shutil.disk_usage(tempfile.gettempdir()).free / (1024 * MIB)
    fill = min(a.fill_gib, max(1.0, free_gib - 4.0))
    if fill < a.fill_gib:
        print(f"{free_gib:.1f} GiB free under {tempfile.gettempdir()}: filling {fill:.1f} GiB, not {a.fill_gib:g}", flush=True)
    sections = []
    if a.minio_bin:
        connect_results = run_connect_comparison(a.server_bin, a.minio_bin, a.mc_bin, a.rounds)
        sections.append(render_connect(connect_results, a.rounds, cmd))
    results = []
    # Twice, in opposite orders: one pass cannot tell a difference from the runner's drift.
    for round_no, order in enumerate((CONFIGS, list(reversed(CONFIGS))), 1):
        for label, env in order:
            print(f"round {round_no}: {label} ...", flush=True)
            results.append((f"{label}, round {round_no}", measure(a.server_bin, env, fill, a.seconds, a.rounds)))
            print(render(results[-1:], fill, "(partial)"), flush=True)
    text = render(results, fill, cmd)
    if sections:
        text = "\n".join(sections) + "\n" + text
    print(text)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as fh:
            fh.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
