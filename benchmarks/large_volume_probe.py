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
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from budget_rss import create_bucket  # noqa: E402
from large_object_check import NotMeasured, get, in_parallel, percentile, put  # noqa: E402
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


def measure(server_bin: str, env_extra: dict, fill_gib: float, seconds: float, rounds: int):
    data = tempfile.mkdtemp(prefix="volume_probe_")
    port = get_free_port()
    with open(os.path.join(data, "server.log"), "ab") as log:
        proc = subprocess.Popen([server_bin, "--port", str(port), "--data-dir", data, "--allow-anonymous", "--auto-compaction", "off"],
                                stdout=log, stderr=subprocess.STDOUT, env=dict(os.environ, **env_extra))
    try:
        if not wait_port(port, timeout=40.0):
            raise SystemExit(f"the server did not answer on port {port} with {env_extra}")
        create_bucket(port, "probe")
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
        small = os.urandom(SMALL)
        lat = []
        for i in range(rounds):
            key = f"/probe/raw-{i}"
            put(port, key, small)
            _, total = get(port, key, SMALL)
            lat.append(total * 1000.0)
        out["read_after_write"] = (percentile(lat, 0.50), percentile(lat, 0.99))
        # A fill object from the start of the volume is still what was written.
        import zlib
        get(port, "/probe/fill-0", FILL_OBJECT, want_crc=zlib.crc32(body))
        out["rss"] = get_process_peak_rss_mb(proc.pid)
        return out
    finally:
        proc.kill()
        proc.wait()
        shutil.rmtree(data, ignore_errors=True)


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
    row("peak RSS, MiB", lambda r: f"{r['rss']:.1f}")
    lines.append("")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--fill-gib", type=float, default=10.0)
    ap.add_argument("--seconds", type=float, default=5.0)
    ap.add_argument("--rounds", type=int, default=300)
    ap.add_argument("--report", default="")
    a = ap.parse_args(argv)
    free_gib = shutil.disk_usage(tempfile.gettempdir()).free / (1024 * MIB)
    fill = min(a.fill_gib, max(1.0, free_gib - 4.0))
    if fill < a.fill_gib:
        print(f"{free_gib:.1f} GiB free under {tempfile.gettempdir()}: filling {fill:.1f} GiB, not {a.fill_gib:g}", flush=True)
    results = []
    for label, env in CONFIGS:
        print(f"{label} ...", flush=True)
        results.append((label, measure(a.server_bin, env, fill, a.seconds, a.rounds)))
    cmd = "python benchmarks/large_volume_probe.py " + " ".join(argv if argv is not None else sys.argv[1:])
    text = render(results, fill, cmd)
    print(text)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as fh:
            fh.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
