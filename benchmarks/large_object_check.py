"""How does AwanFS handle objects above 4 MiB, next to MinIO on the same machine?

    python benchmarks/large_object_check.py --server-bin server/target/release/s3-server \
        --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --sizes 16MiB,64MiB,256MiB --runs 3

An experiment, not a gate: it has no baseline and exits 1 only when a phase did not measure
(a failed request, or bytes that do not match). Per object size and server it reports:

- put_c1 / put_c4: MiB/s of whole-object PUTs, 1 and 4 clients.
- get_first: the first GET of each object after its PUT, 1 client: MiB/s and the time to the
  response headers (TTFB). AwanFS writes with direct I/O, so its first GET reads from disk.
- get_again_c4: the same objects again, 4 clients.
- range_1m: 1 MiB ranges at seeded offsets, 4 clients: P50 and P99 ms.
- mpu_c4: one multipart upload of the object in 16 MiB parts, 4 parts at a time, then
  mpu_get, a GET of it.
- small_idle / small_busy: P99 ms of 4 KiB GETs with the server idle, and while 4 clients PUT
  large objects. The gap is what large uploads cost everyone else.

The client is this Python process on the same machine, so absolute MiB/s include its cost; the
two servers get the same client, so compare columns, and compare runs on the same machine.
"""
import argparse
import http.client
import os
import platform
import random
import shutil
import statistics
import sys
import threading
import time
import zlib
from typing import Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from budget_rss import RssSplit, mib_or_nm  # noqa: E402
from ratio_check import BUCKET, start_awanfs, start_minio  # noqa: E402
from run_warp_benchmark import get_process_peak_rss_mb  # noqa: E402

MIB = 1 << 20
PART = 16 * MIB
RANGE_LEN = MIB
SMALL = 4096
SIZES = {"8MiB": 8 * MIB, "16MiB": 16 * MIB, "64MiB": 64 * MIB, "256MiB": 256 * MIB, "1GiB": 1024 * MIB}


class NotMeasured(Exception):
    """A request failed or returned the wrong bytes: the phase has no number."""


def conn_to(port: int) -> http.client.HTTPConnection:
    return http.client.HTTPConnection("127.0.0.1", port, timeout=600.0)


def put(port: int, path: str, body) -> float:
    """Seconds one PUT took. Raises NotMeasured unless the answer is 200."""
    c = conn_to(port)
    try:
        t0 = time.perf_counter()
        c.request("PUT", path, body=body, headers={"Content-Length": str(len(body))})
        r = c.getresponse()
        r.read()
        took = time.perf_counter() - t0
        if r.status != 200:
            raise NotMeasured(f"PUT {path}: HTTP {r.status}")
        return took
    finally:
        c.close()


def get(port: int, path: str, want_len: int, rng: Optional[Tuple[int, int]] = None, want_crc: Optional[int] = None) -> Tuple[float, float]:
    """(seconds to the response headers, seconds to the last byte). `rng` is an inclusive byte
    range. The length is always checked; the bytes are checked when `want_crc` is given, which
    costs client time, so timed phases pass it on one object only."""
    c = conn_to(port)
    try:
        headers = {} if rng is None else {"Range": f"bytes={rng[0]}-{rng[1]}"}
        t0 = time.perf_counter()
        c.request("GET", path, headers=headers)
        r = c.getresponse()
        ttfb = time.perf_counter() - t0
        ok = 200 if rng is None else 206
        if r.status != ok:
            r.read()
            raise NotMeasured(f"GET {path} {headers}: HTTP {r.status}, wanted {ok}")
        buf = bytearray(MIB)
        view = memoryview(buf)
        got, crc = 0, 0
        while True:
            n = r.readinto(view)
            if not n:
                break
            got += n
            if want_crc is not None:
                crc = zlib.crc32(view[:n], crc)
        total = time.perf_counter() - t0
        if got != want_len:
            raise NotMeasured(f"GET {path} {headers}: {got} bytes, wanted {want_len}")
        if want_crc is not None and crc != want_crc:
            raise NotMeasured(f"GET {path} {headers}: the bytes differ from what was stored")
        return ttfb, total
    finally:
        c.close()


def between(text: str, tag: str) -> str:
    a = text.find(f"<{tag}>")
    b = text.find(f"</{tag}>")
    if a < 0 or b < 0:
        raise NotMeasured(f"no <{tag}> in {text[:200]!r}")
    return text[a + len(tag) + 2:b]


def multipart_put(port: int, path: str, body: memoryview, conc: int) -> float:
    """Seconds for create, every 16 MiB part (`conc` at a time) and complete."""
    t0 = time.perf_counter()
    c = conn_to(port)
    try:
        c.request("POST", f"{path}?uploads")
        r = c.getresponse()
        text = r.read().decode("utf-8", "replace")
        if r.status != 200:
            raise NotMeasured(f"create multipart {path}: HTTP {r.status}")
        upload = between(text, "UploadId")
    finally:
        c.close()
    parts = [(i + 1, off) for i, off in enumerate(range(0, len(body), PART))]
    etags: Dict[int, str] = {}

    def one(item):
        n, off = item
        pc = conn_to(port)
        try:
            piece = body[off:off + PART]
            pc.request("PUT", f"{path}?partNumber={n}&uploadId={upload}", body=piece, headers={"Content-Length": str(len(piece))})
            pr = pc.getresponse()
            pr.read()
            if pr.status != 200:
                raise NotMeasured(f"part {n} of {path}: HTTP {pr.status}")
            etags[n] = pr.getheader("ETag", "")
        finally:
            pc.close()

    in_parallel(one, parts, conc)
    xml = "<CompleteMultipartUpload>" + "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{etags[n]}</ETag></Part>" for n, _ in parts) + "</CompleteMultipartUpload>"
    c = conn_to(port)
    try:
        c.request("POST", f"{path}?uploadId={upload}", body=xml.encode())
        r = c.getresponse()
        text = r.read().decode("utf-8", "replace")
        if r.status != 200 or "<Error>" in text:
            raise NotMeasured(f"complete multipart {path}: HTTP {r.status} {text[:200]!r}")
    finally:
        c.close()
    return time.perf_counter() - t0


def in_parallel(fn: Callable, items: list, conc: int) -> list:
    """fn(item) for every item on `conc` threads, results in item order. The first exception
    is raised after every thread stops, so a failed request cannot pass as a fast one."""
    out = [None] * len(items)
    errors: List[BaseException] = []
    lock = threading.Lock()
    nxt = [0]

    def worker():
        while True:
            with lock:
                if errors or nxt[0] >= len(items):
                    return
                i = nxt[0]
                nxt[0] += 1
            try:
                out[i] = fn(items[i])
            except BaseException as e:  # noqa: BLE001
                with lock:
                    errors.append(e)
                return

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(max(1, conc))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
    return out


def percentile(values: List[float], p: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, int(p * len(s)))]


def small_gets(port: int, path: str, seconds: float, stop: Optional[threading.Event] = None) -> List[float]:
    """Latencies in ms of 4 KiB GETs on one keep-alive connection for `seconds`."""
    c = conn_to(port)
    out = []
    try:
        end = time.perf_counter() + seconds
        while time.perf_counter() < end and not (stop and stop.is_set()):
            t0 = time.perf_counter()
            c.request("GET", path)
            r = c.getresponse()
            n = len(r.read())
            if r.status != 200 or n != SMALL:
                raise NotMeasured(f"GET {path}: HTTP {r.status}, {n} bytes")
            out.append((time.perf_counter() - t0) * 1000.0)
    finally:
        c.close()
    if not out:
        raise NotMeasured(f"no small GET finished in {seconds}s")
    return out


def count_for(size: int, budget: int) -> int:
    """How many objects of `size` a phase moves: `budget` bytes' worth, at least 4 so that 4
    clients each have one, at most 32."""
    return max(4, min(32, budget // size))


def measure_size(port: int, size: int, body: bytes, budget: int, small_seconds: float, seed: int) -> Dict[str, float]:
    """Every metric for one object size against one running server."""
    name = f"s{size // MIB}"
    view = memoryview(body)
    crc = zlib.crc32(view)
    n = count_for(size, budget)
    m: Dict[str, float] = {}

    keys1 = [f"/{BUCKET}/{name}-a-{i}" for i in range(n)]
    t0 = time.perf_counter()
    for k in keys1:
        put(port, k, view)
    m["put_c1_mibs"] = n * size / MIB / (time.perf_counter() - t0)

    keys4 = [f"/{BUCKET}/{name}-b-{i}" for i in range(n)]
    t0 = time.perf_counter()
    in_parallel(lambda k: put(port, k, view), keys4, 4)
    m["put_c4_mibs"] = n * size / MIB / (time.perf_counter() - t0)

    t0 = time.perf_counter()
    firsts = [get(port, k, size, want_crc=crc if i == 0 else None) for i, k in enumerate(keys1)]
    m["get_first_mibs"] = n * size / MIB / (time.perf_counter() - t0)
    m["get_first_ttfb_ms"] = statistics.median(t for t, _ in firsts) * 1000.0

    t0 = time.perf_counter()
    in_parallel(lambda k: get(port, k, size), keys1, 4)
    m["get_again_c4_mibs"] = n * size / MIB / (time.perf_counter() - t0)

    rnd = random.Random(seed)
    ranges = []
    for i in range(64):
        off = rnd.randrange(0, size - RANGE_LEN + 1)
        ranges.append((keys4[i % n], off))

    def one_range(item):
        k, off = item
        check = zlib.crc32(view[off:off + RANGE_LEN])
        return get(port, k, RANGE_LEN, rng=(off, off + RANGE_LEN - 1), want_crc=check)[1] * 1000.0

    lat = in_parallel(one_range, ranges, 4)
    m["range_1m_p50_ms"] = percentile(lat, 0.50)
    m["range_1m_p99_ms"] = percentile(lat, 0.99)

    mkey = f"/{BUCKET}/{name}-mpu"
    m["mpu_c4_mibs"] = size / MIB / multipart_put(port, mkey, view, 4)
    _, total = get(port, mkey, size, want_crc=crc)
    m["mpu_get_mibs"] = size / MIB / total

    skey = f"/{BUCKET}/{name}-small"
    put(port, skey, os.urandom(SMALL))
    m["small_idle_p99_ms"] = percentile(small_gets(port, skey, small_seconds), 0.99)
    stop = threading.Event()
    errors: List[BaseException] = []

    def load(j):
        i = 0
        while not stop.is_set():
            try:
                put(port, f"/{BUCKET}/{name}-load-{j}-{i % 2}", view)
            except BaseException as e:  # noqa: BLE001
                errors.append(e)
                return
            i += 1

    loaders = [threading.Thread(target=load, args=(j,), daemon=True) for j in range(4)]
    for t in loaders:
        t.start()
    try:
        time.sleep(0.2)
        m["small_busy_p99_ms"] = percentile(small_gets(port, skey, small_seconds), 0.99)
    finally:
        stop.set()
        for t in loaders:
            t.join()
    if errors:
        raise errors[0]
    return m


METRICS = [
    ("put_c1_mibs", "PUT, 1 client, MiB/s", True),
    ("put_c4_mibs", "PUT, 4 clients, MiB/s", True),
    ("get_first_mibs", "first GET, 1 client, MiB/s", True),
    ("get_first_ttfb_ms", "first GET, ms to headers (median)", False),
    ("get_again_c4_mibs", "GET again, 4 clients, MiB/s", True),
    ("range_1m_p50_ms", "1 MiB range, P50 ms", False),
    ("range_1m_p99_ms", "1 MiB range, P99 ms", False),
    ("mpu_c4_mibs", "multipart upload, 16 MiB parts x4, MiB/s", True),
    ("mpu_get_mibs", "GET of the multipart object, MiB/s", True),
    ("small_idle_p99_ms", "4 KiB GET P99 ms, server idle", False),
    ("small_busy_p99_ms", "4 KiB GET P99 ms, during 4 large PUTs", False),
]


def better(awanfs: float, minio: float, higher_is_better: bool) -> Optional[float]:
    """How many times better AwanFS is than MinIO (above 1 is better), or None."""
    if not awanfs or not minio:
        return None
    return awanfs / minio if higher_is_better else minio / awanfs


def render(results: Dict[str, Dict[str, Dict[str, List[float]]]], mem: Dict[str, dict], cmd: str, runs: int) -> str:
    lines = [
        f"Command: `{cmd}`",
        "",
        f"# Large objects: AwanFS next to MinIO ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs). {runs} run(s) per server, order "
        "alternating; medians. The client is a Python process on the same machine. \"AwanFS better by\" is "
        "AwanFS/MinIO for MiB/s and MinIO/AwanFS for ms, so above 1 is always AwanFS ahead.",
    ]
    for size_name, per_server in results.items():
        lines += ["", f"## {size_name} objects", "", "| metric | AwanFS | MinIO | AwanFS better by |", "|---|---|---|---|"]
        for key, label, higher in METRICS:
            a = per_server.get("awanfs", {}).get(key)
            b = per_server.get("minio", {}).get(key)
            am = statistics.median(a) if a else None
            bm = statistics.median(b) if b else None
            r = better(am, bm, higher) if am is not None and bm is not None else None
            lines.append(f"| {label} | {mib_or_nm(am)} | {mib_or_nm(bm)} | {'not measured' if r is None else f'{r:.2f}x'} |")
    lines += ["", "## Memory over the whole run (highest of the runs)", "",
              "| server | peak RSS MiB | peak heap (RssAnon) MiB | peak file pages (RssFile) MiB |", "|---|---|---|---|"]
    for server in ("awanfs", "minio"):
        s = mem.get(server, {})
        lines.append(f"| {server} | {mib_or_nm(s.get('peak'))} | {mib_or_nm(s.get('anon'))} | {mib_or_nm(s.get('file'))} |")
    lines.append("")
    return "\n".join(lines)


def run_server(start, sizes: List[str], bodies: Dict[str, bytes], budget: int, small_seconds: float, seed: int):
    """({size: metrics}, memory) for one freshly started server."""
    proc, port, data = start()
    try:
        out = {}
        with RssSplit(proc.pid) as split:
            for s in sizes:
                print(f"  {s} ...", flush=True)
                out[s] = measure_size(port, SIZES[s], bodies[s], budget, small_seconds, seed)
        return out, {"peak": get_process_peak_rss_mb(proc.pid) or None, "anon": split.anon, "file": split.file}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except Exception:  # noqa: BLE001
            proc.kill()
        shutil.rmtree(data, ignore_errors=True)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--minio-bin", default="")
    ap.add_argument("--mc-bin", default="")
    ap.add_argument("--sizes", default="16MiB,64MiB,256MiB")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--budget-mib", type=int, default=512, help="bytes each PUT phase moves, per object size")
    ap.add_argument("--small-seconds", type=float, default=3.0)
    ap.add_argument("--seed", type=int, default=20261114)
    ap.add_argument("--report", default="")
    return ap


def main(argv=None) -> int:
    a = build_parser().parse_args(argv)
    sizes = [s for s in a.sizes.split(",") if s]
    for s in sizes:
        if s not in SIZES:
            sys.exit(f"--sizes: {s!r} is not one of {sorted(SIZES)}")
    bodies = {s: os.urandom(SIZES[s]) for s in sizes}
    starters = {"awanfs": lambda: start_awanfs(a.server_bin)}
    if a.minio_bin:
        starters["minio"] = lambda: start_minio(a.minio_bin, a.mc_bin)
    results: Dict[str, Dict[str, Dict[str, List[float]]]] = {s: {k: {} for k in starters} for s in sizes}
    mem: Dict[str, dict] = {k: {} for k in starters}
    failed = []
    for run in range(a.runs):
        order = list(starters) if run % 2 == 0 else list(reversed(list(starters)))
        for server in order:
            print(f"run {run + 1}/{a.runs}: {server}", flush=True)
            try:
                per_size, m = run_server(starters[server], sizes, bodies, a.budget_mib * MIB, a.small_seconds, a.seed + run)
            except NotMeasured as e:
                failed.append(f"{server}, run {run + 1}: {e}")
                print(f"  NOT MEASURED: {e}", flush=True)
                continue
            for s, metrics in per_size.items():
                for k, v in metrics.items():
                    results[s][server].setdefault(k, []).append(v)
            for k, v in m.items():
                if v is not None:
                    mem[server][k] = max(mem[server].get(k) or 0.0, v)
    cmd = "python benchmarks/large_object_check.py " + " ".join(argv if argv is not None else sys.argv[1:])
    report = render(results, mem, cmd, a.runs)
    if failed:
        report += "\n**NOT MEASURED** (a number above from fewer runs, or none):\n" + "".join(f"- {f}\n" for f in failed)
    print(report)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as fh:
            fh.write(report)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
