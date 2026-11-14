"""What does one durable append cost on this machine's disk?

    python benchmarks/sync_probe.py [--dir DIR ...] [--count 200] [--report sync_probe.md]

AwanFS acknowledges a PUT after appending to one volume file and flushing it and its journal
(two flushes per group commit). This measures that pattern directly, without the server, so a
slow PUT can be split into its parts. Each variant appends `--count` blocks of 64 KiB to a
fresh file and reports the median and P90 milliseconds per append:

- write only: no flush. What the write itself costs.
- flush: one flush per append (fsync / FlushFileBuffers).
- flush x2: the data file, then a 42-byte record appended to a second file and flushed.
  This is what the server does per commit.
- growing / preallocated: the file grows with each append, or is set to its final length
  first so appends never move the end of the file.
- unbuffered (Windows only): the file is opened with FILE_FLAG_NO_BUFFERING, as the server
  opens its volumes.

Run it on every directory that matters: on GitHub's Windows runners the temp directory is on
C: and the workspace on D:, and they are different disks.
"""
import argparse
import mmap
import os
import platform
import statistics
import sys
import tempfile
import time

BLOCK = 64 * 1024
IDX_RECORD = 42
O_BINARY = getattr(os, "O_BINARY", 0)


def open_unbuffered(path: str) -> int:
    """A file descriptor on `path` opened with FILE_FLAG_NO_BUFFERING (Windows)."""
    import ctypes
    import msvcrt
    from ctypes import wintypes

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.CreateFileW.restype = wintypes.HANDLE
    k32.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
                                wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    GENERIC_RW, SHARE_RW, OPEN_ALWAYS, NO_BUFFERING = 0xC0000000, 0x3, 4, 0x20000000
    handle = k32.CreateFileW(path, GENERIC_RW, SHARE_RW, None, OPEN_ALWAYS, NO_BUFFERING, None)
    if handle == wintypes.HANDLE(-1).value or handle is None:
        raise OSError(ctypes.get_last_error(), f"CreateFileW({path})")
    return msvcrt.open_osfhandle(handle, O_BINARY)


def run_variant(directory: str, name: str, count: int, flushes: int, preallocate: bool, unbuffered: bool):
    """Per-append latencies in ms for one variant, or None when it does not apply here."""
    if unbuffered and os.name != "nt":
        return None
    data_path = os.path.join(directory, f"probe_{name}.dat")
    idx_path = os.path.join(directory, f"probe_{name}.idx")
    for p in (data_path, idx_path):
        if os.path.exists(p):
            os.remove(p)
    fd = open_unbuffered(data_path) if unbuffered else os.open(data_path, os.O_RDWR | os.O_CREAT | O_BINARY)
    idx = os.open(idx_path, os.O_RDWR | os.O_CREAT | O_BINARY)
    block = mmap.mmap(-1, BLOCK)  # page-aligned, which unbuffered writes need
    block.write(os.urandom(BLOCK))
    record = os.urandom(IDX_RECORD)
    out = []
    try:
        if preallocate:
            os.ftruncate(fd, BLOCK * count)
            os.fsync(fd)
        os.lseek(fd, 0, os.SEEK_SET)
        for _ in range(count):
            t0 = time.perf_counter()
            os.write(fd, block)
            if flushes >= 1:
                os.fsync(fd)
            if flushes >= 2:
                os.write(idx, record)
                os.fsync(idx)
            out.append((time.perf_counter() - t0) * 1000.0)
    finally:
        os.close(fd)
        os.close(idx)
        block.close()
        for p in (data_path, idx_path):
            try:
                os.remove(p)
            except OSError:
                pass
    return out


VARIANTS = [
    # name, label, flushes, preallocate, unbuffered
    ("w_grow", "write only, growing", 0, False, False),
    ("f1_grow", "flush, growing", 1, False, False),
    ("f2_grow", "flush x2, growing", 2, False, False),
    ("f1_pre", "flush, preallocated", 1, True, False),
    ("f2_pre", "flush x2, preallocated", 2, True, False),
    ("u_f1_grow", "unbuffered, flush, growing", 1, False, True),
    ("u_f2_grow", "unbuffered, flush x2, growing (what the server does)", 2, False, True),
    ("u_f1_pre", "unbuffered, flush, preallocated", 1, True, True),
    ("u_f2_pre", "unbuffered, flush x2, preallocated", 2, True, True),
]


def percentile(values, p):
    s = sorted(values)
    return s[min(len(s) - 1, int(p * len(s)))]


def render(results, count, cmd):
    lines = [
        f"Command: `{cmd}`",
        "",
        f"# What one durable 64 KiB append costs ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs). {count} appends per variant, one writer.",
    ]
    for directory, rows in results.items():
        lines += ["", f"## {directory}", "", "| variant | median ms | P90 ms | appends/s |", "|---|---|---|---|"]
        for label, lat in rows:
            if lat is None:
                lines.append(f"| {label} | not measured (Windows only) | | |")
            else:
                med = statistics.median(lat)
                lines.append(f"| {label} | {med:.3f} | {percentile(lat, 0.90):.3f} | {len(lat) / (sum(lat) / 1000.0):.0f} |")
    lines.append("")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", action="append", default=[])
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--report", default="")
    a = ap.parse_args(argv)
    dirs = a.dir or [tempfile.gettempdir()]
    results = {}
    for d in dirs:
        work = tempfile.mkdtemp(prefix="sync_probe_", dir=d)
        try:
            rows = []
            for name, label, flushes, pre, unbuf in VARIANTS:
                print(f"{d}: {label} ...", flush=True)
                rows.append((label, run_variant(work, name, a.count, flushes, pre, unbuf)))
            results[d] = rows
        finally:
            try:
                os.rmdir(work)
            except OSError:
                pass
    cmd = "python benchmarks/sync_probe.py " + " ".join(argv if argv is not None else sys.argv[1:])
    text = render(results, a.count, cmd)
    print(text)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as fh:
            fh.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
