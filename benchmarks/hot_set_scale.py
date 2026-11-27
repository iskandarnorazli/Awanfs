"""#210: today's server past its 100,000-object hot set: PUT, GET and boot at 1M objects.

    python benchmarks/hot_set_scale.py --server server/target/release/awanfs \\
        --client server/target/release/examples/hot_set_bench --objects 1000000 \\
        --data-dir <dir> --results hot_set.jsonl --report hot_set.md

Starts the server, runs `hot_set_bench load` (one JSON line per window of PUTs), adds the
server's peak RSS to each line, kills the server and times a restart until it answers, then runs
`hot_set_bench verify`. Exit 0 when every answer was right, 1 when one was not (the numbers mean
nothing then), 2 when a phase is missing.
"""

import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ratio_check import wait_port  # noqa: E402
from run_warp_benchmark import get_free_port, get_process_peak_rss_mb  # noqa: E402

HOT_SET = 100_000  # AwanStorageEngine::new(100_000, ...) in server/src/s3_api.rs


def start(server_bin: str, data_dir: str, budget: str, boot_timeout: float):
    """The server, its port, and the seconds until it answered. Its output goes to a file in the
    data directory, never a pipe (#135)."""
    port = get_free_port()
    log = open(os.path.join(data_dir, "server.log"), "ab")
    t0 = time.monotonic()
    proc = subprocess.Popen(
        [server_bin, "--port", str(port), "--data-dir", data_dir, "--allow-anonymous",
         "--memory-budget", budget, "--auto-compaction", "off"],
        stdout=log, stderr=subprocess.STDOUT,
    )
    if not wait_port(port, timeout=boot_timeout):
        proc.kill()
        sys.exit(f"the server did not answer on port {port} within {boot_timeout:.0f} s")
    return proc, port, time.monotonic() - t0


def client(client_bin: str, mode: str, port: int, objects: int, extra, on_line, out_path):
    """Runs the client; calls on_line(record) for each JSON line it prints, as it prints it.
    Its output goes to out_path, which this follows, never to a pipe (#135). Its stderr goes to
    the caller's stderr. Returns its exit code."""
    with open(out_path, "w", encoding="utf-8") as sink, open(out_path, encoding="utf-8") as follow:
        proc = subprocess.Popen([client_bin, mode, "--port", str(port), "--objects", str(objects), *extra],
                                stdout=sink)
        pending = ""
        while True:
            done = proc.poll() is not None
            pending += follow.read()
            *lines, pending = pending.split("\n")
            for line in lines:
                line = line.strip()
                if line.startswith("{"):
                    on_line(json.loads(line))
            if done:
                break
            time.sleep(0.2)
        return proc.returncode


def run(a) -> list:
    os.makedirs(a.data_dir, exist_ok=True)
    out = []
    extra = ["--window", str(a.window), "--threads", str(a.threads)]

    def keep(proc, rec):
        rec["server_peak_rss_mib"] = round(get_process_peak_rss_mb(proc.pid), 1)
        note(rec)

    def note(rec):
        out.append(rec)
        print(json.dumps(rec), flush=True)

    proc, port, boot = start(a.server, a.data_dir, a.budget, 60)
    note({"phase": "first_boot", "objects": 0, "boot_secs": round(boot, 2)})
    try:
        code = client(a.client, "load", port, a.objects, extra, lambda r: keep(proc, r),
                      os.path.join(a.data_dir, "client_load.jsonl"))
        if code != 0:
            note({"phase": "load_failed", "exit": code})
            return out
    finally:
        proc.kill()
        proc.wait(timeout=60)
    proc, port, boot = start(a.server, a.data_dir, a.budget, a.boot_timeout)
    try:
        note({"phase": "restart", "objects": a.objects, "boot_secs": round(boot, 2),
              "server_peak_rss_mib": round(get_process_peak_rss_mb(proc.pid), 1)})
        client(a.client, "verify", port, a.objects, ["--threads", str(a.threads)], lambda r: keep(proc, r),
               os.path.join(a.data_dir, "client_verify.jsonl"))
    finally:
        proc.kill()
        proc.wait(timeout=60)
    return out


def fmt(v, scale=1.0, digits=0):
    if v is None:
        return "not measured"
    return f"{v / scale:,.{digits}f}"


def report(recs, title):
    windows = [r for r in recs if r.get("phase") == "window"]
    restart = next((r for r in recs if r.get("phase") == "restart"), None)
    verify = next((r for r in recs if r.get("phase") == "verify"), None)
    failed = next((r for r in recs if r.get("phase") == "load_failed"), None)
    if failed:
        return 1, f"the load client exited {failed['exit']}: a PUT failed or a GET was wrong\n"
    if not windows or restart is None or verify is None:
        return 2, "missing phases: need windows, a restart and a verify\n"
    lines = [f"## {title}", "",
             f"Hot set: the first {HOT_SET:,} objects are kept in memory; later ones go to the redb spill index.", "",
             "| objects after the window | PUT ops/s | PUT P50 / P99, ms | GET P99 during PUTs, ms | GET ops/s after | GET P99 after, ms | peak RSS so far, MiB |",
             "|---|---|---|---|---|---|---|"]
    for w in windows:
        mark = " (past the hot set)" if w["objects"] > HOT_SET else ""
        during = fmt(w.get("get_during_p99_us"), 1000, 2) if w.get("get_during_count") else "not measured"
        lines.append(
            f"| {w['objects']:,}{mark} | {fmt(w['put_ops'])} | {fmt(w['put_p50_us'], 1000, 2)} / {fmt(w['put_p99_us'], 1000, 2)}"
            f" | {during} | {fmt(w['burst_ops'])} | {fmt(w['burst_p99_us'], 1000, 2)} | {fmt(w.get('server_peak_rss_mib'), 1, 1)} |")
    lines += ["",
              f"Restart with {restart['objects']:,} objects: answered after {restart['boot_secs']:,.1f} s"
              f" (peak RSS during the boot {fmt(restart.get('server_peak_rss_mib'), 1, 1)} MiB).",
              f"After the restart: {verify['gets']:,} GETs at {fmt(verify['get_ops'])} ops/s,"
              f" P99 {fmt(verify['get_p99_us'], 1000, 2)} ms."]
    wrong = sum(r.get("wrong", 0) for r in windows) + verify.get("wrong", 0)
    lines.append(f"Wrong answers: {wrong}.")
    return (0 if wrong == 0 else 1), "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--server", help="awanfs binary (to run the measurement)")
    ap.add_argument("--client", help="hot_set_bench binary")
    ap.add_argument("--data-dir")
    ap.add_argument("--objects", type=int, default=1_000_000)
    ap.add_argument("--window", type=int, default=50_000)
    ap.add_argument("--threads", type=int, default=64)
    ap.add_argument("--budget", default="2GiB")
    ap.add_argument("--boot-timeout", type=float, default=3600)
    ap.add_argument("--results", required=True, help="JSON lines: written by a run, read with --report-only")
    ap.add_argument("--report")
    ap.add_argument("--report-only", action="store_true", help="only render --results")
    ap.add_argument("--title", default="Today's server past its hot set (#210)")
    a = ap.parse_args()
    if a.report_only:
        with open(a.results, encoding="utf-8") as fh:
            recs = [json.loads(l) for l in fh if l.strip().startswith("{")]
    else:
        if not (a.server and a.client and a.data_dir):
            ap.error("--server, --client and --data-dir are needed to run")
        recs = run(a)
        with open(a.results, "w", encoding="utf-8") as fh:
            fh.writelines(json.dumps(r) + "\n" for r in recs)
    code, text = report(recs, a.title)
    print(text)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as fh:
            fh.write(text)
    return code


if __name__ == "__main__":
    sys.exit(main())
