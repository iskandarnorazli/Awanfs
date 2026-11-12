"""One command to benchmark AwanFS on your own machine, against MinIO when it is available.

    python benchmarks/bench.py                 # build, get MinIO, run the ratio check, save a report
    python benchmarks/bench.py --awanfs-only   # AwanFS throughput only, no MinIO needed
    python benchmarks/bench.py --runs 3 --duration 2

Steps:
1. `cargo build --locked --release` of `server/` (needs a Rust toolchain).
2. MinIO and mc in `.tmp/minio/` (pinned to the release CI uses). If they are not there yet:
   Docker on Linux copies them out of quay.io images; otherwise they are built from source
   with git and Go. Neither available: the script says what to install and stops.
3. `benchmarks/ratio_check.py`: AwanFS and MinIO one after the other on this machine, median of
   `--runs` rounds per workload, compared with `benchmarks/ratio_baseline.json`.
4. The report goes to `measurements/bench_<date>_<host>.md`, starting with its command.

Your absolute MiB/s depend on your machine; the AwanFS/MinIO ratio is the number to compare
with CI and with other people. The baseline was recorded on a 2-CPU GitHub runner, so a
"faster" or "REGRESSION" verdict on a very different machine is a hint, not a result.
"""
import argparse
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import ratio_check  # noqa: E402

MINIO_RELEASE = "RELEASE.2025-09-07T16-13-09Z"
EXE = ".exe" if sys.platform == "win32" else ""
TOOLS = os.path.join(ROOT, ".tmp", "minio")


def run(cmd, cwd=None):
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=cwd, check=True)


def build_server() -> str:
    run(["cargo", "build", "--locked", "--release", "--bin", "s3-server",
         "--manifest-path", os.path.join(ROOT, "server", "Cargo.toml")])
    path = os.path.join(ROOT, "server", "target", "release", "s3-server" + EXE)
    if not os.path.isfile(path):
        sys.exit(f"build finished but {path} is missing")
    return path


def from_image(image: str, inner: str, out: str) -> bool:
    if sys.platform != "linux" or not shutil.which("docker"):
        return False
    if subprocess.run(["docker", "pull", image], capture_output=True).returncode != 0:
        return False
    cid = subprocess.run(["docker", "create", image], capture_output=True, text=True).stdout.strip()
    ok = subprocess.run(["docker", "cp", f"{cid}:{inner}", out], capture_output=True).returncode == 0
    subprocess.run(["docker", "rm", cid], capture_output=True)
    return ok


def from_source(repo: str, tag, out: str) -> bool:
    if not (shutil.which("git") and shutil.which("go")):
        return False
    src = os.path.join(ROOT, ".tmp", "src", os.path.basename(repo))
    if not os.path.isdir(src):
        cmd = ["git", "clone", "-q", "--depth", "1"] + (["--branch", tag] if tag else []) + [repo, src]
        run(cmd)
    env = dict(os.environ, CGO_ENABLED="0")
    print("+ go build", repo, flush=True)
    return subprocess.run(["go", "build", "-trimpath", "-o", out, "."], cwd=src, env=env).returncode == 0


def ensure_minio():
    os.makedirs(TOOLS, exist_ok=True)
    minio = os.path.join(TOOLS, "minio" + EXE)
    mc = os.path.join(TOOLS, "mc" + EXE)
    if not os.path.isfile(minio):
        if not (from_image(f"quay.io/minio/minio:{MINIO_RELEASE}", "/usr/bin/minio", minio)
                or from_source("https://github.com/minio/minio", MINIO_RELEASE, minio)):
            sys.exit("MinIO is not in .tmp/minio and could not be fetched: install Go and git (or Docker on "
                     "Linux), or put minio and mc binaries there, or run with --awanfs-only.")
    if not os.path.isfile(mc):
        if not (from_image("quay.io/minio/mc:latest", "/usr/bin/mc", mc)
                or from_source("https://github.com/minio/mc", None, mc)):
            sys.exit("mc is not in .tmp/minio and could not be fetched (see above).")
    if sys.platform != "win32":
        for p in (minio, mc):
            os.chmod(p, 0o755)
    return minio, mc


def report_path(now=None) -> str:
    stamp = time.strftime("%Y-%m-%d", time.gmtime(now))
    host = "".join(c if c.isalnum() or c in "-_" else "-" for c in platform.node()) or "host"
    return os.path.join(ROOT, "measurements", f"bench_{stamp}_{host}.md")


def awanfs_only(server: str, runs: int, duration: float, out: str, cmd: str) -> int:
    results = {name: [] for name in ratio_check.WORKLOADS}
    for r in range(runs):
        print(f"run {r + 1}/{runs}: awanfs", flush=True)
        for name, mib in ratio_check.measure(lambda: ratio_check.start_awanfs(server), duration).items():
            results[name].append(mib)
    lines = [f"Command: `{cmd}`", "",
             f"# AwanFS throughput ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})", "",
             f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs). {runs} runs, "
             "median MiB/s from successful responses. No MinIO in this run, so there is no ratio.", "",
             "| workload | size | clients | MiB/s (median) | min | max |", "|---|---|---|---|---|---|"]
    for name, (size, conc, _mode) in ratio_check.WORKLOADS.items():
        v = results[name]
        lines.append(f"| `{name}` | {size // 1024} KiB | {conc} | {statistics.median(v):.1f} | {min(v):.1f} | {max(v):.1f} |")
    text = "\n".join(lines) + "\n"
    print(text)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(f"report: {out}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--duration", type=float, default=3.0)
    ap.add_argument("--awanfs-only", action="store_true", help="skip MinIO; report AwanFS MiB/s only")
    ap.add_argument("--output", help="report path (default measurements/bench_<date>_<host>.md)")
    a = ap.parse_args(argv)
    cmd = "python benchmarks/bench.py " + " ".join(argv if argv is not None else sys.argv[1:])
    out = a.output or report_path()
    os.makedirs(os.path.dirname(out), exist_ok=True)

    server = build_server()
    if a.awanfs_only:
        return awanfs_only(server, a.runs, a.duration, out, cmd.strip())
    minio, mc = ensure_minio()
    code = ratio_check.main(["--server-bin", server, "--minio-bin", minio, "--mc-bin", mc,
                             "--runs", str(a.runs), "--duration", str(a.duration), "--report", out])
    if os.path.exists(out):
        with open(out, encoding="utf-8") as fh:
            body = fh.read()
        with open(out, "w", encoding="utf-8") as fh:
            fh.write(f"Command: `{cmd.strip()}`\n\n" + body)
        print(f"report: {out}")
    return code


if __name__ == "__main__":
    sys.exit(main())
