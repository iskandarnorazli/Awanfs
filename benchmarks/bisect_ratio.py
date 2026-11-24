"""Bisect a get_64k AwanFS / MinIO throughput ratio change across candidate commits.

Builds `s3-server` at each candidate commit in a detached `git worktree` under the runner's temp
directory, then measures the `get_64k` workload (64 KiB GET, 128 objects, 16 workers) from
`benchmarks/ratio_check.py` against every build and MinIO for `--rounds` rounds (default 7),
rotating the server order each round.

Usage:
  python benchmarks/bisect_ratio.py --commits "ffed2a9 e8290ef d73bbd2^ d73bbd2 528524a 2abdb39" \\
      --minio-bin .tmp/minio/minio --mc-bin .tmp/minio/mc --rounds 7 --duration 3 \\
      --output bisect_report.md

Exit codes: 0 all builds measured, 1 invalid inputs or unmeasured round, 2 build or server setup failed.
"""
import argparse
from dataclasses import asdict, dataclass
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, TypeVar

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import ratio_check as rc  # noqa: E402
import run_warp_benchmark as rwb  # noqa: E402

EXE = ".exe" if sys.platform == "win32" else ""
DEFAULT_COMMITS: Tuple[str, ...] = (
    "ffed2a9",
    "e8290ef",
    "d73bbd2^",
    "d73bbd2",
    "528524a",
    "2abdb39",
)
DEFAULT_ROUNDS = 7
DEFAULT_DURATION = 3.0
WORKLOAD_NAME = "get_64k"

T = TypeVar("T")


@dataclass(frozen=True)
class BuiltCommit:
    commit: str
    sha: str
    subject: str
    bin_path: str


@dataclass(frozen=True)
class BuildSummary:
    commit: str
    sha: str
    subject: str
    ratios: Tuple[float, ...]
    median: float
    minimum: float
    maximum: float
    awanfs_median_mib: float
    minio_median_mib: float


def default_work_dir() -> str:
    return os.environ.get("RUNNER_TEMP") or tempfile.gettempdir()


def parse_commits(raw: object) -> List[str]:
    """Parse space-, newline-, or comma-separated commit specs while preserving order."""
    if raw is None:
        raise ValueError("at least one commit is required")
    items: Iterable[str]
    if isinstance(raw, str):
        items = [raw]
    else:
        items = raw
    commits: List[str] = []
    for item in items:
        for token in re.split(r"[\s,]+", str(item).strip()):
            if token:
                commits.append(token)
    if not commits:
        raise ValueError("at least one commit is required")
    return commits


def rotate_order(items: Sequence[T], round_index: int) -> List[T]:
    """Return `items` rotated left by `round_index % len(items)` so every item is first once in N rounds."""
    if not items:
        return []
    offset = round_index % len(items)
    return list(items[offset:]) + list(items[:offset])


def schedule_rounds(items: Sequence[T], rounds: int) -> List[List[T]]:
    """Return the per-round rotated execution order for `rounds` rounds."""
    if rounds < 1:
        raise ValueError("rounds must be >= 1")
    return [rotate_order(items, r) for r in range(rounds)]


def compute_round_ratios(awanfs_mibs: Sequence[float], minio_mibs: Sequence[float]) -> List[float]:
    """Compute per-round AwanFS / MinIO ratios; rejects non-positive MinIO throughput."""
    if len(awanfs_mibs) != len(minio_mibs):
        raise ValueError(f"mismatched round counts: {len(awanfs_mibs)} vs {len(minio_mibs)}")
    if not awanfs_mibs:
        raise ValueError("at least one round is required")
    ratios: List[float] = []
    for idx, (a_mib, m_mib) in enumerate(zip(awanfs_mibs, minio_mibs), 1):
        if m_mib <= 0.0:
            raise ValueError(f"MinIO moved no bytes in round {idx} ({m_mib})")
        ratios.append(float(a_mib) / float(m_mib))
    return ratios


def summarize_values(values: Sequence[float]) -> Tuple[float, float, float]:
    """Return `(median, min, max)` for a non-empty sequence of numbers."""
    if not values:
        raise ValueError("cannot summarize empty values")
    nums = [float(v) for v in values]
    return float(statistics.median(nums)), float(min(nums)), float(max(nums))


def ranges_overlap(a: BuildSummary, b: BuildSummary) -> bool:
    """True when `[a.minimum, a.maximum]` and `[b.minimum, b.maximum]` share at least one point."""
    return max(a.minimum, b.minimum) <= min(a.maximum, b.maximum)


def ranges_disjoint(a: BuildSummary, b: BuildSummary) -> bool:
    """True when two builds' min-to-max ranges do not overlap."""
    return not ranges_overlap(a, b)


def _format_step_delta(prev_med: float, curr_med: float) -> str:
    delta = curr_med - prev_med
    pct = (delta / prev_med * 100.0) if prev_med else 0.0
    return f"{prev_med:.3f} -> {curr_med:.3f} ({delta:+.3f}, {pct:+.1f}%)"


def evaluate_verdict(summaries: Sequence[BuildSummary]) -> Tuple[bool, str]:
    """Decide whether a step is real: ranges of two builds must not overlap."""
    if len(summaries) < 2:
        return False, "Verdict: the builds are within noise of each other."

    adj_steps = [
        (i, summaries[i - 1], summaries[i])
        for i in range(1, len(summaries))
        if ranges_disjoint(summaries[i - 1], summaries[i])
    ]
    if adj_steps:
        _, prev, curr = adj_steps[0]
        extra = ""
        if len(adj_steps) > 1:
            others = ", ".join(c.commit for _, _, c in adj_steps[1:])
            extra = f"; further step(s) at {others}"
        line = (
            f"Verdict: the step is real and starts at commit {curr.commit} "
            f"(median {_format_step_delta(prev.median, curr.median)}; "
            f"spread {prev.commit} [{prev.minimum:.3f}..{prev.maximum:.3f}] vs "
            f"{curr.commit} [{curr.minimum:.3f}..{curr.maximum:.3f}]{extra})."
        )
        return True, line

    disjoint_pairs = [
        (j - i, -abs(summaries[j].median - summaries[i].median), i, j)
        for i in range(len(summaries))
        for j in range(i + 1, len(summaries))
        if ranges_disjoint(summaries[i], summaries[j])
    ]
    if disjoint_pairs:
        _, _, i, j = min(disjoint_pairs)
        earlier, later = summaries[i], summaries[j]
        best_k = max(
            range(i + 1, j + 1),
            key=lambda k: abs(summaries[k].median - summaries[k - 1].median),
        )
        curr = summaries[best_k]
        line = (
            f"Verdict: the step is real and starts at commit {curr.commit} "
            f"(non-overlapping ranges {earlier.commit} [{earlier.minimum:.3f}..{earlier.maximum:.3f}] "
            f"vs {later.commit} [{later.minimum:.3f}..{later.maximum:.3f}], "
            f"median {_format_step_delta(earlier.median, later.median)}, intermediate builds overlap)."
        )
        return True, line

    return False, "Verdict: the builds are within noise of each other."


def _run_cmd(cmd: List[str], env: Optional[Dict[str, str]] = None, error_prefix: str = "") -> str:
    res = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    if res.returncode != 0:
        detail = (res.stderr or res.stdout or f"exit {res.returncode}").strip()
        prefix = f"{error_prefix}: " if error_prefix else ""
        raise RuntimeError(f"{prefix}{detail[-2000:]}")
    return res.stdout


def build_commits(
    commits: Sequence[str],
    repo_root: str = ROOT,
    work_dir: Optional[str] = None,
) -> List[BuiltCommit]:
    """Check out each commit in a git worktree under `work_dir` and build `s3-server`."""
    base_dir = work_dir or default_work_dir()
    os.makedirs(base_dir, exist_ok=True)
    wt_root = os.path.join(base_dir, "worktrees")
    bins_root = os.path.join(base_dir, "bins")
    target_dir = os.path.join(base_dir, "target")
    os.makedirs(wt_root, exist_ok=True)
    os.makedirs(bins_root, exist_ok=True)

    built: List[BuiltCommit] = []
    cargo_env = dict(os.environ, CARGO_TARGET_DIR=target_dir)

    for idx, ref in enumerate(commits):
        full_sha = _run_cmd(
            ["git", "-C", repo_root, "rev-parse", "--verify", f"{ref}^{{commit}}"],
            error_prefix=f"cannot resolve commit {ref!r}",
        ).strip()
        short_sha = full_sha[:7]
        subject = _run_cmd(
            ["git", "-C", repo_root, "log", "-1", "--format=%s", full_sha],
            error_prefix=f"cannot read subject for {ref!r}",
        ).strip()

        wt_dir = os.path.join(wt_root, f"{idx:02d}_{short_sha}")
        if os.path.exists(wt_dir):
            subprocess.run(["git", "-C", repo_root, "worktree", "remove", "--force", wt_dir], capture_output=True)
            shutil.rmtree(wt_dir, ignore_errors=True)

        _run_cmd(
            ["git", "-C", repo_root, "worktree", "add", "--detach", wt_dir, full_sha],
            error_prefix=f"git worktree add failed for {ref} ({short_sha})",
        )
        try:
            built_bin = os.path.join(target_dir, "release", "s3-server" + EXE)
            if os.path.exists(built_bin):
                os.remove(built_bin)
            manifest = os.path.join(wt_dir, "server", "Cargo.toml")
            print(f"Building commit {ref} ({short_sha}): {subject}", flush=True)
            _run_cmd(
                ["cargo", "build", "--locked", "--release", "--bin", "s3-server", "--manifest-path", manifest],
                env=cargo_env,
                error_prefix=f"cargo build failed for {ref} ({short_sha})",
            )
            if not os.path.isfile(built_bin):
                raise RuntimeError(f"cargo build succeeded for {ref} ({short_sha}) but binary is missing at {built_bin}")
            out_dir = os.path.join(bins_root, f"{idx:02d}_{short_sha}")
            os.makedirs(out_dir, exist_ok=True)
            dest_bin = os.path.join(out_dir, "s3-server" + EXE)
            shutil.copy2(built_bin, dest_bin)
            built.append(BuiltCommit(commit=ref, sha=short_sha, subject=subject, bin_path=dest_bin))
        finally:
            subprocess.run(["git", "-C", repo_root, "worktree", "remove", "--force", wt_dir], capture_output=True)
            shutil.rmtree(wt_dir, ignore_errors=True)
            subprocess.run(["git", "-C", repo_root, "worktree", "prune"], capture_output=True)

    return built


def measure_one_get_64k(start_fn: Callable[[], Tuple[object, int, str]], duration: float) -> Tuple[float, float, Optional[float]]:
    """Run only `ratio_check.WORKLOADS['get_64k']` against one server instance and clean up."""
    size, conc, mode = rc.WORKLOADS[WORKLOAD_NAME]
    proc, port, data = start_fn()
    try:
        mib_val, cpu_val = rc._measure_phase(proc, port, size, conc, duration, mode)
        peak = rc.get_process_peak_rss_mb(proc.pid)
        return float(mib_val), float(peak), cpu_val
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(data, ignore_errors=True)


def measure_bisect(
    builds: Sequence[BuiltCommit],
    minio_bin: str,
    mc_bin: str,
    rounds: int = DEFAULT_ROUNDS,
    duration: float = DEFAULT_DURATION,
) -> Tuple[List[BuildSummary], List[Dict[str, object]]]:
    """Measure every build and MinIO for `rounds` rounds in rotating order."""
    if not builds:
        raise ValueError("at least one build is required")
    participants: List[Tuple[str, str, Optional[BuiltCommit]]] = [
        ("build", b.commit, b) for b in builds
    ] + [("minio", "minio", None)]
    round_schedules = schedule_rounds(participants, rounds)

    awan_mibs: Dict[str, List[float]] = {b.commit: [] for b in builds}
    minio_mibs: List[float] = []
    rounds_data: List[Dict[str, object]] = []

    for r_idx, order in enumerate(round_schedules):
        order_labels = [label for _, label, _ in order]
        print(f"=== Round {r_idx + 1}/{rounds}: order = {', '.join(order_labels)} ===", flush=True)
        round_entry: Dict[str, object] = {
            "round": r_idx + 1,
            "order": order_labels,
            "builds": {},
            "minio_mib": 0.0,
        }
        round_build_mibs: Dict[str, float] = {}
        round_minio_mib = 0.0

        for kind, label, built in order:
            print(f"  round {r_idx + 1}/{rounds}: {label}", flush=True)
            if kind == "minio":
                mib_val, _peak, _cpu = measure_one_get_64k(
                    lambda: rc.start_minio(minio_bin, mc_bin),
                    duration,
                )
                if mib_val <= 0.0:
                    raise RuntimeError(f"MinIO moved no bytes in round {r_idx + 1} ({mib_val})")
                round_minio_mib = mib_val
            else:
                assert built is not None
                mib_val, _peak, _cpu = measure_one_get_64k(
                    lambda bp=built.bin_path: rc.start_awanfs(bp),
                    duration,
                )
                if mib_val <= 0.0:
                    raise RuntimeError(f"AwanFS {label} moved no bytes in round {r_idx + 1} ({mib_val})")
                round_build_mibs[label] = mib_val

        minio_mibs.append(round_minio_mib)
        round_entry["minio_mib"] = round_minio_mib
        for b in builds:
            a_val = round_build_mibs[b.commit]
            awan_mibs[b.commit].append(a_val)
            round_entry["builds"][b.commit] = {
                "mib_per_sec": a_val,
                "ratio": a_val / round_minio_mib,
            }
        rounds_data.append(round_entry)

    minio_med = float(statistics.median(minio_mibs))
    summaries: List[BuildSummary] = []
    for b in builds:
        ratios = compute_round_ratios(awan_mibs[b.commit], minio_mibs)
        med, lo, hi = summarize_values(ratios)
        summaries.append(
            BuildSummary(
                commit=b.commit,
                sha=b.sha,
                subject=b.subject,
                ratios=tuple(ratios),
                median=med,
                minimum=lo,
                maximum=hi,
                awanfs_median_mib=float(statistics.median(awan_mibs[b.commit])),
                minio_median_mib=minio_med,
            )
        )
    return summaries, rounds_data


def render_report(
    cmd: str,
    summaries: Sequence[BuildSummary],
    rounds_data: Sequence[Dict[str, object]],
    rounds: int,
    duration: float,
    verdict_line: str,
    run_id: Optional[str] = None,
    run_url: Optional[str] = None,
    pin_labels: Optional[Tuple[str, str]] = None,
) -> str:
    """Format the markdown report starting with `Command:` and ending with `verdict_line`."""
    size, conc, _ = rc.WORKLOADS[WORKLOAD_NAME]
    pool_keys = 4 if size >= 1024 * 1024 else 8
    objects = conc * pool_keys

    lines = [f"Command: `{cmd}`", ""]
    if run_id:
        if run_url:
            lines.append(f"Run id: [{run_id}]({run_url})")
        else:
            lines.append(f"Run id: {run_id}")
        lines.append("")

    lines += [
        f"# get_64k AwanFS / MinIO ratio bisect ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})",
        "",
        f"Machine: {platform.node()} ({platform.platform()}, {os.cpu_count()} CPUs). "
        f"Workload: `{WORKLOAD_NAME}` ({size // 1024} KiB GET, {objects} objects, {conc} workers), "
        f"{rounds} rounds per build of {duration:g}s each, order rotating each round.",
    ]
    if pin_labels:
        lines.append(f"Core pinning: server `{pin_labels[0]}`, client `{pin_labels[1]}`.")

    lines += [
        "",
        "| commit | sha | subject | AwanFS MiB/s (median) | MinIO MiB/s (median) | median ratio | min ratio | max ratio |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for s in summaries:
        subj = s.subject.replace("|", "\\|")
        lines.append(
            f"| `{s.commit}` | `{s.sha}` | {subj} | {s.awanfs_median_mib:.2f} | {s.minio_median_mib:.2f} "
            f"| {s.median:.3f} | {s.minimum:.3f} | {s.maximum:.3f} |"
        )

    if rounds_data:
        headers = ["round", "order", "MinIO MiB/s"] + [f"`{s.commit}` MiB/s (ratio)" for s in summaries]
        lines += [
            "",
            "## Per-round measurements (`get_64k`)",
            "",
            "| " + " | ".join(headers) + " |",
            "|" + "|".join("---" for _ in headers) + "|",
        ]
        for r in rounds_data:
            order_str = ", ".join(str(x) for x in r["order"])
            row = [str(r["round"]), order_str, f"{float(r['minio_mib']):.2f}"]
            bmap = r["builds"]
            for s in summaries:
                entry = bmap[s.commit]
                row.append(f"{float(entry['mib_per_sec']):.2f} ({float(entry['ratio']):.3f})")
            lines.append("| " + " | ".join(row) + " |")

    lines += ["", verdict_line, ""]
    return "\n".join(lines)


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--commits",
        nargs="+",
        default=list(DEFAULT_COMMITS),
        help="Commit refs to build and measure in order (space- or comma-separated).",
    )
    ap.add_argument("--minio-bin", default=None, help="Path to minio binary (auto-detected under .tmp/minio if omitted)")
    ap.add_argument("--mc-bin", default=None, help="Path to mc binary (auto-detected under .tmp/minio if omitted)")
    ap.add_argument("--rounds", "--runs", dest="rounds", type=int, default=DEFAULT_ROUNDS, help="Rounds per build (default 7)")
    ap.add_argument("--duration", type=float, default=DEFAULT_DURATION, help="Seconds per get_64k phase (default 3.0)")
    ap.add_argument("--work-dir", default=None, help="Temp directory for git worktrees and build artifacts (defaults to RUNNER_TEMP)")
    ap.add_argument("--repo-root", default=ROOT, help="Git repository root")
    ap.add_argument("--output", "--report", dest="output", help="Write the markdown report here")
    ap.add_argument("--results", help="Write JSON results here")
    ap.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID") or None, help="CI run id for the report")
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = build_parser()
    a = ap.parse_args(argv)
    try:
        commits = parse_commits(a.commits)
    except ValueError as e:
        print(f"invalid commits: {e}", file=sys.stderr)
        return 1
    if a.rounds < 1:
        print("rounds must be >= 1", file=sys.stderr)
        return 1
    if a.duration <= 0:
        print("duration must be > 0", file=sys.stderr)
        return 1

    minio_bin, mc_bin = a.minio_bin, a.mc_bin
    if not minio_bin or not mc_bin:
        found_minio, found_mc = rwb.find_minio_binaries()
        minio_bin = minio_bin or found_minio
        mc_bin = mc_bin or found_mc
    if not minio_bin or not mc_bin or not os.path.isfile(minio_bin) or not os.path.isfile(mc_bin):
        print(f"MinIO and mc binaries are required (got minio={minio_bin!r}, mc={mc_bin!r})", file=sys.stderr)
        return 2

    argv_list = list(argv) if argv is not None else sys.argv[1:]
    cmd = ("python benchmarks/bisect_ratio.py " + " ".join(argv_list)).strip()

    pin_labels = None
    if rc.can_pin():
        _, cli_mask = rc.affinity_masks()
        rc.set_affinity_mask(None, cli_mask)
        pin_labels = rc.affinity_labels()

    base_work = a.work_dir or default_work_dir()
    os.makedirs(base_work, exist_ok=True)
    run_work = tempfile.mkdtemp(prefix="bisect_ratio_", dir=base_work)

    run_url = None
    if a.run_id and os.environ.get("GITHUB_SERVER_URL") and os.environ.get("GITHUB_REPOSITORY"):
        run_url = f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{a.run_id}"

    try:
        try:
            builds = build_commits(commits, repo_root=a.repo_root, work_dir=run_work)
        except RuntimeError as e:
            print(f"build failed: {e}", file=sys.stderr)
            return 2

        try:
            summaries, rounds_data = measure_bisect(
                builds,
                minio_bin=minio_bin,
                mc_bin=mc_bin,
                rounds=a.rounds,
                duration=a.duration,
            )
        except RuntimeError as e:
            print(f"measurement failed: {e}", file=sys.stderr)
            return 2
        except ValueError as e:
            print(f"invalid measurement: {e}", file=sys.stderr)
            return 1
    finally:
        shutil.rmtree(run_work, ignore_errors=True)

    is_real, verdict_line = evaluate_verdict(summaries)
    report = render_report(
        cmd=cmd,
        summaries=summaries,
        rounds_data=rounds_data,
        rounds=a.rounds,
        duration=a.duration,
        verdict_line=verdict_line,
        run_id=a.run_id,
        run_url=run_url,
        pin_labels=pin_labels,
    )
    print(report)
    if a.output:
        ensure_parent(a.output)
        with open(a.output, "w", encoding="utf-8") as fh:
            fh.write(report)
    if a.results:
        ensure_parent(a.results)
        payload = {
            "command": cmd,
            "run_id": a.run_id,
            "is_real_step": is_real,
            "verdict": verdict_line,
            "summaries": [asdict(s) for s in summaries],
            "rounds": rounds_data,
        }
        with open(a.results, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
