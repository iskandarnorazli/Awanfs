"""Measure what AwanFS compaction copies when a volume holds live large and small objects (#269).

    python benchmarks/compaction_cost.py --bin server/target/release/examples/compaction_cost \
        --data-dir "$RUNNER_TEMP/compaction_cost" --results results.jsonl --output report.md

Fills a volume with interleaved small (4 KiB) and large (64 MiB) objects across three mixes,
deletes a given fraction of the small ones, runs online compaction via `compaction_cost`, and
reports bytes copied, bytes reclaimed, seconds, and the share of copied bytes from large objects.
"""

import argparse
from dataclasses import dataclass
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile

MIB = 1 << 20
KIB = 1 << 10
EXE = ".exe" if sys.platform == "win32" else ""
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@dataclass(frozen=True)
class MixSpec:
    name: str
    small_count: int
    small_size: int
    large_count: int
    large_size: int
    label: str


DEFAULT_MIXES = (
    MixSpec(
        "small-heavy",
        32768,
        4 * KIB,
        1,
        64 * MIB,
        "32,768 x 4 KiB + 1 x 64 MiB (33% large payload)",
    ),
    MixSpec(
        "balanced",
        16384,
        4 * KIB,
        4,
        64 * MIB,
        "16,384 x 4 KiB + 4 x 64 MiB (80% large payload)",
    ),
    MixSpec(
        "90-10",
        90,
        4 * KIB,
        10,
        64 * MIB,
        "90 x 4 KiB + 10 x 64 MiB (90% / 10% by count)",
    ),
)

MIX_BY_NAME = {m.name: m for m in DEFAULT_MIXES}
DEFAULT_DELETE_FRACTIONS = (0.50, 0.90)
DEFAULT_RUNS = 3


def default_binary() -> str:
    return os.path.join(
        ROOT, "server", "target", "release", "examples", "compaction_cost" + EXE
    )


def default_data_dir() -> str:
    return os.environ.get("RUNNER_TEMP") or tempfile.gettempdir()


def load(lines) -> list:
    out = []
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            continue
        out.append(json.loads(line))
    return out


def format_bytes(n: float) -> str:
    if n >= MIB:
        return f"{n / MIB:,.2f} MiB"
    if n >= KIB:
        return f"{n / KIB:,.1f} KiB"
    return f"{n:,.0f} B"


def _run_failed_reason(r: dict) -> str:
    if r.get("busy"):
        return "reclaimed nothing (volume busy)"
    if r.get("volume_removed") is False:
        return "reclaimed nothing (volume not removed)"
    if (r.get("bytes_reclaimed") or 0) <= 0:
        return "reclaimed nothing"
    return ""


def _summarize(runs: list) -> dict:
    copied = statistics.median(r["bytes_copied"] for r in runs)
    small_copied = statistics.median(r["small_bytes_copied"] for r in runs)
    large_copied = statistics.median(r["large_bytes_copied"] for r in runs)
    reclaimed = statistics.median(r["bytes_reclaimed"] for r in runs)
    secs = [float(r["seconds"]) for r in runs]
    med_secs = statistics.median(secs)
    shares = [
        (r["large_bytes_copied"] / r["bytes_copied"]) if r["bytes_copied"] else 0.0
        for r in runs
    ]
    reasons = [msg for r in runs if (msg := _run_failed_reason(r))]
    return {
        "runs": len(runs),
        "bytes_copied": copied,
        "small_bytes_copied": small_copied,
        "large_bytes_copied": large_copied,
        "bytes_reclaimed": reclaimed,
        "seconds": med_secs,
        "min_seconds": min(secs),
        "max_seconds": max(secs),
        "large_share": statistics.median(shares),
        "write_amp": (copied / reclaimed) if reclaimed > 0 else None,
        "failed_reason": reasons[0] if reasons else "",
    }


def _group_records(records: list, kind: str) -> dict:
    groups = {}
    for r in records:
        if r.get("kind", "mix") != kind:
            continue
        key = (r["mix"], round(float(r["delete_fraction"]), 6))
        groups.setdefault(key, []).append(r)
    return groups


def report(
    records: list,
    title: str = "Compaction cost with live large and small objects (#269)",
    mixes=None,
    delete_fractions=None,
) -> "tuple[int, str]":
    mix_names = tuple(mixes) if mixes is not None else tuple(m.name for m in DEFAULT_MIXES)
    fracs = (
        tuple(float(f) for f in delete_fractions)
        if delete_fractions is not None
        else DEFAULT_DELETE_FRACTIONS
    )
    mix_groups = _group_records(records, "mix")
    base_groups = _group_records(records, "small-only")

    missing = [
        (m, f)
        for m in mix_names
        for f in fracs
        if (m, round(f, 6)) not in mix_groups
    ]
    if missing:
        return 2, f"missing runs for: {missing}\n"

    lines = [
        f"# {title}",
        "",
        "Each row reports the median across runs (seconds also shows min..max). "
        "`bytes copied` and `bytes reclaimed` measure on-disk 4 KiB sector-aligned needle bytes.",
        "",
    ]
    desc_rows = [MIX_BY_NAME[m] for m in mix_names if m in MIX_BY_NAME]
    if desc_rows:
        lines += ["Mixes:"]
        for spec in desc_rows:
            lines.append(f"- `{spec.name}`: {spec.label}")
        lines.append("")

    lines += [
        "| mix | small delete | runs | bytes copied | large copied | small copied | bytes reclaimed | seconds (range) | large share | write amp | status |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    zero_reasons = []
    summaries = {}
    for m in mix_names:
        for f in fracs:
            key = (m, round(f, 6))
            s = _summarize(mix_groups[key])
            summaries[key] = s
            status = s["failed_reason"] or "ok"
            if s["failed_reason"]:
                zero_reasons.append(f"{m} (delete {f:.0%}): {s['failed_reason']}")
            amp = f"{s['write_amp']:.2f}x" if s["write_amp"] is not None else "n/a"
            rng = f"{s['seconds']:.3f} ({s['min_seconds']:.3f}..{s['max_seconds']:.3f})"
            lines.append(
                f"| `{m}` | {f:.0%} | {s['runs']} | {format_bytes(s['bytes_copied'])} "
                f"| {format_bytes(s['large_bytes_copied'])} | {format_bytes(s['small_bytes_copied'])} "
                f"| {format_bytes(s['bytes_reclaimed'])} | {rng} | {s['large_share'] * 100:.2f}% "
                f"| {amp} | {status} |"
            )

    base_keys = [k for k in summaries if k in base_groups]
    if base_keys:
        lines += [
            "",
            "## Against live small objects alone (without large objects)",
            "",
            "| mix | small delete | mix seconds | small-only seconds | extra seconds from large | mix copied | small-only copied |",
            "|---|---|---|---|---|---|---|",
        ]
        for m in mix_names:
            for f in fracs:
                key = (m, round(f, 6))
                if key not in base_groups:
                    continue
                ms = summaries[key]
                bs = _summarize(base_groups[key])
                if bs["failed_reason"]:
                    zero_reasons.append(
                        f"{m} small-only (delete {f:.0%}): {bs['failed_reason']}"
                    )
                extra = ms["seconds"] - bs["seconds"]
                lines.append(
                    f"| `{m}` | {f:.0%} | {ms['seconds']:.3f} | {bs['seconds']:.3f} "
                    f"| {extra:+.3f} | {format_bytes(ms['bytes_copied'])} "
                    f"| {format_bytes(bs['bytes_copied'])} |"
                )

    lines.append("")
    if zero_reasons:
        lines.append("Result invalid: compaction reclaimed nothing:")
        for reason in zero_reasons:
            lines.append(f"- {reason}")
        return 1, "\n".join(lines) + "\n"

    return 0, "\n".join(lines) + "\n"


def run_one(
    binary: str,
    base_data_dir: str,
    mix_name: str,
    small_count: int,
    small_size: int,
    large_count: int,
    large_size: int,
    delete_fraction: float,
    compaction_rate: int,
    seed: int,
    kind: str,
) -> dict:
    run_dir = tempfile.mkdtemp(prefix=f"cc_{mix_name}_{kind}_", dir=base_data_dir)
    stdout_path = os.path.join(run_dir, "stdout.jsonl")
    stderr_path = os.path.join(run_dir, "stderr.log")
    store_dir = os.path.join(run_dir, "store")
    try:
        cmd = [
            binary,
            "--dir",
            store_dir,
            "--small-count",
            str(small_count),
            "--small-size",
            str(small_size),
            "--large-count",
            str(large_count),
            "--large-size",
            str(large_size),
            "--delete-fraction",
            str(delete_fraction),
            "--compaction-rate",
            str(compaction_rate),
            "--seed",
            str(seed),
        ]
        with open(stdout_path, "w", encoding="utf-8") as out_fh, open(
            stderr_path, "w", encoding="utf-8"
        ) as err_fh:
            proc = subprocess.run(cmd, stdout=out_fh, stderr=err_fh, check=False)
        with open(stdout_path, encoding="utf-8") as fh:
            recs = load(fh)
        if proc.returncode != 0 or not recs:
            with open(stderr_path, encoding="utf-8") as efh:
                err_tail = efh.read()[-1000:]
            raise RuntimeError(
                f"compaction_cost exited {proc.returncode} for {mix_name}/{kind}: {err_tail}"
            )
        rec = recs[-1]
        rec["mix"] = mix_name
        rec["kind"] = kind
        return rec
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bin", default=default_binary(), help="compaction_cost example binary")
    ap.add_argument(
        "--data-dir",
        default=default_data_dir(),
        help="parent directory for temporary volume files (use RUNNER_TEMP on CI)",
    )
    ap.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    ap.add_argument(
        "--mixes",
        default=",".join(m.name for m in DEFAULT_MIXES),
        help="comma-separated mixes (small-heavy,balanced,90-10)",
    )
    ap.add_argument(
        "--delete-fractions",
        default="0.50,0.90",
        help="comma-separated fractions of small objects to delete",
    )
    ap.add_argument("--compaction-rate", type=int, default=0, help="bytes/sec throttle (0=unlimited)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--no-small-baseline",
        action="store_true",
        help="skip matched small-only runs without large objects",
    )
    ap.add_argument("--results", help="JSON lines path (written on run, read with --report-only)")
    ap.add_argument("--output", "--report", dest="output", help="Markdown report output path")
    ap.add_argument("--report-only", action="store_true", help="only render --results")
    ap.add_argument(
        "--title",
        default=f"Compaction cost with live large and small objects (#269, {platform.system()})",
    )
    a = ap.parse_args(argv)

    mixes = [m.strip() for m in a.mixes.split(",") if m.strip()]
    for m in mixes:
        if m not in MIX_BY_NAME:
            ap.error(f"unknown mix {m!r}; choose from {sorted(MIX_BY_NAME)}")
    fracs = [float(x.strip()) for x in a.delete_fractions.split(",") if x.strip()]

    argv_list = argv if argv is not None else sys.argv[1:]
    cmd = ("python benchmarks/compaction_cost.py " + " ".join(argv_list)).strip()

    if a.report_only:
        if not a.results:
            ap.error("--report-only requires --results")
        with open(a.results, encoding="utf-8") as fh:
            records = load(fh)
    else:
        if not os.path.isfile(a.bin):
            ap.error(
                f"binary not found: {a.bin} (build with cargo build --locked --release --example compaction_cost --manifest-path server/Cargo.toml)"
            )
        os.makedirs(a.data_dir, exist_ok=True)
        records = []
        for mix_name in mixes:
            spec = MIX_BY_NAME[mix_name]
            for frac in fracs:
                for run_idx in range(a.runs):
                    seed = a.seed + run_idx
                    print(
                        f"[{mix_name}] delete={frac:.0%} run {run_idx + 1}/{a.runs} (mix)...",
                        flush=True,
                    )
                    rec = run_one(
                        a.bin,
                        a.data_dir,
                        spec.name,
                        spec.small_count,
                        spec.small_size,
                        spec.large_count,
                        spec.large_size,
                        frac,
                        a.compaction_rate,
                        seed,
                        "mix",
                    )
                    records.append(rec)
                    print(json.dumps(rec), flush=True)
                    if not a.no_small_baseline:
                        print(
                            f"[{mix_name}] delete={frac:.0%} run {run_idx + 1}/{a.runs} (small-only)...",
                            flush=True,
                        )
                        base_rec = run_one(
                            a.bin,
                            a.data_dir,
                            spec.name,
                            spec.small_count,
                            spec.small_size,
                            0,
                            spec.large_size,
                            frac,
                            a.compaction_rate,
                            seed,
                            "small-only",
                        )
                        records.append(base_rec)
                        print(json.dumps(base_rec), flush=True)
        if a.results:
            ensure_parent(a.results)
            with open(a.results, "w", encoding="utf-8") as fh:
                fh.writelines(json.dumps(r) + "\n" for r in records)

    code, body = report(records, title=a.title, mixes=mixes, delete_fractions=fracs)
    full = f"Command: `{cmd}`\n\n" + body
    print(full)
    if a.output:
        ensure_parent(a.output)
        with open(a.output, "w", encoding="utf-8") as fh:
            fh.write(full)
    return code


if __name__ == "__main__":
    sys.exit(main())
