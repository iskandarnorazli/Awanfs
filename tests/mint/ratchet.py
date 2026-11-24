"""Ratchet and log adapter for MinIO Mint conformance runs (#278).

    python3 tests/mint/ratchet.py log.json [more.json ...] tests/mint/passing.txt [--report report.md]
    python3 tests/mint/ratchet.py --check-removals origin/main

Reads Mint's JSON log output (JSON lines, concatenated JSON documents, or JSON arrays).
Exits 1 when a test listed in passing.txt did not pass or when 0 tests were parsed;
otherwise 0. Prints per-SDK pass/fail/skip counts and unlisted passing tests.
"""
import argparse
import subprocess
import sys
from collections import Counter, OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from features import collect_mint_results, decode_mint_entries  # noqa: E402

MINT_STATUS_NAMES = {"pass": "passed", "skip": "skipped", "fail": "failed"}


def parse_mint_logs(paths: Sequence[Path]) -> "OrderedDict[str, Tuple[str, str, str]]":
    """Return `{sdk::function: (sdk, status, detail)}` from one or more Mint JSON logs."""
    return collect_mint_results(paths, status_names=MINT_STATUS_NAMES)


def parse_passing_list(text: str) -> List[str]:
    """Return ordered test names from `passing.txt`, skipping blanks and `#` comments."""
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def per_sdk_counts(
    results: "OrderedDict[str, Tuple[str, str, str]]",
) -> "OrderedDict[str, Dict[str, int]]":
    """Aggregate pass/fail/skip/total counts per SDK."""
    counts: "OrderedDict[str, Counter]" = OrderedDict()
    for sdk, status, _ in results.values():
        counts.setdefault(sdk, Counter())[status] += 1
    out: "OrderedDict[str, Dict[str, int]]" = OrderedDict()
    for sdk in sorted(counts):
        c = counts[sdk]
        out[sdk] = {
            "passed": c["passed"],
            "failed": c["failed"],
            "skipped": c["skipped"],
            "total": c["passed"] + c["failed"] + c["skipped"],
        }
    return out


def build_report(
    results: "OrderedDict[str, Tuple[str, str, str]]",
    listed: Sequence[str],
    passing_path_label: str = "tests/mint/passing.txt",
) -> Tuple[int, str]:
    """Build Mint ratchet markdown report and return `(exit_code, text)`."""
    if not results:
        lines = [
            "# MinIO Mint against AwanFS",
            "",
            "Error: Mint produced 0 test results.",
            "",
        ]
        return 1, "\n".join(lines)

    listed_set: Set[str] = set(listed)
    regressions = [t for t in listed if results.get(t, ("", "", ""))[1] != "passed"]
    new_passes = sorted(
        key for key, (_, st, _) in results.items() if st == "passed" and key not in listed_set
    )
    totals = Counter(st for _, st, _ in results.values())
    sdk_rows = per_sdk_counts(results)

    lines = [
        "# MinIO Mint against AwanFS",
        "",
        f"{len(results)} tests: {totals['passed']} passed, {totals['failed']} failed, {totals['skipped']} skipped.",
        f"Ratchet: {len(listed)} listed, {len(regressions)} of them no longer pass, {len(new_passes)} new passes not listed.",
        "",
        "## Per-SDK summary",
        "",
        "| SDK | pass | fail | skip | total |",
        "|---|---|---|---|---|",
    ]
    for sdk, c in sdk_rows.items():
        lines.append(f"| {sdk} | {c['passed']} | {c['failed']} | {c['skipped']} | {c['total']} |")
    lines.append(
        f"| **Total** | **{totals['passed']}** | **{totals['failed']}** | **{totals['skipped']}** | **{len(results)}** |"
    )
    lines.append("")

    if regressions:
        lines += ["## Listed tests that no longer pass", ""]
        for t in regressions:
            if t in results:
                _, st, detail = results[t]
                suffix = f": {detail}" if detail else ""
                lines.append(f"- `{t}` ({st}){suffix}")
            else:
                lines.append(f"- `{t}` (not run)")
        lines.append("")

    if new_passes:
        lines += [f"## Passing but not listed (add to {passing_path_label})", "", "```"]
        lines.extend(new_passes)
        lines += ["```", ""]

    return (1 if regressions else 0), "\n".join(lines)


def _parse_ratchet_log_table(text: str) -> Dict[str, str]:
    entries: Dict[str, str] = {}
    saw_header = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if not cells or all(set(c) <= {"-", ":", " "} for c in cells):
            continue
        if not saw_header:
            saw_header = True
            continue
        test_name = cells[0].strip("` ")
        decision = cells[3].strip() if len(cells) >= 4 else ""
        if test_name:
            entries[test_name] = decision
    return entries


def check_removals(
    base_ref: str,
    passing_path: Path = Path("tests/mint/passing.txt"),
    ratchet_log_path: Path = Path("tests/mint/RATCHET_LOG.md"),
    cwd: Optional[Path] = None,
) -> List[str]:
    """Return tests removed from `passing_path` since `base_ref` lacking a `RATCHET_LOG.md` decision."""
    run_cwd = str(cwd) if cwd else None
    subprocess.run(
        ["git", "rev-parse", "--verify", f"{base_ref}^{{commit}}"],
        cwd=run_cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    rel_passing = passing_path.as_posix()
    if cwd and passing_path.is_absolute():
        rel_passing = passing_path.relative_to(cwd).as_posix()
    res = subprocess.run(
        ["git", "show", f"{base_ref}:{rel_passing}"],
        cwd=run_cwd,
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        return []
    base_tests = set(parse_passing_list(res.stdout))
    current_tests = set(parse_passing_list(passing_path.read_text(encoding="utf-8"))) if passing_path.is_file() else set()
    removed = sorted(base_tests - current_tests)
    if not removed:
        return []
    log_entries = (
        _parse_ratchet_log_table(ratchet_log_path.read_text(encoding="utf-8"))
        if ratchet_log_path.is_file()
        else {}
    )
    return [t for t in removed if not log_entries.get(t)]


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("paths", nargs="*", help="Mint log JSON path(s) followed by passing.txt")
    ap.add_argument("--report", help="Write markdown summary report to this path")
    ap.add_argument("--check-removals", metavar="BASE_REF", help="Verify removed tests have RATCHET_LOG.md rows")
    ap.add_argument("--passing", type=Path, default=Path("tests/mint/passing.txt"))
    ap.add_argument("--ratchet-log", type=Path, default=Path("tests/mint/RATCHET_LOG.md"))
    args = ap.parse_args(argv)

    if args.check_removals:
        try:
            missing = check_removals(args.check_removals, args.passing, args.ratchet_log)
        except subprocess.CalledProcessError as exc:
            print(f"error: invalid base-ref {args.check_removals!r}: {exc.stderr.strip()}")
            return 1
        if missing:
            print(f"{len(missing)} test(s) removed from {args.passing} without a {args.ratchet_log} decision row:")
            for t in missing:
                print(f"  - {t}")
            return 1
        print(f"0 unlogged removals from {args.passing} since {args.check_removals}")
        return 0

    if len(args.paths) < 2:
        ap.error("expected <mint-log.json> [<more.json> ...] <passing.txt>")

    log_paths = [Path(p) for p in args.paths[:-1]]
    passing_path = Path(args.paths[-1])

    results = parse_mint_logs(log_paths)
    listed = parse_passing_list(passing_path.read_text(encoding="utf-8")) if passing_path.is_file() else []
    rc, text = build_report(results, listed)
    print(text)
    if args.report:
        Path(args.report).write_text(text, encoding="utf-8")
    return rc


if __name__ == "__main__":
    sys.exit(main())
