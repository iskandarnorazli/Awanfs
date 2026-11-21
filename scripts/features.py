"""Feature registry checker and report generator for AwanFS (`features.toml`).

    python3 scripts/features.py check [--ceph-suite test_s3.py test_headers.py]
    python3 scripts/features.py report --ceph-junit results.xml [--mint-log log.json]

Exit codes for `check`: 0 when valid, 1 when any problem is found.
"""
import argparse
import ast
import json
import os
import re
import sys
import tomllib
import xml.etree.ElementTree as ET
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

DEFAULT_ROOT = Path(__file__).resolve().parent.parent
VALID_STATUSES = ("done", "partial", "planned", "out-of-scope")
J_FN_RE = re.compile(r"^\s*(?:function\s+)?(j_[A-Za-z0-9_]+|doctor)\s*\(\s*\)", re.MULTILINE)


def load_registry(path: Path) -> List[dict]:
    """Load and return `[[feature]]` entries from `features.toml`."""
    with open(path, "rb") as fh:
        data = tomllib.load(fh)
    feats = data.get("feature")
    if not isinstance(feats, list):
        raise ValueError(f"{path}: expected [[feature]] array of tables")
    return feats


def _is_url(value: object) -> bool:
    return isinstance(value, str) and value.startswith(("https://", "http://"))


def suite_tests(paths: Sequence[Path]) -> "OrderedDict[str, List[str]]":
    """Parse pytest `test_*` functions and their markers from Python suite files."""
    out: "OrderedDict[str, List[str]]" = OrderedDict()
    for path in paths:
        prefix = "test_headers::" if path.name == "test_headers.py" else "test_s3::"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                marks: List[str] = []
                for dec in node.decorator_list:
                    m = re.match(r"pytest\.mark\.(\w+)", ast.unparse(dec))
                    if m:
                        marks.append(m.group(1))
                out[f"{prefix}{node.name}"] = marks
    return out


def parse_passing_file(path: Path, default_prefix: str = "") -> Set[str]:
    """Read test function names (stripping `[param]` suffixes) from a passing.txt file."""
    out: Set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        base = line.split("[")[0]
        if default_prefix and "::" not in base:
            base = f"{default_prefix}{base}"
        out.add(base)
    return out


def parse_ceph_junit(paths: Sequence[Path]) -> Dict[str, Tuple[str, str]]:
    """Return `{qualified_func: (status, first_failure_line)}` from pytest JUnit XMLs."""
    rank = {"pass": 0, "skip": 1, "fail": 2}
    out: Dict[str, Tuple[str, str]] = {}
    for path in paths:
        root = ET.parse(path).getroot()
        for case in root.iter("testcase"):
            cls = case.get("classname", "")
            mod = cls.split(".")[-1] if cls else "test_s3"
            base = case.get("name", "").split("[")[0]
            qname = f"{mod}::{base}" if mod else f"test_s3::{base}"
            status, why = "pass", ""
            for child in case:
                if child.tag in ("failure", "error"):
                    status = "fail"
                    raw = (child.get("message") or child.text or "").strip()
                    why = raw.splitlines()[0][:160] if raw else ""
                elif child.tag == "skipped" and status != "fail":
                    status = "skip"
                    msg = (child.get("message") or "").strip()
                    txt = (child.text or "").strip()
                    raw = txt if (not msg or msg.lower() == "skipped") and txt else (msg or txt)
                    why = raw.splitlines()[0][:160] if raw else ""
            if qname not in out or rank[status] > rank[out[qname][0]]:
                out[qname] = (status, why)
    return out


def parse_mint_log(paths: Sequence[Path]) -> Dict[str, Tuple[str, str]]:
    """Return `{sdk::function: (status, message)}` from Mint JSON lines or JSON arrays."""
    rank = {"pass": 0, "skip": 1, "fail": 2}
    status_map = {
        "PASS": "pass",
        "PASSED": "pass",
        "OK": "pass",
        "FAIL": "fail",
        "FAILED": "fail",
        "ERROR": "fail",
        "NA": "skip",
        "SKIP": "skip",
        "SKIPPED": "skip",
    }
    out: Dict[str, Tuple[str, str]] = {}
    for path in paths:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            continue
        if text.startswith("["):
            entries = json.loads(text)
        else:
            entries = [json.loads(line) for line in text.splitlines() if line.strip()]
        for item in entries:
            sdk = item.get("name") or item.get("sdk") or ""
            fn = item.get("function") or item.get("test") or ""
            if sdk and fn:
                key = f"{sdk}::{fn}"
            else:
                key = str(sdk or fn)
            if not key:
                continue
            raw_status = str(item.get("status", "")).strip().upper()
            status = status_map.get(raw_status, "fail")
            why = str(item.get("message") or item.get("error") or "").strip().splitlines()[0][:160] if (item.get("message") or item.get("error")) else ""
            if key not in out or rank[status] > rank[out[key][0]]:
                out[key] = (status, why)
    return out


def drive_journeys(drive_script: Path) -> Set[str]:
    """Extract journey names from `scripts/verify_drive.sh` (`j_<name>` -> `<name>`, `doctor`)."""
    if not drive_script.is_file():
        return set()
    text = drive_script.read_text(encoding="utf-8")
    out: Set[str] = set()
    for fn in J_FN_RE.findall(text):
        out.add(fn[2:] if fn.startswith("j_") else fn)
    return out


def fuzz_targets(root: Path) -> Set[str]:
    """Return fuzz target names that have both Cargo `[[bin]]` and `fuzz_targets/<name>.rs`."""
    cargo_toml = root / "server" / "fuzz" / "Cargo.toml"
    targets_dir = root / "server" / "fuzz" / "fuzz_targets"
    if not cargo_toml.is_file() or not targets_dir.is_dir():
        return set()
    with open(cargo_toml, "rb") as fh:
        data = tomllib.load(fh)
    bins = {b["name"] for b in data.get("bin", []) if isinstance(b, dict) and "name" in b}
    return {b for b in bins if (targets_dir / f"{b}.rs").is_file()}


def matcher_hits(patterns: Sequence[str], test_name: str, markers: Sequence[str] = ()) -> bool:
    """Return True if `test_name` (with optional `markers`) matches any pattern in `patterns`."""
    ms = set(markers)
    for pat in patterns:
        if pat.startswith("re:"):
            if re.search(pat[3:], test_name):
                return True
        elif pat.startswith("mark:"):
            if pat[5:] in ms:
                return True
        elif test_name == pat:
            return True
    return False


def _validate_feature_schema(feats: List[dict]) -> Tuple[List[str], Dict[str, dict]]:
    problems: List[str] = []
    by_id: Dict[str, dict] = {}
    for idx, feat in enumerate(feats):
        fid = feat.get("id")
        if not isinstance(fid, str) or not fid.strip():
            problems.append(f"feature[{idx}]: missing or empty 'id'")
            continue
        if fid in by_id:
            problems.append(f"feature {fid!r}: duplicate id")
            continue
        by_id[fid] = feat

        name = feat.get("name")
        if not isinstance(name, str) or not name.strip():
            problems.append(f"{fid}: missing or empty 'name'")

        status = feat.get("status")
        if status not in VALID_STATUSES:
            problems.append(f"{fid}: invalid status {status!r}; expected one of {', '.join(VALID_STATUSES)}")

        if status == "out-of-scope" and not _is_url(feat.get("decision")):
            problems.append(f"{fid}: out-of-scope feature requires a decision URL in 'decision'")

        for suite_key in ("ceph", "mint"):
            pats = feat.get(suite_key, [])
            if not isinstance(pats, list):
                problems.append(f"{fid}: {suite_key!r} must be a list")
                continue
            for pat in pats:
                if not isinstance(pat, str) or not pat:
                    problems.append(f"{fid}: invalid empty pattern in {suite_key!r}")
                elif pat.startswith("re:"):
                    try:
                        re.compile(pat[3:])
                    except re.error as exc:
                        problems.append(f"{fid}: invalid regex {pat!r} in {suite_key!r}: {exc}")

    for fid, feat in by_id.items():
        moved = feat.get("moved", [])
        if not isinstance(moved, list):
            problems.append(f"{fid}: 'moved' must be a list of tables")
            continue
        for entry in moved:
            if not isinstance(entry, dict):
                problems.append(f"{fid}: invalid 'moved' entry {entry!r}")
                continue
            target = entry.get("to")
            if not isinstance(target, str) or target not in by_id:
                problems.append(f"{fid}: moved test {entry.get('test')!r} targets unknown feature {target!r}")
            if not _is_url(entry.get("decision")):
                problems.append(f"{fid}: moved test {entry.get('test')!r} requires a decision URL")
    return problems, by_id


def _check_done_means_passing(feats: List[dict], ceph_tests: "OrderedDict[str, List[str]]", passing: Set[str]) -> List[str]:
    problems: List[str] = []
    status_of = {f.get("id"): f.get("status") for f in feats if isinstance(f.get("id"), str)}
    for feat in feats:
        fid = feat.get("id")
        if not isinstance(fid, str):
            continue
        waits = feat.get("waits_on", {})
        if not isinstance(waits, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in waits.items()):
            problems.append(f"{fid}: waits_on must be a table of ceph test name = feature id")
            continue
        own = [t for t, marks in ceph_tests.items() if matcher_hits(feat.get("ceph", []), t, marks)]
        for test, other in sorted(waits.items()):
            if test not in own:
                problems.append(f"{fid}: waits_on names {test!r}, which is not one of its tests")
            elif test in passing:
                problems.append(f"{fid}: waits_on lists {test!r}, which passes now: remove it")
            if other not in status_of:
                problems.append(f"{fid}: waits_on {test!r} names an unknown feature {other!r}")
            elif status_of[other] == "done":
                problems.append(f"{fid}: waits_on {test!r} names {other!r}, which is done")
        if feat.get("status") != "done":
            continue
        for test in own:
            if test not in passing and test not in waits:
                problems.append(
                    f"{fid}: marked done, but its ceph test {test!r} is not in passing.txt: "
                    "make it pass, list it under waits_on with the feature it needs, or mark the feature partial"
                )
    return problems


def _check_suite_coverage(
    suite_label: str,
    feats: List[dict],
    field: str,
    known_tests: "OrderedDict[str, List[str]]",
    verify_named_exist: bool,
) -> List[str]:
    problems: List[str] = []
    # 1. Check literal names for duplicates across features, and existence if full suite is known.
    literal_owners: Dict[str, List[str]] = {}
    for feat in feats:
        fid = feat.get("id", "<unknown>")
        for pat in feat.get(field, []):
            if not isinstance(pat, str) or pat.startswith(("re:", "mark:")):
                continue
            literal_owners.setdefault(pat, []).append(fid)
            if verify_named_exist and pat not in known_tests:
                problems.append(f"{fid}: {suite_label} named test {pat!r} does not exist in suite")

    for test_name, owners in sorted(literal_owners.items()):
        if len(owners) > 1:
            problems.append(
                f"{suite_label} test {test_name!r} is claimed by multiple features: {', '.join(owners)}"
            )

    # 2. Check every test in known_tests matches exactly one feature.
    unassigned: List[str] = []
    for test_name, marks in known_tests.items():
        hits = [
            feat["id"]
            for feat in feats
            if isinstance(feat.get("id"), str)
            and matcher_hits(feat.get(field, []), test_name, marks)
        ]
        if len(hits) == 0:
            unassigned.append(test_name)
        elif len(hits) > 1:
            problems.append(
                f"{suite_label} test {test_name!r} matches multiple features: {', '.join(hits)}"
            )

    for test_name in unassigned:
        problems.append(f"{suite_label} unassigned test: {test_name}")
    return problems


def check_registry(
    root: Path = DEFAULT_ROOT,
    registry_path: Optional[Path] = None,
    ceph_suites: Optional[Sequence[Path]] = None,
    ceph_passing: Optional[Path] = None,
    ceph_junits: Optional[Sequence[Path]] = None,
    mint_passing: Optional[Path] = None,
    mint_logs: Optional[Sequence[Path]] = None,
) -> List[str]:
    """Validate `features.toml` and return a list of problem strings (empty if valid)."""
    reg_path = registry_path or (root / "features.toml")
    if not reg_path.is_file():
        return [f"missing registry file: {reg_path}"]

    try:
        feats = load_registry(reg_path)
    except Exception as exc:
        return [f"failed to load {reg_path}: {exc}"]

    problems, _ = _validate_feature_schema(feats)
    journeys = drive_journeys(root / "scripts" / "verify_drive.sh")
    fuzzes = fuzz_targets(root)

    for feat in feats:
        fid = feat.get("id")
        if not isinstance(fid, str) or not fid:
            continue
        status = feat.get("status")
        user_visible = feat.get("user_visible", True)
        srv_tests = feat.get("server_tests", [])
        drv = feat.get("drive", [])
        fzz = feat.get("fuzz", [])

        for rel in srv_tests:
            if not (root / rel).is_file():
                problems.append(f"{fid}: missing server_tests file {rel!r}")
        for j in drv:
            if j not in journeys:
                problems.append(f"{fid}: missing drive journey {j!r} in scripts/verify_drive.sh")
        for f in fzz:
            if f not in fuzzes:
                problems.append(f"{fid}: missing fuzz target {f!r} in server/fuzz/")

        if status == "done":
            if not srv_tests:
                problems.append(f"{fid}: marked done without any server_tests")
            if user_visible and not drv:
                problems.append(f"{fid}: user-visible feature marked done without any drive journey")

    # Build ceph test map from suites, junits, and passing.txt.
    ceph_tests: "OrderedDict[str, List[str]]" = OrderedDict()
    verify_ceph_literals = False
    if ceph_suites:
        ceph_tests.update(suite_tests(ceph_suites))
        verify_ceph_literals = True
    if ceph_junits:
        for qname in parse_ceph_junit(ceph_junits):
            ceph_tests.setdefault(qname, [])
        verify_ceph_literals = True

    passing_path = ceph_passing if ceph_passing is not None else (root / "tests" / "s3tests" / "passing.txt")
    if passing_path.is_file():
        for qname in sorted(parse_passing_file(passing_path, default_prefix="test_s3::")):
            ceph_tests.setdefault(qname, [])

    if ceph_tests or verify_ceph_literals:
        problems.extend(_check_suite_coverage("ceph", feats, "ceph", ceph_tests, verify_ceph_literals))

    # A feature is done only when its ceph tests pass, or each one that does not names, under
    # `waits_on`, the unfinished feature it needs (owner, 2026-11-21). Checked when the whole
    # suite is known, against passing.txt.
    if verify_ceph_literals and passing_path.is_file():
        problems.extend(_check_done_means_passing(feats, ceph_tests, set(parse_passing_file(passing_path, default_prefix="test_s3::"))))

    # Build mint test map if mint logs or tests/mint/passing.txt exist.
    mint_tests: "OrderedDict[str, List[str]]" = OrderedDict()
    verify_mint_literals = False
    if mint_logs:
        for key in parse_mint_log(mint_logs):
            mint_tests.setdefault(key, [])
        verify_mint_literals = True

    mint_pass_path = mint_passing if mint_passing is not None else (root / "tests" / "mint" / "passing.txt")
    if mint_pass_path.is_file():
        for key in sorted(parse_passing_file(mint_pass_path)):
            mint_tests.setdefault(key, [])

    if mint_tests or verify_mint_literals:
        problems.extend(_check_suite_coverage("mint", feats, "mint", mint_tests, verify_mint_literals))

    return problems


def _format_owner(feat: dict) -> str:
    if feat.get("status") == "out-of-scope":
        issue = feat.get("issue", "")
        if issue and issue != "out of scope":
            return f"out of scope ({issue})"
        return "out of scope"
    return feat.get("issue") or "—"


def build_report(
    root: Path = DEFAULT_ROOT,
    registry_path: Optional[Path] = None,
    ceph_junits: Optional[Sequence[Path]] = None,
    ceph_passing: Optional[Path] = None,
    ceph_suites: Optional[Sequence[Path]] = None,
    mint_logs: Optional[Sequence[Path]] = None,
    run_id: Optional[str] = None,
) -> str:
    """Generate `FEATURES.md` markdown content from `features.toml` and test results."""
    reg_path = registry_path or (root / "features.toml")
    feats = load_registry(reg_path)

    ceph_results = parse_ceph_junit(ceph_junits) if ceph_junits else {}
    passing_path = ceph_passing if ceph_passing is not None else (root / "tests" / "s3tests" / "passing.txt")
    passing_set = parse_passing_file(passing_path, default_prefix="test_s3::") if passing_path.is_file() else set()
    suite_marks = suite_tests(ceph_suites) if ceph_suites else OrderedDict()

    # Combine all known ceph test names preserving suite/junit order.
    all_ceph: "OrderedDict[str, List[str]]" = OrderedDict()
    for name, marks in suite_marks.items():
        all_ceph[name] = marks
    for name in ceph_results:
        all_ceph.setdefault(name, [])

    mint_results = parse_mint_log(mint_logs) if mint_logs else {}
    has_mint = bool(mint_results) or any(feat.get("mint") for feat in feats)

    # Effective ceph status per test (passing.txt overrides only if junit didn't fail; wait —
    # in report from a PR run, junit IS the live result; if passing_set is used without junit, mark pass).
    effective_ceph: Dict[str, Tuple[str, str]] = {}
    for name in all_ceph:
        if name in ceph_results:
            effective_ceph[name] = ceph_results[name]
        elif name in passing_set:
            effective_ceph[name] = ("pass", "")
        else:
            effective_ceph[name] = ("unknown", "")

    rid = run_id or os.environ.get("GITHUB_RUN_ID") or "local"
    ceph_pass_total = sum(1 for st, _ in effective_ceph.values() if st == "pass")
    ceph_fail_total = sum(1 for st, _ in effective_ceph.values() if st == "fail")
    ceph_skip_total = sum(1 for st, _ in effective_ceph.values() if st == "skip")

    lines = [
        "# AwanFS feature registry",
        "",
        f"Generated by `scripts/features.py report` from `features.toml` (s3tests run `{rid}`). Do not edit by hand.",
        "",
        f"{len(feats)} features; ceph s3-tests: {len(effective_ceph)} functions "
        f"({ceph_pass_total} pass, {ceph_fail_total} fail, {ceph_skip_total} skipped).",
        "",
    ]

    if has_mint:
        mint_pass = sum(1 for st, _ in mint_results.values() if st == "pass")
        lines.insert(
            5,
            f"Mint: {len(mint_results)} tests ({mint_pass} pass).",
        )

    lines += [
        "| feature | status | owner | ceph | mint | server_tests | drive | fuzz |",
        "|---|---|---|---|---|---|---|---|",
    ]

    done_failing: List[Tuple[str, List[Tuple[str, str, str]]]] = []
    done_missing_layers: List[Tuple[str, List[str]]] = []

    for feat in feats:
        fid = feat["id"]
        status = feat["status"]
        owner = _format_owner(feat)
        pats = feat.get("ceph", [])
        matched_ceph = [
            (name, *effective_ceph[name])
            for name, marks in all_ceph.items()
            if matcher_hits(pats, name, marks)
        ]
        if matched_ceph:
            cp = sum(1 for _, st, _ in matched_ceph if st == "pass")
            ceph_cell = f"{cp}/{len(matched_ceph)}"
        elif pats:
            ceph_cell = f"0/{len(pats)}"
        else:
            ceph_cell = "—"

        mint_pats = feat.get("mint", [])
        if has_mint and (mint_pats or mint_results):
            matched_mint = [
                (name, *mint_results[name])
                for name in mint_results
                if matcher_hits(mint_pats, name)
            ]
            if matched_mint:
                mp = sum(1 for _, st, _ in matched_mint if st == "pass")
                mint_cell = f"{mp}/{len(matched_mint)}"
            elif mint_pats:
                mint_cell = f"0/{len(mint_pats)}"
            else:
                mint_cell = "—"
        else:
            matched_mint = []
            mint_cell = "—"

        srv = feat.get("server_tests", [])
        drv = feat.get("drive", [])
        fzz = feat.get("fuzz", [])
        srv_cell = str(len(srv)) if srv else "0"
        drv_cell = ", ".join(drv) if drv else ("— (internal)" if not feat.get("user_visible", True) else "0")
        fzz_cell = ", ".join(fzz) if fzz else "0"

        lines.append(
            f"| {fid} | {status} | {owner} | {ceph_cell} | {mint_cell} | {srv_cell} | {drv_cell} | {fzz_cell} |"
        )

        if status == "done":
            non_pass = [(n, st, why) for n, st, why in matched_ceph if st != "pass"]
            non_pass += [(n, st, why) for n, st, why in matched_mint if st != "pass"]
            if non_pass:
                done_failing.append((fid, non_pass))

            missing: List[str] = []
            if not matched_ceph and not pats:
                missing.append("ceph")
            if has_mint and not matched_mint and not mint_pats:
                missing.append("mint")
            if not srv:
                missing.append("server_tests")
            if feat.get("user_visible", True) and not drv:
                missing.append("drive")
            if not fzz:
                missing.append("fuzz")
            if missing:
                done_missing_layers.append((fid, missing))

    lines += ["", "## Gaps", ""]
    lines += ["### Done features with failing or non-passing listed tests", ""]
    if done_failing:
        for fid, items in done_failing:
            lines.append(f"- **{fid}** ({len(items)} not passing):")
            for name, st, why in items:
                suffix = f": {why.replace('|', '/')}" if why else ""
                lines.append(f"  - `{name}` ({st}){suffix}")
    else:
        lines.append("- None.")

    lines += ["", "### Done features missing evidence in some layer", ""]
    if done_missing_layers:
        for fid, missing in done_missing_layers:
            lines.append(f"- **{fid}**: no {', '.join(missing)}")
    else:
        lines.append("- None.")

    lines.append("")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    ck = sub.add_parser("check", help="Validate features.toml and suite coverage")
    ck.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    ck.add_argument("--registry", type=Path, default=None)
    ck.add_argument("--ceph-suite", action="extend", nargs="+", type=Path, default=[])
    ck.add_argument("--ceph-passing", type=Path, default=None)
    ck.add_argument("--ceph-junit", action="extend", nargs="+", type=Path, default=[])
    ck.add_argument("--mint-passing", type=Path, default=None)
    ck.add_argument("--mint-log", action="extend", nargs="+", type=Path, default=[])

    rp = sub.add_parser("report", help="Generate FEATURES.md from JUnit / Mint logs")
    rp.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    rp.add_argument("--registry", type=Path, default=None)
    rp.add_argument("--ceph-junit", action="extend", nargs="+", type=Path, required=True)
    rp.add_argument("--ceph-passing", type=Path, default=None)
    rp.add_argument("--ceph-suite", action="extend", nargs="+", type=Path, default=[])
    rp.add_argument("--mint-log", action="extend", nargs="+", type=Path, default=[])
    rp.add_argument("--run-id", default=None)
    rp.add_argument("--output", default=None, help="Output markdown path (default: <root>/FEATURES.md; '-' for stdout)")

    args = ap.parse_args(argv)
    if args.cmd == "check":
        problems = check_registry(
            root=args.root,
            registry_path=args.registry,
            ceph_suites=args.ceph_suite or None,
            ceph_passing=args.ceph_passing,
            ceph_junits=args.ceph_junit or None,
            mint_passing=args.mint_passing,
            mint_logs=args.mint_log or None,
        )
        for p in problems:
            print(p)
        if not problems:
            reg = args.registry or (args.root / "features.toml")
            feats = load_registry(reg)
            print(f"features.toml: {len(feats)} features OK, 0 problems")
        return 1 if problems else 0

    text = build_report(
        root=args.root,
        registry_path=args.registry,
        ceph_junits=args.ceph_junit,
        ceph_passing=args.ceph_passing,
        ceph_suites=args.ceph_suite or None,
        mint_logs=args.mint_log or None,
        run_id=args.run_id,
    )
    if args.output == "-":
        print(text, end="")
    else:
        out_path = Path(args.output) if args.output else (args.root / "FEATURES.md")
        out_path.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
