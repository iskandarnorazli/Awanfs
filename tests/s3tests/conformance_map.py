"""Which ceph s3-tests belong to which area of work, and where each area stands.

    python3 tests/s3tests/conformance_map.py /tmp/s3-tests/s3tests/functional/test_s3.py \
        [/tmp/s3-tests/s3tests/functional/test_headers.py] tests/s3tests/passing.txt \
        --junit /tmp/s3tests-results.xml [--junit /tmp/s3tests-headers-results.xml] \
        --report CONFORMANCE_MAP.md

Every test function in the pinned suite is assigned to an area read from `features.toml`
at the repository root (single source of truth). Each feature with `ceph` rules names its
area, owner issue (or "out of scope") and the section of CONFORMANCE_DESIGN.md that says how
it is built. A test no rule matches is "unassigned", and the script exits 1: a new upstream
test has to be given an owner in `features.toml` before the map is trusted again.

Status per test: "pass" when it is in passing.txt; otherwise what the junit file says
("fail", "skip"), or "unknown" without one. The junit file also gives each failure's first
line, which the report lists so the owner of an area starts from the symptom.
"""
import argparse
import ast
import os
import re
import sys
import tomllib
import xml.etree.ElementTree as ET
from collections import Counter, OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REGISTRY_PATH = Path(__file__).resolve().parent.parent.parent / "features.toml"


def load_areas(registry_path: Path = REGISTRY_PATH) -> List[Tuple[str, str, str, Tuple[str, ...]]]:
    """Load `(area, owner, design_section, ceph_patterns)` rules from `features.toml`."""
    with open(registry_path, "rb") as fh:
        data = tomllib.load(fh)
    rules: List[Tuple[str, str, str, Tuple[str, ...]]] = []
    for feat in data.get("feature", []):
        pats = feat.get("ceph", [])
        if not pats:
            continue
        area = feat.get("area") or feat["id"]
        owner = "out of scope" if feat.get("status") == "out-of-scope" else feat.get("issue", "")
        section = feat.get("design_section", "")
        rules.append((area, owner, section, tuple(pats)))
    return rules


AREAS = load_areas()


def suite_tests(path: str, prefix: str = "") -> "OrderedDict[str, List[str]]":
    """{test function: its pytest marker names}, in file order."""
    with open(path, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    out: "OrderedDict[str, List[str]]" = OrderedDict()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
            marks = []
            for d in node.decorator_list:
                m = re.match(r"pytest\.mark\.(\w+)", ast.unparse(d))
                if m:
                    marks.append(m.group(1))
            out[f"{prefix}{node.name}"] = marks
    return out


def _matches(patterns: Tuple[str, ...], qname: str, markers: set) -> bool:
    for pat in patterns:
        if pat.startswith("re:"):
            if re.search(pat[3:], qname):
                return True
        elif pat.startswith("mark:"):
            if pat[5:] in markers:
                return True
        elif qname == pat:
            return True
    return False


def area_of(name: str, markers) -> Optional[Tuple[str, str, str]]:
    ms = set(markers)
    qname = name if "::" in name else f"test_s3::{name}"
    for area, issue, section, patterns in AREAS:
        if _matches(patterns, qname, ms):
            return area, issue, section
    return None


def passing_names(path: str) -> set:
    out = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                if line.startswith("test_s3::"):
                    line = line[len("test_s3::"):]
                out.add(line.split("[")[0])
    return out


def junit_status(paths) -> Dict[str, Tuple[str, str]]:
    """{test function: (status, first line of the failure)}. A parametrized function is
    "fail" when any case failed, else "skip" when any was skipped, else "pass"."""
    if isinstance(paths, (str, os.PathLike)):
        paths = [paths]
    rank = {"pass": 0, "skip": 1, "fail": 2}
    out: Dict[str, Tuple[str, str]] = {}
    for path in paths:
        for case in ET.parse(path).getroot().iter("testcase"):
            cls = case.get("classname", "")
            mod = cls.split(".")[-1] if cls else ""
            base = case.get("name", "").split("[")[0]
            name = f"{mod}::{base}" if mod and mod != "test_s3" else base
            status, why = "pass", ""
            for child in case:
                if child.tag in ("failure", "error"):
                    status = "fail"
                    why = (child.get("message") or child.text or "").strip().splitlines()[0][:160] if (child.get("message") or child.text) else ""
                elif child.tag == "skipped" and status != "fail":
                    status = "skip"
                    msg = (child.get("message") or "").strip()
                    txt = (child.text or "").strip()
                    raw = txt if (not msg or msg.lower() == "skipped") and txt else (msg or txt)
                    why = raw.splitlines()[0][:160] if raw else ""
            if name not in out or rank[status] > rank[out[name][0]]:
                out[name] = (status, why)
    return out


def build(tests, passing: set, junit: Optional[Dict[str, Tuple[str, str]]]):
    """(rows, unassigned). rows: [(test, area, issue, section, status, why)]."""
    rows, unassigned = [], []
    for name, marks in tests.items():
        a = area_of(name, marks)
        if a is None:
            unassigned.append(name)
            a = ("unassigned", "", "")
        if name in passing:
            status, why = "pass", ""
        elif junit is not None:
            status, why = junit.get(name, ("unknown", ""))
            if status == "pass":
                why = "passes; add it to passing.txt"
        else:
            status, why = "unknown", ""
        rows.append((name, a[0], a[1], a[2], status, why))
    return rows, unassigned


def render(rows, unassigned, suite_ref: str) -> str:
    order = list(OrderedDict.fromkeys(a[0] for a in AREAS)) + ["unassigned"]
    info = {a[0]: (a[1], a[2]) for a in AREAS}
    counts = {a: Counter() for a in order}
    for _, area, _, _, status, _ in rows:
        counts[area][status] += 1
    total = Counter(r[4] for r in rows)
    lines = [
        "# ceph s3-tests: the conformance map",
        "",
        f"Generated by `tests/s3tests/conformance_map.py` from {suite_ref}. Do not edit by hand.",
        "",
        f"{len(rows)} test functions: {total['pass']} pass, {total['fail']} fail, {total['skip']} skipped, {total['unknown']} unknown. "
        "The nightly job counts parametrized cases, so its totals are higher.",
        "",
        "| area | owner | design section | pass | fail | skip |",
        "|---|---|---|---|---|---|",
    ]
    for a in order:
        c = counts[a]
        if not sum(c.values()):
            continue
        issue, section = info.get(a, ("", ""))
        lines.append(f"| {a} | {issue} | {section} | {c['pass']} | {c['fail']} | {c['skip'] + c['unknown']} |")
    for a in order:
        todo = [r for r in rows if r[1] == a and r[4] != "pass"]
        if not todo:
            continue
        lines += ["", f"## {a}: {len(todo)} not passing", ""]
        for name, _, _, _, status, why in todo:
            lines.append(f"- `{name}` ({status}){': ' + why.replace('|', '/') if why else ''}")
    if unassigned:
        lines += ["", "**Unassigned tests (add a rule to AREAS):** " + ", ".join(f"`{n}`" for n in unassigned)]
    lines.append("")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("suite", nargs="+", help="s3tests/functional/test_s3.py (and optionally test_headers.py) of the pinned suite")
    ap.add_argument("passing")
    ap.add_argument("--junit", action="append", default=[])
    ap.add_argument("--suite-ref", default="the pinned suite")
    ap.add_argument("--report", default="")
    a = ap.parse_args(argv)
    tests: "OrderedDict[str, List[str]]" = OrderedDict()
    for path in a.suite:
        prefix = "test_headers::" if os.path.basename(path) == "test_headers.py" else ""
        tests.update(suite_tests(path, prefix=prefix))
    junits = [j for j in a.junit if j]
    rows, unassigned = build(tests, passing_names(a.passing), junit_status(junits) if junits else None)
    text = render(rows, unassigned, a.suite_ref)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as fh:
            fh.write(text)
    else:
        print(text)
    if unassigned:
        print(f"{len(unassigned)} unassigned test(s): {', '.join(unassigned)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
