"""Which ceph s3-tests belong to which area of work, and where each area stands.

    python3 tests/s3tests/conformance_map.py /tmp/s3-tests/s3tests/functional/test_s3.py \
        [/tmp/s3-tests/s3tests/functional/test_headers.py] tests/s3tests/passing.txt \
        --junit /tmp/s3tests-results.xml [--junit /tmp/s3tests-headers-results.xml] \
        --report CONFORMANCE_MAP.md

Every test function in the pinned suite is assigned to exactly one area by the first rule
that matches its pytest markers or its name (AREAS, in order). An area names the issue that
owns it and the section of CONFORMANCE_DESIGN.md that says how it is built. A test no rule
matches is "unassigned", and the script exits 1: a new upstream test has to be given an owner
before the map is trusted again.

Status per test: "pass" when it is in passing.txt; otherwise what the junit file says
("fail", "skip"), or "unknown" without one. The junit file also gives each failure's first
line, which the report lists so the owner of an area starts from the symptom.
"""
import argparse
import ast
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter, OrderedDict
from typing import Dict, List, Optional, Tuple

# (area, issue, design section, name regex or None, markers). First match wins, so the order
# is part of the map: narrow areas come before the broad ones that would also match.
AREAS: List[Tuple[str, str, str, Optional[str], frozenset]] = [
    # SigV2 request signing: out of scope (owner, 2026-11-17; CONFORMANCE_DESIGN.md section 9).
    ("sigv2", "out of scope", "9", r"_aws2$|^test_cors_presigned_(get|put)_object(_tenant)?_v2$", frozenset({"auth_aws2"})),
    # Botocore and urllib3 put Content-Length back, so no client can send this request; upstream
    # marks it fails_on_rgw (owner, 2026-11-18; #160).
    ("request-headers-unsendable", "out of scope", "9", r"^test_headers::test_object_create_bad_contentlength_none$", frozenset()),
    ("request-headers", "#160", "3", r"^test_headers::", frozenset()),
    ("rgw-only", "out of scope", "9", r"^test_(head_bucket_usage|account_usage|bucket_head_extended|get_object_torrent)$|_unordered$|_tenant$", frozenset()),
    # An anonymous DELETE is refused before the bucket is looked up: authorization, #122 (owner, 2026-11-18; #140).
    ("anonymous-bucket-gone", "#122", "5", r"^test_object_delete_key_bucket_gone$", frozenset()),
    ("bucket-logging", "#153", "8.4", r"logging", frozenset({"bucket_logging", "bucket_logging_cleanup", "fails_without_logging_rollover"})),
    ("storage-classes", "out of scope", "9", r"transition|cloud|restore|storage_class", frozenset({"lifecycle_transition", "cloud_transition", "cloud_restore"})),
    ("encryption", "#121", "8.2", r"sse|encrypt|kms", frozenset({"encryption", "sse_s3", "bucket_encryption"})),
    ("lifecycle", "#118", "8.3", r"lifecycle", frozenset({"lifecycle", "lifecycle_expiration"})),
    ("object-lock", "#120", "8.1", r"object_lock|legal_hold|retention", frozenset()),
    ("post-object", "#119", "7.3", r"^test_post_object|presigned_post", frozenset()),
    ("cors", "#116", "7.2", r"cors", frozenset()),
    ("checksums", "#117", "7.1", r"checksum", frozenset({"checksum"})),
    ("object-attributes-api", "#117", "7.1", r"object_attributes", frozenset()),
    ("tagging", "#115", "6.2", r"tagging|_tags?_|_tags$", frozenset({"tagging"})),
    ("policy", "#122", "5", r"policy|public_block|block_public|ignore_public", frozenset({"bucket_policy"})),
    ("acl-ownership", "#122", "5", r"acl|grant|ownership|owner|access_bucket|_not_owned", frozenset({"object_ownership"})),
    ("conditional", "#112", "6.3", r"if_?match|if_?none_?match|ifnonmatch|if_?modified|if_?unmodified|conditional", frozenset({"conditional_write"})),
    ("multipart", "#111", "6.4", r"multipart|upload_part|get_part", frozenset()),
    ("object-metadata", "#152", "4", r"metadata|content_?type|cache_control|expires$|content_encoding|response_headers|copy_verify", frozenset()),
    ("anonymous-and-presigned", "#122", "5", r"anon|raw_get|raw_put|x_amz_expires|presign|authenticated|_public", frozenset()),
    ("copy", "#152", "4", r"copy", frozenset({"copy"})),
    ("versioning", "#113", "6.5", r"version|delete_marker", frozenset({"delete_marker"})),
    ("listing", "#114", "6.6", r"list", frozenset({"list_objects_v2"})),
    ("bucket-basics", "#140", "6.7", r"^test_bucket_|bucket_create|naming|buckets_|bucket_gone", frozenset()),
    ("object-basics", "#140", "6.7", r"^test_object|^test_get_obj|^test_put_obj|^test_delete|^test_multi_object|^test_ranged|^test_atomic|range|^test_head|100_continue|^test_abort|^test_set_|^test_upload_", frozenset()),
]


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


def area_of(name: str, markers) -> Optional[Tuple[str, str, str]]:
    ms = set(markers)
    for area, issue, section, rx, marks in AREAS:
        if ms & marks or (rx and re.search(rx, name)):
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
    order = [a[0] for a in AREAS] + ["unassigned"]
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
