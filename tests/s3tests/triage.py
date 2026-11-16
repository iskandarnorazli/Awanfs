"""Group ceph s3-tests failures by normalised cause per conformance area.

    python3 tests/s3tests/triage.py /tmp/s3tests-results.xml tests/s3tests/CONFORMANCE_MAP.md \
        --out /tmp/TRIAGE.md
"""
import argparse
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from typing import Dict, List, Tuple


_DATETIME_RE = re.compile(r"datetime\.datetime\([^()]*(?:\([^()]*\)[^()]*)*\)")
_S3_URI_RE = re.compile(r"s3://[^/\s'\"]+(/[^\s'\"]*)?")
_REQ_ID_FIELD_RE = re.compile(r"(['\"]?RequestId['\"]?\s*[:=]\s*['\"])[^'\"]+(['\"])")
_REQ_ID_HEADER_RE = re.compile(
    r"(?i)\b(x-amz-request-id|x-amz-id-2|request_id)\s*[:=]\s*['\"]?[^\s'\",}]+['\"]?"
)
_BUCKET_FIELD_RE = re.compile(r"(['\"]?[Bb]ucket['\"]?\s*[:=]\s*['\"])[^'\"]+(['\"])")
_KEY_FIELD_RE = re.compile(r"(['\"]?[Kk]ey['\"]?\s*[:=]\s*['\"])[^'\"]+(['\"])")
_HEX_PREFIX_RE = re.compile(r"\b0x[0-9a-fA-F]+\b")
_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_LONG_HEX_RE = re.compile(
    r"\b(?=[0-9a-fA-F]*[0-9])(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{8,}\b"
)
_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9_.])(\d+\.\d+|\d+)(?![A-Za-z0-9_.])|(?<![A-Za-z0-9_.])(\d+\.\d+)(?=[A-Za-z])")


def _replace_number(match: re.Match) -> str:
    tok = match.group(1) or match.group(2)
    if "." not in tok and len(tok) == 3 and tok[0] in "12345":
        return tok
    return "<N>"


def normalise(line: str) -> str:
    """Normalise a failure line: strip request ids, bucket/key names, numbers and hex;
    keep the exception type, the S3 error code, and HTTP status codes."""
    s = re.sub(r"\s+", " ", line).strip()
    s = _DATETIME_RE.sub("<DATETIME>", s)
    s = _S3_URI_RE.sub(
        lambda m: "s3://<BUCKET>/<KEY>" if m.group(1) else "s3://<BUCKET>", s
    )
    s = _REQ_ID_FIELD_RE.sub(r"\1<REQ_ID>\2", s)
    s = _REQ_ID_HEADER_RE.sub(r"\1: <REQ_ID>", s)
    s = _BUCKET_FIELD_RE.sub(r"\1<BUCKET>\2", s)
    s = _KEY_FIELD_RE.sub(r"\1<KEY>\2", s)
    s = _HEX_PREFIX_RE.sub("<HEX>", s)
    s = _UUID_RE.sub("<HEX>", s)
    s = _LONG_HEX_RE.sub("<HEX>", s)
    s = _NUMBER_RE.sub(_replace_number, s)
    return s


def parse_map(path: str) -> Tuple[List[str], Dict[str, str], Dict[str, str]]:
    """Parse CONFORMANCE_MAP.md -> (area_order, area_owners, test_to_area)."""
    area_order: List[str] = []
    area_owners: Dict[str, str] = {}
    test_to_area: Dict[str, str] = {}
    current_area = ""

    table_row_re = re.compile(
        r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*[^|]+?\s*\|\s*\d+\s*\|\s*\d+\s*\|\s*\d+\s*\|$"
    )
    section_re = re.compile(r"^##\s+([^:]+):")
    test_re = re.compile(r"^-\s+`([^`]+)`\s+\(")

    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            m_row = table_row_re.match(line)
            if m_row:
                area, owner = m_row.group(1).strip(), m_row.group(2).strip()
                if area != "area" and not set(area) <= {"-"}:
                    area_order.append(area)
                    area_owners[area] = owner
                continue
            m_sec = section_re.match(line)
            if m_sec:
                current_area = m_sec.group(1).strip()
                continue
            m_test = test_re.match(line)
            if m_test and current_area:
                test_to_area[m_test.group(1)] = current_area

    return area_order, area_owners, test_to_area


def junit_failures(path: str) -> Dict[str, str]:
    """{failing test function: first line of failure message}."""
    out: Dict[str, str] = {}
    root = ET.parse(path).getroot()
    for case in root.iter("testcase"):
        name = case.get("name", "").split("[")[0]
        for child in case:
            if child.tag in ("failure", "error"):
                raw = (child.get("message") or child.text or "").strip()
                first_line = raw.splitlines()[0] if raw else ""
                if name not in out:
                    out[name] = first_line
                break
    return out


def build_triage(
    junit_path: str, map_path: str
) -> Tuple[List[Dict], Dict[str, List[Dict]]]:
    """Return (summary_rows, per_area_groups)."""
    area_order, area_owners, test_to_area = parse_map(map_path)
    failures = junit_failures(junit_path)

    grouped: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    for test_name, raw_line in failures.items():
        area = test_to_area.get(test_name, "unassigned")
        cause = normalise(raw_line) or "<empty>"
        grouped[area][cause].append(test_name)

    all_areas = list(area_order)
    for area in grouped:
        if area not in all_areas:
            all_areas.append(area)

    summary: List[Dict] = []
    per_area: Dict[str, List[Dict]] = {}
    for area in all_areas:
        causes = grouped.get(area)
        if not causes:
            continue
        owner = area_owners.get(area, "")
        groups = [
            {
                "count": len(tests),
                "cause": cause,
                "tests": sorted(tests),
                "owner": owner,
            }
            for cause, tests in causes.items()
        ]
        groups.sort(key=lambda g: (-g["count"], g["cause"]))
        per_area[area] = groups
        summary.append(
            {
                "area": area,
                "owner": owner,
                "failing": sum(g["count"] for g in groups),
                "groups": len(groups),
                "largest": groups[0]["count"],
            }
        )

    return summary, per_area


def render(summary: List[Dict], per_area: Dict[str, List[Dict]]) -> str:
    total_failing = sum(r["failing"] for r in summary)
    total_groups = sum(r["groups"] for r in summary)
    lines = [
        "# ceph s3-tests: failure triage",
        "",
        "Generated by `tests/s3tests/triage.py`. Do not edit by hand.",
        "",
        f"{total_failing} failing tests across {len(summary)} areas ({total_groups} groups).",
        "",
        "| area | failing tests | groups | largest group |",
        "|---|---|---|---|",
    ]
    for row in summary:
        lines.append(
            f"| {row['area']} | {row['failing']} | {row['groups']} | {row['largest']} |"
        )

    for row in summary:
        area = row["area"]
        owner_suffix = f" ({row['owner']})" if row["owner"] else ""
        lines += [
            "",
            f"## {area}{owner_suffix}: {row['failing']} failing, {row['groups']} groups",
            "",
            "| count | cause | tests | owner |",
            "|---|---|---|---|",
        ]
        for g in per_area[area]:
            safe_cause = g["cause"].replace("|", "/").replace("`", "'")
            tests_str = ", ".join(f"`{t}`" for t in g["tests"])
            lines.append(
                f"| {g['count']} | `{safe_cause}` | {tests_str} | {g['owner']} |"
            )

    lines.append("")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("junit", help="Path to pytest junit XML results")
    ap.add_argument("map", help="Path to CONFORMANCE_MAP.md")
    ap.add_argument("--out", default="", help="Output markdown path (default: stdout)")
    args = ap.parse_args(argv)

    summary, per_area = build_triage(args.junit, args.map)
    text = render(summary, per_area)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
