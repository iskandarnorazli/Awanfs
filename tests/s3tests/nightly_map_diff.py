"""Compare counts between two CONFORMANCE_MAP.md files, ignoring header/run-id diffs.

    python3 tests/s3tests/nightly_map_diff.py tests/s3tests/CONFORMANCE_MAP.md /tmp/CONFORMANCE_MAP.md
"""
import argparse
import re
import sys
from typing import Dict, List, Optional, Tuple

TOTAL_RE = re.compile(
    r"^(\d+)\s+test functions:\s+(\d+)\s+pass,\s+(\d+)\s+fail,\s+(\d+)\s+skipped,\s+(\d+)\s+unknown\b",
    re.MULTILINE,
)
ROW_RE = re.compile(
    r"^\|\s*([^|]+?)\s*\|\s*[^|]*?\s*\|\s*[^|]*?\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*$"
)

Counts = Tuple[Optional[Tuple[int, int, int, int, int]], Dict[str, Tuple[int, int, int]]]


def extract_counts(text: str) -> Counts:
    """Extract (total_tuple, {area: (pass, fail, skip)}) from a CONFORMANCE_MAP.md document."""
    m = TOTAL_RE.search(text)
    totals: Optional[Tuple[int, int, int, int, int]] = (
        tuple(int(x) for x in m.groups()) if m else None  # type: ignore[assignment]
    )
    areas: Dict[str, Tuple[int, int, int]] = {}
    for raw in text.splitlines():
        stripped = raw.strip()
        if stripped.startswith("## "):
            break
        row = ROW_RE.match(stripped)
        if not row:
            continue
        area = row.group(1).strip()
        areas[area] = (int(row.group(2)), int(row.group(3)), int(row.group(4)))
    if totals is None and not areas:
        raise ValueError("no conformance counts found in map text")
    return totals, areas


def counts_differ(committed_text: str, generated_text: str) -> bool:
    """Return True if total or per-area counts differ between two map documents."""
    return extract_counts(committed_text) != extract_counts(generated_text)


def describe_diff(committed_text: str, generated_text: str) -> List[str]:
    """Return human-readable lines describing any total or per-area count changes."""
    old_totals, old_areas = extract_counts(committed_text)
    new_totals, new_areas = extract_counts(generated_text)
    lines: List[str] = []
    if old_totals != new_totals:
        lines.append(f"totals: {old_totals} -> {new_totals}")
    all_areas = list(dict.fromkeys(list(old_areas) + list(new_areas)))
    for area in all_areas:
        before = old_areas.get(area)
        after = new_areas.get(area)
        if before != after:
            lines.append(f"{area}: {before} -> {after}")
    return lines


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Compare conformance map counts.")
    ap.add_argument("committed", help="Path to committed CONFORMANCE_MAP.md")
    ap.add_argument("generated", help="Path to newly generated CONFORMANCE_MAP.md")
    ap.add_argument("--github-output", default="", help="Optional GITHUB_OUTPUT file to append changed=true|false")
    args = ap.parse_args(argv)

    try:
        with open(args.committed, encoding="utf-8") as fh:
            committed_text = fh.read()
        with open(args.generated, encoding="utf-8") as fh:
            generated_text = fh.read()
        changed = counts_differ(committed_text, generated_text)
        details = describe_diff(committed_text, generated_text) if changed else []
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    flag = "true" if changed else "false"
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as fh:
            fh.write(f"changed={flag}\n")

    print(f"changed={flag}")
    for line in details:
        print(f"  {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
