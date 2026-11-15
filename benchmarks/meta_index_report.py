"""Report for #207: redb against the segment prototype, from meta_index_bench's JSON lines.

    python benchmarks/meta_index_report.py results.jsonl --report meta_index.md

Exit 0 when both stores answered every query the same and without a wrong answer, 1 when they
did not (the comparison means nothing then), 2 when a phase is missing.
"""

import argparse
import json
import sys

ROWS = [
    ("build", "puts_per_s", "inserts per second"),
    ("build", "secs", "build time, s"),
    ("build", "bytes_on_disk", "bytes on disk"),
    ("build", "peak_rss_mib", "peak RSS while building, MiB"),
    ("query", "open_secs", "open time, s"),
    ("query", "peak_rss_mib", "peak RSS while querying, MiB"),
    ("query", "reads_per_get", "read calls per get"),
    ("query", "get_p50_us", "get P50, us"),
    ("query", "get_p99_us", "get P99, us"),
    ("query", "reads_per_absent_get", "read calls per absent get"),
    ("query", "absent_p50_us", "absent get P50, us"),
    ("query", "absent_p99_us", "absent get P99, us"),
    ("query", "scan_ms_each", "prefix scan of up to 1000 keys, ms"),
]


def load(lines):
    """{(store, phase): record}; a later line for the same pair wins (a warm run after a cold one
    is given a different phase name by the workflow)."""
    out = {}
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            continue
        r = json.loads(line)
        out[(r["store"], r["phase"])] = r
    return out


def fmt(v):
    if v is None:
        return "not measured"
    if isinstance(v, float):
        return f"{v:,.3f}" if v < 100 else f"{v:,.0f}"
    return f"{v:,}"


def report(recs, title):
    missing = [(s, p) for s in ("redb", "seg") for p in ("build", "query") if (s, p) not in recs]
    if missing:
        return 2, f"missing phases: {missing}\n"
    q = {s: recs[(s, "query")] for s in ("redb", "seg")}
    lines = [f"## {title}", "", "| | redb (AwanFS's spill index) | segments (prototype) |", "|---|---|---|"]
    for phase, field, label in ROWS:
        lines.append(f"| {label} | {fmt(recs[('redb', phase)].get(field))} | {fmt(recs[('seg', phase)].get(field))} |")
    for phase in ("query_cold",):
        if ("redb", phase) in recs and ("seg", phase) in recs:
            lines.append(f"| open time after dropping the page cache, s | {fmt(recs[('redb', phase)].get('open_secs'))} | {fmt(recs[('seg', phase)].get('open_secs'))} |")
            lines.append(f"| get P50 after dropping the page cache, us | {fmt(recs[('redb', phase)].get('get_p50_us'))} | {fmt(recs[('seg', phase)].get('get_p50_us'))} |")
    same = q["redb"]["answers"] == q["seg"]["answers"]
    wrong = q["redb"]["wrong"] + q["seg"]["wrong"]
    lines += ["", "redb as AwanFS uses it today is cleared at every boot and rebuilt from the volumes, so its"
              " boot cost is its build time, not its open time; the segments' open time is a boot from the"
              " checkpoint #26 would keep.", "", f"Keys: {q['seg']['keys']:,}. Answers hash: redb {q['redb']['answers']}, segments {q['seg']['answers']}: "
              + ("the same." if same else "DIFFERENT."), f"Wrong answers: {wrong}."]
    return (0 if same and wrong == 0 else 1), "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("results")
    ap.add_argument("--title", default="Metadata index: redb against segments (#207)")
    ap.add_argument("--report")
    a = ap.parse_args()
    with open(a.results, encoding="utf-8") as fh:
        code, text = report(load(fh), a.title)
    print(text)
    if a.report:
        with open(a.report, "w", encoding="utf-8") as fh:
            fh.write(text)
    return code


if __name__ == "__main__":
    sys.exit(main())
