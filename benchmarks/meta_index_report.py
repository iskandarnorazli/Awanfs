"""Report for #207 and #26: redb against the segment stores, from meta_index_bench's JSON lines.

    python benchmarks/meta_index_report.py results.jsonl --report meta_index.md

Exit 0 when every store answered every query the same and without a wrong answer, 1 when they
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


NAMES = {"redb": "redb (the spill index before #26 stage 6)", "seg": "segments (#207 prototype)", "prod": "segments (production, #26)"}


def report(recs, title):
    """redb and at least one segment store must have both phases; every store present must give
    the same answers hash and no wrong answer."""
    stores = [s for s in ("redb", "seg", "prod") if (s, "build") in recs or (s, "query") in recs]
    missing = [(s, p) for s in stores for p in ("build", "query") if (s, p) not in recs]
    if "redb" not in stores:
        missing.append(("redb", "build"))
    if len(stores) < 2:
        missing.append(("seg or prod", "build"))
    if missing:
        return 2, f"missing phases: {missing}\n"
    q = {s: recs[(s, "query")] for s in stores}
    lines = [f"## {title}", "", "| | " + " | ".join(NAMES[s] for s in stores) + " |", "|---|" + "---|" * len(stores)]
    for phase, field, label in ROWS:
        lines.append(f"| {label} | " + " | ".join(fmt(recs[(s, phase)].get(field)) for s in stores) + " |")
    if all((s, "query_cold") in recs for s in stores):
        for field, label in (("open_secs", "open time after dropping the page cache, s"), ("get_p50_us", "get P50 after dropping the page cache, us")):
            lines.append(f"| {label} | " + " | ".join(fmt(recs[(s, "query_cold")].get(field)) for s in stores) + " |")
    same = len({q[s]["answers"] for s in stores}) == 1
    wrong = sum(q[s]["wrong"] for s in stores)
    lines += ["", "redb as AwanFS used it is cleared at every boot and rebuilt from the volumes, so its"
              " boot cost is its build time, not its open time; the segments' open time is a boot from the"
              " checkpoint #26 keeps.", "", f"Keys: {q[stores[0]]['keys']:,}. Answers hash: "
              + ", ".join(f"{s} {q[s]['answers']}" for s in stores) + ": " + ("the same." if same else "DIFFERENT."), f"Wrong answers: {wrong}."]
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
