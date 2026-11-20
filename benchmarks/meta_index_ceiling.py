"""#26's peak RSS gate: every build and query of the production metadata store stays under a
committed ceiling.

    python benchmarks/meta_index_ceiling.py results.jsonl --ceiling-mib 120

`results.jsonl` holds `meta_index_bench` lines. Exit 0 when every `prod` build and query line
has a peak RSS at or under the ceiling, 1 when one is over, 2 when there is none to check or a
line has no RSS: a check that measured nothing does not pass.
"""

import argparse
import json
import sys


def check(lines, ceiling_mib, store="prod"):
    """(exit code, report lines)."""
    rows = []
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            continue
        r = json.loads(line)
        if r.get("store") == store and r.get("phase") in ("build", "query", "query_cold"):
            rows.append(r)
    if not rows:
        return 2, ["no %s build or query lines: nothing was measured" % store]
    code, out = 0, []
    for r in rows:
        rss = r.get("peak_rss_mib")
        if not isinstance(rss, (int, float)):
            code = max(code, 2)
            out.append("%s %s: no peak RSS" % (store, r["phase"]))
            continue
        ok = rss <= ceiling_mib
        if not ok:
            code = max(code, 1)
        out.append("%s %s at %s keys: peak RSS %.1f MiB, ceiling %.1f: %s" % (store, r["phase"], r.get("keys"), rss, ceiling_mib, "ok" if ok else "OVER"))
    return code, out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--ceiling-mib", type=float, required=True)
    ap.add_argument("--store", default="prod")
    a = ap.parse_args()
    with open(a.results, encoding="utf-8") as f:
        code, out = check(f.readlines(), a.ceiling_mib, a.store)
    print("\n".join(out))
    return code


if __name__ == "__main__":
    sys.exit(main())
