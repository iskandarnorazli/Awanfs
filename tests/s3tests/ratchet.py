"""Ratchet for the ceph s3-tests run (#47).

    python tests/s3tests/ratchet.py results.xml tests/s3tests/passing.txt [--report report.md]

Reads the JUnit XML pytest wrote. Exit 1 when a test listed in passing.txt did not pass;
otherwise 0. Prints the tests that pass now but are not listed (add them to passing.txt to
raise the ratchet) and writes a markdown report with the counts.
"""
import argparse
import sys
import xml.etree.ElementTree as ET


def outcomes(path):
    out = {}
    for case in ET.parse(path).getroot().iter("testcase"):
        name = case.get("name", "")
        cls = case.get("classname", "")
        key = f"{cls.split('.')[-1]}::{name}" if cls else name
        if case.find("failure") is not None or case.find("error") is not None:
            out[key] = "failed"
        elif case.find("skipped") is not None:
            out[key] = "skipped"
        else:
            out[key] = "passed"
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("passing")
    ap.add_argument("--report")
    a = ap.parse_args(argv)
    res = outcomes(a.results)
    try:
        listed = [l.strip() for l in open(a.passing, encoding="utf-8") if l.strip() and not l.startswith("#")]
    except FileNotFoundError:
        listed = []
    regressions = [t for t in listed if res.get(t) != "passed"]
    new = sorted(t for t, o in res.items() if o == "passed" and t not in set(listed))
    counts = {k: sum(1 for o in res.values() if o == k) for k in ("passed", "failed", "skipped")}
    lines = [
        "# ceph s3-tests against AwanFS",
        "",
        f"{len(res)} tests: {counts['passed']} passed, {counts['failed']} failed, {counts['skipped']} skipped.",
        f"Ratchet: {len(listed)} listed, {len(regressions)} of them no longer pass, {len(new)} new passes not listed.",
        "",
    ]
    if regressions:
        lines += ["## Listed tests that no longer pass", ""] + [f"- `{t}` ({res.get(t, 'not run')})" for t in regressions] + [""]
    if new:
        lines += ["## Passing but not listed (add to tests/s3tests/passing.txt)", "", "```"] + new + ["```", ""]
    text = "\n".join(lines)
    print(text)
    if a.report:
        open(a.report, "w", encoding="utf-8").write(text)
    return 1 if regressions else 0


if __name__ == "__main__":
    sys.exit(main())
