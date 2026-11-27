"""Does this run of s3-tests.yml need the ceph suite? (#458)

Reads the changed files, one per line, on stdin. Prints `run=true` or `run=false`, and the
job summary line for a skip, and appends them to $GITHUB_OUTPUT and $GITHUB_STEP_SUMMARY
when those are set.

- A dispatch on the map branch, or a pull request, that changes only the conformance map
  skips: the map came from a full run of the suite in the nightly run that wrote it.
- A pull request that changes no server code, s3-tests file or this workflow skips.
- Anything else runs the suite.
"""

import argparse
import os
import re
import sys

MAP = "tests/s3tests/CONFORMANCE_MAP.md"
MAP_BRANCH = "refs/heads/bot/conformance-map"
SUITE_PATHS = re.compile(r"^(server/src/|server/Cargo\.(toml|lock)$|tests/s3tests/|\.github/workflows/s3-tests\.yml$)")

MAP_ONLY = "s3-tests skipped: conformance map refresh; the suite already ran in the nightly run that wrote it."
NO_SERVER_CODE = "s3-tests skipped: this pull request changes no server code, s3-tests file or this workflow."


def decide(event, ref, files):
    """(run, summary): whether the suite runs, and the summary line when it does not."""
    files = [f for f in files if f]
    map_only = bool(files) and all(f == MAP for f in files)
    if event == "workflow_dispatch" and ref == MAP_BRANCH:
        return (False, MAP_ONLY) if map_only else (True, None)
    if event != "pull_request":
        return True, None
    if map_only:
        return False, MAP_ONLY
    if any(SUITE_PATHS.match(f) for f in files):
        return True, None
    return False, NO_SERVER_CODE


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--event", required=True)
    ap.add_argument("--ref", default="")
    args = ap.parse_args(argv)
    files = [line.strip() for line in sys.stdin.read().splitlines()]
    run, summary = decide(args.event, args.ref, files)
    line = "run=" + ("true" if run else "false")
    print(line)
    if summary:
        print(summary)
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    step = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary and step:
        with open(step, "a", encoding="utf-8") as f:
            f.write(summary + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
