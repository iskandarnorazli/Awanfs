"""Check that every test removed from tests/s3tests/passing.txt has a RATCHET_LOG.md row.

    python3 scripts/check_ratchet_log.py <base-ref>

Compares tests/s3tests/passing.txt at <base-ref> with the working tree.
Every removed test must have a row in tests/s3tests/RATCHET_LOG.md with a
non-empty decision column.

Exit codes: 0 when no unlogged removals exist, 1 otherwise.
"""
import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set

PASSING_PATH = Path("tests/s3tests/passing.txt")
LOG_PATH = Path("tests/s3tests/RATCHET_LOG.md")


def parse_passing(text: str) -> Set[str]:
    """Return the set of listed test names, ignoring blank lines and comments."""
    return {
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    }


def parse_ratchet_log(text: str) -> Dict[str, str]:
    """Return {test_name: decision} from the markdown table in RATCHET_LOG.md."""
    entries: Dict[str, str] = {}
    saw_header = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if not cells or all(set(c) <= {"-", ":", " "} for c in cells):
            continue
        if not saw_header:
            saw_header = True
            continue
        test_name = cells[0].strip("` ")
        decision = cells[3].strip() if len(cells) >= 4 else ""
        if test_name:
            entries[test_name] = decision
    return entries


def read_base_passing(base_ref: str) -> str:
    """Read tests/s3tests/passing.txt at base_ref."""
    subprocess.run(
        ["git", "rev-parse", "--verify", f"{base_ref}^{{commit}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    res = subprocess.run(
        ["git", "show", f"{base_ref}:{PASSING_PATH.as_posix()}"],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        return ""
    return res.stdout


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("base_ref", help="Git ref to compare tests/s3tests/passing.txt against")
    args = ap.parse_args(argv)

    try:
        base_text = read_base_passing(args.base_ref)
    except subprocess.CalledProcessError as exc:
        print(f"error: invalid base-ref {args.base_ref!r}: {exc.stderr.strip()}")
        return 1

    if not PASSING_PATH.is_file():
        print(f"error: {PASSING_PATH} not found")
        return 1

    base_tests = parse_passing(base_text)
    current_tests = parse_passing(PASSING_PATH.read_text(encoding="utf-8"))
    removed = sorted(base_tests - current_tests)

    if not removed:
        print(f"0 tests removed from {PASSING_PATH} since {args.base_ref}")
        return 0

    log_entries = (
        parse_ratchet_log(LOG_PATH.read_text(encoding="utf-8"))
        if LOG_PATH.is_file()
        else {}
    )
    missing = [t for t in removed if not log_entries.get(t)]
    if missing:
        print(
            f"{len(missing)} test(s) removed from {PASSING_PATH} without a "
            f"{LOG_PATH} row with non-empty decision:"
        )
        for test in missing:
            print(f"  - {test}")
        return 1

    print(f"{len(removed)} removed test(s) accounted for in {LOG_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
