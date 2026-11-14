"""STATE_MACHINES.md stays true: every explicit row names a function and tests that exist.

    python3 scripts/check_state_machines.py [STATE_MACHINES.md]

Exit 1 with one line per problem. An explicit row with no function or no test is a problem
too: "explicit" means the transition function and its tests are there.
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def rows(text):
    """[(machine, status, [(file, fn)], [test])] from the table."""
    out = []
    for line in text.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 7 or cells[0] in ("Machine", "") or set(cells[0]) <= {"-"}:
            continue
        status = cells[2].split()[0] if cells[2] else ""
        fns = re.findall(r"`([\w/.]+)::(\w+)`", cells[3])
        tests = re.findall(r"`(\w+)`", cells[4])
        out.append((cells[0], status, fns, tests))
    return out


def problems(text, root=ROOT):
    found = []
    table = rows(text)
    if not table:
        return ["no table rows found"]
    for machine, status, fns, tests in table:
        if status not in ("explicit", "wired", "implicit", "not"):
            found.append(f"{machine}: status {status!r} is not explicit, wired, implicit or not built")
        if status not in ("explicit", "wired"):
            continue
        if not fns:
            found.append(f"{machine}: {status} but no `file::function`")
        if not tests:
            found.append(f"{machine}: {status} but no tests named")
        sources = {}
        for path, fn in fns:
            full = os.path.join(root, path)
            if not os.path.isfile(full):
                found.append(f"{machine}: {path} does not exist")
                continue
            src = sources.setdefault(path, open(full, encoding="utf-8").read())
            if not re.search(rf"\bfn {re.escape(fn)}\b", src):
                found.append(f"{machine}: fn {fn} is not in {path}")
        for t in tests:
            if not any(re.search(rf"#\[test\]\s*\n\s*fn {re.escape(t)}\b", s) for s in sources.values()):
                found.append(f"{machine}: test {t} is not in {', '.join(sources) or 'any named file'}")
    return found


def main(argv=None):
    args = argv if argv is not None else sys.argv[1:]
    path = args[0] if args else os.path.join(ROOT, "STATE_MACHINES.md")
    found = problems(open(path, encoding="utf-8").read())
    for p in found:
        print(p)
    if not found:
        print("STATE_MACHINES.md: every explicit row has its function and tests")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
