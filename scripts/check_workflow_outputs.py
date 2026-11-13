"""No benchmark job writes its report over a committed file.

A report step with `if: always()` prints whatever is at its path. If that path holds a file
committed in the repo, a job that measured nothing prints someone else's numbers as its own
output. Run 43398517562 did exactly that: it timed out measuring nothing and printed
measurements/s3_warp_minio_vs_awan.md, a 16-core laptop's report, into its job summary.

    python scripts/check_workflow_outputs.py

Exit codes: 0 every --output path is untracked, 1 one of them is a tracked file.
"""
import re
import subprocess
import sys
from pathlib import Path

WORKFLOWS = Path(".github/workflows")
OUTPUT = re.compile(r"--output[= ]([A-Za-z0-9_./${}-]+)")


def tracked_files() -> set:
    out = subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True).stdout
    return set(out.split())


def outputs() -> list:
    found = []
    for f in sorted(WORKFLOWS.glob("*.yml")) + sorted(WORKFLOWS.glob("*.yaml")):
        for path in OUTPUT.findall(f.read_text(encoding="utf-8")):
            found.append((f.name, path))
    return found


def main() -> int:
    if not WORKFLOWS.is_dir():
        print(f"no {WORKFLOWS}: run this from the repository root")
        return 1
    tracked, bad = tracked_files(), []
    found = outputs()
    for wf, path in found:
        if "${{" in path:
            continue  # an expression, not a path this script can resolve
        if path in tracked:
            bad.append(f"{wf}: --output {path} is a committed file; write to a path the run creates")
    for wf, path in found:
        print(f"{wf}: --output {path}")
    if bad:
        print("\n" + "\n".join(bad))
        return 1
    print(f"\n{len(found)} --output paths, none of them committed files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
