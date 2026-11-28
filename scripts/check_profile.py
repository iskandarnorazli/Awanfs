"""Conformance profile check: start_server.sh, s3-tests.yml, PROFILE.md, CONFORMANCE_MAP.md.

    python3 scripts/check_profile.py

Checks:
  (a) .github/workflows/s3-tests.yml uses start_server.sh and has no awanfs
      invocation outside start_server.sh.
  (b) Every CLI flag and AWANFS_* env var in tests/s3tests/start_server.sh has a row
      in tests/s3tests/PROFILE.md, and vice versa.
  (c) Skip reasons in tests/s3tests/CONFORMANCE_MAP.md are documented in PROFILE.md's
      Skips table with what would un-skip them.

Exit 1 with one line per problem; 0 when all checks pass.
"""
import re
import sys
from pathlib import Path
from typing import List, Set, Tuple

ROOT = Path(__file__).resolve().parent.parent


def _markdown_tables(text: str) -> dict:
    """Return {heading_lower: [[cell, ...], ...]} for markdown tables."""
    tables = {}
    heading = ""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            heading = stripped.lstrip("#").strip().lower()
            continue
        if not stripped.startswith("|"):
            continue
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if not cells or all(set(c) <= {"-", ":"} for c in cells):
            continue
        tables.setdefault(heading, []).append(cells)
    # Drop header row of each table
    return {k: v[1:] for k, v in tables.items() if len(v) > 1}


def _strip_comments_and_continuations(text: str) -> str:
    lines = []
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("#"):
            continue
        lines.append(line)
    return re.sub(r"\\\r?\n", " ", "\n".join(lines))


def script_settings(script_text: str) -> Tuple[Set[str], Set[str]]:
    """Extract (cli_flags, awanfs_env_vars) from the awanfs command in start_server.sh."""
    joined = _strip_comments_and_continuations(script_text)
    flags: Set[str] = set()
    envs: Set[str] = set()
    for line in joined.splitlines():
        if '"$BIN"' in line or re.search(r"(?:^|[;&|\s'\"/])awanfs(?:\.exe)?(?=\s|$)", line):
            flags.update(re.findall(r"(?<!\S)(--[a-z0-9][a-z0-9-]*)", line))
            envs.update(re.findall(r"\b(AWANFS_[A-Z0-9_]+)=", line))
    return flags, envs


def profile_settings(profile_text: str) -> Tuple[Set[str], Set[str], List[str]]:
    """Extract (cli_flags, awanfs_env_vars, problems) from PROFILE.md Settings table."""
    problems: List[str] = []
    tables = _markdown_tables(profile_text)
    rows = tables.get("settings", [])
    if not rows:
        return set(), set(), ["PROFILE.md: missing Settings table"]
    flags: Set[str] = set()
    envs: Set[str] = set()
    for cells in rows:
        if len(cells) < 5:
            problems.append(f"PROFILE.md: Settings row has fewer than 5 columns: {cells}")
            continue
        if any(not c for c in cells[:5]):
            problems.append(f"PROFILE.md: Settings row has empty cell: {cells[0]}")
        setting = cells[0]
        flags.update(re.findall(r"`(--[a-z0-9][a-z0-9-]*)`", setting))
        envs.update(re.findall(r"`(AWANFS_[A-Z0-9_]+)`", setting))
    return flags, envs, problems


def profile_skips(profile_text: str) -> Tuple[List[str], List[str]]:
    """Extract (skip_reason_cells, problems) from PROFILE.md Skips table."""
    problems: List[str] = []
    tables = _markdown_tables(profile_text)
    rows = tables.get("skips", [])
    if not rows:
        return [], ["PROFILE.md: missing Skips table"]
    reasons: List[str] = []
    for cells in rows:
        if len(cells) < 3 or not cells[0] or not cells[2]:
            problems.append(f"PROFILE.md: Skips row needs reason, area, and un-skip condition: {cells}")
            continue
        reasons.append(cells[0])
    return reasons, problems


def map_skip_reasons(map_text: str) -> Set[str]:
    """Extract distinct non-placeholder skip reasons from CONFORMANCE_MAP.md."""
    reasons: Set[str] = set()
    for m in re.finditer(r"^- `[^`]+` \(skip\):\s*(.+)$", map_text, re.MULTILINE):
        reason = m.group(1).strip()
        if reason and reason.lower() != "skipped":
            reasons.add(reason)
    return reasons


def check_workflow(workflow_text: str) -> List[str]:
    """Verify s3-tests.yml uses start_server.sh and has no inline awanfs invocation."""
    problems: List[str] = []
    used_script = False
    for lineno, line in enumerate(workflow_text.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if "start_server.sh" in stripped:
            used_script = True
        if re.search(r"(?:^|[;&|\s'\"/])awanfs(?:\.exe)?(?=\s|$)", stripped):
            if re.search(r"\bcargo\s+build\b.*--bin\s+awanfs\b", stripped):
                continue
            problems.append(
                f".github/workflows/s3-tests.yml:{lineno}: invokes awanfs outside start_server.sh"
            )
    if not used_script:
        problems.append(".github/workflows/s3-tests.yml: does not invoke start_server.sh")
    return problems


def problems(root: Path = ROOT) -> List[str]:
    found: List[str] = []
    wf_path = root / ".github/workflows/s3-tests.yml"
    sh_path = root / "tests/s3tests/start_server.sh"
    prof_path = root / "tests/s3tests/PROFILE.md"
    map_path = root / "tests/s3tests/CONFORMANCE_MAP.md"

    for p in (wf_path, sh_path, prof_path, map_path):
        if not p.is_file():
            found.append(f"missing required file: {p.relative_to(root)}")
    if found:
        return found

    found.extend(check_workflow(wf_path.read_text(encoding="utf-8")))

    sh_flags, sh_envs = script_settings(sh_path.read_text(encoding="utf-8"))
    prof_text = prof_path.read_text(encoding="utf-8")
    prof_flags, prof_envs, prof_probs = profile_settings(prof_text)
    found.extend(prof_probs)

    for f in sorted(sh_flags - prof_flags):
        found.append(f"start_server.sh flag {f} has no row in PROFILE.md")
    for f in sorted(prof_flags - sh_flags):
        found.append(f"PROFILE.md flag {f} is not passed in start_server.sh")
    for e in sorted(sh_envs - prof_envs):
        found.append(f"start_server.sh env {e} has no row in PROFILE.md")
    for e in sorted(prof_envs - sh_envs):
        found.append(f"PROFILE.md env {e} is not set in start_server.sh")

    skip_entries, skip_probs = profile_skips(prof_text)
    found.extend(skip_probs)
    documented = [s.lower() for s in skip_entries]
    for reason in sorted(map_skip_reasons(map_path.read_text(encoding="utf-8"))):
        r_low = reason.lower()
        if not any(r_low in doc or doc in r_low for doc in documented):
            found.append(f"CONFORMANCE_MAP.md skip reason {reason!r} is not listed in PROFILE.md")

    return found


def main(argv=None) -> int:
    root = Path(argv[0]) if argv else ROOT
    found = problems(root)
    for p in found:
        print(p)
    if not found:
        print("Conformance profile: start_server.sh, s3-tests.yml, PROFILE.md, and CONFORMANCE_MAP.md match")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
