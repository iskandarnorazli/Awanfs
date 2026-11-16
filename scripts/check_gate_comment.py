"""Check that an issue's latest evidence comment answers every gate line.

    python3 scripts/check_gate_comment.py <issue>
    python3 scripts/check_gate_comment.py --body-file body.md --comment-file comment.md [issue]

Exit codes:
    0  every gate line is answered
    1  one or more gate lines are not answered
    2  the issue has no gate section
"""
import argparse
import json
import re
import subprocess
import sys

HEADING_RE = re.compile(
    r"^#{1,6}\s+(?:Gate|Done when|Definition of done)\b.*$",
    re.MULTILINE | re.IGNORECASE,
)
NEXT_HEADING_RE = re.compile(r"^#{1,6}\s+", re.MULTILINE)
DETAILS_RE = re.compile(r"<details>.*?</details>", re.DOTALL | re.IGNORECASE)
BULLET_RE = re.compile(r"^[ \t]*[-*]\s+(.+)$")
TOP_BULLET_RE = re.compile(r"^[ \t]{0,1}[-*]\s+")
BACKTICK_RE = re.compile(r"`([^`]+)`")

RESULT_PATTERNS = (
    re.compile(r"\b\d{8,}\b"),
    re.compile(r"\bOK\b", re.IGNORECASE),
    re.compile(r"\bpassed\b", re.IGNORECASE),
    re.compile(r"\bexit\s+0\b", re.IGNORECASE),
    re.compile(r"\b\d+(?:\.\d+)?\b"),
    re.compile(r"\bnot\s+met\b\s*[:\-–—(]?\s*\w+", re.IGNORECASE),
)


def _gh_api(endpoint: str) -> str:
    proc = subprocess.run(
        ["gh", "api", endpoint],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return proc.stdout


def fetch_issue_body(issue: str) -> str:
    data = json.loads(_gh_api(f"repos/{{owner}}/{{repo}}/issues/{issue}"))
    return data.get("body") or ""


def fetch_evidence_comment(issue: str) -> str:
    proc = subprocess.run(
        ["gh", "api", "--paginate", f"repos/{{owner}}/{{repo}}/issues/{issue}/comments", "--jq", ".[] | @json"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    comments = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
    gate_comments = [c.get("body") or "" for c in comments if "Gate" in (c.get("body") or "")]
    return gate_comments[-1] if gate_comments else ""


def extract_gate_lines(body: str):
    """Return list of gate bullet lines, or None if there is no gate heading."""
    m = HEADING_RE.search(body)
    if not m:
        return None
    rest = body[m.end():]
    nxt = NEXT_HEADING_RE.search(rest)
    section = rest[:nxt.start()] if nxt else rest
    section = DETAILS_RE.sub("", section)
    lines = []
    for raw in section.splitlines():
        bm = BULLET_RE.match(raw)
        if bm:
            lines.append(bm.group(1).strip())
    return lines


def split_blocks(comment: str) -> list[str]:
    """Split comment into paragraphs and bullet blocks."""
    blocks = []
    current = []
    in_bullet = False

    def flush():
        nonlocal current, in_bullet
        if current:
            blocks.append("\n".join(current))
            current = []
        in_bullet = False

    for line in comment.splitlines():
        if not line.strip() or NEXT_HEADING_RE.match(line):
            flush()
            continue
        if TOP_BULLET_RE.match(line):
            flush()
            current.append(line)
            in_bullet = True
            continue
        if in_bullet and re.match(r"^[ \t]{2,}\S", line):
            current.append(line)
            continue
        if in_bullet:
            flush()
        current.append(line)
    flush()
    return blocks


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("`", "").replace("**", "")).strip()


def has_result(text: str) -> bool:
    cleaned = text.replace("`", "")
    return any(p.search(cleaned) for p in RESULT_PATTERNS)


def match_gate_line(gate_line: str, blocks: list[str]) -> tuple[bool, str]:
    cmd_match = BACKTICK_RE.search(gate_line)
    cmd = cmd_match.group(1) if cmd_match else None
    words = _clean_text(gate_line).split()
    first_five = " ".join(words[:5]).rstrip(".,;:")

    for block in blocks:
        remainder = None
        if cmd and cmd in block:
            remainder = block.replace(cmd, "", 1)
        elif first_five:
            norm_block = _clean_text(block)
            idx = norm_block.lower().find(first_five.lower())
            if idx != -1:
                remainder = norm_block[:idx] + norm_block[idx + len(first_five):]
        if remainder is not None and has_result(remainder):
            evidence = re.sub(r"\s+", " ", block).strip()
            return True, evidence
    return False, ""


def check_gate(body: str, comment: str) -> tuple[int, list[tuple[str, bool, str]]]:
    gate_lines = extract_gate_lines(body)
    if gate_lines is None:
        return 2, []
    blocks = split_blocks(comment)
    rows = []
    for line in gate_lines:
        ok, evidence = match_gate_line(line, blocks)
        rows.append((line, ok, evidence))
    exit_code = 0 if all(ok for _, ok, _ in rows) else 1
    return exit_code, rows


def _cell(text: str) -> str:
    return text.replace("|", "\\|")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Check that an issue's evidence comment answers every gate line.")
    parser.add_argument("issue", nargs="?", help="GitHub issue number")
    parser.add_argument("--body-file", help="Path to a file containing the issue body")
    parser.add_argument("--comment-file", help="Path to a file containing the evidence comment")
    args = parser.parse_args(argv)

    if (args.body_file is None or args.comment_file is None) and not args.issue:
        parser.error("issue number is required unless both --body-file and --comment-file are given")

    if args.body_file:
        with open(args.body_file, encoding="utf-8") as fh:
            body = fh.read()
    else:
        body = fetch_issue_body(args.issue)
    if args.comment_file:
        with open(args.comment_file, encoding="utf-8") as fh:
            comment = fh.read()
    else:
        comment = fetch_evidence_comment(args.issue)

    exit_code, rows = check_gate(body, comment)
    if exit_code == 2:
        print("No gate section found (expected heading: Gate, Done when, or Definition of done).")
        return 2

    print("| Gate line | Answered | Evidence |")
    print("|---|---|---|")
    for line, ok, evidence in rows:
        print(f"| {_cell(line)} | {'yes' if ok else 'no'} | {_cell(evidence)} |")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
