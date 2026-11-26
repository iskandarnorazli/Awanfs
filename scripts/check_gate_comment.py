"""Check that an issue's latest evidence comment answers every gate line, or check
an issue's Must-pass s3-tests against a PR's passing.txt and s3tests job log.

    python3 scripts/check_gate_comment.py <issue>
    python3 scripts/check_gate_comment.py --body-file body.md --comment-file comment.md [issue]
    python3 scripts/check_gate_comment.py <issue> --pr <pr>
    python3 scripts/check_gate_comment.py [issue] --body body.md --passing passing.txt --log s3tests.log [--comments comments.json]

Exit codes:
    0  every gate line is answered (or every Must-pass test is passing / moved by owner)
    1  one or more gate lines are not answered (or a Must-pass test is not passing)
    2  the issue has no gate section or gate bullets (or no Must-pass section in --pr / --passing / --log mode)
"""
import argparse
import base64
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
DETAILS_BLOCK_RE = re.compile(
    r"<details>\s*<summary>(.*?)</summary>(.*?)</details>",
    re.DOTALL | re.IGNORECASE,
)
BULLET_RE = re.compile(r"^[ \t]*[-*]\s+(.+)$")
GATE_BULLET_RE = re.compile(r"^ ?[-*]\s+(.+)$")
TOP_BULLET_RE = re.compile(r"^[ \t]{0,1}[-*]\s+")
FENCE_OPEN_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
FENCE_CLOSE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})\s*$")
BACKTICK_RE = re.compile(r"`([^`]+)`")
# A gate line that opens with a bold label ("**Docs in step:**") is answered only by a block
# that opens with the same label, compared up to its first comma, parenthesis or colon (#462).
LABEL_RE = re.compile(r"\*\*([^*]+?)\*\*")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
GH_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z\s+")
FAILED_LINE_RE = re.compile(
    r"^(?:FAILED|ERROR)\s+(?:[^\s:]+/)?([^/\s:]+?)(?:\.py)?::([^\s\[]+)(?:\[([^\]]+)\])?(?:\s+-\s+(.*))?$"
)
FAILED_BARE_RE = re.compile(
    r"^(?:FAILED|ERROR)\s+(test_[A-Za-z0-9_]+)(?:\[([^\]]+)\])?(?:\s+-\s+(.*))?$"
)
ISSUE_LINK_RE = re.compile(r"(?<![A-Za-z0-9_/])(?:#(\d+)|/issues/(\d+))\b")

RESULT_PATTERNS = (
    re.compile(r"\b\d{8,}\b"),
    re.compile(r"\bOK\b", re.IGNORECASE),
    re.compile(r"\bpassed\b", re.IGNORECASE),
    re.compile(r"\bexit\s+0\b", re.IGNORECASE),
    re.compile(r"\b\d+(?:\.\d+)?\b"),
    re.compile(r"\bnot\s+met\b\s*[:\-–—(]?\s*\w+", re.IGNORECASE),
)


def _gh_api(endpoint: str, allow_escape: bool = False) -> str:
    cmd = ["gh", "api", endpoint]
    if allow_escape:
        cmd.append("--allow-escape-sequences")
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    )
    return proc.stdout


def _gh_api_paginated(endpoint: str) -> list:
    proc = subprocess.run(
        ["gh", "api", "--paginate", endpoint, "--jq", ".[] | @json"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    )
    return [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]


def fetch_issue(issue: str) -> dict:
    return json.loads(_gh_api(f"repos/{{owner}}/{{repo}}/issues/{issue}"))


def fetch_issue_body(issue: str) -> str:
    return fetch_issue(issue).get("body") or ""


def fetch_issue_comments(issue: str) -> list:
    return _gh_api_paginated(f"repos/{{owner}}/{{repo}}/issues/{issue}/comments")


def fetch_evidence_comment(issue: str) -> str:
    """Return the latest comment starting with '## Gate evidence', or fall back to the latest containing 'Gate'."""
    bodies = [c.get("body") or "" for c in fetch_issue_comments(issue)]
    evidence = [
        b for b in bodies
        if (b.splitlines() or [""])[0].lstrip().lower().startswith("## gate evidence")
    ]
    if evidence:
        return evidence[-1]
    gate_comments = [b for b in bodies if "Gate" in b]
    return gate_comments[-1] if gate_comments else ""


def fetch_repo_owner() -> str:
    data = json.loads(_gh_api("repos/{owner}/{repo}"))
    return (data.get("owner") or {}).get("login") or ""


def fetch_pr_head_sha(pr: str) -> str:
    data = json.loads(_gh_api(f"repos/{{owner}}/{{repo}}/pulls/{pr}"))
    return data["head"]["sha"]


def fetch_pr_passing(sha: str) -> str:
    data = json.loads(_gh_api(f"repos/{{owner}}/{{repo}}/contents/tests/s3tests/passing.txt?ref={sha}"))
    return base64.b64decode(data["content"]).decode("utf-8")


def fetch_pr_s3tests_log(sha: str) -> str:
    data = json.loads(
        _gh_api(f"repos/{{owner}}/{{repo}}/commits/{sha}/check-runs?check_name=s3tests&per_page=100")
    )
    runs = [r for r in data.get("check_runs", []) if r.get("name") == "s3tests"]
    if not runs:
        raise RuntimeError(f"No s3tests check run found for PR head {sha}.")
    latest = sorted(runs, key=lambda r: (r.get("started_at") or "", r.get("id") or 0))[-1]
    return _gh_api(f"repos/{{owner}}/{{repo}}/actions/jobs/{latest['id']}/logs", allow_escape=True)


def _mask_fenced_code_blocks(text: str) -> str:
    """Replace fenced code block lines with empty lines so headings and bullets inside fences are ignored."""
    out = []
    fence_char = ""
    fence_len = 0
    for line in text.splitlines():
        if fence_char:
            out.append("")
            cm = FENCE_CLOSE_RE.match(line)
            if cm and cm.group(1)[0] == fence_char and len(cm.group(1)) >= fence_len:
                fence_char = ""
                fence_len = 0
            continue
        om = FENCE_OPEN_RE.match(line)
        if om:
            marker, info = om.groups()
            if not (marker[0] == "`" and "`" in info):
                fence_char = marker[0]
                fence_len = len(marker)
                out.append("")
                continue
        out.append(line)
    return "\n".join(out)


def extract_gate_lines(body: str):
    """Return list of top-level gate bullet lines, or None if there is no gate heading."""
    masked = _mask_fenced_code_blocks(body)
    m = HEADING_RE.search(masked)
    if not m:
        return None
    rest = masked[m.end():]
    nxt = NEXT_HEADING_RE.search(rest)
    section = rest[:nxt.start()] if nxt else rest
    section = DETAILS_RE.sub("", section)
    lines = []
    for raw in section.splitlines():
        bm = GATE_BULLET_RE.match(raw)
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
    label_match = LABEL_RE.match(gate_line)
    label = re.split(r"[,(:]", _clean_text(label_match.group(1)))[0].strip().lower() if label_match else None

    for block in blocks:
        if label is not None and not _clean_text(TOP_BULLET_RE.sub("", block, count=1)).lower().startswith(label):
            continue
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
    if not gate_lines:
        return 2, []
    blocks = split_blocks(comment)
    rows = []
    for line in gate_lines:
        ok, evidence = match_gate_line(line, blocks)
        rows.append((line, ok, evidence))
    exit_code = 0 if all(ok for _, ok, _ in rows) else 1
    return exit_code, rows


def extract_must_pass_tests(body: str):
    """Return ordered list of Must-pass test names, or None if no Must-pass section exists."""
    blocks = DETAILS_BLOCK_RE.findall(body)
    found_section = False
    tests = []
    seen = set()

    for summary, inner in blocks:
        if "must pass" not in summary.lower():
            continue
        found_section = True
        mixed = "owned elsewhere" in summary.lower()
        table_lines = [ln.strip() for ln in inner.splitlines() if ln.strip().startswith("|")]

        if table_lines:
            for raw in table_lines:
                parts = raw.strip("|").split("|", 1)
                if len(parts) < 2:
                    continue
                first = parts[0].strip().strip("`").strip()
                second = parts[1].strip()
                if first.lower() == "test" or set(first) <= set("-: "):
                    if "owned elsewhere" in second.lower():
                        mixed = True
                    continue
                if mixed and not re.match(r"^(?:\*\*)?must pass(?:\*\*)?\s*:", second, re.I):
                    continue
                if first and first not in seen:
                    seen.add(first)
                    tests.append(first)
        else:
            for raw in inner.splitlines():
                bm = BULLET_RE.match(raw)
                if not bm:
                    continue
                item = bm.group(1).strip()
                m = BACKTICK_RE.search(item) or re.search(r"\b((?:[\w-]+::)?test_[A-Za-z0-9_]+(?:\[[^\]]+\])?)", item)
                if m:
                    name = m.group(1).strip()
                    if name not in seen:
                        seen.add(name)
                        tests.append(name)

    return tests if found_section else None


def _split_test_name(raw: str) -> tuple[str, str, str]:
    cleaned = raw.strip().strip("`").strip()
    if "::" in cleaned:
        mod_part, func_part = cleaned.split("::", 1)
        mod = mod_part.rsplit("/", 1)[-1]
        if mod.endswith(".py"):
            mod = mod[:-3]
    else:
        mod, func_part = "", cleaned
    base = func_part.split("[", 1)[0]
    return mod, func_part, base


def parse_passing_txt(text: str) -> tuple[set[str], set[str], set[str], set[str]]:
    """Return (exact_qualified, base_qualified, exact_bare, base_bare) sets from passing.txt."""
    exact_q, base_q, exact_b, base_b = set(), set(), set(), set()
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        mod, func, base = _split_test_name(line)
        if mod:
            exact_q.add(f"{mod}::{func}")
            base_q.add(f"{mod}::{base}")
        else:
            # Bare entries in passing.txt correspond to test_s3 tests.
            exact_q.add(f"test_s3::{func}")
            base_q.add(f"test_s3::{base}")
        exact_b.add(func)
        base_b.add(base)
    return exact_q, base_q, exact_b, base_b


def parse_failed_log(log_text: str) -> tuple[dict[str, str], dict[str, str], dict[str, str], dict[str, str]]:
    """Return (exact_qualified, base_qualified, exact_bare, base_bare) failure reasons."""
    exact_q, base_q, exact_b, base_b = {}, {}, {}, {}
    for raw in log_text.splitlines():
        line = ANSI_RE.sub("", raw).strip()
        line = GH_TIMESTAMP_RE.sub("", line).strip()
        if not (line.startswith("FAILED ") or line.startswith("ERROR ")):
            continue
        m = FAILED_LINE_RE.match(line)
        if m:
            mod, func_name, param, reason = m.groups()
        else:
            bm = FAILED_BARE_RE.match(line)
            if not bm:
                continue
            mod = ""
            func_name, param, reason = bm.groups()
        full_func = f"{func_name}[{param}]" if param is not None else func_name
        why = (reason or "").strip() or "failed in s3tests log"
        if mod:
            exact_q.setdefault(f"{mod}::{full_func}", why)
            base_q.setdefault(f"{mod}::{func_name}", why)
        exact_b.setdefault(full_func, why)
        base_b.setdefault(func_name, why)
    return exact_q, base_q, exact_b, base_b


def _is_owner_comment(comment: dict, repo_owner: str | None) -> bool:
    assoc = (comment.get("author_association") or comment.get("authorAssociation") or "").upper()
    user_obj = comment.get("user") or comment.get("author") or {}
    login = (user_obj.get("login") or "").lower()
    if assoc and assoc != "OWNER":
        return False
    if repo_owner and login and login != repo_owner.lower():
        return False
    if not assoc and not repo_owner:
        return login == "iskandarnorazli"
    return bool(assoc == "OWNER" or (repo_owner and login == repo_owner.lower()))


def _is_non_decision_comment(body: str) -> bool:
    non_empty = [ln.strip().lower() for ln in body.splitlines() if ln.strip()]
    if not non_empty:
        return True
    head = "\n".join(non_empty[:4])
    if re.search(r"^#{1,6}\s*(?:gate\b|audit\b)", head, re.MULTILINE):
        return True
    first = non_empty[0]
    prefixes = (
        "audit (",
        "stopping with",
        "implemented in batch",
        "completed audit follow-ups",
    )
    return first.startswith(prefixes)


def _find_moved_issue(
    comments: list[dict],
    test_name: str,
    current_issue: str | None,
    repo_owner: str | None,
    issue_created_at: str | None,
) -> tuple[str, str] | None:
    _, _, short = _split_test_name(test_name)
    token_re = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(short)}(?![A-Za-z0-9_])")
    ordered = sorted(
        enumerate(comments),
        key=lambda item: (item[1].get("created_at") or item[1].get("createdAt") or "", item[0]),
    )
    for _, comment in ordered:
        created = comment.get("created_at") or comment.get("createdAt") or ""
        if issue_created_at and created and created < issue_created_at:
            continue
        if not _is_owner_comment(comment, repo_owner):
            continue
        body = comment.get("body") or ""
        if _is_non_decision_comment(body):
            continue
        if not token_re.search(body):
            continue
        for m in ISSUE_LINK_RE.finditer(body):
            num = m.group(1) or m.group(2)
            if current_issue and num == str(current_issue):
                continue
            prefix = body[max(0, m.start() - 16):m.start()]
            if m.group(1) and re.search(r"\b(?:pr|pull(?:\s+request)?|run)\s*$", prefix, re.I):
                continue
            url = comment.get("html_url") or comment.get("url") or ""
            return num, url
    return None


def check_must_pass(
    body: str,
    passing_text: str,
    log_text: str,
    comments: list[dict] | None = None,
    current_issue: str | None = None,
    repo_owner: str | None = None,
    issue_created_at: str | None = None,
) -> tuple[int, list[tuple[str, str, str]]]:
    """Check Must-pass tests against passing.txt, s3tests log, and owner comments.

    Returns (exit_code, rows) where each row is (test_name, status, detail).
    """
    tests = extract_must_pass_tests(body)
    if tests is None:
        return 2, []

    exact_p, base_p, exact_pb, base_pb = parse_passing_txt(passing_text)
    exact_f, base_f, exact_fb, base_fb = parse_failed_log(log_text)
    comment_list = comments or []

    rows = []
    all_ok = True
    for test in tests:
        mod, func, base = _split_test_name(test)
        is_param = func != base
        qualified = f"{mod}::{func}" if mod else func
        qualified_base = f"{mod}::{base}" if mod else base

        if mod:
            fail_reason = exact_f.get(qualified) if is_param else base_f.get(qualified_base)
            is_listed = (qualified in exact_p) if is_param else (qualified_base in base_p)
        else:
            fail_reason = exact_fb.get(func) if is_param else base_fb.get(base)
            is_listed = (func in exact_pb) if is_param else (base in base_pb)

        if fail_reason is None and is_listed:
            rows.append((test, "passing", "listed and passing"))
            continue

        moved = _find_moved_issue(comment_list, test, current_issue, repo_owner, issue_created_at)
        if moved is not None:
            new_issue, url = moved
            detail = f"moved to #{new_issue} ({url})" if url else f"moved to #{new_issue} by owner comment"
            rows.append((test, "excused", detail))
            continue

        all_ok = False
        if fail_reason is not None:
            rows.append((test, "failing", fail_reason))
        else:
            rows.append((test, "not listed", "not in passing.txt"))

    return (0 if all_ok else 1), rows


def _read_text(path: str, errors: str = "strict") -> str:
    with open(path, encoding="utf-8", errors=errors) as fh:
        return fh.read()


def _load_comments_file(path: str) -> list[dict]:
    data = json.loads(_read_text(path))
    if isinstance(data, dict) and isinstance(data.get("comments"), list):
        return data["comments"]
    if isinstance(data, list):
        return data
    raise ValueError(f"Expected a JSON list of comments or {{'comments': [...]}} in {path}")


def _print_table(headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> None:
    print("| " + " | ".join(headers) + " |")
    print("|" + "|".join("---" for _ in headers) + "|")
    for row in rows:
        print("| " + " | ".join(cell.replace("|", "\\|") for cell in row) + " |")


def _run_must_pass_cli(args, parser) -> int:
    if args.body_file is None and not args.issue:
        parser.error("issue number or --body/--body-file is required")
    if args.passing is None and args.pr is None:
        parser.error("--pr or --passing is required")
    if args.log is None and args.pr is None:
        parser.error("--pr or --log is required")

    issue_created_at = None
    if args.body_file:
        body = _read_text(args.body_file)
    else:
        issue_data = fetch_issue(args.issue)
        body = issue_data.get("body") or ""
        issue_created_at = issue_data.get("created_at")

    sha = fetch_pr_head_sha(args.pr) if (args.pr is not None and (args.passing is None or args.log is None)) else None
    passing_text = _read_text(args.passing) if args.passing is not None else fetch_pr_passing(sha)
    log_text = _read_text(args.log, errors="replace") if args.log is not None else fetch_pr_s3tests_log(sha)

    repo_owner = None
    if args.comments is not None:
        comments = _load_comments_file(args.comments)
    elif args.pr is not None and args.issue:
        comments = fetch_issue_comments(args.issue)
        repo_owner = fetch_repo_owner()
    else:
        comments = []

    exit_code, rows = check_must_pass(
        body,
        passing_text,
        log_text,
        comments=comments,
        current_issue=args.issue,
        repo_owner=repo_owner,
        issue_created_at=issue_created_at,
    )
    if exit_code == 2:
        print("No Must pass section found (expected <details><summary>...Must pass...</summary>).")
        return 2

    _print_table(
        ("Test", "Status", "Detail"),
        [(f"`{test}`", status, detail) for test, status, detail in rows],
    )
    return exit_code


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Check that an issue's evidence comment answers every gate line.")
    parser.add_argument("issue", nargs="?", help="GitHub issue number")
    parser.add_argument("--body-file", "--body", dest="body_file", help="Path to a file containing the issue body")
    parser.add_argument("--comment-file", help="Path to a file containing the evidence comment")
    parser.add_argument("--pr", help="GitHub pull request number to check Must-pass tests against")
    parser.add_argument("--passing", help="Path to a saved tests/s3tests/passing.txt file")
    parser.add_argument("--log", help="Path to a saved s3tests job log file")
    parser.add_argument("--comments", help="Path to a saved issue comments JSON file")
    args = parser.parse_args(argv)

    if any(v is not None for v in (args.pr, args.passing, args.log)):
        return _run_must_pass_cli(args, parser)

    if (args.body_file is None or args.comment_file is None) and not args.issue:
        parser.error("issue number is required unless both --body-file and --comment-file are given")

    body = _read_text(args.body_file) if args.body_file else fetch_issue_body(args.issue)
    comment = _read_text(args.comment_file) if args.comment_file else fetch_evidence_comment(args.issue)

    exit_code, rows = check_gate(body, comment)
    if exit_code == 2:
        if extract_gate_lines(body) is None:
            print("No gate section found (expected heading: Gate, Done when, or Definition of done).")
        else:
            print("Gate section has no gate bullets (expected at least one top-level bullet).")
        return 2

    _print_table(
        ("Gate line", "Answered", "Evidence"),
        [(line, "yes" if ok else "no", evidence) for line, ok, evidence in rows],
    )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
