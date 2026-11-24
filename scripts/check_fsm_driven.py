"""Every call to a server/src/fsm.rs `*_step` function acts on its result.

    python3 scripts/check_fsm_driven.py [ROOT]

Scans `server/src/**/*.rs` (excluding `server/src/fsm.rs` and tests) for calls
to any `pub fn *_step` defined in `server/src/fsm.rs` whose return value is
discarded (`let _ = ...`, `_ = ...`, a bare statement `..._step(...);`, or
`..._step(...).ok();`).

Prints `<relative-path>:<line>: <source-line>` per finding and exits 1 if any.
Exits 0 when every call's result is matched, assigned, `?`-propagated, or
returned.
"""
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import List, Optional, Set, Tuple

DEFAULT_ROOT = Path(__file__).resolve().parent.parent

MULTI_CHAR_PUNCT = (
    "<<=",
    ">>=",
    "..=",
    "::",
    "=>",
    "->",
    "==",
    "!=",
    "<=",
    ">=",
    "&&",
    "||",
    "+=",
    "-=",
    "*=",
    "/=",
    "%=",
    "&=",
    "|=",
    "^=",
    "<<",
    ">>",
    "..",
)

GROUPING_PREFIX_KEYWORDS = {
    "as",
    "break",
    "else",
    "if",
    "in",
    "match",
    "return",
    "while",
    "yield",
}

BLOCK_CONTINUATION_TOKENS = {
    ".",
    "?",
    "else",
    "while",
    ";",
    ",",
    ")",
    "]",
    "}",
    "=>",
    "as",
    "=",
    "+",
    "-",
    "*",
    "/",
    "%",
    "&",
    "|",
    "^",
    "&&",
    "||",
    "==",
    "!=",
    "<",
    ">",
    "<=",
    ">=",
    "<<",
    ">>",
    "..",
    "..=",
}


@dataclass(frozen=True)
class Token:
    kind: str
    text: str
    line: int


def _is_ident_start(ch: str) -> bool:
    return ch.isalpha() or ch == "_"


def _is_ident_cont(ch: str) -> bool:
    return ch.isalnum() or ch == "_"


def _raw_string_prefix(src: str, pos: int) -> Optional[Tuple[int, int]]:
    """Return (prefix_len, hash_count) if a raw string starts at `pos`, else None."""
    n = len(src)
    j = pos
    if j < n and src[j] in ("b", "c"):
        j += 1
    if j >= n or src[j] != "r":
        return None
    j += 1
    hashes = 0
    while j < n and src[j] == "#":
        hashes += 1
        j += 1
    if j < n and src[j] == '"':
        return (j + 1 - pos, hashes)
    return None


def tokenize(src: str) -> List[Token]:
    """Tokenize Rust source while skipping comments and neutralizing literals."""
    tokens: List[Token] = []
    n = len(src)
    i = 0
    line = 1

    while i < n:
        ch = src[i]

        if ch in " \t\r\n":
            if ch == "\n":
                line += 1
            i += 1
            continue

        # Line comment
        if ch == "/" and i + 1 < n and src[i + 1] == "/":
            while i < n and src[i] != "\n":
                i += 1
            continue

        # Nested block comment
        if ch == "/" and i + 1 < n and src[i + 1] == "*":
            depth = 1
            i += 2
            while i < n and depth > 0:
                if src[i] == "/" and i + 1 < n and src[i + 1] == "*":
                    depth += 1
                    i += 2
                elif src[i] == "*" and i + 1 < n and src[i + 1] == "/":
                    depth -= 1
                    i += 2
                else:
                    if src[i] == "\n":
                        line += 1
                    i += 1
            continue

        # Raw string literal: r"...", r#"..."#, br#"..."#, cr#"..."#
        raw = _raw_string_prefix(src, i)
        if raw is not None:
            prefix_len, hashes = raw
            start_line = line
            i += prefix_len
            closing = '"' + ("#" * hashes)
            while i < n:
                if src.startswith(closing, i):
                    i += len(closing)
                    break
                if src[i] == "\n":
                    line += 1
                i += 1
            tokens.append(Token("STRING", '""', start_line))
            continue

        # Normal/byte/c string literal
        if ch == '"' or (ch in ("b", "c") and i + 1 < n and src[i + 1] == '"'):
            start_line = line
            i += 1 if ch == '"' else 2
            while i < n:
                if src[i] == "\\":
                    if i + 1 < n and src[i + 1] == "\n":
                        line += 1
                    i += 2
                elif src[i] == '"':
                    i += 1
                    break
                else:
                    if src[i] == "\n":
                        line += 1
                    i += 1
            tokens.append(Token("STRING", '""', start_line))
            continue

        # Byte character literal: b'x' or b'\x..'
        if ch == "b" and i + 1 < n and src[i + 1] == "'":
            start_line = line
            i += 2
            if i < n and src[i] == "\\":
                i += 2
                while i < n and src[i] not in ("'", "\n"):
                    i += 1
            elif i < n and src[i] != "\n":
                i += 1
            if i < n and src[i] == "'":
                i += 1
            tokens.append(Token("CHAR", "''", start_line))
            continue

        # Character literal vs lifetime
        if ch == "'":
            start_line = line
            if i + 1 < n and src[i + 1] == "\\":
                i += 2
                while i < n and src[i] not in ("'", "\n"):
                    i += 1
                if i < n and src[i] == "'":
                    i += 1
                tokens.append(Token("CHAR", "''", start_line))
                continue
            if i + 2 < n and src[i + 1] not in ("'", "\n", "\r") and src[i + 2] == "'":
                i += 3
                tokens.append(Token("CHAR", "''", start_line))
                continue
            if i + 1 < n and _is_ident_start(src[i + 1]):
                j = i + 2
                while j < n and _is_ident_cont(src[j]):
                    j += 1
                tokens.append(Token("LIFETIME", src[i:j], start_line))
                i = j
                continue
            tokens.append(Token("PUNCT", "'", start_line))
            i += 1
            continue

        # Raw identifier r#ident
        if ch == "r" and i + 2 < n and src[i + 1] == "#" and _is_ident_start(src[i + 2]):
            j = i + 3
            while j < n and _is_ident_cont(src[j]):
                j += 1
            tokens.append(Token("IDENT", src[i:j], line))
            i = j
            continue

        # Identifier / keyword
        if _is_ident_start(ch):
            j = i + 1
            while j < n and _is_ident_cont(src[j]):
                j += 1
            tokens.append(Token("IDENT", src[i:j], line))
            i = j
            continue

        # Number literal
        if ch.isdigit():
            j = i + 1
            while j < n and _is_ident_cont(src[j]):
                j += 1
            tokens.append(Token("NUMBER", src[i:j], line))
            i = j
            continue

        # Multi-character punctuator
        matched = None
        for op in MULTI_CHAR_PUNCT:
            if src.startswith(op, i):
                matched = op
                break
        if matched is not None:
            tokens.append(Token("PUNCT", matched, line))
            i += len(matched)
            continue

        # Single-character punctuator
        tokens.append(Token("PUNCT", ch, line))
        i += 1

    return tokens


def _matching_close(tokens: List[Token], open_idx: int) -> int:
    """Given index of `(`, `[`, or `{`, return index of matching closing bracket."""
    open_ch = tokens[open_idx].text
    close_ch = {"(": ")", "[": "]", "{": "}"}[open_ch]
    depth = 0
    for idx in range(open_idx, len(tokens)):
        t = tokens[idx].text
        if t == open_ch:
            depth += 1
        elif t == close_ch:
            depth -= 1
            if depth == 0:
                return idx
    return len(tokens) - 1


def _is_test_attr(inner: List[Token]) -> bool:
    """True for `#[cfg(test)]`, `#[cfg(all(..., test))]`, `#[test]`, `#[...::test]`."""
    texts = [t.text for t in inner]
    head = texts[: texts.index("(")] if "(" in texts else texts
    if head == ["test"]:
        return True
    if len(head) >= 3 and head[-2:] == ["::", "test"]:
        return True
    if len(texts) >= 4 and texts[0] == "cfg" and texts[1] == "(" and texts[-1] == ")":
        cfg_body = texts[2:-1]
        if cfg_body == ["test"]:
            return True
        # Handle #[cfg(all(test, ...))] while keeping #[cfg(not(test))]
        for k, tok in enumerate(cfg_body):
            if tok == "test":
                if not (k >= 2 and cfg_body[k - 2] == "not" and cfg_body[k - 1] == "("):
                    return True
    return False


def _skip_item(tokens: List[Token], idx: int) -> int:
    """Return index immediately after the Rust item starting at `idx`."""
    n = len(tokens)
    paren_depth = 0
    bracket_depth = 0
    brace_depth = 0
    saw_brace = False
    while idx < n:
        t = tokens[idx].text
        if t == "(":
            paren_depth += 1
        elif t == ")":
            paren_depth = max(0, paren_depth - 1)
        elif t == "[":
            bracket_depth += 1
        elif t == "]":
            bracket_depth = max(0, bracket_depth - 1)
        elif t == "{":
            brace_depth += 1
            saw_brace = True
        elif t == "}":
            brace_depth = max(0, brace_depth - 1)
            if saw_brace and brace_depth == 0:
                return idx + 1
        elif t == ";" and paren_depth == 0 and bracket_depth == 0 and brace_depth == 0:
            return idx + 1
        idx += 1
    return n


def strip_test_items(tokens: List[Token]) -> List[Token]:
    """Remove `#[cfg(test)]` and `#[test]` items while preserving token line numbers."""
    out: List[Token] = []
    n = len(tokens)
    i = 0
    while i < n:
        if tokens[i].text == "#" and i + 1 < n and tokens[i + 1].text == "[":
            attr_start = i
            is_test = False
            j = i
            while j + 1 < n and tokens[j].text == "#" and tokens[j + 1].text == "[":
                close_b = _matching_close(tokens, j + 1)
                if _is_test_attr(tokens[j + 2 : close_b]):
                    is_test = True
                j = close_b + 1
            if is_test:
                i = _skip_item(tokens, j)
            else:
                out.extend(tokens[attr_start:j])
                i = j
            continue
        out.append(tokens[i])
        i += 1
    return out


def extract_step_fns(fsm_src: str) -> Set[str]:
    """Extract all `pub fn *_step` names from `server/src/fsm.rs`."""
    tokens = strip_test_items(tokenize(fsm_src))
    names: Set[str] = set()
    n = len(tokens)
    i = 0
    while i < n:
        if tokens[i].kind == "IDENT" and tokens[i].text == "pub":
            j = i + 1
            if j < n and tokens[j].text == "(":
                j = _matching_close(tokens, j) + 1
            while j < n and tokens[j].text in ("const", "async", "unsafe", "extern"):
                j += 1
                if j < n and tokens[j - 1].text == "extern" and tokens[j].kind == "STRING":
                    j += 1
            if (
                j + 1 < n
                and tokens[j].kind == "IDENT"
                and tokens[j].text == "fn"
                and tokens[j + 1].kind == "IDENT"
                and tokens[j + 1].text.endswith("_step")
            ):
                names.add(tokens[j + 1].text)
        i += 1
    return names


def _skip_turbofish(tokens: List[Token], idx: int) -> int:
    """If `::<...>` starts at `idx`, return index after `>`, else `idx`."""
    n = len(tokens)
    if idx + 1 >= n or tokens[idx].text != "::" or tokens[idx + 1].text != "<":
        return idx
    depth = 0
    j = idx + 1
    while j < n:
        t = tokens[j].text
        if t == "<":
            depth += 1
        elif t == ">":
            depth -= 1
            if depth == 0:
                return j + 1
        elif t == ">>":
            depth -= 2
            if depth <= 0:
                return j + 1
        elif t in (";", "{", "}"):
            return idx
        j += 1
    return idx


def _scan_postfix(tokens: List[Token], start_idx: int) -> Tuple[int, Optional[Tuple[str, str]]]:
    """Scan `.method(...)`, `?`, `[...]` starting at `start_idx`."""
    n = len(tokens)
    j = start_idx
    last: Optional[Tuple[str, str]] = None
    while j < n:
        t = tokens[j].text
        if t == "." and j + 1 < n and tokens[j + 1].kind == "IDENT":
            method = tokens[j + 1].text
            j += 2
            j = _skip_turbofish(tokens, j)
            if j < n and tokens[j].text == "(":
                j = _matching_close(tokens, j) + 1
            last = ("method", method)
        elif t == "?":
            j += 1
            last = ("try", "?")
        elif t == "[":
            j = _matching_close(tokens, j) + 1
            last = ("index", "[]")
        else:
            break
    return j, last


def _is_grouping_paren(tokens: List[Token], open_idx: int) -> bool:
    """True when `(` at `open_idx` is expression grouping, not a call/macro."""
    if open_idx == 0:
        return True
    prev = tokens[open_idx - 1]
    if prev.text in ("!", ")", "]"):
        return False
    if prev.kind == "IDENT" and prev.text not in GROUPING_PREFIX_KEYWORDS:
        return False
    return True


def _strip_leading_attrs(prefix: List[Token]) -> List[Token]:
    """Drop leading `#[ ... ]` attribute tokens from a statement prefix."""
    i = 0
    n = len(prefix)
    while i + 1 < n and prefix[i].text == "#" and prefix[i + 1].text == "[":
        depth = 0
        j = i + 1
        while j < n:
            if prefix[j].text == "[":
                depth += 1
            elif prefix[j].text == "]":
                depth -= 1
                if depth == 0:
                    i = j + 1
                    break
            j += 1
        else:
            break
    return prefix[i:]


def _is_wildcard_assign(prefix: List[Token]) -> bool:
    """True when statement prefix directly binds the expression to `_`."""
    clean = _strip_leading_attrs(prefix)
    if not clean:
        return False
    texts = [t.text for t in clean]
    if texts == ["_", "="]:
        return True
    if texts[0] != "let" or texts[-1] != "=":
        return False
    pos = 1
    if pos < len(texts) and texts[pos] == "mut":
        pos += 1
    if pos >= len(texts) or texts[pos] != "_":
        return False
    rest = texts[pos + 1 : -1]
    return not rest or rest[0] == ":"


def find_discarded_calls(src: str, step_fns: Set[str]) -> List[int]:
    """Return 1-based line numbers of discarded `*_step` calls in `src`."""
    if not step_fns:
        return []
    tokens = strip_test_items(tokenize(src))
    n = len(tokens)
    # Stack entries: [kind, open_idx, stmt_start_idx]
    stack: List[List[object]] = [["ROOT", -1, 0]]
    flagged_lines: List[int] = []
    i = 0

    while i < n:
        tok = tokens[i]
        t = tok.text

        if t in ("{", "(", "["):
            stack.append([t, i, i + 1])
            i += 1
            continue

        if t == "}":
            if len(stack) > 1:
                stack.pop()
            if stack[-1][0] in ("ROOT", "{") and i + 1 < n:
                if tokens[i + 1].text not in BLOCK_CONTINUATION_TOKENS:
                    stack[-1][2] = i + 1
            i += 1
            continue

        if t in (")", "]"):
            if len(stack) > 1:
                stack.pop()
            i += 1
            continue

        if t in (";", ",", "=>"):
            stack[-1][2] = i + 1
            i += 1
            continue

        if (
            tok.kind == "IDENT"
            and tok.text in step_fns
            and not (i > 0 and tokens[i - 1].text in (".", "fn"))
        ):
            call_paren = _skip_turbofish(tokens, i + 1)
            if call_paren < n and tokens[call_paren].text == "(":
                close_paren = _matching_close(tokens, call_paren)

                path_start = i
                while (
                    path_start >= 2
                    and tokens[path_start - 1].text == "::"
                    and tokens[path_start - 2].kind == "IDENT"
                ):
                    path_start -= 2
                if path_start >= 1 and tokens[path_start - 1].text == "::":
                    path_start -= 1

                expr_start = path_start
                expr_end, last_postfix = _scan_postfix(tokens, close_paren + 1)
                frame_idx = len(stack) - 1

                while frame_idx > 0 and stack[frame_idx][0] == "(":
                    open_p = int(stack[frame_idx][1])
                    close_p = _matching_close(tokens, open_p)
                    if (
                        expr_start == open_p + 1
                        and expr_end == close_p
                        and _is_grouping_paren(tokens, open_p)
                    ):
                        expr_start = open_p
                        new_end, new_last = _scan_postfix(tokens, close_p + 1)
                        if new_end > close_p + 1:
                            last_postfix = new_last
                            expr_end = new_end
                        else:
                            expr_end = close_p + 1
                        frame_idx -= 1
                    else:
                        break

                frame_kind = str(stack[frame_idx][0])
                if frame_kind in ("ROOT", "{"):
                    stmt_start = int(stack[frame_idx][2])
                    prefix = _strip_leading_attrs(tokens[stmt_start:expr_start])
                    after_tok = tokens[expr_end].text if expr_end < n else "EOF"

                    discarded = False
                    if _is_wildcard_assign(prefix):
                        discarded = True
                    elif not prefix and after_tok == ";":
                        if last_postfix != ("try", "?"):
                            discarded = True

                    if discarded:
                        flagged_lines.append(tok.line)

        i += 1

    return flagged_lines


def problems(root: Path = DEFAULT_ROOT) -> List[str]:
    """Return finding strings `<rel_path>:<line>: <source_line>` under `root`."""
    fsm_path = root / "server" / "src" / "fsm.rs"
    if not fsm_path.is_file():
        return [f"missing required file: {fsm_path.relative_to(root).as_posix()}"]

    step_fns = extract_step_fns(fsm_path.read_text(encoding="utf-8"))
    if not step_fns:
        return ["no `pub fn *_step` functions found in server/src/fsm.rs"]

    src_dir = root / "server" / "src"
    found: List[str] = []
    for path in sorted(src_dir.rglob("*.rs")):
        rel = path.relative_to(root).as_posix()
        if rel == "server/src/fsm.rs":
            continue
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()
        for lineno in find_discarded_calls(text, step_fns):
            snippet = lines[lineno - 1].strip() if 1 <= lineno <= len(lines) else ""
            found.append(f"{rel}:{lineno}: {snippet}")
    return found


def _default_root() -> Path:
    cwd = Path.cwd()
    if (cwd / "server" / "src" / "fsm.rs").is_file():
        return cwd
    return DEFAULT_ROOT


def main(argv=None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    root = Path(args[0]) if args else _default_root()
    found = problems(root)
    for item in found:
        print(item)
    if not found:
        print("fsm-driven: every call to server/src/fsm.rs *_step acts on its result")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
