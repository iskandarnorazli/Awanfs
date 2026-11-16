"""Self-tests for check_gate_comment.py.

    python3 scripts/test_check_gate_comment.py
"""
import io
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURES_DIR = os.path.join(SCRIPTS_DIR, "fixtures", "gate_comment")
sys.path.insert(0, SCRIPTS_DIR)
import check_gate_comment as c  # noqa: E402


class CheckGateCommentTest(unittest.TestCase):
    def _run_main(self, body, comment):
        with tempfile.TemporaryDirectory() as tmp:
            body_path = os.path.join(tmp, "body.md")
            comment_path = os.path.join(tmp, "comment.md")
            with open(body_path, "w", encoding="utf-8") as fh:
                fh.write(body)
            with open(comment_path, "w", encoding="utf-8") as fh:
                fh.write(comment)
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = c.main(["--body-file", body_path, "--comment-file", comment_path, "0"])
            return rc, buf.getvalue()

    def test_187_complete_comment_exits_0(self):
        body_path = os.path.join(FIXTURES_DIR, "issue_187_body.md")
        comment_path = os.path.join(FIXTURES_DIR, "issue_187_comment.md")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = c.main(["--body-file", body_path, "--comment-file", comment_path, "187"])
        self.assertEqual(rc, 0, buf.getvalue())

    def test_140_real_2026_11_16_comment_exits_1_and_lists_missing(self):
        body_path = os.path.join(FIXTURES_DIR, "issue_140_body.md")
        comment_path = os.path.join(FIXTURES_DIR, "issue_140_comment_2026_11_16.md")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = c.main(["--body-file", body_path, "--comment-file", comment_path, "140"])
        out = buf.getvalue()
        self.assertEqual(rc, 1, out)
        for label in ("Conformance", "Driven end to end", "Speed and memory"):
            self.assertIn(label, out)

    def test_command_only_with_no_result_is_not_answered(self):
        body = "## Gate\n- `cargo test` green on both platforms.\n"
        comment = "### Gate\n- `cargo test`\n"
        rc, out = self._run_main(body, comment)
        self.assertEqual(rc, 1, out)

    def test_command_containing_number_without_result_is_not_answered(self):
        body = "## Gate\n- `python3 scripts/check_gate_comment.py 219`: exit 0.\n"
        comment = "### Gate\n- `python3 scripts/check_gate_comment.py 219`\n"
        rc, out = self._run_main(body, comment)
        self.assertEqual(rc, 1, out)

    def test_no_gate_section_exits_2(self):
        body = "## Goal\nSomething without a gate.\n"
        comment = "### Gate\n- `cargo test`: OK\n"
        rc, out = self._run_main(body, comment)
        self.assertEqual(rc, 2, out)

    def test_done_when_and_definition_of_done_headings(self):
        for heading in ("Done when", "Definition of done"):
            body = f"## {heading}\n- `pytest` passes.\n"
            comment = "Gate:\n- `pytest`: passed\n"
            rc, out = self._run_main(body, comment)
            self.assertEqual(rc, 0, f"{heading}: {out}")

    def test_first_five_words_match_and_result_variants(self):
        body = (
            "## Gate\n"
            "- The three tests in the suite pass.\n"
            "- **Speed and memory:** run bench-ratio.\n"
            "- **Model:** run tlc.\n"
        )
        comment = (
            "Gate:\n"
            "- The three tests in the suite: OK\n"
            "- Speed and memory: run bench-ratio — 43904970500\n"
            "- Model: run tlc — not met (Java missing locally)\n"
        )
        rc, out = self._run_main(body, comment)
        self.assertEqual(rc, 0, out)

    def test_not_met_without_reason_is_not_answered(self):
        body = "## Gate\n- `bash verification/run_tlc.sh` green.\n"
        comment = "Gate:\n- `bash verification/run_tlc.sh`: not met\n"
        rc, out = self._run_main(body, comment)
        self.assertEqual(rc, 1, out)

    def test_result_in_different_paragraph_is_not_answered(self):
        body = "## Gate\n- `cargo test` green.\n"
        comment = "Gate:\n`cargo test`\n\nAll tests passed.\n"
        rc, out = self._run_main(body, comment)
        self.assertEqual(rc, 1, out)

    def test_cli_invocation_exit_codes(self):
        body_path = os.path.join(FIXTURES_DIR, "issue_187_body.md")
        comment_path = os.path.join(FIXTURES_DIR, "issue_187_comment.md")
        script = os.path.join(SCRIPTS_DIR, "check_gate_comment.py")
        proc = subprocess.run(
            [sys.executable, script, "187", "--body-file", body_path, "--comment-file", comment_path],
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)


if __name__ == "__main__":
    unittest.main()
