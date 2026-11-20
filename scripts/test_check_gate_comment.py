"""Self-tests for check_gate_comment.py.

    python3 scripts/test_check_gate_comment.py
"""
import base64
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

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

    def test_133_gate_extracts_all_5_lines_and_matches_evidence(self):
        body_path = os.path.join(FIXTURES_DIR, "issue_133_body.md")
        comment_path = os.path.join(FIXTURES_DIR, "issue_133_comment.md")
        with open(body_path, encoding="utf-8") as fh:
            body_133 = fh.read()
        lines = c.extract_gate_lines(body_133)
        self.assertEqual(len(lines), 5, lines)
        self.assertIn("gh pr checks <pr>", lines[0])
        self.assertIn("peak RSS per workload", lines[1])
        self.assertIn("measurements/<date>-large-object-rss.md", lines[2])
        self.assertIn("self-hosted report fails", lines[3])
        self.assertIn("bench-ratio.yml", lines[4])

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = c.main(["--body-file", body_path, "--comment-file", comment_path, "133"])
        out = buf.getvalue()
        self.assertEqual(rc, 0, out)
        self.assertEqual(out.count("| yes |"), 5, out)

    def test_code_blocks_paragraphs_and_sub_bullets_in_gate(self):
        body = (
            "## Problem\n"
            "```markdown\n"
            "## Gate\n"
            "- fake gate before real gate\n"
            "```\n\n"
            "## Gate\n"
            "- First top-level check with details:\n"
            "  - indented sub-bullet one\n"
            "\t- tab-indented sub-bullet two\n\n"
            "Paragraph between bullets.\n\n"
            "```bash\n"
            "# bash comment must not end gate section\n"
            "- not a gate line\n"
            "```\n\n"
            "~~~~\n"
            "## tilde heading inside fence\n"
            "* also not a gate line\n"
            "~~~~\n\n"
            "````markdown\n"
            "```\n"
            "### nested fence heading\n"
            "```\n"
            "````\n\n"
            "* Second top-level check.\n"
            " - Third top-level check.\n"
            "\n## Files\n"
            "- `scripts/check_gate_comment.py`\n"
        )
        lines = c.extract_gate_lines(body)
        self.assertEqual(
            lines,
            [
                "First top-level check with details:",
                "Second top-level check.",
                "Third top-level check.",
            ],
        )

    def test_gate_heading_with_no_bullets_exits_2_with_message(self):
        comment = "### Gate\n- `cargo test`: OK\n"
        bodies = [
            "## Gate\n",
            "## Gate\n\n## Files\n- `a.py`\n",
            "## Gate\nJust prose between headings.\n\n## Files\n- `a.py`\n",
            "## Gate\n```sh\n- bullet inside code block\n```\n",
            "## Gate\n<details><summary>Must pass</summary>\n\n- `test_s3::test_a`\n</details>\n",
            "## Done when\n  - indented bullet only\n",
        ]
        for body in bodies:
            rc, out = self._run_main(body, comment)
            self.assertEqual(rc, 2, f"Expected exit 2 for {body!r}, got {rc}: {out}")
            self.assertTrue(out.strip(), f"Expected non-empty message for {body!r}")

    def _run_must_pass(self, body, passing, log, comments=None, issue=None):
        with tempfile.TemporaryDirectory() as tmp:
            body_path = os.path.join(tmp, "body.md")
            passing_path = os.path.join(tmp, "passing.txt")
            log_path = os.path.join(tmp, "s3tests.log")
            with open(body_path, "w", encoding="utf-8") as fh:
                fh.write(body)
            with open(passing_path, "w", encoding="utf-8") as fh:
                fh.write(passing)
            with open(log_path, "w", encoding="utf-8") as fh:
                fh.write(log)
            argv = ["--body", body_path, "--passing", passing_path, "--log", log_path]
            if comments is not None:
                comments_path = os.path.join(tmp, "comments.json")
                with open(comments_path, "w", encoding="utf-8") as fh:
                    json.dump(comments, fh)
                argv.extend(["--comments", comments_path])
            if issue is not None:
                argv.insert(0, str(issue))
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = c.main(argv)
            return rc, buf.getvalue()

    def test_must_pass_listed_and_passing_exits_0(self):
        body = (
            "## Gate\n"
            "<details><summary>Must pass: 1 tests</summary>\n\n"
            "| test | first line of the failure |\n|---|---|\n"
            "| `test_s3::test_alpha_passes` | assert 404 == 200 |\n\n</details>\n"
        )
        with open(os.path.join(FIXTURES_DIR, "must_pass_passing.txt"), encoding="utf-8") as fh:
            passing = fh.read()
        with open(os.path.join(FIXTURES_DIR, "must_pass_log.txt"), encoding="utf-8") as fh:
            log = fh.read()
        rc, out = self._run_must_pass(body, passing, log)
        self.assertEqual(rc, 0, out)
        self.assertIn("test_s3::test_alpha_passes", out)
        self.assertIn("passing", out.lower())

    def test_must_pass_not_listed_exits_1(self):
        body = (
            "## Gate\n"
            "<details><summary>Must pass: 1 tests</summary>\n\n"
            "| test | first line of the failure |\n|---|---|\n"
            "| `test_s3::test_beta_not_listed` | assert 404 == 200 |\n\n</details>\n"
        )
        with open(os.path.join(FIXTURES_DIR, "must_pass_passing.txt"), encoding="utf-8") as fh:
            passing = fh.read()
        with open(os.path.join(FIXTURES_DIR, "must_pass_log.txt"), encoding="utf-8") as fh:
            log = fh.read()
        rc, out = self._run_must_pass(body, passing, log)
        self.assertEqual(rc, 1, out)
        self.assertIn("test_s3::test_beta_not_listed", out)
        self.assertIn("not listed", out.lower())

    def test_must_pass_failing_in_log_exits_1_and_prints_reason(self):
        body = (
            "## Gate\n"
            "<details><summary>Must pass: 1 tests</summary>\n\n"
            "| test | first line of the failure |\n|---|---|\n"
            "| `test_s3::test_gamma_fails` | assert 500 == 200 |\n\n</details>\n"
        )
        # Failing in log must fail even if also listed in passing.txt
        passing = "test_s3::test_gamma_fails\n"
        with open(os.path.join(FIXTURES_DIR, "must_pass_log.txt"), encoding="utf-8") as fh:
            log = fh.read()
        rc, out = self._run_must_pass(body, passing, log)
        self.assertEqual(rc, 1, out)
        self.assertIn("test_s3::test_gamma_fails", out)
        self.assertIn("failing", out.lower())
        self.assertIn("AssertionError: assert 500 == 200", out)

    def test_must_pass_all_outcomes_from_saved_fixtures(self):
        body_path = os.path.join(FIXTURES_DIR, "must_pass_body.md")
        passing_path = os.path.join(FIXTURES_DIR, "must_pass_passing.txt")
        log_path = os.path.join(FIXTURES_DIR, "must_pass_log.txt")
        comments_path = os.path.join(FIXTURES_DIR, "must_pass_comments.json")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = c.main([
                "999",
                "--body", body_path,
                "--passing", passing_path,
                "--log", log_path,
                "--comments", comments_path,
            ])
        out = buf.getvalue()
        self.assertEqual(rc, 1, out)
        self.assertIn("test_alpha_passes", out)
        self.assertIn("test_beta_not_listed", out)
        self.assertIn("not listed", out.lower())
        self.assertIn("test_gamma_fails", out)
        self.assertIn("AssertionError: assert 500 == 200", out)
        self.assertIn("test_delta_moved", out)
        self.assertIn("#122", out)

    def test_must_pass_moved_by_owner_comment_exits_0(self):
        body = (
            "## Gate\n"
            "<details><summary>Must pass: 2 tests</summary>\n\n"
            "| test | first line of the failure |\n|---|---|\n"
            "| `test_s3::test_alpha_passes` | assert 400 == 200 |\n"
            "| `test_s3::test_delta_moved` | assert 403 == 200 |\n\n</details>\n"
        )
        with open(os.path.join(FIXTURES_DIR, "must_pass_passing.txt"), encoding="utf-8") as fh:
            passing = fh.read()
        with open(os.path.join(FIXTURES_DIR, "must_pass_log.txt"), encoding="utf-8") as fh:
            log = fh.read()
        with open(os.path.join(FIXTURES_DIR, "must_pass_comments.json"), encoding="utf-8") as fh:
            comments = json.load(fh)
        rc, out = self._run_must_pass(body, passing, log, comments=comments, issue="999")
        self.assertEqual(rc, 0, out)
        self.assertIn("test_s3::test_delta_moved", out)
        self.assertIn("#122", out)

    def test_must_pass_non_owner_or_non_decision_comment_does_not_excuse(self):
        body = (
            "## Gate\n"
            "<details><summary>Must pass: 1 tests</summary>\n\n"
            "| test | first line of the failure |\n|---|---|\n"
            "| `test_s3::test_delta_moved` | assert 403 == 200 |\n\n</details>\n"
        )
        with open(os.path.join(FIXTURES_DIR, "must_pass_passing.txt"), encoding="utf-8") as fh:
            passing = fh.read()
        with open(os.path.join(FIXTURES_DIR, "must_pass_log.txt"), encoding="utf-8") as fh:
            log = fh.read()
        bad_comment_sets = [
            # Non-owner contributor
            [{
                "user": {"login": "contributor"},
                "author_association": "CONTRIBUTOR",
                "body": "Move `test_s3::test_delta_moved` to #122.",
            }],
            # Builder Gate Evidence comment claiming owner decision without separate owner comment
            [{
                "user": {"login": "iskandarnorazli"},
                "author_association": "OWNER",
                "body": "## Gate Evidence (PR #246)\nPer owner decision, `test_s3::test_delta_moved` moves to #122.",
            }],
            # Stopping / needs-decision comment
            [{
                "user": {"login": "iskandarnorazli"},
                "author_association": "OWNER",
                "body": "Stopping with `needs-decision`: `test_s3::test_delta_moved` fails; #122 owns ACLs.",
            }],
            # Audit comment asking owner to confirm
            [{
                "user": {"login": "iskandarnorazli"},
                "author_association": "OWNER",
                "body": "Audit (Claude, 2026-11-19): `test_s3::test_delta_moved` moves to #122, asked owner to confirm.",
            }],
            # Comment mentioning only PR number and same issue number, not a new owner issue
            [{
                "user": {"login": "iskandarnorazli"},
                "author_association": "OWNER",
                "body": "In PR #246 on #999, `test_s3::test_delta_moved` failed.",
            }],
        ]
        for comments in bad_comment_sets:
            rc, out = self._run_must_pass(body, passing, log, comments=comments, issue="999")
            self.assertEqual(rc, 1, f"Should not excuse {comments}: {out}")

    def test_140_with_pr_246_saved_fixtures_exits_1(self):
        body_path = os.path.join(FIXTURES_DIR, "issue_140_body.md")
        passing_path = os.path.join(FIXTURES_DIR, "pr_246_passing.txt")
        log_path = os.path.join(FIXTURES_DIR, "pr_246_s3tests_log.txt")
        script = os.path.join(SCRIPTS_DIR, "check_gate_comment.py")
        proc = subprocess.run(
            [
                sys.executable,
                script,
                "140",
                "--body", body_path,
                "--passing", passing_path,
                "--log", log_path,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 1, out)
        self.assertIn("test_object_delete_key_bucket_gone", out)
        self.assertIn("assert 403 == 404", out)
        self.assertIn("test_bucket_get_location", out)
        self.assertIn("test_object_read_unreadable", out)

    def test_160_combined_table_format_extracts_only_must_pass(self):
        body_path = os.path.join(FIXTURES_DIR, "issue_160_body.md")
        passing_path = os.path.join(FIXTURES_DIR, "pr_246_passing.txt")
        log_path = os.path.join(FIXTURES_DIR, "pr_246_s3tests_log.txt")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = c.main([
                "160",
                "--body", body_path,
                "--passing", passing_path,
                "--log", log_path,
            ])
        out = buf.getvalue()
        self.assertEqual(rc, 1, out)
        self.assertIn("test_headers::test_object_create_bad_contentlength_none", out)
        self.assertIn("AssertionError: ClientError not raised", out)
        # Owned elsewhere rows from the combined table must not be checked as Must pass
        self.assertNotIn("test_object_create_bad_authorization_empty", out)
        self.assertNotIn("test_object_create_bad_md5_invalid_garbage_aws2", out)

    def test_must_pass_no_section_exits_2(self):
        body_path = os.path.join(FIXTURES_DIR, "issue_187_body.md")
        passing_path = os.path.join(FIXTURES_DIR, "must_pass_passing.txt")
        log_path = os.path.join(FIXTURES_DIR, "must_pass_log.txt")
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = c.main(["187", "--body", body_path, "--passing", passing_path, "--log", log_path])
        self.assertEqual(rc, 2, buf.getvalue())

    def test_pr_mode_fetches_head_passing_and_s3tests_log(self):
        with open(os.path.join(FIXTURES_DIR, "issue_140_body.md"), encoding="utf-8") as fh:
            body_140 = fh.read()
        with open(os.path.join(FIXTURES_DIR, "pr_246_passing.txt"), encoding="utf-8") as fh:
            passing_246 = fh.read()
        with open(os.path.join(FIXTURES_DIR, "pr_246_s3tests_log.txt"), encoding="utf-8") as fh:
            log_246 = fh.read()
        sha = "b38b9827cb6493679a09f0d0d13cae37a6ca8e7c"

        def fake_gh_api(endpoint, allow_escape=False):
            if endpoint == "repos/{owner}/{repo}/issues/140":
                return json.dumps({"body": body_140, "created_at": "2026-11-13T00:00:00Z"})
            if endpoint == "repos/{owner}/{repo}/pulls/246":
                return json.dumps({"head": {"sha": sha}})
            if endpoint == f"repos/{{owner}}/{{repo}}/contents/tests/s3tests/passing.txt?ref={sha}":
                return json.dumps({
                    "encoding": "base64",
                    "content": base64.b64encode(passing_246.encode("utf-8")).decode("ascii"),
                })
            if endpoint == f"repos/{{owner}}/{{repo}}/commits/{sha}/check-runs?check_name=s3tests&per_page=100":
                return json.dumps({
                    "check_runs": [{
                        "id": 135314895672,
                        "name": "s3tests",
                        "status": "completed",
                        "conclusion": "success",
                        "started_at": "2026-11-19T03:23:28Z",
                    }]
                })
            if endpoint == "repos/{owner}/{repo}/actions/jobs/135314895672/logs":
                return log_246
            if endpoint == "repos/{owner}/{repo}/issues/140/comments":
                return json.dumps([])
            if endpoint == "repos/{owner}/{repo}":
                return json.dumps({"owner": {"login": "iskandarnorazli"}})
            raise AssertionError(f"Unexpected endpoint: {endpoint}")

        buf = io.StringIO()
        with mock.patch.object(c, "_gh_api", side_effect=fake_gh_api):
            with mock.patch.object(c, "_gh_api_paginated", side_effect=lambda ep: json.loads(fake_gh_api(ep))):
                with redirect_stdout(buf):
                    rc = c.main(["140", "--pr", "246"])
        out = buf.getvalue()
        self.assertEqual(rc, 1, out)
        self.assertIn("test_object_delete_key_bucket_gone", out)
        self.assertIn("assert 403 == 404", out)


if __name__ == "__main__":
    unittest.main()
