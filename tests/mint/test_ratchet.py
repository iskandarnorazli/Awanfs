"""Self-tests for tests/mint/ratchet.py.

    python3 tests/mint/test_ratchet.py
"""
import contextlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ratchet  # noqa: E402


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "mint_sample_log.json"


def tmp(text, suffix):
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


class MintRatchetTest(unittest.TestCase):
    def setUp(self):
        self._temps = []
        self.report = self._tmp("", ".md")

    def tearDown(self):
        for path in self._temps:
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            elif os.path.exists(path):
                os.unlink(path)

    def _tmp(self, text, suffix):
        p = tmp(text, suffix)
        self._temps.append(p)
        return p

    def _run(self, argv):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = ratchet.main(argv)
        return rc, buf.getvalue()

    def test_fixture_all_listed_pass_and_per_sdk_summary(self):
        passing = self._tmp(
            "# comment\n"
            "aws-sdk-go-v2::testBasicObjectOperations\n"
            "aws-sdk-go-v2::testObjectTagging\n"
            "awscli::test_create_bucket\n",
            ".txt",
        )
        rc, out = self._run([str(FIXTURE), passing, "--report", self.report])
        self.assertEqual(rc, 0, out)
        text = Path(self.report).read_text(encoding="utf-8")
        self.assertIn("7 tests: 4 passed, 2 failed, 1 skipped.", text)
        self.assertIn("Ratchet: 3 listed, 0 of them no longer pass, 1 new passes not listed.", text)
        self.assertIn("minio-py::stat_object", text)
        self.assertIn("## Per-SDK summary", text)
        self.assertIn("| aws-sdk-go-v2 | 2 | 1 | 0 | 3 |", text)
        self.assertIn("| awscli | 1 | 1 | 0 | 2 |", text)
        self.assertIn("| minio-py | 1 | 0 | 1 | 2 |", text)

    def test_regression_fails_with_reason(self):
        passing = self._tmp(
            "aws-sdk-go-v2::testBasicObjectOperations\n"
            "aws-sdk-go-v2::testSelectObject\n",
            ".txt",
        )
        rc, out = self._run([str(FIXTURE), passing, "--report", self.report])
        self.assertEqual(rc, 1, out)
        text = Path(self.report).read_text(encoding="utf-8")
        self.assertIn("Ratchet: 2 listed, 1 of them no longer pass, 3 new passes not listed.", text)
        self.assertIn("- `aws-sdk-go-v2::testSelectObject` (failed)", text)

    def test_missing_from_run_counts_as_regression(self):
        passing = self._tmp(
            "aws-sdk-go-v2::testBasicObjectOperations\n"
            "minio-go::removedTest\n",
            ".txt",
        )
        rc, _ = self._run([str(FIXTURE), passing, "--report", self.report])
        self.assertEqual(rc, 1)
        text = Path(self.report).read_text(encoding="utf-8")
        self.assertIn("- `minio-go::removedTest` (not run)", text)

    def test_multiline_json_documents_and_json_array(self):
        multiline = self._tmp(
            json.dumps({"name": "mc", "function": "mb", "status": "PASS"}, indent=2)
            + "\n"
            + json.dumps({"name": "mc", "function": "ls", "status": "NA"}, indent=2)
            + "\n",
            ".json",
        )
        arr = self._tmp(
            json.dumps([
                {"name": "s3cmd", "function": "test_make_bucket", "status": "PASS"},
                {"name": "mc", "function": "mb", "status": "FAIL", "error": "boom"},
            ]),
            ".json",
        )
        passing = self._tmp("s3cmd::test_make_bucket\n", ".txt")
        rc, out = self._run([multiline, arr, passing, "--report", self.report])
        self.assertEqual(rc, 0, out)
        text = Path(self.report).read_text(encoding="utf-8")
        self.assertIn("3 tests: 1 passed, 1 failed, 1 skipped.", text)
        self.assertIn("| mc | 0 | 1 | 1 | 2 |", text)
        self.assertIn("| s3cmd | 1 | 0 | 0 | 1 |", text)

    def test_empty_log_fails(self):
        empty_log = self._tmp("\n", ".json")
        passing = self._tmp("", ".txt")
        rc, out = self._run([empty_log, passing, "--report", self.report])
        self.assertEqual(rc, 1, out)
        self.assertIn("0 test results", out)

    def test_check_removals_requires_ratchet_log_row(self):
        repo = Path(tempfile.mkdtemp(prefix="mint_ratchet_git_"))
        self._temps.append(str(repo))
        subprocess.check_call(["git", "init", "-b", "main"], cwd=repo, stdout=subprocess.DEVNULL)
        subprocess.check_call(["git", "config", "user.email", "t@example.com"], cwd=repo)
        subprocess.check_call(["git", "config", "user.name", "Test"], cwd=repo)
        (repo / "tests" / "mint").mkdir(parents=True)
        passing = repo / "tests" / "mint" / "passing.txt"
        rlog = repo / "tests" / "mint" / "RATCHET_LOG.md"
        passing.write_text("awscli::test_a\nawscli::test_b\n", encoding="utf-8")
        rlog.write_text(
            "# Mint ratchet log\n\n| test | removed in | reason | decision |\n|---|---|---|---|\n",
            encoding="utf-8",
        )
        subprocess.check_call(["git", "add", "tests/mint/passing.txt", "tests/mint/RATCHET_LOG.md"], cwd=repo)
        subprocess.check_call(["git", "commit", "-m", "base"], cwd=repo, stdout=subprocess.DEVNULL)
        subprocess.check_call(["git", "checkout", "-b", "pr"], cwd=repo, stdout=subprocess.DEVNULL)

        # Removal without RATCHET_LOG.md row must fail
        passing.write_text("awscli::test_a\n", encoding="utf-8")
        missing = ratchet.check_removals("main", passing, rlog, cwd=repo)
        self.assertEqual(missing, ["awscli::test_b"])

        # Removal with empty decision must fail
        rlog.write_text(
            "# Mint ratchet log\n\n| test | removed in | reason | decision |\n|---|---|---|---|\n"
            "| awscli::test_b | deadbeef | broken |   |\n",
            encoding="utf-8",
        )
        self.assertEqual(ratchet.check_removals("main", passing, rlog, cwd=repo), ["awscli::test_b"])

        # Removal with populated decision must pass
        rlog.write_text(
            "# Mint ratchet log\n\n| test | removed in | reason | decision |\n|---|---|---|---|\n"
            "| awscli::test_b | deadbeef | broken | owner, 2026-11-20, #278 |\n",
            encoding="utf-8",
        )
        self.assertEqual(ratchet.check_removals("main", passing, rlog, cwd=repo), [])


if __name__ == "__main__":
    unittest.main()
