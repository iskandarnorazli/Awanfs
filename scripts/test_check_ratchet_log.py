"""Self-tests for check_ratchet_log.py.

    python3 scripts/test_check_ratchet_log.py
"""
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_ratchet_log  # noqa: E402


LOG_HEADER = """# s3-tests ratchet log

| test | removed in | reason | decision |
|---|---|---|---|
"""


def _git(cwd: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", *args],
        cwd=cwd,
        text=True,
        stderr=subprocess.STDOUT,
    )


class CheckRatchetLogTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="ratchet_log_test_"))
        self.orig_cwd = Path.cwd()
        _git(self.tmp, "init", "-b", "main")
        _git(self.tmp, "config", "user.email", "test@example.com")
        _git(self.tmp, "config", "user.name", "Test")
        (self.tmp / "tests" / "s3tests").mkdir(parents=True)
        (self.tmp / "tests" / "s3tests" / "passing.txt").write_text(
            "# comment\ntest_s3::test_a\ntest_s3::test_b\n",
            encoding="utf-8",
        )
        (self.tmp / "tests" / "s3tests" / "RATCHET_LOG.md").write_text(
            LOG_HEADER,
            encoding="utf-8",
        )
        _git(self.tmp, "add", "tests/s3tests/passing.txt", "tests/s3tests/RATCHET_LOG.md")
        _git(self.tmp, "commit", "-m", "base")
        _git(self.tmp, "checkout", "-b", "pr")
        os.chdir(self.tmp)

    def tearDown(self):
        os.chdir(self.orig_cwd)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = check_ratchet_log.main(["main"])
        return rc, buf.getvalue()

    def test_no_removal_passes(self):
        rc, _ = self._run()
        self.assertEqual(rc, 0)

    def test_removal_with_row_passes(self):
        (self.tmp / "tests" / "s3tests" / "passing.txt").write_text(
            "# comment\ntest_s3::test_a\n",
            encoding="utf-8",
        )
        (self.tmp / "tests" / "s3tests" / "RATCHET_LOG.md").write_text(
            LOG_HEADER
            + "| test_s3::test_b | abc1234 | 501 now | owner, 2026-11-17, #148 |\n",
            encoding="utf-8",
        )
        rc, _ = self._run()
        self.assertEqual(rc, 0)

    def test_removal_without_row_fails_and_names_it(self):
        (self.tmp / "tests" / "s3tests" / "passing.txt").write_text(
            "# comment\ntest_s3::test_a\n",
            encoding="utf-8",
        )
        rc, out = self._run()
        self.assertEqual(rc, 1)
        self.assertIn("test_s3::test_b", out)

    def test_addition_without_row_passes(self):
        (self.tmp / "tests" / "s3tests" / "passing.txt").write_text(
            "# comment\ntest_s3::test_a\ntest_s3::test_b\ntest_s3::test_c\n",
            encoding="utf-8",
        )
        rc, _ = self._run()
        self.assertEqual(rc, 0)

    def test_row_with_empty_decision_fails(self):
        (self.tmp / "tests" / "s3tests" / "passing.txt").write_text(
            "# comment\ntest_s3::test_a\n",
            encoding="utf-8",
        )
        (self.tmp / "tests" / "s3tests" / "RATCHET_LOG.md").write_text(
            LOG_HEADER + "| test_s3::test_b | abc1234 | 501 now |   |\n",
            encoding="utf-8",
        )
        rc, out = self._run()
        self.assertEqual(rc, 1)
        self.assertIn("test_s3::test_b", out)


if __name__ == "__main__":
    unittest.main()
