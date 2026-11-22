"""Self-tests for ratchet.py.

    python3 tests/s3tests/test_ratchet.py
"""
import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ratchet  # noqa: E402


S3_JUNIT = """<testsuite>
<testcase classname="s3tests.functional.test_s3" name="test_bucket_create_delete"/>
<testcase classname="s3tests.functional.test_s3" name="test_dup"/>
<testcase classname="s3tests.functional.test_s3" name="test_unlisted_s3"/>
</testsuite>"""

HEADERS_JUNIT = """<testsuite>
<testcase classname="s3tests.functional.test_headers" name="test_bad_content_length"/>
<testcase classname="s3tests.functional.test_headers" name="test_dup"><failure message="403 != 400"/></testcase>
<testcase classname="s3tests.functional.test_headers" name="test_unlisted_header"/>
<testcase classname="s3tests.functional.test_headers" name="test_skipped_header"><skipped message="needs sigv2"/></testcase>
</testsuite>"""


def tmp(text, suffix):
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


class RatchetTest(unittest.TestCase):
    def setUp(self):
        self.s3_xml = tmp(S3_JUNIT, ".xml")
        self.headers_xml = tmp(HEADERS_JUNIT, ".xml")
        self.report = tmp("", ".md")

    def _run_ratchet(self, argv):
        stdout_buf, stderr_buf = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
            rc = ratchet.main(argv)
        return rc, stdout_buf.getvalue(), stderr_buf.getvalue()

    def test_single_junit_all_listed_pass(self):
        passing = tmp("test_s3::test_bucket_create_delete\ntest_s3::test_dup\n", ".txt")
        rc, out, err = self._run_ratchet([self.s3_xml, passing, "--report", self.report])
        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        with open(self.report, encoding="utf-8") as fh:
            text = fh.read()
        self.assertEqual(out.rstrip("\r\n"), text.rstrip("\r\n"))
        self.assertIn("3 tests: 3 passed, 0 failed, 0 skipped.", text)
        self.assertIn("Ratchet: 2 listed, 0 of them no longer pass, 1 new passes not listed.", text)
        self.assertIn("test_s3::test_unlisted_s3", text)

    def test_multiple_junits_combined_ratchet_and_new_passes(self):
        passing = tmp(
            "# comment\ntest_s3::test_bucket_create_delete\ntest_headers::test_bad_content_length\n",
            ".txt",
        )
        rc, out, err = self._run_ratchet([self.s3_xml, self.headers_xml, passing, "--report", self.report])
        self.assertEqual(rc, 0)
        self.assertEqual(err, "")
        with open(self.report, encoding="utf-8") as fh:
            text = fh.read()
        self.assertEqual(out.rstrip("\r\n"), text.rstrip("\r\n"))
        self.assertIn("7 tests: 5 passed, 1 failed, 1 skipped.", text)
        self.assertIn("Ratchet: 2 listed, 0 of them no longer pass, 3 new passes not listed.", text)
        self.assertIn("test_headers::test_unlisted_header", text)
        self.assertIn("test_s3::test_unlisted_s3", text)

    def test_regression_in_second_junit_fails(self):
        passing = tmp(
            "test_s3::test_bucket_create_delete\ntest_s3::test_dup\ntest_headers::test_dup\n",
            ".txt",
        )
        rc, out, err = self._run_ratchet([self.s3_xml, self.headers_xml, passing, "--report", self.report])
        self.assertEqual(rc, 1)
        self.assertEqual(err, "")
        with open(self.report, encoding="utf-8") as fh:
            text = fh.read()
        self.assertEqual(out.rstrip("\r\n"), text.rstrip("\r\n"))
        self.assertIn("Ratchet: 3 listed, 1 of them no longer pass, 3 new passes not listed.", text)
        self.assertIn("- `test_headers::test_dup` (failed)", text)

    def test_synthetic_fixtures_do_not_leak_to_process_output(self):
        proc = subprocess.run(
            [
                sys.executable,
                os.path.abspath(__file__),
                "RatchetTest.test_single_junit_all_listed_pass",
                "RatchetTest.test_multiple_junits_combined_ratchet_and_new_passes",
                "RatchetTest.test_regression_in_second_junit_fails",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        combined = proc.stdout + proc.stderr
        for marker in (
            "new passes not listed",
            "Listed tests that no longer pass",
            "test_headers::test_unlisted_header",
            "test_s3::test_unlisted_s3",
            "test_headers::test_dup",
        ):
            self.assertNotIn(marker, combined)


if __name__ == "__main__":
    unittest.main()
