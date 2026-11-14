"""Self-tests for conformance_map.py.

    python3 tests/s3tests/test_conformance_map.py
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import conformance_map as cm  # noqa: E402

SUITE = '''
import pytest

@pytest.mark.encryption
def test_put_obj_enc_conflict():
    pass

def test_object_lock_put_obj_retention():
    pass

@pytest.mark.parametrize("x", [1, 2])
def test_multipart_upload_small(x):
    pass

def test_bucket_list_empty():
    pass

def helper():
    pass
'''

JUNIT = '''<testsuite>
<testcase name="test_put_obj_enc_conflict"><failure message="assert 400 == 200&#10;second line"/></testcase>
<testcase name="test_multipart_upload_small[1]"/>
<testcase name="test_multipart_upload_small[2]"><error message="boom"/></testcase>
<testcase name="test_bucket_list_empty"><skipped message="no reason"/></testcase>
<testcase name="test_object_lock_put_obj_retention"/>
</testsuite>'''


def tmp(text, suffix):
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


class Map(unittest.TestCase):
    def setUp(self):
        self.suite = tmp(SUITE, ".py")
        self.junit = tmp(JUNIT, ".xml")
        self.passing = tmp("# c\ntest_s3::test_bucket_list_empty\n", ".txt")

    def rows(self, junit=True):
        return cm.build(cm.suite_tests(self.suite), cm.passing_names(self.passing), cm.junit_status(self.junit) if junit else None)

    def test_only_test_functions_are_read_with_their_markers(self):
        t = cm.suite_tests(self.suite)
        self.assertEqual(list(t), ["test_put_obj_enc_conflict", "test_object_lock_put_obj_retention", "test_multipart_upload_small", "test_bucket_list_empty"])
        self.assertEqual(t["test_put_obj_enc_conflict"], ["encryption"])

    def test_each_test_gets_one_area_and_its_owner(self):
        rows, unassigned = self.rows()
        got = {r[0]: (r[1], r[2]) for r in rows}
        self.assertEqual(got["test_put_obj_enc_conflict"], ("encryption", "#121"))
        self.assertEqual(got["test_object_lock_put_obj_retention"], ("object-lock", "#120"))
        self.assertEqual(got["test_multipart_upload_small"], ("multipart", "#111"))
        self.assertEqual(got["test_bucket_list_empty"], ("listing", "#114"))
        self.assertEqual(unassigned, [])

    def test_the_first_rule_wins(self):
        # "lifecycle" is in the name, but the logging marker's rule comes first.
        self.assertEqual(cm.area_of("test_lifecycle_x", ["bucket_logging"])[0], "bucket-logging")
        self.assertEqual(cm.area_of("test_lifecycle_x", [])[0], "lifecycle")

    def test_status(self):
        rows, _ = self.rows()
        got = {r[0]: (r[4], r[5]) for r in rows}
        self.assertEqual(got["test_put_obj_enc_conflict"], ("fail", "assert 400 == 200"))
        self.assertEqual(got["test_multipart_upload_small"], ("fail", "boom"))  # one case failed
        self.assertEqual(got["test_bucket_list_empty"], ("pass", ""))  # passing.txt wins
        self.assertEqual(got["test_object_lock_put_obj_retention"], ("pass", "passes; add it to passing.txt"))

    def test_without_junit_the_rest_is_unknown(self):
        rows, _ = self.rows(junit=False)
        self.assertEqual({r[4] for r in rows}, {"pass", "unknown"})

    def test_a_test_no_rule_matches_fails_the_run(self):
        suite = tmp("def test_zzz():\n    pass\n", ".py")
        out = tmp("", ".md")
        self.assertEqual(cm.main([suite, self.passing, "--report", out]), 1)
        self.assertIn("Unassigned", open(out, encoding="utf-8").read())
        self.assertEqual(cm.main([self.suite, self.passing, "--report", out]), 0)

    def test_report_table(self):
        rows, unassigned = self.rows()
        text = cm.render(rows, unassigned, "suite x")
        self.assertIn("| encryption | #121 | 8.2 | 0 | 1 | 0 |", text)
        self.assertIn("- `test_put_obj_enc_conflict` (fail): assert 400 == 200", text)


if __name__ == "__main__":
    unittest.main()
