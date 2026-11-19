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

def test_lifecycle_transition():
    pass

def test_bucket_logging_flush_j():
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
<testcase name="test_lifecycle_transition"><skipped message="needs storage classes"/></testcase>
<testcase name="test_bucket_logging_flush_j"><skipped type="pytest.skip" message="skipped">test_s3.py:10: need ceph extension</skipped></testcase>
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
        self.assertEqual(list(t), ["test_put_obj_enc_conflict", "test_object_lock_put_obj_retention", "test_multipart_upload_small", "test_bucket_list_empty", "test_lifecycle_transition", "test_bucket_logging_flush_j"])
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
        self.assertEqual(got["test_lifecycle_transition"], ("skip", "needs storage classes"))
        self.assertEqual(got["test_bucket_logging_flush_j"], ("skip", "test_s3.py:10: need ceph extension"))

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
        self.assertIn("- `test_lifecycle_transition` (skip): needs storage classes", text)
        self.assertIn("- `test_bucket_logging_flush_j` (skip): test_s3.py:10: need ceph extension", text)


    def test_sigv2_tests_are_out_of_scope(self):
        """Owner, 2026-11-17: SigV2 is out of scope. It must win over request-headers, which
        claims every test_headers:: test."""
        self.assertEqual(cm.area_of("test_headers::test_object_create_bad_md5_invalid_garbage_aws2", ["auth_aws2"])[:2], ("sigv2", "out of scope"))
        self.assertEqual(cm.area_of("test_headers::test_bucket_create_bad_ua_empty_aws2", [])[:2], ("sigv2", "out of scope"))
        self.assertEqual(cm.area_of("test_cors_presigned_put_object_tenant_v2", [])[0], "sigv2")
        self.assertEqual(cm.area_of("test_bucket_listv2_maxkeys_one", ["list_objects_v2"])[0], "listing", "ListObjectsV2 is not SigV2")
        self.assertEqual(cm.area_of("test_headers::test_object_create_bad_md5_invalid_short", ["auth_common"])[0], "request-headers")

    def test_owner_decisions_of_2026_11_18(self):
        """#160: the request no client can send is out of scope; #140: the anonymous DELETE on a
        gone bucket belongs to #122. Neighbours keep their areas."""
        self.assertEqual(cm.area_of("test_headers::test_object_create_bad_contentlength_none", [])[:2], ("request-headers-unsendable", "out of scope"))
        self.assertEqual(cm.area_of("test_headers::test_object_create_bad_contentlength_empty", [])[0], "request-headers")
        self.assertEqual(cm.area_of("test_object_delete_key_bucket_gone", [])[:2], ("anonymous-bucket-gone", "#122"))
        self.assertEqual(cm.area_of("test_object_read_not_exist", [])[0], "object-basics")

    def test_owner_decision_2026_11_19_cors(self):
        """#116: the two CORS tests that read a public-read bucket anonymously belong to #122;
        the other CORS tests stay with #116, and the SigV2 ones stay out of scope."""
        self.assertEqual(cm.area_of("test_cors_origin_response", [])[:2], ("anonymous-cors", "#122"))
        self.assertEqual(cm.area_of("test_cors_origin_wildcard", [])[:2], ("anonymous-cors", "#122"))
        self.assertEqual(cm.area_of("test_cors_header_option", [])[:2], ("cors", "#116"))
        self.assertEqual(cm.area_of("test_cors_presigned_get_object_v2", [])[0], "sigv2")

    def test_headers_suite_assigned_to_request_headers(self):
        headers_suite = '''
def test_put_obj_bad_content_length():
    pass

def test_headers_missing_host():
    pass
'''
        headers_junit = '''<testsuite>
<testcase classname="s3tests.functional.test_headers" name="test_put_obj_bad_content_length"/>
<testcase classname="s3tests.functional.test_headers" name="test_headers_missing_host"><failure message="400 != 403"/></testcase>
</testsuite>'''
        s3_junit = '''<testsuite>
<testcase classname="s3tests.functional.test_s3" name="test_put_obj_enc_conflict"><failure message="assert 400 == 200"/></testcase>
<testcase classname="s3tests.functional.test_s3" name="test_multipart_upload_small[1]"/>
<testcase classname="s3tests.functional.test_s3" name="test_multipart_upload_small[2]"><error message="boom"/></testcase>
<testcase classname="s3tests.functional.test_s3" name="test_bucket_list_empty"><skipped message="no reason"/></testcase>
<testcase classname="s3tests.functional.test_s3" name="test_object_lock_put_obj_retention"/>
<testcase classname="s3tests.functional.test_s3" name="test_lifecycle_transition"><skipped message="needs storage classes"/></testcase>
<testcase classname="s3tests.functional.test_s3" name="test_bucket_logging_flush_j"><skipped type="pytest.skip" message="skipped">test_s3.py:10: need ceph extension</skipped></testcase>
</testsuite>'''
        with tempfile.TemporaryDirectory() as d:
            s3_path = os.path.join(d, "test_s3.py")
            headers_path = os.path.join(d, "test_headers.py")
            s3_xml = os.path.join(d, "s3.xml")
            headers_xml = os.path.join(d, "headers.xml")
            passing_path = os.path.join(d, "passing.txt")
            out = os.path.join(d, "map.md")
            for path, content in (
                (s3_path, SUITE),
                (headers_path, headers_suite),
                (s3_xml, s3_junit),
                (headers_xml, headers_junit),
                (passing_path, "test_s3::test_bucket_list_empty\ntest_headers::test_put_obj_bad_content_length\n"),
            ):
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write(content)
            rc = cm.main([
                s3_path, headers_path, passing_path,
                "--junit", s3_xml, "--junit", headers_xml,
                "--report", out,
            ])
            self.assertEqual(rc, 0)
            with open(out, encoding="utf-8") as fh:
                text = fh.read()
            self.assertIn("| request-headers | #160 | 3 | 1 | 1 | 0 |", text)
            self.assertIn("## request-headers: 1 not passing", text)
            self.assertIn("- `test_headers::test_headers_missing_host` (fail): 400 != 403", text)
            self.assertNotIn("test_headers::test_put_obj_bad_content_length", text)


if __name__ == "__main__":
    unittest.main()
