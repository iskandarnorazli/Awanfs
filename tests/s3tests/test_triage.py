"""Self-tests for triage.py.

    python3 tests/s3tests/test_triage.py
"""
import json
import os
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import triage  # noqa: E402


FIXTURE_PATH = os.path.join(HERE, "fixtures", "triage_failures.json")

SAMPLE_MAP = """# ceph s3-tests: the conformance map

7 test functions: 1 pass, 6 fail, 0 skipped, 0 unknown.

| area | owner | design section | pass | fail | skip |
|---|---|---|---|---|---|
| encryption | #121 | 8.2 | 0 | 3 | 0 |
| listing | #114 | 6.6 | 1 | 2 | 0 |
| bucket-basics | #140 | 6.7 | 0 | 1 | 0 |

## encryption: 3 not passing

- `test_enc_a` (fail): assert 200 == 204
- `test_enc_b` (fail): assert 200 == 204
- `test_enc_c` (fail): KeyError: 'x-amz-server-side-encryption'

## listing: 2 not passing

- `test_list_a` (fail): AssertionError: assert not 'VersionId' in {'ResponseMetadata': {'RequestId': '6AF7B29D00000000000029E2', 'HTTPStatusCode': 200}}
- `test_list_b` (fail): AssertionError: assert not 'VersionId' in {'ResponseMetadata': {'RequestId': 'AAAA1111BBBB2222CCCC3333', 'HTTPStatusCode': 200}}

## bucket-basics: 1 not passing

- `test_bucket_a` (fail): botocore.errorfactory.NoSuchBucket: An error occurred (NoSuchBucket) when calling the PutObject operation: Bucket='test-bucket-one', Key='k1'
"""

SAMPLE_JUNIT = """<testsuite>
<testcase name="test_enc_a"><failure message="assert 200 == 204"/></testcase>
<testcase name="test_enc_b"><failure message="assert 200 == 204&#10;extra line"/></testcase>
<testcase name="test_enc_c"><error message="KeyError: 'x-amz-server-side-encryption'"/></testcase>
<testcase name="test_list_a"><failure message="AssertionError: assert not 'VersionId' in {'ResponseMetadata': {'RequestId': '6AF7B29D00000000000029E2', 'HTTPStatusCode': 200}}"/></testcase>
<testcase name="test_list_b"><failure message="AssertionError: assert not 'VersionId' in {'ResponseMetadata': {'RequestId': 'AAAA1111BBBB2222CCCC3333', 'HTTPStatusCode': 200}}"/></testcase>
<testcase name="test_bucket_a"><failure message="botocore.errorfactory.NoSuchBucket: An error occurred (NoSuchBucket) when calling the PutObject operation: Bucket='test-bucket-two', Key='k2'"/></testcase>
<testcase name="test_list_ok"/>
</testsuite>"""


def tmp(text, suffix):
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


class TriageTest(unittest.TestCase):
    def test_normalise_fixture_table(self):
        with open(FIXTURE_PATH, encoding="utf-8") as fh:
            cases = json.load(fh)
        for case in cases:
            with self.subTest(case=case["name"]):
                self.assertEqual(triage.normalise(case["raw"]), case["expected"])

    def test_same_group_for_bucket_or_request_id_difference(self):
        junit_xml = """<testsuite>
<testcase name="test_list_a"><failure message="AssertionError: missing Bucket='bucket-alpha-111', RequestId='1111AAAA2222BBBB'"/></testcase>
<testcase name="test_list_b"><failure message="AssertionError: missing Bucket='bucket-beta-999', RequestId='CCCC3333DDDD4444'"/></testcase>
</testsuite>"""
        junit_path = tmp(junit_xml, ".xml")
        map_path = tmp(SAMPLE_MAP, ".md")
        try:
            summary, per_area = triage.build_triage(junit_path, map_path)
            self.assertEqual(len(per_area["listing"]), 1)
            group = per_area["listing"][0]
            self.assertEqual(group["count"], 2)
            self.assertEqual(sorted(group["tests"]), ["test_list_a", "test_list_b"])
        finally:
            os.remove(junit_path)
            os.remove(map_path)

    def test_report_covers_every_failing_test_exactly_once(self):
        junit_path = tmp(SAMPLE_JUNIT, ".xml")
        map_path = tmp(SAMPLE_MAP, ".md")
        out_path = tmp("", ".md")
        try:
            rc = triage.main([junit_path, map_path, "--out", out_path])
            self.assertEqual(rc, 0)
            summary, per_area = triage.build_triage(junit_path, map_path)

            # Count failing tests in junit
            junit_fail_count = 0
            for case in ET.parse(junit_path).getroot().iter("testcase"):
                if any(c.tag in ("failure", "error") for c in case):
                    junit_fail_count += 1

            seen_tests = []
            for area_groups in per_area.values():
                for g in area_groups:
                    seen_tests.extend(g["tests"])

            self.assertEqual(sum(g["count"] for groups in per_area.values() for g in groups), junit_fail_count)
            self.assertEqual(len(seen_tests), junit_fail_count)
            self.assertEqual(len(set(seen_tests)), junit_fail_count)

            # Summary table totals match
            self.assertEqual(sum(row["failing"] for row in summary), junit_fail_count)

            with open(out_path, encoding="utf-8") as fh:
                report = fh.read()
            self.assertIn("| area | failing tests | groups | largest group |", report)
            self.assertIn("| 2 | `assert 200 == 204` | `test_enc_a`, `test_enc_b` | #121 |", report)
            # Largest group first in encryption (2 before 1)
            self.assertLess(report.index("`test_enc_a`, `test_enc_b`"), report.index("`test_enc_c`"))
        finally:
            os.remove(junit_path)
            os.remove(map_path)
            os.remove(out_path)


if __name__ == "__main__":
    unittest.main()
