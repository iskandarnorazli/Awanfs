"""Self-tests for features.py.

    python3 scripts/test_features.py
"""
import contextlib
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import features  # noqa: E402


MINIMAL_DRIVE_SH = """#!/usr/bin/env bash
doctor() { :; }
j_cors() { :; }
j_objects() { :; }
"""

MINIMAL_FUZZ_CARGO = """[package]
name = "awanfs-fuzz"

[[bin]]
name = "cors_xml"
path = "fuzz_targets/cors_xml.rs"
"""

MINIMAL_SUITE_PY = """
import pytest

def test_set_cors():
    pass

def test_cors_header_option():
    pass

@pytest.mark.auth_aws2
def test_bad_aws2():
    pass
"""

MINIMAL_JUNIT_XML = """<testsuite>
<testcase classname="s3tests.functional.test_s3" name="test_set_cors"/>
<testcase classname="s3tests.functional.test_s3" name="test_cors_header_option"><failure message="assert 403 == 200"/></testcase>
<testcase classname="s3tests.functional.test_s3" name="test_bad_aws2"><failure message="AuthorizationHeaderMalformed"/></testcase>
</testsuite>"""

VALID_TOML = """
[[feature]]
id = "cors"
name = "Bucket CORS"
status = "done"
issue = "#116"
operations = ["PutBucketCors", "GetBucketCors", "DeleteBucketCors"]
ceph = ["test_s3::test_set_cors", "test_s3::test_cors_header_option"]
server_tests = ["server/tests/cors_test.rs"]
drive = ["cors"]
fuzz = ["cors_xml"]

[[feature]]
id = "sigv2"
name = "SigV2 request signing"
status = "out-of-scope"
issue = "out of scope"
decision = "https://github.com/iskandarnorazli/Awanfs/pull/232"
ceph = ["mark:auth_aws2"]

[[feature]]
id = "health"
name = "Health endpoints"
status = "done"
issue = "#44"
user_visible = false
server_tests = ["server/tests/health_test.rs"]
"""


def cors_waits_on(other):
    """VALID_TOML with cors's failing test listed as waiting on `other`."""
    return VALID_TOML.replace(
        'fuzz = ["cors_xml"]\n',
        'fuzz = ["cors_xml"]\n\n[feature.waits_on]\n"test_s3::test_cors_header_option" = "%s"\n' % other,
    )


class FeaturesTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "scripts").mkdir(parents=True)
        (self.root / "server" / "tests").mkdir(parents=True)
        (self.root / "server" / "fuzz" / "fuzz_targets").mkdir(parents=True)
        (self.root / "tests" / "s3tests").mkdir(parents=True)

        (self.root / "scripts" / "verify_drive.sh").write_text(MINIMAL_DRIVE_SH, encoding="utf-8")
        (self.root / "server" / "tests" / "cors_test.rs").write_text("", encoding="utf-8")
        (self.root / "server" / "tests" / "health_test.rs").write_text("", encoding="utf-8")
        (self.root / "server" / "fuzz" / "Cargo.toml").write_text(MINIMAL_FUZZ_CARGO, encoding="utf-8")
        (self.root / "server" / "fuzz" / "fuzz_targets" / "cors_xml.rs").write_text("", encoding="utf-8")
        (self.root / "tests" / "s3tests" / "passing.txt").write_text(
            "test_s3::test_set_cors\ntest_s3::test_cors_header_option\n", encoding="utf-8"
        )

        self.suite = self.root / "test_s3.py"
        self.suite.write_text(MINIMAL_SUITE_PY, encoding="utf-8")
        self.junit = self.root / "junit.xml"
        self.junit.write_text(MINIMAL_JUNIT_XML, encoding="utf-8")
        self.registry = self.root / "features.toml"
        self.registry.write_text(VALID_TOML, encoding="utf-8")

    def tearDown(self):
        self._tmp.cleanup()

    def _run_check(self, with_suite=True):
        argv = ["check", "--root", str(self.root), "--registry", str(self.registry)]
        if with_suite:
            argv += ["--ceph-suite", str(self.suite)]
        with contextlib.redirect_stdout(io.StringIO()):
            return features.main(argv)

    def test_valid_registry_passes(self):
        self.assertEqual(self._run_check(), 0)

    # A feature is done only when its ceph tests pass, or each one that does not names the
    # unfinished feature it waits on (owner, 2026-11-21).
    def _passing(self, *names):
        (self.root / "tests" / "s3tests" / "passing.txt").write_text("".join(n + "\n" for n in names), encoding="utf-8")

    def _probs(self):
        return features.check_registry(root=self.root, registry_path=self.registry, ceph_suites=[self.suite])

    def test_a_done_feature_with_a_failing_ceph_test_fails(self):
        self._passing("test_s3::test_set_cors")
        probs = self._probs()
        self.assertTrue(any("cors" in p and "test_s3::test_cors_header_option" in p and "done" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_a_failing_test_listed_under_waits_on_an_unfinished_feature_passes(self):
        self._passing("test_s3::test_set_cors")
        self.registry.write_text(cors_waits_on("sigv2"), encoding="utf-8")
        self.assertEqual(self._probs(), [])

    def test_waits_on_an_unknown_or_done_feature_fails(self):
        self._passing("test_s3::test_set_cors")
        for other, word in (("nosuch", "unknown"), ("health", "done")):
            self.registry.write_text(cors_waits_on(other), encoding="utf-8")
            probs = self._probs()
            self.assertTrue(any("waits_on" in p and other in p and word in p for p in probs), (other, probs))

    def test_a_waits_on_entry_for_a_passing_test_is_stale_and_fails(self):
        self.registry.write_text(cors_waits_on("sigv2"), encoding="utf-8")
        probs = self._probs()
        self.assertTrue(any("waits_on" in p and "test_cors_header_option" in p and "passes" in p for p in probs), probs)

    def test_a_waits_on_entry_for_another_features_test_fails(self):
        self._passing("test_s3::test_set_cors")
        self.registry.write_text(
            VALID_TOML.replace('server_tests = ["server/tests/health_test.rs"]',
                               'server_tests = ["server/tests/health_test.rs"]\n\n[feature.waits_on]\n"test_s3::test_cors_header_option" = "sigv2"'),
            encoding="utf-8",
        )
        probs = self._probs()
        self.assertTrue(any("health" in p and "waits_on" in p and "not one of its tests" in p for p in probs), probs)

    def test_test_in_no_feature_fails(self):
        self.suite.write_text(
            MINIMAL_SUITE_PY + "\ndef test_unassigned_one():\n    pass\n",
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("test_unassigned_one" in p and "unassigned" in p.lower() for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_test_in_two_features_fails(self):
        self.registry.write_text(
            VALID_TOML + '\n[[feature]]\nid = "dup"\nname = "Dup"\nstatus = "partial"\nceph = ["test_s3::test_set_cors"]\n',
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("test_set_cors" in p and "cors" in p and "dup" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_missing_named_test_fails(self):
        self.registry.write_text(
            VALID_TOML.replace("test_cors_header_option", "test_does_not_exist"),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("test_does_not_exist" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_missing_server_test_file_fails(self):
        self.registry.write_text(
            VALID_TOML.replace("server/tests/cors_test.rs", "server/tests/missing_test.rs"),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("missing_test.rs" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_missing_drive_journey_fails(self):
        self.registry.write_text(
            VALID_TOML.replace('drive = ["cors"]', 'drive = ["nonexistent_journey"]'),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("nonexistent_journey" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_missing_fuzz_target_fails(self):
        self.registry.write_text(
            VALID_TOML.replace('fuzz = ["cors_xml"]', 'fuzz = ["no_such_fuzz"]'),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("no_such_fuzz" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_done_feature_without_server_test_fails(self):
        self.registry.write_text(
            VALID_TOML.replace('server_tests = ["server/tests/cors_test.rs"]', "server_tests = []"),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("cors" in p and "server_tests" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_user_visible_done_feature_without_drive_journey_fails(self):
        self.registry.write_text(
            VALID_TOML.replace('drive = ["cors"]', "drive = []"),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("cors" in p and "drive" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_out_of_scope_without_decision_link_fails(self):
        self.registry.write_text(
            VALID_TOML.replace('decision = "https://github.com/iskandarnorazli/Awanfs/pull/232"\n', ""),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("sigv2" in p and "decision" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

        # Non-URL decision string is also rejected
        self.registry.write_text(
            VALID_TOML.replace(
                "https://github.com/iskandarnorazli/Awanfs/pull/232",
                "owner said so in chat",
            ),
            encoding="utf-8",
        )
        self.assertEqual(self._run_check(), 1)

    def test_moved_target_must_exist_and_have_decision(self):
        bad_moved = VALID_TOML + '\nmoved = [{ test = "test_s3::test_x", to = "missing-feat", decision = "not-a-url" }]\n'
        # Attach moved to the last feature (health)
        self.registry.write_text(bad_moved, encoding="utf-8")
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("missing-feat" in p for p in probs), probs)
        self.assertTrue(any("decision" in p for p in probs), probs)

    def test_mint_suite_unassigned_and_overlap_checked(self):
        mint_log = self.root / "mint.json"
        mint_log.write_text(
            '{"name": "aws-sdk-go", "function": "PutObject", "status": "PASS"}\n'
            '{"name": "aws-sdk-go", "function": "MissingTest", "status": "FAIL"}\n',
            encoding="utf-8",
        )
        reg = VALID_TOML.replace(
            'id = "cors"',
            'id = "cors"\nmint = ["aws-sdk-go::PutObject"]',
        )
        self.registry.write_text(reg, encoding="utf-8")
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
            mint_logs=[mint_log],
        )
        self.assertTrue(any("aws-sdk-go::MissingTest" in p for p in probs), probs)

    def test_report_writes_features_md_with_status_counts_and_gaps(self):
        mint_log = self.root / "mint.json"
        mint_log.write_text(
            '{"name": "awscli", "function": "cors_test", "status": "PASS"}\n',
            encoding="utf-8",
        )
        reg = VALID_TOML.replace(
            'id = "cors"',
            'id = "cors"\nmint = ["awscli::cors_test"]',
        )
        self.registry.write_text(reg, encoding="utf-8")
        out_md = self.root / "FEATURES.md"
        rc = features.main([
            "report",
            "--root", str(self.root),
            "--registry", str(self.registry),
            "--ceph-junit", str(self.junit),
            "--mint-log", str(mint_log),
            "--run-id", "99887766",
            "--output", str(out_md),
        ])
        self.assertEqual(rc, 0)
        text = out_md.read_text(encoding="utf-8")
        self.assertIn("99887766", text)
        self.assertIn("| cors | done | #116 | 1/2 | 1/1 |", text)
        self.assertIn("| sigv2 | out-of-scope |", text)
        self.assertIn("## Gaps", text)
        # cors is done and has a failing test (test_cors_header_option)
        self.assertIn("Done features with failing or non-passing listed tests", text)
        self.assertIn("test_s3::test_cors_header_option", text)
        # health is done and has no ceph or fuzz evidence
        self.assertIn("Done features missing evidence in some layer", text)
        self.assertIn("**health**: no ceph, mint, fuzz", text)

    def test_offline_check_catches_unassigned_passing_test(self):
        # Simulate deleting cors's ceph list when only passing.txt is present (no --ceph-suite).
        self.registry.write_text(
            VALID_TOML.replace(
                'ceph = ["test_s3::test_set_cors", "test_s3::test_cors_header_option"]',
                "ceph = []",
            ),
            encoding="utf-8",
        )
        probs = features.check_registry(root=self.root, registry_path=self.registry)
        self.assertTrue(any("test_set_cors" in p and "unassigned" in p.lower() for p in probs), probs)
        self.assertEqual(self._run_check(with_suite=False), 1)

    def test_repo_registry_passes_check(self):
        repo_root = Path(__file__).resolve().parent.parent
        probs = features.check_registry(root=repo_root)
        self.assertEqual(probs, [])


if __name__ == "__main__":
    unittest.main()
